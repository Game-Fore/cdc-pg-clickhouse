import json
import logging
import signal
import time
import clickhouse_connect
import argparse
import os


from datetime import datetime, timezone
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

KAFKA_CONF = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "cdc-clickhouse",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
}
CH_CONF = dict(host="localhost", port=8123, username="shop", password="shop", database="shop")

BATCH_SIZE = 1000
FLUSH_INTERVAL_SEC = 1.0
MAX_RETRIES = 5


# --- конвертеры полей Debezium -> Python-типы для clickhouse-connect ---
def raw(v):
    return v

def dec(v):
    return Decimal(v) if v is not None else None

def ts(v):
    # Debezium отдаёт timestamptz как ISO-строку: 2026-10-05T08:33:39.947123Z
    return datetime.fromisoformat(v) if v else None

def ts_ms(v):
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc)


# топик -> (таблица в ClickHouse, {колонка: конвертер})
TABLES = {
    "shop.public.customers": ("customers", {
        "id": raw, "email": raw, "full_name": raw, "city": raw, "created_at": ts,
    }),
    "shop.public.products": ("products", {
        "id": raw, "name": raw, "category": raw, "price": dec,
    }),
    "shop.public.orders": ("orders", {
        "id": raw, "customer_id": raw, "status": raw, "total": dec,
        "created_at": ts, "updated_at": ts,
    }),
    "shop.public.order_items": ("order_items", {
        "id": raw, "order_id": raw, "product_id": raw, "quantity": raw, "price": dec,
    }),
}
META_COLUMNS = ["_op", "_lsn", "_source_ts", "_is_deleted"]


def to_row(value: dict, fields: dict) -> list:
    row = [conv(value.get(col)) for col, conv in fields.items()]
    row += [
        value["__op"],
        value["__lsn"],
        ts_ms(value["__source_ts_ms"]),
        1 if value.get("__deleted") == "true" else 0,
    ]
    return row


def flush(ch, buffers: dict) -> int:
    total = 0
    for topic, rows in buffers.items():
        if not rows:
            continue
        table, fields = TABLES[topic]
        columns = list(fields) + META_COLUMNS

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                ch.insert(table, rows, column_names=columns)
                break
            except Exception as e:
                log.warning("insert into %s failed (attempt %d/%d): %s", table, attempt, MAX_RETRIES, e)
                time.sleep(2 ** attempt)
        else:
            raise RuntimeError(f"giving up on insert into {table}")

        total += len(rows)
        rows.clear()
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-after-flush", type=int, default=0,
                        help="упасть после N-го flush, до коммита offset'ов (тест отказа)")
    args = parser.parse_args()
    log.info("pid=%d", os.getpid())
    flushes = 0
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(list(TABLES))
    ch = clickhouse_connect.get_client(**CH_CONF)

    buffers = {topic: [] for topic in TABLES}
    pending = 0
    last_flush = time.monotonic()

    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s", list(TABLES))
    try:
        while running:
            msg = consumer.poll(0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", msg.error())
                elif msg.value() is not None:  # tombstone пропускаем
                    try:
                        value = json.loads(msg.value())
                        _, fields = TABLES[msg.topic()]
                        buffers[msg.topic()].append(to_row(value, fields))
                        pending += 1
                    except Exception as e:
                        log.error("bad message %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), e)

            if pending >= BATCH_SIZE or (pending and time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC):
                n = flush(ch, buffers)
                flushes += 1
                if args.crash_after_flush and flushes >= args.crash_after_flush:
                    log.warning("simulating crash after flush #%d: data inserted, offsets NOT committed", flushes)
                    os._exit(1)  # мгновенный выход без finally и без commit
                consumer.commit(asynchronous=False)
                log.info("flushed %d rows", n)
                pending = 0
                last_flush = time.monotonic()
    finally:
        if pending:
            flush(ch, buffers)
            consumer.commit(asynchronous=False)
        consumer.close()
        log.info("stopped")


if __name__ == "__main__":
    main()