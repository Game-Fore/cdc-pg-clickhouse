"""Сверка Postgres (источник) и ClickHouse (приёмник) после CDC."""
import argparse
import sys
import time
import clickhouse_connect
import psycopg

from decimal import Decimal


PG_DSN = "postgresql://shop:shop@localhost:5432/shop"
CH_CONF = dict(host="localhost", port=8123, username="shop", password="shop", database="shop")

CHECKS = {
    "customers": (
        "SELECT count(*), coalesce(sum(id), 0), count(DISTINCT city) FROM customers",
        "SELECT count(), sum(id), uniqExact(city) FROM shop.customers FINAL",
    ),
    "products": (
        "SELECT count(*), coalesce(sum(price), 0) FROM products",
        "SELECT count(), sum(price) FROM shop.products FINAL",
    ),
    "orders": (
        "SELECT count(*), coalesce(sum(id), 0), coalesce(sum(total), 0) FROM orders",
        "SELECT count(), sum(id), sum(total) FROM shop.orders FINAL",
    ),
    "orders_by_status": (
        "SELECT string_agg(status || '=' || cnt, ',' ORDER BY status) "
        "FROM (SELECT status, count(*) AS cnt FROM orders GROUP BY status) s",
        "SELECT arrayStringConcat(groupArray(status || '=' || toString(cnt)), ',') "
        "FROM (SELECT status, count() AS cnt FROM shop.orders FINAL GROUP BY status ORDER BY status)",
    ),
    "order_items": (
        "SELECT count(*), coalesce(sum(id), 0), coalesce(sum(quantity), 0), coalesce(sum(price * quantity), 0) "
        "FROM order_items",
        "SELECT count(), sum(id), sum(quantity), sum(price * quantity) FROM shop.order_items FINAL",
    ),
}

TABLES = ["customers", "products", "orders", "order_items"]


def norm(row):
    return tuple(Decimal(str(v)) if isinstance(v, (int, float, Decimal)) else (v or "") for v in row)


def run_checks(pg, ch):
    results = []
    for name, (pg_sql, ch_sql) in CHECKS.items():
        pg_row = norm(pg.execute(pg_sql).fetchone())
        ch_row = norm(ch.query(ch_sql).result_rows[0])
        results.append((name, pg_row, ch_row, pg_row == ch_row))
    return results


def duplicates(ch):
    out = {}
    for t in TABLES:
        raw, uniq = ch.query(f"SELECT count(), uniqExact(id, _lsn) FROM shop.{t}").result_rows[0]
        out[t] = (raw, raw - uniq)
    return out


QUIET_SQL = (
    "SELECT (SELECT count(*) FROM customers), (SELECT count(*) FROM orders), "
    "(SELECT max(updated_at) FROM orders), (SELECT count(*) FROM order_items), "
    "(SELECT sum(price) FROM products)"
)


def source_is_quiet(pg, seconds=3):
    before = pg.execute(QUIET_SQL).fetchone()
    time.sleep(seconds)
    return before == pg.execute(QUIET_SQL).fetchone()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stop-merges", action="store_true", help="остановить мержи перед тестом")
    parser.add_argument("--start-merges", action="store_true", help="вернуть мержи после теста")
    parser.add_argument("--wait", type=int, default=30, help="сколько секунд ждать, пока consumer догонит")
    args = parser.parse_args()

    ch = clickhouse_connect.get_client(**CH_CONF)
    if args.stop_merges or args.start_merges:
        cmd = "STOP" if args.stop_merges else "START"
        for t in TABLES:
            ch.command(f"SYSTEM {cmd} MERGES shop.{t}")
        print(f"merges: {cmd}")
        return
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        if not source_is_quiet(pg):
            print("WARNING: source is still changing (generator running?). Stop it and re-run.")
            sys.exit(2)
        deadline = time.monotonic() + args.wait
        while True:
            results = run_checks(pg, ch)
            ok = all(r[3] for r in results)
            if ok or time.monotonic() >= deadline:
                break
            time.sleep(2)

    print(f"{'check':<18} {'status':<6} postgres | clickhouse")
    for name, pg_row, ch_row, match in results:
        status = "OK" if match else "DIFF"
        pg_s = ", ".join(map(str, pg_row))
        ch_s = ", ".join(map(str, ch_row))
        print(f"{name:<18} {status:<6} {pg_s}" + ("" if match else f" | {ch_s}"))

    print("\nraw rows in ClickHouse / duplicates (same id + _lsn, not merged yet):")
    for t, (raw, dup) in duplicates(ch).items():
        print(f"  {t:<12} raw={raw:<8} duplicates={dup}")

    print("\nRESULT:", "CONSISTENT" if ok else "MISMATCH")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()