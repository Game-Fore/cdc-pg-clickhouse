import argparse
import logging
import random
import time
import psycopg



from collections import Counter
from decimal import Decimal
from faker import Faker



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generator")

fake = Faker("ru_RU")
DSN = "postgresql://shop:shop@localhost:5432/shop"

# допустимые переходы статусов заказа
STATUS_FLOW = {
    "new": ["paid", "paid", "paid", "cancelled"],
    "paid": ["shipped", "shipped", "shipped", "cancelled"],
    "shipped": ["delivered"],
}


def create_customer(cur):
    email = f"{fake.user_name()}{random.randint(1000, 999999)}@{fake.free_email_domain()}"
    cur.execute(
        "INSERT INTO customers (email, full_name, city) VALUES (%s, %s, %s) "
        "ON CONFLICT (email) DO NOTHING",
        (email, fake.name(), fake.city()),
    )


def create_order(cur):
    cur.execute("SELECT id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return create_customer(cur)

    cur.execute("INSERT INTO orders (customer_id) VALUES (%s) RETURNING id", row)
    order_id = cur.fetchone()[0]

    cur.execute("SELECT id, price FROM products ORDER BY random() LIMIT %s", (random.randint(1, 4),))
    total = Decimal("0")
    for product_id, price in cur.fetchall():
        qty = random.randint(1, 3)
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, qty, price),
        )
        total += price * qty

    # намеренно отдельный UPDATE: в CDC будет c -> u по одному заказу
    cur.execute("UPDATE orders SET total = %s, updated_at = now() WHERE id = %s", (total, order_id))


def advance_status(cur):
    cur.execute(
        "SELECT id, status FROM orders WHERE status IN ('new', 'paid', 'shipped') "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        return create_order(cur)
    order_id, status = row
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
        (random.choice(STATUS_FLOW[status]), order_id),
    )


def update_customer(cur):
    cur.execute(
        "UPDATE customers SET city = %s "
        "WHERE id = (SELECT id FROM customers ORDER BY random() LIMIT 1)",
        (fake.city(),),
    )


def change_price(cur):
    cur.execute(
        "UPDATE products SET price = round(price * (0.9 + random() * 0.2)::numeric, 2) "
        "WHERE id = (SELECT id FROM products ORDER BY random() LIMIT 1)"
    )


def delete_cancelled(cur):
    # ON DELETE CASCADE -> Debezium пришлёт DELETE и по order_items
    cur.execute(
        "DELETE FROM orders "
        "WHERE id = (SELECT id FROM orders WHERE status = 'cancelled' ORDER BY random() LIMIT 1)"
    )


ACTIONS = [
    (create_order, 40),
    (advance_status, 35),
    (create_customer, 10),
    (update_customer, 5),
    (change_price, 5),
    (delete_cancelled, 5),
]


def main():
    parser = argparse.ArgumentParser(description="OLTP load generator for shop DB")
    parser.add_argument("--dsn", default=DSN)
    parser.add_argument("--rps", type=float, default=5.0, help="actions per second")
    args = parser.parse_args()

    funcs, weights = zip(*ACTIONS)

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        # стартовый набор клиентов
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM customers")
            if cur.fetchone()[0] < 50:
                for _ in range(50):
                    create_customer(cur)
                log.info("seeded 50 customers")

        stats = Counter()
        last_report = time.monotonic()
        log.info("generating load at %.1f actions/sec, Ctrl+C to stop", args.rps)

        try:
            while True:
                action = random.choices(funcs, weights)[0]
                with conn.transaction(), conn.cursor() as cur:
                    action(cur)
                stats[action.__name__] += 1

                if time.monotonic() - last_report >= 10:
                    log.info("last 10s: %s", dict(stats))
                    stats.clear()
                    last_report = time.monotonic()

                time.sleep(1 / args.rps)
        except KeyboardInterrupt:
            log.info("stopped")


if __name__ == "__main__":
    main()