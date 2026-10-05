CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE shop.customers
(
    id           UInt32,
    email        String,
    full_name    String,
    city         Nullable(String),
    created_at   DateTime64(6, 'UTC'),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.products
(
    id           UInt32,
    name         String,
    category     LowCardinality(String),
    price        Decimal(10, 2),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.orders
(
    id           UInt32,
    customer_id  UInt32,
    status       LowCardinality(String),
    total        Decimal(12, 2),
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.order_items
(
    id           UInt32,
    order_id     UInt32,
    product_id   UInt32,
    quantity     UInt32,
    price        Decimal(10, 2),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;