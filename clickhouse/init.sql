CREATE DATABASE IF NOT EXISTS oneshop;

CREATE TABLE IF NOT EXISTS oneshop.purchases_kafka
(
    id UInt64,
    user_id UInt64,
    item_id UInt64,
    quantity UInt32,
    purchase_price Decimal(12, 2),
    created_at Int64,
    __deleted String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'dbz_purchases.public.purchases',
    kafka_group_name = 'clickhouse_purchases_v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS oneshop.purchases
(
    id UInt64,
    user_id UInt64,
    item_id UInt64,
    quantity UInt32,
    purchase_price Decimal(12, 2),
    created_at DateTime64(6, 'UTC'),
    version UInt64,
    deleted UInt8
)
ENGINE = ReplacingMergeTree(version, deleted)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS oneshop.purchases_consumer
TO oneshop.purchases
AS
SELECT
    id,
    user_id,
    item_id,
    quantity,
    purchase_price,
    fromUnixTimestamp64Micro(created_at, 'UTC') AS created_at,
    toUInt64(_offset) AS version,
    toUInt8(__deleted = 'true') AS deleted
FROM oneshop.purchases_kafka;
