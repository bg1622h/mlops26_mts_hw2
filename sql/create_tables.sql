CREATE DATABASE IF NOT EXISTS hw2;

CREATE TABLE IF NOT EXISTS hw2.transaction_queue
(
    transaction_id   String,
    transaction_time DateTime,
    category         String,
    state            String,
    amount           Float64,
    is_fraud         UInt8
)

ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_group',
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS hw2.transactions
(
    transaction_id   String,
    transaction_time DateTime,
    category         String,
    state            String,
    amount           Float64,
    is_fraud         UInt8
)
ENGINE = MergeTree
ORDER BY (state, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_mv
TO hw2.transactions
AS SELECT
    transaction_id,
    transaction_time,
    category,
    state,
    amount,
    is_fraud
FROM hw2.transaction_queue;
