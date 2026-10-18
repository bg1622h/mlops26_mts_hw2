CREATE TABLE hw2.transactions_optimized
(
    transaction_id String,
    transaction_time DateTime,
    category LowCardinality(String),
    state LowCardinality(String),
    amount Float64,
    is_fraud UInt8
)
ENGINE = MergeTree
ORDER BY (state, category, transaction_time);

CREATE TABLE hw2.transactions_state_max
(
    state LowCardinality(String),
    category_state AggregateFunction(argMax, String, Float64),
    amount_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY state;

CREATE MATERIALIZED VIEW hw2.transactions_optimized_mv
TO hw2.transactions_optimized
AS SELECT
    transaction_id,
    transaction_time,
    category,
    state,
    amount,
    is_fraud
FROM hw2.transactions;

CREATE MATERIALIZED VIEW hw2.transactions_state_max_mv
TO hw2.transactions_state_max
AS SELECT
    state,
    argMaxState(toString(category), amount) AS category_state,
    maxState(amount) AS amount_state
FROM hw2.transactions
GROUP BY state;

INSERT INTO hw2.transactions_optimized
SELECT
    transaction_id,
    transaction_time,
    category,
    state,
    amount,
    is_fraud
FROM hw2.transactions;

INSERT INTO hw2.transactions_state_max
SELECT
    state,
    argMaxState(toString(category), amount),
    maxState(amount)
FROM hw2.transactions
GROUP BY state;

CREATE VIEW hw2.max_transactions_by_state AS
SELECT
    state,
    argMaxMerge(category_state) AS category_of_largest_transaction,
    maxMerge(amount_state) AS largest_transaction_amount
FROM hw2.transactions_state_max
GROUP BY state;

SELECT
    (SELECT count() FROM hw2.transactions) AS original_rows,
    (SELECT count() FROM hw2.transactions_optimized) AS optimized_rows;

SELECT count() AS max_amount_mismatches
FROM
(
    SELECT state, max(amount) AS value
    FROM hw2.transactions
    GROUP BY state
) AS original
FULL OUTER JOIN
(
    SELECT state, largest_transaction_amount AS value
    FROM hw2.max_transactions_by_state
) AS optimized
ON original.state = optimized.state
WHERE original.state IS NULL OR optimized.state IS NULL
   OR original.value != optimized.value
SETTINGS join_use_nulls = 1;

SELECT * FROM hw2.max_transactions_by_state ORDER BY state;