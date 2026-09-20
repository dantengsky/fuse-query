CREATE OR REPLACE TABLE activity_source (
    id STRING,
    member_id INT64,
    coin STRING,
    amount DECIMAL(28, 8),
    kind INT32,
    state INT32,
    auto_state STRING,
    apply_at TIMESTAMP,
    network STRING,
    note STRING
);
