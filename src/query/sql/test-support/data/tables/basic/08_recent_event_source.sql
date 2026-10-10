CREATE OR REPLACE TABLE recent_event_source (
    key_b STRING,
    key_a INT64,
    status INT32,
    ratio DECIMAL(10, 8),
    created_at TIMESTAMP,
    channel STRING,
    payload STRING
);
