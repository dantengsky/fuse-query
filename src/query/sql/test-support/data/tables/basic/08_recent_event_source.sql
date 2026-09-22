CREATE OR REPLACE TABLE recent_event_source (
    event_id STRING,
    member_id INT64,
    status INT32,
    ratio DECIMAL(10, 8),
    created_at TIMESTAMP,
    channel STRING,
    payload STRING
);
