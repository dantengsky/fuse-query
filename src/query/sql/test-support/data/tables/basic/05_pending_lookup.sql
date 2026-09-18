CREATE OR REPLACE TABLE pending_lookup (
    req_ref STRING,
    member_id INT64,
    txn_ref STRING,
    updated_at TIMESTAMP
);
