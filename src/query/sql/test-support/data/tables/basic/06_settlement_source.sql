CREATE OR REPLACE TABLE settlement_source (
    txn_ref STRING,
    member_id INT64,
    coin STRING,
    network STRING,
    moved_amount DECIMAL(40, 18),
    state INT32,
    return_state INT32
);
