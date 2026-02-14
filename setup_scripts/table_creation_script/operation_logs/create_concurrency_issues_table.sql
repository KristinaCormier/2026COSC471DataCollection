CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.concurrency_issues (
    issue_id     BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    lock_type    TEXT,
    pid          INTEGER,
    details      TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);