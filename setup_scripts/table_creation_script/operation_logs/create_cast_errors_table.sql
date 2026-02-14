CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.cast_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    column_name  TEXT,
    raw_value    TEXT,
    target_type  TEXT,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);