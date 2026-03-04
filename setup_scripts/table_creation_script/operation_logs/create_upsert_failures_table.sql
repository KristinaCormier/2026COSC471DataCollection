CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.upsert_failures (
    failure_id   BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);