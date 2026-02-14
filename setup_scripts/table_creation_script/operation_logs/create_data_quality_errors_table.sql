CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.data_quality_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_type   TEXT,    -- constraint_violation, fractional_volume
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);