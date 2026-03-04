CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.dedup_conflicts (
    conflict_id  BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    existing_row JSONB,
    incoming_row JSONB,
    resolution   TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);