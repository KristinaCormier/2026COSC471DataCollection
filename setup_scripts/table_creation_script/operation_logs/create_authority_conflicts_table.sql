CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.authority_conflicts (
    conflict_id      BIGSERIAL PRIMARY KEY,
    symbol           TEXT,
    ts               TIMESTAMPTZ,
    source_a         TEXT,
    source_b         TEXT,
    preferred_source TEXT,
    log_time         TIMESTAMPTZ DEFAULT now()
);