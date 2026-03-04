CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.pipeline_watermarks (
    pipeline_name      TEXT PRIMARY KEY,
    last_processed_ts  TIMESTAMPTZ,
    status             TEXT,
    updated_at         TIMESTAMPTZ DEFAULT now()
);