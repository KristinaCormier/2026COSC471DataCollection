CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.backup_logs (
    backup_id    BIGSERIAL PRIMARY KEY,
    backup_type  TEXT,
    file_path    TEXT,
    status       TEXT,
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    log_time     TIMESTAMPTZ DEFAULT now()
);