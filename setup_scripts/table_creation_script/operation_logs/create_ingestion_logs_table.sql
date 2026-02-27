CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE IF NOT EXISTS operation_logs.ingestion_log (
   log_id       BIGSERIAL PRIMARY KEY,
   symbol       TEXT,
   start_date   DATE,
   end_date     DATE,
   rows_loaded  INTEGER,
   status       TEXT,
   error_msg    TEXT,
   logged_at    TIMESTAMPTZ DEFAULT now()
);
