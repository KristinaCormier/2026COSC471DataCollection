BEGIN;

--------------------------------------------------
-- 1️⃣ Ensure schema exists
--------------------------------------------------

CREATE SCHEMA IF NOT EXISTS operation_logs;

--------------------------------------------------
-- 2️⃣ Create pipeline_logs table
--------------------------------------------------

CREATE TABLE IF NOT EXISTS operation_logs.pipeline_logs (

    log_id          BIGSERIAL PRIMARY KEY,

    pipeline_stage  TEXT NOT NULL,      -- e.g. 'stg_to_core_5m'
    status          TEXT NOT NULL,      -- running | success | failed | warning

    message         TEXT,               -- optional descriptive detail

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()

);

--------------------------------------------------
-- 3️⃣ Enforce valid status values
--------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_pipeline_logs_status'
    ) THEN
        ALTER TABLE operation_logs.pipeline_logs
        ADD CONSTRAINT chk_pipeline_logs_status
        CHECK (status IN ('running', 'success', 'failed', 'warning'));
    END IF;
END $$;

--------------------------------------------------
-- 4️⃣ Add helpful index for querying recent logs
--------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_stage_time
ON operation_logs.pipeline_logs (pipeline_stage, created_at DESC);

COMMIT;