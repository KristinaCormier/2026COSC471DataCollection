-- ============================================================================
-- Truncate Staging Layer
-- ============================================================================
--
-- Purpose:
--     Safely clear all tables in the stg_raw schema after successful export to core.
--     Restarts identity sequences so the next ingestion starts from sequence 1.
--
-- Execution Context:
--     Called by: src/run_scheduled_operations.py (via cron, after export_stg_to_core.sql)
--     Executes as: Single transaction
--     Dependencies: Assumes export_stg_to_core.sql has already run and succeeded
--
-- What It Does:
--     1. Iterates over all tables in stg_raw schema
--     2. For each table:
--        - Truncates all rows (DELETE)
--        - Restarts identity sequences (RESTART IDENTITY)
--        - Cascades to dependent tables if any exist
--
-- Side Effects:
--     ⚠️  DESTRUCTIVE - Permanently deletes all rows from stg_raw schema
--     - Does NOT delete audit logs (operation_logs tables are untouched)
--     - ingest_errors are also truncated (preserve only what was logged)
--
-- Idempotency:
--     Safe if no new data was written to stg_raw between export and truncate.
--     Running twice in a row will be a no-op (no rows to delete).
--
-- Execution Sequence:
--     1. export_stg_to_core.sql (moves valid data to core)
--     2. truncate_stg_raw.sql (cleans up staging for next cycle)
--
-- Warning:
--     If called before export_stg_to_core.sql completes, data loss will occur.
--     Always verify export_stg_to_core.sql succeeded (check operation_logs.pipeline_logs).
--
-- Recovery:
--     If truncated by mistake, recover from backups via:
--     - PostgreSQL PITR (if WAL archiving is enabled)
--     - Manual base backup restore (run within backup retention window)
--
-- Author: Data Collection Team
-- License: MIT
-- ============================================================================

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'stg_raw'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE stg_raw.%I RESTART IDENTITY CASCADE;',
            r.tablename
        );
    END LOOP;
END $$;
