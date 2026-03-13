-- ============================================================================
-- Export & Transform: stg_raw.market_data → core_dbms.market_data_5m
-- ============================================================================
--
-- Purpose:
--     Move, deduplicate, validate, and transform 5-minute bars from the staging
--     layer (raw API ingest) to the core warehouse (validated canonical data).
--
-- Execution Context:
--     Called by: src/run_scheduled_operations.py (via cron, default: 2 AM UTC daily)
--     Executes as: Single transaction (BEGIN; ... COMMIT; or ROLLBACK;)
--     Dependencies: stg_raw.market_data must exist with rows to process
--
-- What It Does:
--     1. RANK: Deduplicates on (symbol, ts) by keeping the latest ingest only
--     2. LOG: Records discarded duplicates to operation_logs.dedup_conflicts
--     3. VALIDATE: Checks OHLCV completeness, price validity, volume sanity
--     4. LOG: Records quality failures to operation_logs.data_quality_errors
--     5. UPSERT: Inserts or updates core_dbms.market_data_5m with valid rows
--
-- Side Effects:
--     - Writes to operation_logs.dedup_conflicts (duplicates, if any)
--     - Writes to operation_logs.data_quality_errors (invalid rows, if any)
--     - Execution logged by run_scheduled_operations.py to operation_logs.pipeline_logs
--     - Does NOT delete from stg_raw; call truncate_stg_raw.sql separately
--
-- Idempotency:
--     Safe to run multiple times. Uses ON CONFLICT DO UPDATE, so re-running
--     will overwrite prior exports with the same (symbol, ts) if staging has changed.
--
-- Performance Notes:
--     - Typical runtime: < 1 second on < 1M rows
--     - Window functions (ROW_NUMBER) scan staging once
--     - Indexes on (symbol, ts) recommended for both tables
--
-- Rollback:
--     On error, the entire transaction rolls back. No cleanup needed.
--     Staging data remains untouched for retry/investigation.
--
-- Author: Data Collection Team
-- License: MIT
-- ============================================================================

BEGIN;

--------------------------------------------------
-- Rank staging rows (latest ingest wins)
--------------------------------------------------

WITH ranked AS (
    SELECT
        symbol,
        ts,
        open,
        high,
        low,
        close,
        volume,
        asset_type,
        source,
        ingest_time,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, ts
            ORDER BY ingest_time DESC
        ) AS rn
    FROM stg_raw.market_data
    WHERE asset_type = 'stock'
),

--------------------------------------------------
-- Log duplicate rows
--------------------------------------------------

log_duplicates AS (
    INSERT INTO operation_logs.dedup_conflicts (
        symbol,
        ts,
        existing_row,
        incoming_row,
        resolution
    )
    SELECT
        symbol,
        ts,
        NULL,
        to_jsonb(ranked),
        'discarded_duplicate_in_staging'
    FROM ranked
    WHERE rn > 1
),

--------------------------------------------------
-- Filter clean rows
--------------------------------------------------

clean AS (
    SELECT * FROM ranked WHERE rn = 1
),

--------------------------------------------------
-- Data quality checks
--------------------------------------------------

quality_checks AS (
    SELECT
        *,
        CASE
            WHEN close IS NULL OR close <= 0 THEN 'invalid_price'
            WHEN open IS NULL OR high IS NULL OR low IS NULL THEN 'incomplete_ohlc'
            WHEN volume IS NULL OR volume < 0 THEN 'invalid_volume'
            WHEN ts IS NULL THEN 'missing_timestamp'
            ELSE NULL
        END AS quality_issue
    FROM clean
),

log_quality_errors AS (
    INSERT INTO operation_logs.data_quality_errors (
        symbol,
        ts,
        error_type,
        error_detail
    )
    SELECT
        symbol,
        ts,
        quality_issue,
        to_jsonb(quality_checks)
    FROM quality_checks
    WHERE quality_issue IS NOT NULL
),

valid AS (
    SELECT * FROM quality_checks WHERE quality_issue IS NULL
),

--------------------------------------------------
-- Insert into core
--------------------------------------------------

core_insert AS (
    INSERT INTO core_dbms.market_data_5m (
        symbol,
        ts,
        open,
        high,
        low,
        close,
        volume,
        asset_type,
        source
    )
    SELECT
        symbol,
        ts,
        open,
        high,
        low,
        close,
        volume::BIGINT,
        asset_type,
        source
    FROM valid
    ON CONFLICT (symbol, ts) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
),

--------------------------------------------------
-- Update watermarks
--------------------------------------------------

watermark_update AS (
    INSERT INTO operation_logs.pipeline_watermarks (
        pipeline_name,
        last_processed_ts
    )
    SELECT
        'export_stg_to_core',
        MAX(ts)
    FROM valid
    ON CONFLICT (pipeline_name) DO UPDATE SET
        last_processed_ts = EXCLUDED.last_processed_ts
)

SELECT 'export_stg_to_core execution complete';

COMMIT;
