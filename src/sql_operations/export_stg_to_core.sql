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
