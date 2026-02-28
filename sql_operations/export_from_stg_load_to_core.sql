BEGIN;

--------------------------------------------------
-- 1️⃣ Rank staging rows (latest ingest wins)
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
        raw_payload,
        ingest_time,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, ts
            ORDER BY ingest_time DESC
        ) AS rn
    FROM stg_raw.market_data
    WHERE asset_type = 'stock'
),

--------------------------------------------------
-- 2️⃣ Log duplicate rows
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
-- 3️⃣ Log fractional volume
--------------------------------------------------

log_fractional_volume AS (
    INSERT INTO operation_logs.data_quality_errors (
        symbol,
        ts,
        error_type,
        error_detail
    )
    SELECT
        symbol,
        ts,
        'fractional_volume',
        'Volume contains decimals for 5m bar'
    FROM ranked
    WHERE rn = 1
      AND volume <> FLOOR(volume)
),

--------------------------------------------------
-- 4️⃣ Log invalid OHLC structure
--------------------------------------------------

log_invalid_ohlc AS (
    INSERT INTO operation_logs.data_quality_errors (
        symbol,
        ts,
        error_type,
        error_detail
    )
    SELECT
        symbol,
        ts,
        'invalid_ohlc_structure',
        'OHLC violates price consistency rules'
    FROM ranked
    WHERE rn = 1
      AND NOT (
            high >= low
        AND high >= open
        AND high >= close
        AND low  <= open
        AND low  <= close
        AND open  > 0
        AND high  > 0
        AND low   > 0
        AND close > 0
      )
),

--------------------------------------------------
-- 5️⃣ Insert only fully valid rows into core
--------------------------------------------------

valid_rows AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
      AND volume = FLOOR(volume)
      AND high >= low
      AND high >= open
      AND high >= close
      AND low  <= open
      AND low  <= close
      AND open  > 0
      AND high  > 0
      AND low   > 0
      AND close > 0
)

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
FROM valid_rows
ON CONFLICT (symbol, ts)
DO UPDATE SET
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source;

--------------------------------------------------
-- 6️⃣ Update watermark
--------------------------------------------------

INSERT INTO operation_logs.pipeline_watermarks (
    pipeline_name,
    last_processed_ts,
    status
)
VALUES (
    'stg_to_core_5m',
    (SELECT MAX(ts) FROM stg_raw.market_data WHERE asset_type = 'stock'),
    'success'
)
ON CONFLICT (pipeline_name)
DO UPDATE SET
    last_processed_ts = EXCLUDED.last_processed_ts,
    status = 'success',
    updated_at = now();

COMMIT;
