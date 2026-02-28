-- Test 1 — Invalid OHLC Blocked
BEGIN;

INSERT INTO stg_raw.market_data
(ingest_id, symbol, ts, open, high, low, close, volume, asset_type, source, ingest_time, raw_payload)
VALUES
(1000, 'TSLA', '2024-01-01 09:40', 100, 90, 95, 102, 10000, 'stock', 'CSV_bulk_load', now(), '{}');

-- Run pipeline

DO $$
DECLARE cnt INT;
BEGIN
   SELECT COUNT(*) INTO cnt
   FROM core_dbms.market_data_5m
   WHERE symbol = 'TSLA';

   IF cnt <> 0 THEN
       RAISE EXCEPTION 'Invalid OHLC incorrectly inserted';
   END IF;
END $$;

ROLLBACK;


-- Test 2 — Fractional Volume Blocked
BEGIN;

INSERT INTO stg_raw.market_data
(ingest_id, symbol, ts, open, high, low, close, volume, asset_type, source, ingest_time, raw_payload)
VALUES
(1000, 'NVDA', '2024-01-01 09:45', 100, 105, 99, 103, 10000.55, 'stock', 'CSV_bulk_load', now(),  '{}');

-- Run pipeline

DO $$
DECLARE cnt INT;
BEGIN
   SELECT COUNT(*) INTO cnt
   FROM core_dbms.market_data_5m
   WHERE symbol = 'NVDA';

   IF cnt <> 0 THEN
       RAISE EXCEPTION 'Fractional volume incorrectly inserted';
   END IF;
END $$;

ROLLBACK;
