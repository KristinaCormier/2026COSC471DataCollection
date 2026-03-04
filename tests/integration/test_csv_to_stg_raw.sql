-- Test 1 — Inserts One Valid Row
BEGIN;

-- Arrange
INSERT INTO stg_raw.market_data
(ingest_id, symbol, ts, open, high, low, close, volume, asset_type, source, ingest_time, raw_payload)
VALUES
(1000, 'AAPL', '2024-01-01 09:30', 100, 101, 99, 100.5, 10000, 'stock', 'CSV_bulk_load', now(), '{}');

-- Assert
DO $$
DECLARE cnt INT;
BEGIN
   SELECT COUNT(*) INTO cnt
   FROM stg_raw.market_data
   WHERE symbol = 'AAPL'
     AND ts = '2024-01-01 09:30';

   IF cnt <> 1 THEN
       RAISE EXCEPTION 'Test failed: Expected 1 row, got %', cnt;
   END IF;
END $$;

ROLLBACK;
