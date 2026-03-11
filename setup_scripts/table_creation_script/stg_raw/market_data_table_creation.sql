CREATE SCHEMA IF NOT EXISTS stg_raw;


CREATE TABLE IF NOT EXISTS stg_raw.market_data (
    ingest_id     BIGSERIAL PRIMARY KEY,
    symbol        TEXT,
    ts            TIMESTAMPTZ,
    open          NUMERIC(18,6),
    high          NUMERIC(18,6),
    low           NUMERIC(18,6),
    close         NUMERIC(18,6),
    volume        NUMERIC(20,4),     -- allows 0.0, doubles
    asset_type    TEXT,              -- stock, bond, commodity, index
    source        TEXT,       -- FMP_intraday, FMP_hist, CSV, OtherAPI
    ingest_time   TIMESTAMPTZ DEFAULT now(),
    raw_payload   JSONB,             -- exact source row
    CONSTRAINT unique_symbol_ts_source UNIQUE (symbol, ts)
);


CREATE INDEX idx_stg_raw_symbol_ts
ON stg_raw.market_data(symbol, ts);


CREATE INDEX idx_stg_raw_ingest_time
ON stg_raw.market_data(ingest_time);