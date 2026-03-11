CREATE SCHEMA IF NOT EXISTS core_dbms;

CREATE TABLE IF NOT EXISTS core_dbms.market_data_5m (
    market_data_id BIGSERIAL PRIMARY KEY,
    symbol         TEXT NOT NULL,
    ts             TIMESTAMPTZ NOT NULL,
    open           NUMERIC(18,6) NOT NULL,
    high           NUMERIC(18,6) NOT NULL,
    low            NUMERIC(18,6) NOT NULL,
    close          NUMERIC(18,6)NOT NULL,
    volume         BIGINT NOT NULL,   -- discrete shares
    asset_type     TEXT NOT NULL,
    source         TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT unique_symbol_ts UNIQUE (symbol, ts)
);
