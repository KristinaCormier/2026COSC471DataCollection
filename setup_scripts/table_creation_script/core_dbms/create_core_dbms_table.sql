CREATE SCHEMA IF NOT EXISTS core_dbms;

CREATE TABLE IF NOT EXISTS core_dbms.market_data_5m (
    market_data_id BIGSERIAL PRIMARY KEY,
    symbol         TEXT NOT NULL,
    ts             TIMESTAMPTZ NOT NULL,
    open           NUMERIC(18,6),
    high           NUMERIC(18,6),
    low            NUMERIC(18,6),
    close          NUMERIC(18,6),
    volume         BIGINT NOT NULL,   -- discrete shares
    asset_type     TEXT NOT NULL,
    source         TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, ts)
);
