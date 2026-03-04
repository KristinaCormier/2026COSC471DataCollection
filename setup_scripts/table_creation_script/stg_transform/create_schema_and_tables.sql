CREATE SCHEMA IF NOT EXISTS stg_transform;
CREATE TABLE IF NOT EXISTS stg_transform.market_data (
symbol      TEXT,
ts          TIMESTAMPTZ,
open        NUMERIC(18,6),
high        NUMERIC(18,6),
low         NUMERIC(18,6),
close       NUMERIC(18,6),
volume      BIGINT,
vwap        NUMERIC(20,6),	-- average price of a security weighted by traded volume over a time window
PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS stg_transform.transform_errors (
error_id     BIGSERIAL PRIMARY KEY,
symbol       TEXT,
ts           TIMESTAMPTZ,
error_type   TEXT,     -- divide_by_zero, missing_window
error_detail TEXT,
log_time     TIMESTAMPTZ DEFAULT now()
);