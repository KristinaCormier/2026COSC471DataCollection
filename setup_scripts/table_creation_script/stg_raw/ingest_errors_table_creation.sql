CREATE TABLE IF NOT EXISTS stg_raw.ingest_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    ingest_id    BIGINT,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    asset_type   TEXT,
    source       TEXT,
    error_type   TEXT,  -- parse_error, schema_mismatch, missing_field
    error_detail TEXT,
    raw_payload  JSONB,
    log_time     TIMESTAMPTZ DEFAULT now()
);