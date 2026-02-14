# 📊 Market Data Platform – Pre-Data Warehouse Architecture

## Overview

This document describes the **pre-data warehouse layer** of the market data platform, covering all schemas responsible for ingesting, staging, transforming, and logging market data before it reaches the analytics or ML-ready warehouse.

The architecture ensures:

- Clean, validated ingestion
- Normalized storage for time-series OHLCV data
- Aggregation and transformation for derived metrics
- Centralized operational monitoring and governance

---

# 🧱 Schema Architecture

```
External Sources (API, CSV, Feeds)
        ↓
stg_raw            ← Staging & validation
        ↓
core_dbms          ← Normalized 5-min OHLCV storage
        ↓
stg_transform      ← 15-min aggregation + VWAP
        ↓
Data Warehouse      ← (Future analytics layer)
operation_logs      ← Cross-layer monitoring
```

---

# 1️⃣ stg_raw – Staging Layer

**Purpose:**  
Landing zone for raw market data with light validation.  

**Key Characteristics:**

- One table per ticker / instrument (500+ tickers)
- OHLCV fields plus metadata: `asset_type`, `source`, `ingest_time`
- Raw JSON payload storage for traceability
- Validated views to ensure `date` and `volume` integrity

**Sample SQL:**

```sql
CREATE SCHEMA IF NOT EXISTS stg_raw;

-- Example ticker table creation
CREATE TABLE IF NOT EXISTS stg_raw.aapl (
    stock_id BIGSERIAL PRIMARY KEY,
    date TIMESTAMP,
    open NUMERIC(18,6),
    low NUMERIC(18,6),
    high NUMERIC(18,6),
    close NUMERIC(18,6),
    volume NUMERIC(20,4),
    asset_type TEXT,     -- stock, bond, commodity, index
    source TEXT,         -- FMP_intraday, FMP_hist, CSV, OtherAPI
    ingest_time TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB
);

-- Validated view
CREATE OR REPLACE VIEW stg_raw.valid_aapl AS
SELECT *
FROM stg_raw.aapl
WHERE date IS NOT NULL
  AND volume >= 0;
```

**Role:**  
First structured layer after ingestion. Prepares data for normalized storage.

---

# 2️⃣ core_dbms – Central Storage

**Purpose:**  
Authoritative storage of normalized market data.  

**Key Table:**

```sql
CREATE SCHEMA IF NOT EXISTS core_dbms;

CREATE TABLE core_dbms.market_data_5m (
    market_data_id BIGSERIAL PRIMARY KEY,
    symbol         TEXT NOT NULL,
    ts             TIMESTAMPTZ NOT NULL,
    open           NUMERIC(18,6),
    high           NUMERIC(18,6),
    low            NUMERIC(18,6),
    close          NUMERIC(18,6),
    volume         BIGINT NOT NULL,
    asset_type     TEXT NOT NULL,
    source         TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, ts)
);
```

**Characteristics:**

- 5-minute OHLCV bars
- Enforced uniqueness `(symbol, ts)`
- Idempotent upserts supported
- Source of truth for all downstream aggregation

**Role:**  
Normalized and authoritative time-series store.

---

# 3️⃣ stg_transform – Transformation Layer

**Purpose:**  
Aggregates and derives metrics from core storage.  

**Key Table:**

```sql
CREATE SCHEMA IF NOT EXISTS stg_transform;

CREATE TABLE stg_transform.market_data_15min (
    symbol      TEXT,
    ts          TIMESTAMPTZ,
    open        NUMERIC(18,6),
    high        NUMERIC(18,6),
    low         NUMERIC(18,6),
    close       NUMERIC(18,6),
    volume      BIGINT,
    vwap        NUMERIC(20,6),    -- volume-weighted average price
    PRIMARY KEY (symbol, ts)
);

-- Transformation error logging
CREATE TABLE stg_transform.transform_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_type   TEXT,     -- divide_by_zero, missing_window
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

**Role:**  
Provides analytics-ready aggregated tables, including derived metrics like VWAP, and logs errors encountered during transformations.

---

# 4️⃣ operation_logs – Operational Observability

**Purpose:**  
Centralized monitoring and governance for all layers.

**Key Tables & Sample SQL:**

```sql
CREATE SCHEMA IF NOT EXISTS operation_logs;

CREATE TABLE operation_logs.data_quality_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_type   TEXT,    -- constraint_violation, fractional_volume
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.dedup_conflicts (
    conflict_id  BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    existing_row JSONB,
    incoming_row JSONB,
    resolution   TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.cast_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    column_name  TEXT,
    raw_value    TEXT,
    target_type  TEXT,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.authority_conflicts (
    conflict_id      BIGSERIAL PRIMARY KEY,
    symbol           TEXT,
    ts               TIMESTAMPTZ,
    source_a         TEXT,
    source_b         TEXT,
    preferred_source TEXT,
    log_time         TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.upsert_failures (
    failure_id   BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.concurrency_issues (
    issue_id     BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    lock_type    TEXT,
    pid          INTEGER,
    details      TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.pipeline_watermarks (
    pipeline_name      TEXT PRIMARY KEY,
    last_processed_ts  TIMESTAMPTZ,
    status             TEXT,
    updated_at         TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE operation_logs.backup_logs (
    backup_id    BIGSERIAL PRIMARY KEY,
    backup_type  TEXT,
    file_path    TEXT,
    status       TEXT,
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

**Role:**  
Tracks all operational errors, conflicts, pipeline state, and backup events, ensuring governance, observability, and auditability.

---

# 🔄 Pre-Data Warehouse Flow Summary

```
External Sources (API / CSV / Feeds)
        ↓
stg_raw           ← Ingestion & validation
        ↓
core_dbms         ← Normalized 5-min OHLCV
        ↓
stg_transform     ← 15-min aggregation + VWAP, error logging
        ↓
Data Warehouse    ← (analytics-ready, ML features)
        ↓
BI/ML (XGBoost)
        ↓
operation_logs    ← monitors all layers
```

---

# 🎯 Key Principles

- **Traceable:** Keep raw payloads for auditing  
- **Validated:** Ensure data integrity before aggregation  
- **Deterministic:** Transformations produce reproducible results  
- **Observability:** All errors and conflicts are logged centrally  
- **Scalable:** Supports hundreds of instruments and high-frequency data  

---

# 🏁 Summary

The pre-Data Warehouse architecture provides:

- Clean staging of raw market data  
- Authoritative storage of 5-min OHLCV bars  
- Aggregation to 15-min bars with VWAP  
- Centralized operational logging and monitoring  

It forms the foundation for analytics, feature engineering, and machine learning pipelines like XGBoost.

