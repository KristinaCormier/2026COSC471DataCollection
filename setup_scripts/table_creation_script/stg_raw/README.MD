# 📊 Market Data Staging Layer (PostgreSQL)

## Overview

This project creates a **PostgreSQL staging layer** for market data ingestion and validation.

It:

1. Creates a `stg_raw` schema  
2. Dynamically creates **500+ ticker-specific tables**
3. Logs ingestion errors
4. Builds validated views from raw staging tables

The architecture separates:

- `raw_stg` → raw ingested data (source of truth)
- `stg_raw` → structured staging + validation layer

---

# 🏗 Architecture

```
                +-------------------+
                |   Data Sources    |
                |  (API, CSV, FMP)  |
                +---------+---------+
                          |
                          v
                +-------------------+
                |     raw_stg.*     |
                |  500+ tables      |
                +---------+---------+
                          |
                          v
                +-------------------+
                |  stg_raw.valid_*  |
                |  Cleaned Views    |
                +---------+---------+
                          |
                          v
                Analytics / Mart Layer
```

---

# 📂 Schema Structure

## 1️⃣ `stg_raw` Schema

Created with:

```sql
CREATE SCHEMA IF NOT EXISTS stg_raw;
```

Contains:

- 500+ ticker tables
- `ingest_errors`
- validation views

---

# 📈 Ticker Tables

A table is created for each ticker symbol.

Example:

```
stg_raw.aapl
stg_raw.msft
stg_raw.tsla
stg_raw.spy
...
```

### Table Structure

```sql
stock_id     BIGSERIAL PRIMARY KEY
date         TIMESTAMP
open         NUMERIC(18,6)
low          NUMERIC(18,6)
high         NUMERIC(18,6)
close        NUMERIC(18,6)
volume       NUMERIC(20,4)
asset_type   TEXT        -- stock, bond, commodity, index
source       TEXT        -- FMP_intraday, FMP_hist, CSV, OtherAPI
ingest_time  TIMESTAMPTZ DEFAULT now()
raw_payload  JSONB       -- exact source row
```

---

# 🧾 Asset Coverage

Includes:

- S&P 500 equities
- ETFs
- Bonds (`IRX`, `FVX`, `TNX`)
- Indices (`DJI`, `IXIC`, `GSPC`)
- Commodities (`GCF`, `CLF`)

Total: **500+ instrument tables**

---

# 🚨 Ingestion Error Logging

Error table:

```sql
CREATE TABLE stg_raw.ingest_errors (
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
```

Used to capture:

- Parse failures
- Schema mismatches
- Missing required fields
- Invalid data formats

---

# ✅ Data Validation Layer

For every table in `raw_stg`, a validated view is created:

Example:

```
stg_raw.valid_aapl
stg_raw.valid_msft
```

Each view applies:

```sql
WHERE date IS NOT NULL
  AND volume IS NOT NULL
  AND volume >= 0
```

This ensures:

- No missing timestamps
- No null volume
- No negative volume

These views are safe for analytics consumption.

---

# 🔁 Dynamic Table Creation

Tables are generated using a PL/pgSQL loop over a ticker array.

Benefits:

- No manual table creation
- Easy to add/remove symbols
- Idempotent (`CREATE IF NOT EXISTS`)

---

# ⚙ Execution Order

Run scripts in this order:

1. Create `stg_raw` schema
2. Execute ticker table creation block
3. Create `ingest_errors` table
4. Run validation view generation block

---

# 📊 Design Notes

## Why 1 Table per Ticker?

Pros:
- Simple ingestion logic
- Isolation between symbols
- Easy per-symbol truncation/reload

Cons:
- Large number of tables
- Harder cross-symbol queries
- Not ideal for high-scale analytics

---

# 🚀 Recommended Future Improvements

For better scalability:

### Option A — Unified Table
Use a single table:

```sql
stg_raw.market_data
    symbol TEXT
    date
    open
    ...
```

Partitioned by:

- `symbol`
- `date`

### Option B — Declarative Partitioning
Postgres native partitioning by:

```sql
PARTITION BY LIST (symbol)
```

Benefits:
- Cleaner architecture
- Faster cross-symbol queries
- Better indexing strategy

---

# 🔐 Data Quality Strategy

Validation rules applied:

- Non-null timestamp
- Non-null volume
- Non-negative volume

Future enhancements could include:

- Duplicate detection
- Price sanity checks
- Gap detection
- Outlier detection
- Source reconciliation

---

# 📌 PostgreSQL Requirements

Recommended:

- PostgreSQL 13+
- JSONB support
- PL/pgSQL enabled (default)

---

# 🏁 Summary

This project builds a structured staging layer for:

- Multi-asset ingestion
- Data validation
- Error tracking
- Clean analytical handoff

It is designed for:

- Quant research
- Trading systems
- Market data warehousing
- Financial analytics pipelines

---

## Author

Market Data Engineering Layer  
PostgreSQL-Based Financial Staging Architecture
