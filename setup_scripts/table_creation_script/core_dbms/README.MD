# 📈 Core DBMS – 5-Minute Market Data Table

## Overview

This module defines the **core market data table** for 5-minute interval bars.

It is designed to serve as the **central, normalized time-series store** for:

- Equities
- Bonds
- Indices
- Commodities
- ETFs

The table enforces uniqueness, ensures time-series integrity, and supports analytical workloads.

---

# 🏗 Architecture Layer

```
Raw Ingestion (APIs, CSV, Feeds)
            ↓
Raw Staging Layer (stg_raw)
            ↓
Validation Layer (views)
            ↓
core_dbms.market_data_5m   ← THIS TABLE
            ↓
Transformation Staging Layer (stg_transformation)
            ↓
Data Warehouse
            ↓
Analytics / Trading / BI
```

This table represents the **authoritative structured dataset**.

---

# 📂 Schema Creation

```sql
CREATE SCHEMA IF NOT EXISTS core_dbms;
```

All core analytical tables live inside `core_dbms`.

---

# 📊 Table Definition

```sql
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

---

# 🧱 Column Definitions

| Column | Type | Description |
|--------|------|-------------|
| `market_data_id` | BIGSERIAL | Surrogate primary key |
| `symbol` | TEXT | Instrument ticker (AAPL, TSLA, SPY, etc.) |
| `ts` | TIMESTAMPTZ | 5-minute interval timestamp (UTC recommended) |
| `open` | NUMERIC(18,6) | Opening price |
| `high` | NUMERIC(18,6) | Highest price in interval |
| `low` | NUMERIC(18,6) | Lowest price in interval |
| `close` | NUMERIC(18,6) | Closing price |
| `volume` | BIGINT | Discrete traded shares/contracts |
| `asset_type` | TEXT | stock, bond, commodity, index |
| `source` | TEXT | Data provider identifier |
| `created_at` | TIMESTAMPTZ | Record insert timestamp |

---

# 🔐 Data Integrity Rules

## 1️⃣ Primary Key
Ensures row identity.

```
market_data_id BIGSERIAL PRIMARY KEY
```

## 2️⃣ Uniqueness Constraint

```
UNIQUE (symbol, ts)
```

Guarantees:

- No duplicate 5-minute bars
- Idempotent ingestion
- Safe upserts

---

# ⏱ Timeframe Specification

- Interval: **5 minutes**
- Expected cadence: 12 bars per hour
- 78 bars per US trading session (equities)
- Timestamp should represent **bar close time**

Example:

| Symbol | ts |
|--------|----|
| AAPL | 2025-01-10 09:35:00+00 |
| AAPL | 2025-01-10 09:40:00+00 |

---

# 📈 Supported Assets

- Equities
- ETFs
- Bonds
- Commodities
- Indices
- Crypto (optional extension)

---

# 🚀 Recommended Indexing (Production)

For performance at scale:

```sql
CREATE INDEX idx_market_data_symbol_ts
ON core_dbms.market_data_5m (symbol, ts DESC);

CREATE INDEX idx_market_data_ts
ON core_dbms.market_data_5m (ts DESC);
```

---

# 🔁 Recommended Upsert Pattern

```sql
INSERT INTO core_dbms.market_data_5m (...)
VALUES (...)
ON CONFLICT (symbol, ts)
DO UPDATE SET
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source;
```

This enables safe incremental ingestion.

---

# 📊 Scalability Considerations

For high-frequency or multi-year datasets:

### Option A — Partition by Date

```sql
PARTITION BY RANGE (ts);
```

### Option B — Partition by Symbol

```sql
PARTITION BY LIST (symbol);
```

### Option C — Hybrid (Large Scale)

- Partition by month
- Index on symbol

---

# 📏 Storage Considerations

- `NUMERIC(18,6)` ensures precision for financial calculations
- `BIGINT` for volume prevents overflow
- `TIMESTAMPTZ` ensures timezone safety
- `UNIQUE (symbol, ts)` prevents duplicates

---

# 📌 PostgreSQL Requirements

- PostgreSQL 13+
- JSON not required
- PL/pgSQL not required
- Recommended: autovacuum tuning for time-series workloads

---

# 🧠 Design Philosophy

This table is designed to:

- Be the **single source of truth**
- Eliminate per-symbol table sprawl
- Support analytics, trading systems, and ML
- Provide clean time-series consistency

---

# 🏁 Summary

`core_dbms.market_data_5m` is the centralized, normalized,
production-ready 5-minute OHLCV market data store.

It provides:

- Strong data integrity
- Duplicate prevention
- Analytical performance
- Clean architectural separation

---

## Core Data Layer – Financial Time-Series DBMS

