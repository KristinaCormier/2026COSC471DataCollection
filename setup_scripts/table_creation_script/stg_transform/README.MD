# 🔄 Staging Transform Layer – 15-Minute Aggregated Market Data

## Overview

The `stg_transform` schema represents the **data transformation layer** of the market data pipeline.

It stores:

- Aggregated 15-minute OHLCV bars
- Volume Weighted Average Price (VWAP)
- Transformation error logs

This layer sits between:

- `core_dbms` (5-minute raw normalized data)
- Analytics / Trading / Feature Engineering layers

---

# 🏗 Architecture Position

```
Raw Ingestion (API / CSV / Feeds)
            ↓
Staging (stg_raw / raw_stg)
            ↓
Core Storage (core_dbms.market_data_5m)
            ↓
stg_transform.market_data_15min   ← THIS LAYER
            ↓
Analytics / Indicators / ML
```

This schema is responsible for **timeframe aggregation and derived metrics**.

---

# 📂 Schema Creation

```sql
CREATE SCHEMA IF NOT EXISTS stg_transform;
```

All transformation-layer objects live inside this namespace.

---

# 📊 Table: market_data_15min

```sql
CREATE TABLE stg_transform.market_data_15min (
    symbol      TEXT,
    ts          TIMESTAMPTZ,
    open        NUMERIC(18,6),
    high        NUMERIC(18,6),
    low         NUMERIC(18,6),
    close       NUMERIC(18,6),
    volume      BIGINT,
    vwap        NUMERIC(20,6),
    PRIMARY KEY (symbol, ts)
);
```

---

## 🧱 Column Definitions

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Instrument ticker |
| `ts` | TIMESTAMPTZ | 15-minute bar close timestamp |
| `open` | NUMERIC(18,6) | First trade price in 15-min window |
| `high` | NUMERIC(18,6) | Highest trade price in window |
| `low` | NUMERIC(18,6) | Lowest trade price in window |
| `close` | NUMERIC(18,6) | Last trade price in window |
| `volume` | BIGINT | Total traded volume in window |
| `vwap` | NUMERIC(20,6) | Volume-weighted average price |

---

# 📐 Aggregation Rules

15-minute bars are derived from:

```
core_dbms.market_data_5m
```

Each 15-minute bar consists of:

- 3 consecutive 5-minute intervals
- Grouped by `(symbol, 15-min bucket)`

---

## 🧮 VWAP Calculation

VWAP is computed as:

```
SUM(price * volume) / SUM(volume)
```

Where:

- `price` is typically the trade price or close price per 5-minute interval
- If `SUM(volume) = 0`, log a `divide_by_zero` error

---

# 🔐 Data Integrity

### Primary Key

```sql
PRIMARY KEY (symbol, ts)
```

Guarantees:

- No duplicate 15-minute bars
- Safe incremental upserts
- Deterministic aggregation

---

# 🚨 Transformation Error Logging

```sql
CREATE TABLE stg_transform.transform_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_type   TEXT,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

---

## Logged Error Types

| Error Type | Description |
|------------|------------|
| `divide_by_zero` | VWAP calculation failed due to zero volume |
| `missing_window` | Missing one or more 5-minute bars in aggregation window |

This ensures:

- Auditability
- Observability
- Data quality monitoring

---

# 🔁 Recommended Upsert Pattern

```sql
INSERT INTO stg_transform.market_data_15min (...)
VALUES (...)
ON CONFLICT (symbol, ts)
DO UPDATE SET
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap   = EXCLUDED.vwap;
```

---

# 📊 Performance Recommendations

For production workloads:

```sql
CREATE INDEX idx_15min_symbol_ts
ON stg_transform.market_data_15min (symbol, ts DESC);

CREATE INDEX idx_15min_ts
ON stg_transform.market_data_15min (ts DESC);
```

---

# ⏱ Time Window Expectations

For US equities:

- 4 bars per hour
- 26 bars per full trading session
- 390 minutes ÷ 15 = 26

---

# 🚀 Future Enhancements

Possible extensions:

- Add `asset_type`
- Add `source`
- Add `session_id`
- Add `is_complete_window BOOLEAN`
- Partition by month or year
- Add rolling indicators (EMA, RSI, ATR)

---

# 📌 PostgreSQL Requirements

- PostgreSQL 13+
- TIMESTAMPTZ support
- BIGINT volume
- NUMERIC precision for financial safety

---

# 🧠 Design Philosophy

The `stg_transform` layer:

- Aggregates normalized data
- Adds derived metrics (VWAP)
- Enforces transformation-level integrity
- Logs computational anomalies

It acts as a **feature-ready data preparation layer**.

---

# 🏁 Summary

`stg_transform.market_data_15min` provides:

- Clean 15-minute OHLCV bars
- Computed VWAP
- Deterministic aggregation
- Error observability
- Analytical readiness

This layer enables:

- Strategy backtesting
- Intraday analytics
- Quant feature engineering
- Real-time trading models

---

## Transform Layer – Market Data Engineering
