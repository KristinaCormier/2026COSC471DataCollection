# 🛠 operation_logs Schema – Operational Telemetry & Governance Layer

## Overview

The `operation_logs` schema centralizes operational logging across the entire market data pipeline.

It captures:

- Data quality violations
- Deduplication conflicts
- Type casting failures
- Source authority conflicts
- Upsert failures
- Concurrency issues
- Pipeline watermark tracking
- Backup execution logs

This schema transforms the database into an **observable, auditable system**.

---

# 🏗 Architecture Position

```
Ingestion Layer
      ↓
Staging Layer
      ↓
Core DBMS
      ↓
Transform Layer
      ↓
operation_logs  ← Operational Observability
```

All layers write operational events into this schema.

---

# 📂 Schema Creation

```sql
CREATE SCHEMA IF NOT EXISTS operation_logs;
```

---

# 🚨 1️⃣ Data Quality Errors

Logs validation and constraint issues.

```sql
CREATE TABLE operation_logs.data_quality_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_type   TEXT,    -- constraint_violation, fractional_volume
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

### Example Error Types

- `constraint_violation`
- `fractional_volume`
- `dedup_conflicts`
- `cast_errors`
- `authority_conflicts`
- `upsert_failures`
- `concurrency_issues`

---

# 🔁 2️⃣ Deduplication Conflicts

Triggered when duplicate `(symbol, ts)` records contain differing values.

```sql
CREATE TABLE operation_logs.dedup_conflicts (
    conflict_id  BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    existing_row JSONB,
    incoming_row JSONB,
    resolution   TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

---

# 🔄 3️⃣ Authority Conflicts

Logged when two data sources disagree.

```sql
CREATE TABLE operation_logs.authority_conflicts (
    conflict_id      BIGSERIAL PRIMARY KEY,
    symbol           TEXT,
    ts               TIMESTAMPTZ,
    source_a         TEXT,
    source_b         TEXT,
    preferred_source TEXT,
    log_time         TIMESTAMPTZ DEFAULT now()
);
```

---

# 🧮 4️⃣ Cast Errors

Logs failed type conversions during ingestion or transformation.

```sql
CREATE TABLE operation_logs.cast_errors (
    error_id     BIGSERIAL PRIMARY KEY,
    symbol       TEXT,
    column_name  TEXT,
    raw_value    TEXT,
    target_type  TEXT,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

---

# 🔁 5️⃣ Upsert Failures

Captures failed inserts or updates.

```sql
CREATE TABLE operation_logs.upsert_failures (
    failure_id   BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    symbol       TEXT,
    ts           TIMESTAMPTZ,
    error_detail TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

---

# ⚙ 6️⃣ Concurrency Issues

Tracks lock conflicts and deadlocks.

```sql
CREATE TABLE operation_logs.concurrency_issues (
    issue_id     BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    lock_type    TEXT,
    pid          INTEGER,
    details      TEXT,
    log_time     TIMESTAMPTZ DEFAULT now()
);
```

---

# ⏱ 7️⃣ Pipeline Watermarks

Tracks ingestion and transformation progress.

```sql
CREATE TABLE operation_logs.pipeline_watermarks (
    pipeline_name      TEXT PRIMARY KEY,
    last_processed_ts  TIMESTAMPTZ,
    status             TEXT,
    updated_at         TIMESTAMPTZ DEFAULT now()
);
```

Used to:

- Resume pipelines safely
- Detect data lag
- Monitor pipeline health

---

# 💾 8️⃣ Backup Logs

Tracks backup execution events.

```sql
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

---

# 📊 Recommended Indexing

```sql
CREATE INDEX idx_dq_symbol_ts
ON operation_logs.data_quality_errors (symbol, ts);

CREATE INDEX idx_upsert_symbol_ts
ON operation_logs.upsert_failures (symbol, ts);

CREATE INDEX idx_watermark_status
ON operation_logs.pipeline_watermarks (status);
```

---

# 📦 Retention Strategy (Recommended)

Operational logs grow quickly.

Best practice:

- Partition by `log_time`
- Retain 6–12 months online
- Archive older partitions
- Vacuum regularly

Example partition strategy:

```sql
PARTITION BY RANGE (log_time);
```

---

# 🎯 Design Goals

This schema provides:

- Full audit trail
- Deterministic error classification
- Source conflict resolution tracking
- Pipeline state visibility
- Production debugging support
- Compliance readiness

---

# 🏁 Summary

The `operation_logs` schema enables:

✔ Centralized operational telemetry  
✔ Governance and auditability  
✔ Conflict traceability  
✔ Reliable production observability  
✔ Scalable log management  

It turns your database into a monitored, enterprise-ready data platform.

---

## Operational Observability Layer – Market Data System
