📘 stg_raw Schema – Raw Market Data Staging Layer
Overview

The stg_raw schema serves as the initial landing zone for all incoming financial market data within the DRAC-based ELT pipeline.

This layer is designed to:

Preserve source fidelity

Allow duplicate records

Store unvalidated raw payloads

Capture ingestion errors separately

Maintain full auditability and reproducibility

It acts as the immutable foundation of the financial data pipeline before transformation into structured core and warehouse layers.

🏗 Architecture Context
Source APIs / CSV
        ↓
stg_raw (landing zone)
        ↓
core_dbms (validated & deduplicated)
        ↓
stg_transform (aggregation & feature prep)
        ↓
Data Warehouse (Star Schema)
        ↓
XGBoost Training

The stg_raw schema intentionally performs no transformations or constraints beyond basic storage.

📊 Table: stg_raw.market_data
Purpose

Stores raw OHLCV financial market data exactly as received from external sources.

This table:

Allows duplicates

Does not enforce NOT NULL constraints (except PK)

Preserves original source row via raw_payload

Tracks ingestion metadata

Structure
Column	Type	Description
ingest_id	BIGSERIAL (PK)	Unique ingestion record ID
symbol	TEXT	Asset ticker symbol
ts	TIMESTAMPTZ	Market timestamp (source-provided)
open	NUMERIC(18,6)	Opening price
high	NUMERIC(18,6)	High price
low	NUMERIC(18,6)	Low price
close	NUMERIC(18,6)	Closing price
volume	NUMERIC(20,4)	Trade volume (allows decimals for some assets)
asset_type	TEXT	stock, bond, commodity, index
source	TEXT	Data origin (FMP_intraday, CSV, etc.)
ingest_time	TIMESTAMPTZ	System ingestion timestamp
raw_payload	JSONB	Exact source record
Design Principles
1️⃣ Immutable Raw Layer

No updates or deletes should occur in this table except for administrative cleanup.

2️⃣ Source Preservation

raw_payload ensures:

Full reproducibility

Traceability of transformations

Ability to reprocess data if logic changes

3️⃣ Duplicate Tolerance

Duplicates are intentionally allowed at this stage. Deduplication occurs in downstream layers.

4️⃣ Minimal Constraints

Validation is deferred to the core_dbms layer.

🚨 Table: stg_raw.ingest_errors
Purpose

Captures ingestion failures without interrupting the primary pipeline.

This ensures:

Fault tolerance

Error auditing

Data quality monitoring

Structure
Column	        Type	        Description
error_id	    BIGSERIAL (PK)	Unique error record ID
ingest_id	    BIGINT	        Related market_data ingest ID (if applicable)
symbol	        TEXT	        Asset symbol
ts	            TIMESTAMPTZ	    Timestamp involved in error
asset_type	    TEXT	        Asset classification
source	        TEXT	        Data origin
error_type	    TEXT	        parse_error, schema_mismatch, missing_field
error_detail	TEXT	        Detailed error message
raw_payload	    JSONB	        Original problematic record
log_time	    TIMESTAMPTZ	    Error logging timestamp

Error Handling Strategy

Errors are logged instead of rejected outright to:

Prevent pipeline disruption

Enable post-ingestion validation

Support observability and data quality metrics

🔍 Data Governance & Auditability

This schema guarantees:

Complete ingestion traceability

Time-based audit of when data entered system

Ability to reconstruct training datasets

Compliance with reproducibility standards for ML research

📈 Performance Considerations

Recommended indexing (if scaling):

CREATE INDEX idx_stg_raw_symbol_ts
ON stg_raw.market_data(symbol, ts);

CREATE INDEX idx_stg_raw_ingest_time
ON stg_raw.market_data(ingest_time);

Partitioning by ts should be considered when volume exceeds ~50M rows.

🧠 Role in Financial Time-Series Pipeline

The stg_raw layer is intentionally:

Non-normalized

Non-validated

Non-deduplicated

It provides the foundation for:

OHLCV validation

Deduplication logic

5-minute to 15-minute aggregation

Feature engineering

Star-schema warehouse loading

Distributed XGBoost training

🔐 Operational Guidelines

✔ Insert-only pattern
✔ Never transform in-place
✔ Always preserve raw_payload
✔ Log ingestion anomalies
✔ Downstream layers enforce quality rules

📌 Summary

The stg_raw schema functions as the immutable, auditable entry point for high-frequency financial market data within the scalable ELT pipeline. It prioritizes source fidelity, fault tolerance, and reproducibility before structured transformation and predictive modeling.