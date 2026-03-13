# FMP 5-Minute Market Data Collection Pipeline

A production ETL system that ingests, transforms, and warehouses 5-minute OHLCV stock data from the Financial Modeling Prep (FMP) API into PostgreSQL, with built-in deduplication, quality checks, and operational observability.

## Overview

This repository implements a three-layer data pipeline:

```
API Ingest              Staging Layer           Core Warehouse           Operations Log
───────────────────────────────────────────────────────────────────────────────────────
FMP API
  │
  ├─→ intraday_data_collection.py (scheduled)
  │   └─→ stg_raw.market_data
  │       (raw 5-min bars, deduped by symbol+ts)
  │
  └─→ gather_past_data.py (on-demand backfill)
      └─→ stg_raw.market_data
          │
          ├─→ run_scheduled_operations.py (scheduled)
          │   │
          │   └─→ export_stg_to_core.sql
          │       ├─ Ranks rows by ingest time
          │       ├─ Deduplicates on (symbol, ts)
          │       ├─ Validates OHLCV completeness
          │       └─ Upserts into core_dbms.market_data_5m
          │
          └─→ operation_logs.*
              ├─ dedup_conflicts
              ├─ data_quality_errors
              ├─ pipeline_logs (execution status)
              └─ authority_conflicts
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Configure Environment
Copy the template and set your credentials:
```bash
cp .env.template .env
# Edit .env with your FMP_API_KEY, PostgreSQL connection, and settings
```

Required environment variables:
- **API**: `FMP_API_KEY`, `SYMBOLS`, `MARKET_TZ`, `WINDOW_MINUTES`
- **Database**: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- **Runtime**: `LOG_DIR`, `MARKET_OPEN`, `MARKET_CLOSE`

See [.env.template](.env.template) for all options.

### 3. Set Up Database & Cron
```bash
# One-time setup (requires sudo and .env)
cd setup_scripts
sudo bash setup_server.sh              # Creates users, replication, backups
sudo bash setup_cronjob_daily_collector.sh    # Schedules intraday collection
sudo bash setup_cronjob_scheduled_operations.sh  # Schedules stg→core export
```

For detailed setup instructions, see [setup_scripts/README.md](setup_scripts/README.md).

### 4. Manual Testing
```bash
# Collect the latest completed interval
python src/intraday_data_collection.py

# Backfill a past date range
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07

# Transform and load to core warehouse
python src/run_scheduled_operations.py
```

## Operational Scripts

Three entry points drive the pipeline:

### `intraday_data_collection.py` — Live Collection
- **Trigger**: Scheduled via cron (default: hourly)
- **Purpose**: Fetch the latest completed 5-minute bar for each symbol and insert/upsert into `stg_raw.market_data`
- **Output**: Rows in `stg_raw.market_data` and error logs in `./logs/`
- **Usage**: `python src/intraday_data_collection.py`

### `gather_past_data.py` — Historical Backfill
- **Trigger**: Manual, on-demand
- **Purpose**: Fetch bars for a user-defined date range and upsert them into staging
- **Output**: Rows in `stg_raw.market_data` and error logs in `./logs/`
- **Usage**:
  ```bash
  python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07
  ```
- **Note**: Cannot backfill future dates; respects market hours configured in `.env`

### `run_scheduled_operations.py` — Transform & Load
- **Trigger**: Scheduled via cron (default: daily at 2 AM UTC)
- **Purpose**: Execute SQL scripts in `src/sql_operations/` in alphabetical order to move/transform data from staging to core warehouse
- **Output**: Rows in `core_dbms.market_data_5m` and audit logs in `operation_logs.*`
- **Usage**: `python src/run_scheduled_operations.py`
- **Logs**: Execution status written to `operation_logs.pipeline_logs`

For detailed usage, see [src/README.md](src/README.md).

## Data Layers

### Staging Layer (`stg_raw` schema)
Raw, unvalidated 5-minute bars from the API, ingested as-is. Single table:
- **`stg_raw.market_data`**: OHLCV rows with symbol, timestamp (ts), and raw JSON payload

**Uniqueness**: `UNIQUE(symbol, ts)` — enforces one row per symbol per interval via upsert on ingest.

### Core Data (`core_dbms` schema)
Validated, deduplicated, and quality-checked 5-minute bars ready for analysis. Single table:
- **`core_dbms.market_data_5m`**: Clean OHLCV with asset_type and source tracking

**Uniqueness**: `UNIQUE(symbol, ts)` — enforces exactly one canonical row per interval.

### Operations Log (`operation_logs` schema)
Audit trail for debugging and monitoring. Tables include:
- **`pipeline_logs`**: Script execution status (name, status, timestamp, error message)
- **`dedup_conflicts`**: Rows discarded as duplicates during de-duplication
- **`data_quality_errors`**: Rows rejected for invalid/missing OHLCV
- **`authority_conflicts`**: Source conflicts (for multi-source merging, future expansion)
- **`backup_logs`**: Backup/restore event history
- **`cast_errors`**: Type conversion failures during ingestion

See [setup_scripts/table_creation_script/operation_logs/README.MD](setup_scripts/table_creation_script/operation_logs/README.MD) for table details.

## Configuration Reference

### Symbols & Timing
- **`SYMBOLS`**: Comma-separated list of stock tickers (e.g., `AAPL,MSFT,TSLA`)
- **`MARKET_TZ`**: Timezone for market hours clamping (e.g., `America/New_York`)
- **`MARKET_OPEN`**: Market open time in HH:MM format (default: `04:00`)
- **`MARKET_CLOSE`**: Market close time in HH:MM format (default: `21:00`)
- **`WINDOW_MINUTES`**: Size of the collection window in minutes (default: `60`)

### API
- **`FMP_API_KEY`**: Financial Modeling Prep API key (required)
- **`FMP_API_URL`**: Base API endpoint (default: `https://financialmodelingprep.com/api/v3`)
- **`FMP_API_DELAY_SECONDS`**: Delay between API calls to respect rate limits (default: `0.2`)

### Database
- **`PGHOST`**: PostgreSQL server hostname
- **`PGPORT`**: PostgreSQL server port (default: `5432`)
- **`PGDATABASE`**: Database name
- **`PGUSER`**: Database user
- **`PGPASSWORD`**: Database password
- **`TEST_DATABASE_URL`**: Separate test database URL for pytest (optional)

### Runtime
- **`LOG_DIR`**: Directory where error/execution logs are written (default: `./logs`)
- **`LOG_LEVEL`**: Logging verbosity (default: `INFO`)
- **`COLLECTION_SCHEDULE`**: Cron schedule for intraday collector (default: `0 * * * *` = hourly)
- **`STG_TO_CORE_SCHEDULE`**: Cron schedule for transform job (default: `0 2 * * *` = 2 AM daily)

## Common Issues

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `401 Unauthorized` | API key missing or invalid | Set `FMP_API_KEY` in `.env` |
| `no matching row in table` | Test database not initialized | Run pytest setup or initialize manually |
| `UNIQUE constraint violation` | Attempted duplicate insert outside upsert | Check caller is using ORM with `on_conflict_do_update` |
| `permission denied on sequence` | Database role lacks privileges | Grant sequence privileges to user in PostgreSQL |
| `Log directory does not exist` | Setup script not run | Execute `sudo bash setup_scripts/setup_cronjob_*.sh` |

## Documentation Map

- **[src/README.md](src/README.md)**: Runnable scripts, supporting modules, and design patterns
- **[src/sql_operations/README.md](src/sql_operations/README.md)**: SQL script execution order and side effects
- **[setup_scripts/README.md](setup_scripts/README.md)**: Server setup, replication, backup, and cron installation
- **[setup_scripts/table_creation_script/](setup_scripts/table_creation_script/)**: Schema definitions and table designs
- **[tests/README.md](tests/README.md)**: Test organization, fixtures, and coverage reporting
- **[.env.template](.env.template)**: Environment variable reference

## Testing

Run the full test suite:
```bash
pytest -v
```

With coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

See [tests/README.md](tests/README.md) for more options and fixture documentation.

## Architecture & Design Notes

- **Idempotency**: All three entry points are safe to run multiple times; they upsert rather than insert
- **Alphabetical SQL execution**: `run_scheduled_operations.py` runs `.sql` files in `src/sql_operations/` in alphabetical order; file naming matters
- **No external broker**: Execution is simple cron + database; no message queue or event system
- **Observability**: All execution is logged to `operation_logs.pipeline_logs` and file-based error CSVs in `./logs/`

## Contributing

Before submitting a PR:
1. Run `pytest` locally and ensure all tests pass
2. Update documentation if you change schema, environment variables, or operational behavior
3. Add tests for new data validation or SQL transformation logic
4. Keep error logging consistent with [src/utils/logging_utils.py](src/utils/logging_utils.py)

See [.github/pull_request_template.md](.github/pull_request_template.md) for the required PR checklist.