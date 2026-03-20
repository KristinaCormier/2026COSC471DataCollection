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
          │   ├─→ Python pipeline steps
          │   │   ├─ Ranks rows by ingest time
          │   │   ├─ Deduplicates on (symbol, ts)
          │   │   ├─ Validates OHLCV completeness
          │   │   └─ Upserts into core_dbms.market_data_5m
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
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -r requirements.txt
```

### 2. Configure Environment
Copy the template and set your credentials:
```bash
cp .env.template .env
# Edit .env with your FMP_API_KEY, PostgreSQL connection, and settings
```

Required environment variables:
- **API**: `FMP_API_KEY`, `SYMBOLS`, `MARKET_TZ`
- **Database**: `DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **Runtime**: `LOG_DIR`, `MARKET_OPEN`, `MARKET_CLOSE`

Breaking change: runtime `PG*` connection keys were removed in favor of `DB_*` keys.

See [.env.template](.env.template) for all options.

### 3. Setup Server 

```bash
cd setup_scripts/server_setup
sudo bash setup_server.sh 
```

For detailed setup instructions, see [setup_scripts/README.md](setup_scripts/README.md).
and [One-Time Server Setup (`setup_server.sh`)](setup_scripts/server_setup/setup_server.sh)

### 4. Initialize Database Schema
```bash
# Navigate back to the project root
cd ../..
python -m alembic upgrade head

# Convenience initializer for disposable local/test databases
python -c "from dotenv import load_dotenv; from src.model.orm_db import build_postgres_url, get_engine, init_db; import os; load_dotenv(); init_db(get_engine(os.getenv('DB_HOST', 'localhost'), int(os.getenv('DB_PORT', '5432')), os.getenv('DB_NAME', 'market_data'), os.getenv('DB_USER', 'user'), os.getenv('DB_PASSWORD', 'password')))"
```

### 5.  Cron Setup
Server cron jobs require privileged access.

```bash
# Local/user cron install (default mode)
sudo bash setup_scripts/setup_cronjob_daily_collector.sh
sudo bash setup_scripts/setup_cronjob_scheduled_operations.sh
```

### 6. Manual Testing
```bash
pytest tests/unit/ -v
# Collect the latest completed interval
python src/intraday_data_collection.py

# Backfill a past date range
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07

# Transform and load to the core schema
python src/run_scheduled_operations.py
```

## Operational Scripts

Three entry points drive the pipeline:

### `intraday_data_collection.py` — Live Collection
- **Trigger**: Scheduled via cron (default: hourly)
- **Purpose**: Fetch completed 5-minute bars from market open through the latest completed interval for each symbol and insert until first existing `(symbol, ts)` conflict
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
- **Purpose**: Execute Python pipeline steps to deduplicate staging data, log quality issues, upsert into core, and clean staging after a successful export
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

See `src/model/models.py` and `alembic/versions/20260312_0001_baseline_schema.py` for the canonical table definitions.

## Configuration Reference

### Symbols & Timing
- **`SYMBOLS`**: Comma-separated list of stock tickers (e.g., `AAPL,MSFT,TSLA`)
- **`MARKET_TZ`**: Timezone for market hours clamping (e.g., `America/New_York`)
- **`MARKET_OPEN`**: Market open time in HH:MM format (default: `04:00`)
- **`MARKET_CLOSE`**: Market close time in HH:MM format (default: `21:00`)

### API
- **`FMP_API_KEY`**: Financial Modeling Prep API key (required)
- **`FMP_API_URL`**: Base API endpoint (default: `https://financialmodelingprep.com/api/v3`)
- **`FMP_API_DELAY_SECONDS`**: Delay between API calls to respect rate limits (default: `0.2`)

### Database
- **`DATABASE_URL`**: Full SQLAlchemy-compatible database URL (optional, takes precedence over component keys)
- **`DB_HOST`**: Database server hostname
- **`DB_PORT`**: Database server port (default: `5432`)
- **`DB_NAME`**: Database name
- **`DB_USER`**: Database user
- **`DB_PASSWORD`**: Database password
- **`TEST_DATABASE_URL`**: Test DB mode selector for pytest.
  - Empty/unset: DB-backed tests run against ephemeral testcontainers PostgreSQL.
  - Set: DB-backed tests run against this external/non-containerized test database.

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
| `Log directory is not writable` | `LOG_DIR` points to a protected location | Set `LOG_DIR` to a writable project-local path such as `./logs` |

## Documentation Map

- **[src/README.md](src/README.md)**: Runnable scripts, supporting modules, and design patterns
- **[src/utils/scheduled_pipeline.py](src/utils/scheduled_pipeline.py)**: Python export and staging cleanup logic used by the scheduled runner
- **[setup_scripts/README.md](setup_scripts/README.md)**: Server setup, replication, backup, and cron installation
- **[alembic/versions/](alembic/versions/)**: Migration history and canonical schema evolution
- **[tests/README.md](tests/README.md)**: Test organization, fixtures, and coverage reporting
- **[.env.template](.env.template)**: Environment variable reference

## Testing

Run the full test suite:
```bash
pytest -v
```

Run by phase:
```bash
# Phase 1 (unit, DB-free)
pytest tests/unit/ -m "not postgres_only" -v

# Phase 2 (integration, PostgreSQL required)
pytest tests/integration/ -m integration -v

# Phase 3 (pipeline, PostgreSQL required)
pytest tests/pipeline/ -m pipeline -v
```

For local DB-backed testing, `tests/conftest.py` checks only `TEST_DATABASE_URL`:
- `TEST_DATABASE_URL` empty/unset -> ephemeral PostgreSQL via `testcontainers` (default)
- `TEST_DATABASE_URL` set -> external/non-containerized test database

With coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

CI uses phase-aware jobs in `.github/workflows/pytest.yml`:
- Phase 1 unit checks are required on PR and merge queue.
- Phase 2 integration and Phase 3 pipeline checks are informational on PR/push and required on merge queue.

See [tests/README.md](tests/README.md) for more options and fixture documentation.

## Architecture & Design Notes

- **Idempotency**: All three entry points are safe to run multiple times; they upsert rather than insert
- **Dependency-aware pipeline execution**: `run_scheduled_operations.py` runs fixed Python steps in order and skips cleanup when export fails
- **No external broker**: Execution is simple cron + database; no message queue or event system
- **Observability**: All execution is logged to `operation_logs.pipeline_logs` and file-based error CSVs in `./logs/`

## Contributing

Before submitting a PR:
1. Run `pytest` locally and ensure all tests pass
2. Update documentation if you change schema, environment variables, or operational behavior
3. Add tests for new data validation or Python pipeline transformation logic
4. Keep error logging consistent with [src/utils/logging_utils.py](src/utils/logging_utils.py)

See [.github/pull_request_template.md](.github/pull_request_template.md) for the required PR checklist.