# Source Scripts & Modules

This directory contains the three operational entry points that drive the data pipeline, plus supporting modules for database access, validation, and logging.

## Operational Entry Points

These are the main scripts that the system calls. All three are safe to run repeatedly; they upsert rather than insert.

### `intraday_data_collection.py`

**Purpose**: Fetch completed 5-minute bars from market open through the latest completed interval for each symbol and insert into staging until first existing `(symbol, ts)` conflict.

**Trigger**: Scheduled via cron (default: hourly, see `COLLECTION_SCHEDULE` in `.env`)

**What It Does**:
1. Loads configuration from environment variables
2. Calculates a market-session window from `MARKET_OPEN` to the latest completed 5-minute interval
3. Clamps the window to `MARKET_OPEN` and `MARKET_CLOSE` time
4. Fetches intraday bars from FMP API for each symbol
5. Filters bars to the market-session window and processes newest bars first
6. Validates each bar for completeness (symbol, ts, OHLC, volume)
7. Inserts rows into `stg_raw.market_data` with `ON CONFLICT DO NOTHING` and stops each symbol at first existing `(symbol, ts)` row
8. Logs API errors, validation errors, and database errors to CSV files in `LOG_DIR`

**Usage**:
```bash
python src/intraday_data_collection.py
```

**Environment Variables** (Required):
- `FMP_API_KEY`: API key for Financial Modeling Prep
- `SYMBOLS`: Comma-separated list of stock tickers (e.g., `AAPL,MSFT,TSLA`)
- `MARKET_TZ`: Timezone for market hours (default: `America/New_York`)
- `MARKET_OPEN`: Market open time in HH:MM format (default: `04:00`)
- `MARKET_CLOSE`: Market close time in HH:MM format (default: `21:00`)
- `FMP_API_URL`: Base API URL (default: `https://financialmodelingprep.com/api/v3`)
- `FMP_API_DELAY_SECONDS`: Delay between API calls (default: `0.2`)

**Environment Variables** (Database):
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `LOG_DIR`: Directory for error logs (default: `./logs`)

**Output**:
- Rows in `stg_raw.market_data`
- Error CSVs in `LOG_DIR/`:
  - `fetch_data_log.csv` — API failures
  - `data_error_log.csv` — Validation failures
  - `db_insert_errors.csv` — Database operation failures

### `gather_past_data.py`

**Purpose**: Backfill historical 5-minute bars for a user-defined date range.

**Trigger**: Manual execution (one-time backfills, recovery from outages)

**What It Does**:
1. Parses `--from-date` and `--to-date` CLI arguments in YYYY-MM-DD format
2. Validates that both dates are in the past and that `from-date <= to-date`
3. Loads configuration from environment variables
4. Optionally overrides the `SYMBOLS` from `.env` with `--symbols` CLI argument
5. Fetches all 5-minute bars from FMP API for the date range
6. Filters rows to the day range (inclusive start, inclusive end at 23:59:59)
7. Validates each bar for completeness
8. Upserts rows into `stg_raw.market_data` with same logic as intraday collector
9. Logs errors to CSV files in `LOG_DIR`

**Usage**:
```bash
# Backfill from Feb 1 to Feb 7, 2026
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07

# Override symbols
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07 --symbols AAPL,MSFT,TSLA
```

**Arguments**:
- `--from-date YYYY-MM-DD`: Inclusive start date (required)
- `--to-date YYYY-MM-DD`: Inclusive end date (required)
- `--symbols TICKER,TICKER,...`: Override `SYMBOLS` from `.env` (optional)

**Constraints**:
- Both dates must be in the past (relative to `MARKET_TZ`)
- Cannot backfill futures data
- Respects `MARKET_OPEN` and `MARKET_CLOSE` for time clamping within each day

**Output**: Same as intraday collector (rows in staging, error CSVs)

### `run_scheduled_operations.py`

**Purpose**: Execute Python transformation steps that move and reconcile data from staging to the core warehouse.

**Trigger**: Scheduled via cron (default: 2 AM UTC daily, see `STG_TO_CORE_SCHEDULE` in `.env`)

**What It Does**:
1. Loads a fixed execution plan:
  - `export_stg_to_core`
  - `truncate_stg_raw` (runs only if export succeeds)
2. For each planned step:
   - Opens a SQLAlchemy session
   - Executes Python/ORM transformation logic in a single transaction
   - Logs the execution (status, duration, error) to `operation_logs.pipeline_logs`
   - Commits on success or rolls back on failure
3. Logs and skips dependent steps when prerequisites fail
4. Creates `LOG_DIR` automatically when needed and writes execution summary to `LOG_DIR/scheduled_operations.log`

**Usage**:
```bash
python src/run_scheduled_operations.py
```

**Environment Variables** (Required):
- `DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `LOG_DIR`: Directory for execution logs (default: `./logs`)

**Output**:
- Rows in `core_dbms.market_data_5m`
- Audit logs in `operation_logs.pipeline_logs`
- Rows in `operation_logs.*` error tables (duplicates, quality errors)
- Execution log: `LOG_DIR/scheduled_operations.log`

**Critical Detail**: The pipeline executes in a **fixed, dependency-aware order**:
1. `export_stg_to_core` — Dedup, validate, and export to core warehouse
2. `truncate_stg_raw` — Clean up staging, but only after successful export

## Supporting Modules

You don't typically run these directly; they are imported by the entry-point scripts.

### `model/` Package

**Purpose**: Define ORM models and database setup.

**Files**:
- **`models.py`**: SQLAlchemy ORM models for all schema layers
  - `MarketData` → `stg_raw.market_data` (staging, raw ingest)
  - `MarketData5m` → `core_dbms.market_data_5m` (core warehouse)
  - `IngestError` → `stg_raw.ingest_errors` (API/validation errors)
  - `AuthorityConflict` → `operation_logs.authority_conflicts` (source conflicts, future use)
  - `BackupLog` → `operation_logs.backup_logs` (backup history)
  - `CastError` → `operation_logs.cast_errors` (type conversion failures)
  - Plus tables for: `DeduplicationConflict`, `DataQualityError`, `PipelineLog`

- **`orm_db.py`**: Database connection and schema initialization
  - `build_postgres_url()` — Constructs PostgreSQL connection string
  - `get_engine()` — Creates SQLAlchemy engine
  - `get_session_factory()` — Creates ORM session factory
  - `init_db()` — Creates all schemas and tables if they don't exist

### `utils/` Package

Shared utility modules imported by entry-point scripts.

**Files**:
- **`collector_shared.py`**: Core collection and insertion logic
  - `_fetch_api_data()` — Call FMP API with rate limiting
  - `_process_data_batch()` — Parse and validate rows
  - `_insert_batch()` — Upsert rows into staging via ORM
  - `_validate_and_parse_row()` — Individual row validation
  - Constants: `STAGING_TABLE_NAME`, `FMP_API_URL`

- **`db_utils.py`**: Database utilities (legacy psycopg helpers and ORM wrappers)
  - Table existence checks
  - Safe statement builders for psycopg
  - Currently deprioritized in favor of ORM in `collector_shared`

- **`data_validation.py`**: Data quality checks
  - `is_row_complete()` — Check if all OHLCV fields are present
  - `is_field_empty()` — Check if a field is null or empty

- **`logging_utils.py`**: Error logging helpers
  - `log_api_error()` — Log API call failures
  - `log_data_error()` — Log validation failures
  - `log_db_error()` — Log database operation failures
  - Writes to CSV files in `LOG_DIR/`

- **`scheduled_pipeline.py`**: Scheduled transform/load operations
  - `export_staging_to_core()` — Deduplicate, validate, log issues, and upsert into `core_dbms.market_data_5m`
  - `clear_staging_tables()` — Remove processed rows from `stg_raw` after successful export
  - Pure helper functions that keep the transform rules unit-testable without a live database

- **`time_utils.py`**: Time parsing and window calculation
  - `parse_hhmm()` — Parse "HH:MM" format
  - `compute_window()` — Calculate 5-minute collection window from current time
  - `ymd()` — Format date as "YYYY-MM-DD"
  - Timezone-aware all the way

## Design Patterns & Standards

### Upsert Strategy

All insertion uses PostgreSQL's `ON CONFLICT DO UPDATE` clause with `index_elements` to handle duplicates portably:

```python
# In ORM (sqlalchemy):
stmt = insert(MarketData).values(...).on_conflict_do_update(
    index_elements=['symbol', 'ts'],
    set_={'close': ..., 'updated_at': ...}
)
```

This avoids constraint-name drift and works across different PostgreSQL environments.

### Error Logging

Three separate CSV streams for operational visibility:
- **API errors**: Network failures, 401, rate limits
- **Validation errors**: Incomplete OHLCV, missing fields
- **DB errors**: Connection failures, transaction rollbacks

Each row includes: symbol, timestamp, error type, error message, raw payload.

### Timezone Handling

All times are timezone-aware. Market hours (`MARKET_OPEN`, `MARKET_CLOSE`) and time windows are in `MARKET_TZ`. Database timestamps use UTC.

## Running from the Repository Root

Always run scripts from the repository root so imports and relative paths resolve correctly:

```bash
# ✓ Correct
python src/intraday_data_collection.py

# ✗ Wrong
cd src
python intraday_data_collection.py
```

## See Also

- [README.md](../README.md) — Project overview and data flow
- [utils/scheduled_pipeline.py](utils/scheduled_pipeline.py) — Python transform/load details
- [../setup_scripts/README.md](../setup_scripts/README.md) — Cron and server setup
- [../tests/README.md](../tests/README.md) — Testing and CI
