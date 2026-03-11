# Source Scripts Guide

This directory contains the Python scripts and helper modules used by the data collection and staging pipeline.

## How The Pipeline Is Used

1. `intraday_data_collection.py` runs on a schedule (typically cron) to ingest the latest 5-minute bars into `stg_raw.market_data`.
2. `gather_past_data.py` is run on-demand to backfill historical date ranges into `stg_raw.market_data`.
3. `run_scheduled_operations.py` runs SQL operations in `src/sql_operations/` to move/transform data from staging.

## Runnable Scripts

### `intraday_data_collection.py`
Purpose: Collect the most recent completed 5-minute market interval for configured symbols and upsert into staging.

Typical usage:
```bash
python src/intraday_data_collection.py
```

How we use it:
- Scheduled via cron for continuous ingestion during market hours.
- Reads configuration from environment variables.
- Applies market-hour clamping using `MARKET_OPEN`, `MARKET_CLOSE`, and `MARKET_TZ`.

Key environment variables:
- `FMP_API_KEY`
- `SYMBOLS`
- `MARKET_TZ`
- `WINDOW_MINUTES`
- `MARKET_OPEN`
- `MARKET_CLOSE`
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

### `gather_past_data.py`
Purpose: Backfill historical 5-minute bars for a user-defined date range and upsert into staging.

Typical usage:
```bash
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07
```

Optional symbol override:
```bash
python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07 --symbols AAPL,MSFT,TSLA
```

How we use it:
- Manual backfills after outages, missed schedules, or historical reloads.
- `--from-date` and `--to-date` are inclusive by day.
- Both dates must be earlier than today in `MARKET_TZ`.
- Uses the same validation and DB upsert logic as the intraday collector.

Key environment variables:
- `FMP_API_KEY`
- `SYMBOLS` (used unless overridden with `--symbols`)
- `MARKET_TZ`
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

### `run_scheduled_operations.py`
Purpose: Execute scheduled SQL scripts from `src/sql_operations/` and log execution status.

Typical usage:
```bash
python src/run_scheduled_operations.py
```

How we use it:
- Scheduled after ingestion to move/transform data from staging.
- Writes runtime logs to `LOG_DIR/scheduled_operations.log`.
- Writes execution status to `operation_logs.pipeline_logs`.
- Current implementation executes `export_stg_to_core.sql` from `src/sql_operations/`.

Key environment variables:
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- `LOG_DIR`

## Supporting Modules

These modules are imported by scripts and are not typically run directly.

- `orm_db.py`: SQLAlchemy engine/session factory and schema initialization.
- `models.py`: ORM models for `stg_raw`, `core_dbms`, and `operation_logs` schemas.
- `time_utils.py`: Time parsing/alignment utilities for 5-minute windows.
- `data_validation.py`: Row completeness and empty-field validation helpers.
- `logging_utils.py`: CSV error logging utilities for API, validation, and DB errors.
- `db_utils.py`: Legacy psycopg helpers for direct SQL table checks and statement builders.
- `__init__.py`: Package marker.

## SQL Operations Folder

- `sql_operations/export_stg_to_core.sql`: Move/transform staged rows into core tables.
- `sql_operations/truncate_stg_raw.sql`: Cleanup/truncate staging tables.
- See `src/sql_operations/README.md` for script order and SQL-specific notes.

## Operational Notes

- Run scripts from the repository root so imports and relative paths resolve consistently.
- Ensure `.env` (or exported environment variables) is loaded before running scripts.
- For cron setup, use scripts under `setup_scripts/`.
