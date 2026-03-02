# Operational SQL Scripts

SQL operations executed by `run_scheduled_operations.py` via cron or manually.

## Execution Order

Scripts run in alphabetical filename order:

1. `export_stg_to_core.sql` — moves data from `stg_raw.market_data` to `core_dbms.market_data` with dedup and quality checks
2. `truncate_stg_raw.sql` — cleans up staging tables after export

## Adding New Scripts

1. Create a `.sql` file in this directory
2. Use `BEGIN;` / `COMMIT;` for transactional scripts
3. Log to `operation_logs.*` tables where appropriate

## Manual Execution

```bash
# Run all scripts in order
python3 src/run_scheduled_operations.py

# Run a single script
psql -f src/sql_operations/export_stg_to_core.sql
```
