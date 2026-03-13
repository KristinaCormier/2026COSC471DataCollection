# Operational SQL Scripts

SQL operations executed by `run_scheduled_operations.py` to transform data from the staging layer to the core warehouse.

## Execution Order & Context

Scripts run in a **fixed pipeline order defined in** `src/run_scheduled_operations.py`:

1. **`export_stg_to_core.sql`** — Transform staging to core warehouse
   - Purpose: Deduplicates staging rows by (symbol, ts), validates OHLCV quality, logs conflicts, upserts to core
   - Source: `stg_raw.market_data`
   - Destination: `core_dbms.market_data_5m` with entries in `operation_logs.dedup_conflicts` and `operation_logs.data_quality_errors`
   - Idempotent: Yes (uses ON CONFLICT DO UPDATE)
   - Returns: no rows, but updates core_dbms and operation_logs

2. **`truncate_stg_raw.sql`** — Cleanup staging after successful export
   - Purpose: Clears `stg_raw.*` tables to reset for the next collection cycle
   - ⚠️ **Destructive**: Permanently deletes rows; only call after successful export
   - Idempotent: Yes (truncate is safe to re-run if no new staging data exists)
   - Returns: no rows

## Critical Detail: Dependency Gate

Execution order and dependencies are controlled in code, not by filename sorting. This is important for safety:
- `export_stg_to_core.sql` must run before `truncate_stg_raw.sql`
- `truncate_stg_raw.sql` is skipped when `export_stg_to_core.sql` fails
- Additional `.sql` files are ignored unless added to the pipeline definition

## Writing New Scripts

When adding a new SQL operation:

1. **Create a `.sql` file** in this directory with a clear name matching execution order
2. **Register it in `src/run_scheduled_operations.py`** so it is part of the managed execution plan
3. **Wrap in a transaction**: Use `BEGIN;` and `COMMIT;` (or `ROLLBACK;` on error)
4. **Use proper headers**: Document purpose, dependencies, side effects, idempotency
5. **Log to operation_logs**: Record execution status and any errors
6. **Test in test database first**: Validate the script before deploying
7. **Update this README**: Document the new script and its execution context

### Example Structure

```sql
-- ============================================================================
-- My New Data Operation
-- ============================================================================
--
-- Purpose: Brief description of what this script does
-- Dependencies: What must exist before running (e.g., stg_raw.market_data)
-- Side effects: What tables are modified (insert/update/delete)
-- Idempotency: Safe to run multiple times? (Yes/No and why)
--
-- ============================================================================

BEGIN;

-- Your SQL operations here

COMMIT;  -- On error, this becomes ROLLBACK
```

## Manual Execution

```bash
# Run all scripts in order (the normal way)
python src/run_scheduled_operations.py

# Or, run scripts manually via psql (for testing)
psql -d "$PGDATABASE" -U "$PGUSER" -h "$PGHOST" -f src/sql_operations/export_stg_to_core.sql

# Execute a single operation with verbose output (useful for debugging)
psql -v ON_ERROR_STOP=1 -f src/sql_operations/export_stg_to_core.sql
```

## Troubleshooting

| Issue | Check | Fix |
|-------|-------|-----|
| Script fails with "table does not exist" | Staging table has no rows | Backfill with `gather_past_data.py` or `load_stg_raw_market_data.sh` |
| Script completes but data not in core | Check `operation_logs.pipeline_logs` status | If marked 'failed', check error message; if 'success', verify table permissions |
| Truncate fails with "permission denied" | User privileges on stg_raw | Grant ALTER privileges to collection user |
| Script runs twice, second run fails | Check for duplicate transactions | Scripts should be idempotent; if not, check design |

## See Also

- [README.md](../../README.md) — Project overview and three-script pipeline
- [src/README.md](../README.md) — Entry-point scripts and execution context
- [export_stg_to_core.sql](export_stg_to_core.sql) — Detailed transformation logic and quality checks
- [truncate_stg_raw.sql](truncate_stg_raw.sql) — Cleanup and staging reset
