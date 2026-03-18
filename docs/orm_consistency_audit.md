# ORM Consistency Audit

Date: 2026-03-16
Branch: refactor/fully-implement-orm-remove-dependence-on-raw-sql

## Scope
This audit tracks remaining inconsistencies with the project goal of SQLAlchemy ORM-first data access, migration-based schema management, and low-friction setup on fresh environments.

## Completed In This Refactor Slice
- Scheduled transform/load orchestration moved to Python/ORM pipeline (`src/run_scheduled_operations.py`, `src/utils/scheduled_pipeline.py`).
- Raw operational SQL scripts removed from scheduled runtime (`src/sql_operations/*.sql` removed).
- Alembic baseline migration path added (`alembic/`, `alembic/versions/20260312_0001_baseline_schema.py`).
- Cron installers now support default user-mode setup with project-local wrappers and no hard requirement on `/etc/cron.d` or `/usr/local/bin`.
- `.env.template` converted back to valid env syntax and updated for cron mode configuration.
- Legacy CSV shell loader replaced by Python ORM loader (`src/historical_csv_data_load.py`).
- README now includes a local bootstrap sequence for fresh-environment setup without cron/root dependencies.
- `tests/README.md` now documents separate unit-only and integration/pipeline execution tracks.
- Deprecated shell loader wrapper removed (`setup_scripts/load_stg_raw_market_data.sh`).
- Legacy table-creation SQL artifact tree removed from active setup docs and repository surface (`setup_scripts/table_creation_script/`).
- Pytest marker taxonomy now includes `postgres_only` to separate PostgreSQL-coupled checks.
- SQLite-friendly unit coverage expanded for CSV parsing/validation behavior.

## Remaining Inconsistencies (Prioritized)

1. Medium: Server replication/backup setup is intentionally system-coupled
- Location: `setup_scripts/database_setup/*.sh`, `setup_scripts/server_setup/*.sh`
- Impact: Uses `/var/lib`, `/etc`, service restarts, and root privileges; not suitable for local onboarding.
- Recommended next step: Keep as production-only path, and maintain a separate local/developer bootstrap path centered on Alembic + user-mode cron setup.

2. Medium: Test suite defaults still center PostgreSQL fixtures
- Location: `tests/conftest.py`, `tests/README.md`, integration tests
- Impact: Local `tests/unit/` execution is easier with marker filtering, but fixture defaults and CI flow remain Postgres-first.
- Recommended next step: Keep integration/pipeline tests on PostgreSQL and evaluate optional SQLite-first fixture defaults for pure unit tracks in a follow-up phase.

## Next Implementation Milestones
1. Keep production-only setup scripts isolated from local onboarding paths across all docs.
2. Continue expanding SQLite-friendly unit coverage for non-PostgreSQL-specific logic.
3. Evaluate `tests/conftest.py` defaults for a possible SQLite-first pure-unit path while retaining PostgreSQL integration coverage.
4. Track downstream automation adoption of Python-only CSV loader command usage.
