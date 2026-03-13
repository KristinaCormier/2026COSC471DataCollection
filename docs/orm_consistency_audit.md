# ORM Consistency Audit

Date: 2026-03-12
Branch: refactor/fully-implement-orm-remove-dependence-on-raw-sql

## Scope
This audit tracks remaining inconsistencies with the project goal of SQLAlchemy ORM-first data access, migration-based schema management, and low-friction setup on fresh environments.

## Completed In This Refactor Slice
- Scheduled transform/load orchestration moved to Python/ORM pipeline (`src/run_scheduled_operations.py`, `src/utils/scheduled_pipeline.py`).
- Raw operational SQL scripts removed from scheduled runtime (`src/sql_operations/*.sql` removed).
- Alembic baseline migration path added (`alembic/`, `alembic/versions/20260312_0001_baseline_schema.py`).
- Cron installers now support default user-mode setup with project-local wrappers and no hard requirement on `/etc/cron.d` or `/usr/local/bin`.
- `.env.template` converted back to valid env syntax and updated for cron mode configuration.
- Legacy CSV shell loader replaced by Python ORM loader (`src/load_stg_raw_market_data.py`) with shell wrapper compatibility.
- `setup_scripts/table_creation_script/` documentation is now explicitly marked as legacy reference, and primary docs point to Alembic/models.
- README now includes a local bootstrap sequence for fresh-environment setup without cron/root dependencies.
- `tests/README.md` now documents separate unit-only and integration/pipeline execution tracks.

## Remaining Inconsistencies (Prioritized)

1. Medium: Legacy table-creation SQL artifacts still present
- Location: `setup_scripts/table_creation_script/**/*.sql`
- Impact: Duplicates schema source-of-truth and risks drift from ORM models + Alembic.
- Recommended next step: Keep as historical references for one transition window, then archive/remove once migration-only rollout is fully validated.

2. Medium: CSV import now uses ORM, but rollout cleanup remains
- Location: `src/load_stg_raw_market_data.py`, `setup_scripts/load_stg_raw_market_data.sh`
- Impact: Core behavior is now ORM-based, but docs/scripts should continue converging on Python-first usage.
- Recommended next step: Keep wrapper for transition, then retire it after downstream automation updates.

3. Medium: Server replication/backup setup is intentionally system-coupled
- Location: `setup_scripts/database_setup/*.sh`, `setup_scripts/server_setup/*.sh`
- Impact: Uses `/var/lib`, `/etc`, service restarts, and root privileges; not suitable for local onboarding.
- Recommended next step: Keep as production-only path, and maintain a separate local/developer bootstrap path centered on Alembic + user-mode cron setup.

4. Medium: Test suite assumes PostgreSQL for broad coverage
- Location: `tests/conftest.py`, `tests/README.md`, integration tests
- Impact: Raises setup cost for contributors and local CI.
- Recommended next step: Keep integration tests on PostgreSQL, but expand SQLite-compatible unit tests for transform logic and utility modules.

## Next Implementation Milestones
1. Remove shell wrapper once all users and automation invoke the Python loader directly.
2. Archive/remove legacy SQL files after migration-only rollout validation.
3. Expand SQLite-friendly unit coverage for non-PostgreSQL-specific logic.
4. Keep production-only setup scripts isolated from local onboarding paths in docs.
