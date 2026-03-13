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

## Remaining Inconsistencies (Prioritized)

1. High: Legacy table-creation SQL scripts still present
- Location: `setup_scripts/table_creation_script/**/*.sql`
- Impact: Duplicates schema source-of-truth and risks drift from ORM models + Alembic.
- Recommended next step: Mark these scripts as legacy/archived and phase out direct usage in setup docs once migration rollout is validated.

2. High: CSV bulk loader still relies on raw SQL + `psql`
- Location: `setup_scripts/load_stg_raw_market_data.sh`
- Impact: Bypasses ORM/session rules and makes behavior harder to test under Python-only workflows.
- Recommended next step: Replace with a Python CLI that parses CSV and bulk inserts via ORM/Core insert statements in a managed session.

3. Medium: Server replication/backup setup is intentionally system-coupled
- Location: `setup_scripts/database_setup/*.sh`, `setup_scripts/server_setup/*.sh`
- Impact: Uses `/var/lib`, `/etc`, service restarts, and root privileges; not suitable for local onboarding.
- Recommended next step: Keep as production-only path, and maintain a separate local/developer bootstrap path centered on Alembic + user-mode cron setup.

4. Medium: Test suite assumes PostgreSQL for broad coverage
- Location: `tests/conftest.py`, `tests/README.md`, integration tests
- Impact: Raises setup cost for contributors and local CI.
- Recommended next step: Keep integration tests on PostgreSQL, but expand SQLite-compatible unit tests for transform logic and utility modules.

## Next Implementation Milestones
1. Add Python replacement for `load_stg_raw_market_data.sh`.
2. Move table-creation SQL docs to "legacy reference" and point setup flow to Alembic.
3. Add a lightweight local bootstrap command sequence in README for first-time setup.
4. Split test instructions into "unit-only (fast/local)" and "integration (PostgreSQL required)" tracks.
