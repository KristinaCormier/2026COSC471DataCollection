# Legacy SQL Schema Scripts

This directory contains legacy SQL table-creation scripts retained for historical reference.

## Current Source of Truth

Use these for active schema management:

- ORM models: `src/model/models.py`
- Alembic migrations: `alembic/versions/`
- Primary initialization command: `python -m alembic upgrade head`

## Status

- New environments should not be initialized with scripts in this directory.
- Setup and runtime paths are migration-first and ORM-first.
- SQL files here are useful for comparing legacy schema behavior or reviewing historical decisions.

## Legacy Subdirectories

- `core_dbms/` legacy core table SQL references
- `operation_logs/` legacy operational log table SQL references
- `stg_raw/` legacy staging table SQL references
- `stg_transform/` legacy transform table SQL references

See `docs/orm_consistency_audit.md` for the ongoing consolidation plan.

