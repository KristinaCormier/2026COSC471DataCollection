# Legacy Reference: `stg_transform`

This folder contains legacy SQL creation scripts for `stg_transform` tables.

## Active Schema Path

- Primary model definitions: `src/model/models.py` (`TransformMarketData`, `TransformError`)
- Migration definition: `alembic/versions/20260312_0001_baseline_schema.py`
- Initialize/update schema: `python -m alembic upgrade head`

## Notes

- SQL files in this folder are historical references, not the primary deployment path.
- Use Alembic migrations for current environments.
