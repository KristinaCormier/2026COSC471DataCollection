# Legacy Reference: `stg_raw`

This folder contains legacy SQL creation scripts for `stg_raw` tables.

## Active Schema Path

- Primary model definitions: `src/model/models.py` (`MarketData`, `IngestError`)
- Migration definition: `alembic/versions/20260312_0001_baseline_schema.py`
- Initialize/update schema: `python -m alembic upgrade head`

## Notes

- These scripts are retained for historical reference.
- New environments should use Alembic migrations and ORM models as the canonical schema path.