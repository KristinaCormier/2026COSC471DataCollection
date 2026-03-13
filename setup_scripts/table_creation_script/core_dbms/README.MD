# Legacy Reference: `core_dbms`

This folder contains legacy SQL creation scripts for `core_dbms` tables.

## Active Schema Path

- Primary model definition: `src/model/models.py` (`MarketData5m`)
- Migration definition: `alembic/versions/20260312_0001_baseline_schema.py`
- Initialize/update schema: `python -m alembic upgrade head`

## Notes

- Treat SQL files in this folder as historical reference only.
- Do not use these scripts as the primary setup method for fresh environments.

