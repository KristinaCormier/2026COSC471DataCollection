# Legacy Reference: `operation_logs`

This folder contains legacy SQL creation scripts for `operation_logs` tables.

## Active Schema Path

- Primary model definitions: `src/model/models.py`
- Migration definition: `alembic/versions/20260312_0001_baseline_schema.py`
- Initialize/update schema: `python -m alembic upgrade head`

## Notes

- These SQL files are retained for historical comparison and troubleshooting.
- Current setup and deployment should use Alembic migrations instead of direct table-creation SQL.
