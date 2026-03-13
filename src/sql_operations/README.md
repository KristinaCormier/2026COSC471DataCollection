# SQL Operations

This directory is kept only as a documentation placeholder during the ORM consolidation.

Scheduled transform/load work no longer runs from `.sql` files. The active implementation lives in:

- [../run_scheduled_operations.py](../run_scheduled_operations.py) — Execution orchestration and step dependencies
- [../utils/scheduled_pipeline.py](../utils/scheduled_pipeline.py) — Python export and staging cleanup logic

If a future scheduled step is needed, add a Python function to `utils/scheduled_pipeline.py`, register it in `run_scheduled_operations.py`, and cover it with unit tests.

