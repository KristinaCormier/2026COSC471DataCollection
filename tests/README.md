# Tests


## Layout
- `tests/conftest.py`: shared fixtures (project root, data samples, Postgres session helpers)
- `tests/unit/`: pure logic tests (feature engineering, model utils)
- `tests/integration/`: database + warehouse integration tests
- `tests/pipeline/`: end‑to‑end training/inference pipelines
- `tests/data/`: small static datasets for tests

## Notes
- Set `TEST_DATABASE_URL` to point at a disposable Postgres database.
- Fixtures use `pytest.importorskip` so missing optional deps will skip related tests.

## Naming conventions
- Test files: `test_<feature>.py`
- Test functions: `test_<feature>_<scenario>`

**Examples:**
- `test_feature_engineering.py`
- `test_feature_engineering_missing_values()`
- `test_feature_engineering_invalid_symbol()`
