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
- Load environment variables from `.env` before running tests (e.g., `set -a && source .env && set +a`).

## Step-by-step: run the test suite
1. Open a terminal at the repo root.
2. Activate the Python virtual environment:
   - `source venv/bin/activate`
3. Load environment variables from `.env`:
   - `set -a && source .env && set +a`
4. (Optional) Ensure database settings are set:
   - `TEST_DATABASE_URL` should point at a disposable Postgres database.
5. Run tests:
   - `pytest -q`

## Running tests with coverage
1. Follow steps 1-4 above to prepare your environment.
2. Run tests with coverage report:
   - `pytest --cov=src --cov-report=term-missing`
3. To generate XML coverage report for Coverage Gutters (VS Code extension):
   - `pytest --cov=src --cov-report=xml`
   - This creates `coverage.xml` in the repo root
   - Install the Coverage Gutters extension in VS Code to see line-by-line coverage
4. To generate both terminal and XML reports:
   - `pytest --cov=src --cov-report=term-missing --cov-report=xml`
5. To test a specific module with coverage:
   - `pytest tests/unit/test_auto_data_collection.py --cov=src.auto_data_collection --cov-report=term-missing`

### Troubleshooting
- If import-time errors occur for missing env vars (e.g., `PGPORT`), confirm `.env` is loaded.
- If database tests fail, verify the test database is reachable and `TEST_DATABASE_URL` is correct.

## Naming conventions
- Test files: `test_<feature>.py`
- Test functions: `test_<feature>_<scenario>`

**Examples:**
- `test_feature_engineering.py`
- `test_feature_engineering_missing_values()`
- `test_feature_engineering_invalid_symbol()`
