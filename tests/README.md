# Tests

This directory contains unit, integration, and pipeline tests for the data collection and ETL system.

- Most unit tests are fast and do not require a database. A small PostgreSQL schema-parity subset is marked `postgres_only`.
- Integration and pipeline tests require a PostgreSQL-compatible test database.

## Test Organization

- **`tests/unit/`**: Tests for data parsing, validation, date arithmetic, and API error handling. Most are database-free; `postgres_only` tests require PostgreSQL.
- **`tests/integration/`**: Tests for database operations, ORM models, and staging/core layer interaction. Requires a test database.
- **`tests/pipeline/`**: End-to-end tests for the complete collection and transformation pipeline. Requires a test database and mock API.
- **`tests/conftest.py`**: Shared pytest fixtures for database connections, project paths, and test utilities.
- **`tests/data/`**: Static test data and fixtures (small CSV files, mock responses, etc.)

## Execution Tracks

### Unit-Only (Fast Local)
- Scope: `tests/unit/`
- Database: Not required for `not postgres_only` subset
- Typical use: local development feedback loop
- Command: `pytest tests/unit/ -m "not postgres_only" -v`

### PostgreSQL-Specific Unit + Integration + Pipeline
- Scope: `tests/unit/` (postgres-only subset), `tests/integration/`, `tests/pipeline/`
- Database:
    - `TEST_DATABASE_URL` empty/unset -> ephemeral PostgreSQL via testcontainers
    - `TEST_DATABASE_URL` set -> external/non-containerized test database
- Typical use: schema/type validation and CI-parity checks
- Commands:
    - `pytest tests/unit/ -m "postgres_only" -v`
    - `pytest tests/integration/ -v`
    - `pytest tests/pipeline/ -v`

### Integration + Pipeline (Database Required)
- Scope: `tests/integration/`, `tests/pipeline/`
- Database:
    - `TEST_DATABASE_URL` empty/unset -> ephemeral PostgreSQL via testcontainers
    - `TEST_DATABASE_URL` set -> external/non-containerized test database
- Typical use: pre-merge validation
- Commands:
    - `pytest tests/integration/ -v`
    - `pytest tests/pipeline/ -v`

## Shared Fixtures

All fixtures are defined in [conftest.py](conftest.py) and available globally:

**Session-Scoped** (initialized once per test run):
- `project_root`: Path to the repository root directory
- `data_dir`: Path to `tests/data/` fixture directory
- `db_engine`: SQLAlchemy engine connected to the test database (auto-disposed after all tests)

**Function-Scoped** (created fresh per test):
- `db_connection`: Database connection with a transaction that rolls back after each test (for isolation)
- `db_session`: SQLAlchemy session bound to the test transaction
- `seed_rows`: Helper to insert rows into test tables

**Utilities**:
- `FakeResponse`: Mock HTTP response for API call testing
- `FakeCursor`: Legacy mock database cursor (for older tests)

## Setup

### Prerequisites
- Python 3.9+
- Virtual environment activated: `source .venv/bin/activate`
- Requirements installed: `pip install -r requirements-dev.txt`
- Docker engine running locally (required for automatic testcontainers DB provisioning)
- Optional external PostgreSQL test database (only when explicitly opting out of testcontainers)

### Environment Variables

Create a `.env` file at the project root (or export variables) with:

**DB mode selector**:
- `TEST_DATABASE_URL`: If non-empty, pytest uses this external/non-containerized test database URL.
- `TEST_DATABASE_URL` empty or unset: pytest provisions an ephemeral PostgreSQL container via testcontainers.

**Optional** (for running individual collectors):
- `FMP_API_KEY`: Set to `test_api_key` for unit tests; not used in mocked integration tests
- `SYMBOLS`: Set to `AAPL,MSFT` (default)
- `MARKET_TZ`: Set to `America/New_York` (default)
- `WINDOW_MINUTES`: Set to `60` (default)
- `LOG_DIR`: Set to `./logs` (default)

**Optional testcontainers tuning**:
- `TESTCONTAINERS_POSTGRES_IMAGE`: Override testcontainers PostgreSQL image (default: `postgres:16`).
- `TESTCONTAINERS_DB_USER`: Username for testcontainers DB (default: `test_user`).
- `TESTCONTAINERS_DB_PASSWORD`: Password for testcontainers DB (default: `test_password`).
- `TESTCONTAINERS_DB_NAME`: Database name for testcontainers DB (default: `test_db`).

### Prepare Test Database (Integration/Pipeline Only)

**Recommended local path**: use testcontainers (default).

```bash
# Run DB-backed tests directly. Testcontainers automatically provisions an ephemeral postgres container.
# Keep TEST_DATABASE_URL empty/unset so pytest uses containerized DB.
# (Docker daemon must be running, but testcontainers handles container startup automatically.)
pytest tests/integration/ -v -m integration
pytest tests/pipeline/ -v -m pipeline
```

**Alternative**: explicitly opt in to an existing PostgreSQL instance.

```bash
# Opt in to external/non-containerized DB usage.
export TEST_DATABASE_URL="postgresql+psycopg://test_user:test_password@localhost:5432/test_db"

# Ensure test database exists and is accessible
psql "${TEST_DATABASE_URL/postgresql+psycopg/postgresql}" -c "SELECT 1"

# Or let pytest initialize schemas/tables on first run (if the test user has permissions)
```

## Running Tests

### Quick Test

```bash
# Run unit tests only (no database needed)
pytest tests/unit/ -m "not postgres_only" -v

# Run PostgreSQL-only unit checks
pytest tests/unit/ -m "postgres_only" -v

# Run integration tests (requires test database)
pytest tests/integration/ -v

# Run pipeline tests (requires test database and mocked API)
pytest tests/pipeline/ -v

# Run all tests (unit + integration + pipeline)
pytest
```

### With Coverage

```bash
# Terminal report
pytest --cov=src --cov-report=term-missing

# XML report (for CI/CD or VS Code Coverage Gutters extension)
pytest --cov=src --cov-report=xml

# Both terminal and XML
pytest --cov=src --cov-report=term-missing --cov-report=xml

# Coverage for a specific module
pytest tests/unit/test_intraday_data_collection.py --cov=src.intraday_data_collection --cov-report=term-missing

# Phase 1 (unit, DB-free) coverage gate
pytest tests/unit/ -v -m "not postgres_only" \
    --cov=utils.collector_shared \
    --cov=utils.data_validation \
    --cov=utils.time_utils \
    --cov=model.orm_db \
    --cov=utils.db_utils \
    --cov=utils.logging_utils \
    --cov-report=term-missing \
    --cov-fail-under=90

# Phase 2 (integration) coverage gate
pytest tests/integration/ -v -m integration \
    --cov=utils.scheduled_pipeline \
    --cov-report=term-missing \
    --cov-fail-under=90

# Phase 3 (pipeline) focused coverage report
pytest tests/pipeline/ -v -m pipeline \
    --cov=intraday_data_collection \
    --cov=run_scheduled_operations \
    --cov-report=term-missing \
    --cov-fail-under=90
```

### With Filtering

```bash
# Run tests matching a pattern
pytest -k "test_parse" -v

# Run a specific test file
pytest tests/unit/test_time_utils.py -v

# Run a specific test function
pytest tests/unit/test_time_utils.py::test_parse_hhmm_accepts_valid_input -v

# Run with minimal output
pytest -q

# Exclude PostgreSQL-only tests from a mixed run
pytest -m "not postgres_only" -v
```

### Load Environment Before Testing

If using a `.env` file:

```bash
# Bash
set -a && source .env && set +a
pytest

# Zsh
export $(cat .env | xargs)
pytest
```

## Continuous Integration

Tests are automatically run on pull requests, pushes to `main` and `dev`, and merge queue runs via [.github/workflows/pytest.yml](../.github/workflows/pytest.yml).

**CI Phase Jobs**:
1. **Phase 1 Unit Tests**: DB-free unit suite with 90% coverage gate for unit-owned modules. Required for PR and merge queue.
2. **Phase 2 Integration Tests**: PostgreSQL-backed integration suite with 90% coverage gate for integration-owned modules. Informational on PR/push, required on merge queue.
3. **Phase 3 Pipeline Tests**: End-to-end pipeline suite (grouped with phase 2 CI policy). Informational on PR/push, required on merge queue.
4. Per-phase coverage artifacts are uploaded and reported to Codecov with flags (`unit`, `integration`, `pipeline`).

**Local CI Simulation**:

To check what CI will run before pushing:

```bash
# Use default local testcontainers behavior for DB-backed phases
export TEST_DATABASE_URL=""
export FMP_API_KEY=test_api_key SYMBOLS="AAPL,MSFT" MARKET_TZ="America/New_York" WINDOW_MINUTES=60

# Run the same commands as CI
pytest tests/unit/ -v -m "not postgres_only" \
    --cov=utils.collector_shared \
    --cov=utils.data_validation \
    --cov=utils.time_utils \
    --cov=model.orm_db \
    --cov=utils.db_utils \
    --cov=utils.logging_utils \
    --cov-report=term-missing \
    --cov-fail-under=90

pytest tests/integration/ -v -m integration \
    --cov=utils.scheduled_pipeline \
    --cov-report=term-missing \
    --cov-fail-under=90

pytest tests/pipeline/ -v -m pipeline \
    --cov=intraday_data_collection \
    --cov=run_scheduled_operations \
    --cov-report=term-missing \
    --cov-fail-under=90
```

## Naming Conventions

- **Test files**: `test_<module>.py` (e.g., `test_intraday_data_collection.py`)
- **Test functions**: `test_<function>_<scenario>` (e.g., `test_parse_iso_date_accepts_valid_value`)
- **Fixtures**: PascalCase (e.g., `FakeResponse`, `FakeCursor`)

## Common Issues

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: No module named 'src'` | Ensure `PYTHONPATH=src` is set or run pytest from repo root |
| `ProgrammingError: relation "..." does not exist` | Test database was not initialized; ensure Docker is running so testcontainers can provision DB |
| `connection refused` | Testcontainers could not provision DB; verify Docker daemon is running |
| DB-backed tests fail to start DB | Ensure Docker is running when `TEST_DATABASE_URL` is empty/unset so pytest can provision ephemeral PostgreSQL |
| Tests are unexpectedly using an external DB | Clear `TEST_DATABASE_URL` (empty/unset) to force containerized DB-backed tests |
| Fixtures not found | Ensure `tests/conftest.py` exists; pytest auto-discovers it |
| Slow tests | Tests are isolated per function; `db_connection` rolls back after each test for clean state |

## Adding New Tests

### Unit Test Template

```python
"""Unit tests for data_validation module."""

import pytest
from src.utils import data_validation as dv

def test_validate_row_accepts_complete_ohlcv():
    """Test that validate_row() returns True for valid OHLCV data."""
    row = {"open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000000}
    assert dv.validate_row(row) is True

def test_validate_row_rejects_missing_close():
    """Test that validate_row() returns False when close is missing."""
    row = {"open": 100, "high": 105, "low": 95, "volume": 1000000}
    assert dv.validate_row(row) is False
```

### Integration Test Template

```python
"""Integration tests for intraday_data_collection module."""

import pytest
from sqlalchemy.orm import Session
from src.model.models import MarketData
from src import intraday_data_collection

@pytest.mark.requires_db
def test_insert_batch_upserts_existing_rows(db_session: Session):
    """Test that inserting duplicate (symbol, ts) updates the existing row."""
    # Given: an existing row
    existing = MarketData(symbol="AAPL", ts="2026-03-12 10:00:00", close=150.0)
    db_session.add(existing)
    db_session.commit()
    
    # When: we insert a row with the same symbol and ts
    new_row = MarketData(symbol="AAPL", ts="2026-03-12 10:00:00", close=151.0)
    # Call intraday_data_collection._insert_batch(...)
    
    # Then: the close price should be updated
    updated = db_session.query(MarketData).filter_by(symbol="AAPL").one()
    assert updated.close == 151.0
```

## See Also

- [README.md](../README.md) — Project overview and architecture
- [src/README.md](../src/README.md) — Source scripts and module documentation
- [.github/workflows/pytest.yml](../.github/workflows/pytest.yml) — CI/CD configuration
