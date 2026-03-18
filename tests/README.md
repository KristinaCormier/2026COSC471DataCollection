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
- Database: Required (`TEST_DATABASE_URL` or `DB_*` variables)
- Typical use: schema/type validation and CI-parity checks
- Commands:
    - `pytest tests/unit/ -m "postgres_only" -v`
    - `pytest tests/integration/ -v`
    - `pytest tests/pipeline/ -v`

### Integration + Pipeline (Database Required)
- Scope: `tests/integration/`, `tests/pipeline/`
- Database: Required (`TEST_DATABASE_URL` or `DB_*` variables)
- Typical use: pre-merge validation
- Commands:
    - `pytest tests/integration/ -v`
    - `pytest tests/pipeline/ -v`

## Shared Fixtures

All fixtures are defined in [conftest.py](conftest.py) and available globally:

**Session-Scoped** (initialized once per test run):
- `project_root`: Path to the repository root directory
- `data_dir`: Path to `tests/data/` fixture directory
- `db_url`: Test database URL from `TEST_DATABASE_URL` env var or default
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
- Test database configured (separate from production) for integration/pipeline tests

### Environment Variables

Create a `.env` file at the project root (or export variables) with:

**Required** (if not set, tests will use defaults):
- `DB_HOST`: Test database hostname (default: `localhost`)
- `DB_PORT`: Test database port (default: `5432`)
- `DB_NAME`: Test database name (default: `cade_test`)
- `DB_USER`: Test database user (default: `cdem`)
- `DB_PASSWORD`: Test database password (default: `COSC2024`)
- `TEST_DATABASE_URL`: Explicit test database URL as `postgresql+psycopg://user:password@host:port/database`

**Optional** (for running individual collectors):
- `FMP_API_KEY`: Set to `test_api_key` for unit tests; not used in mocked integration tests
- `SYMBOLS`: Set to `AAPL,MSFT` (default)
- `MARKET_TZ`: Set to `America/New_York` (default)
- `WINDOW_MINUTES`: Set to `60` (default)
- `LOG_DIR`: Set to `./logs` (default)

### Prepare Test Database (Integration/Pipeline Only)

```bash
# Ensure test database exists and is accessible
psql -h "${DB_HOST:-localhost}" -U "${DB_USER:-cdem}" -c "CREATE DATABASE ${DB_NAME:-cade_test};" 2>/dev/null || echo "Database already exists"

# Or, let pytest create it on the first run (if the test user has superuser privileges)
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

Tests are automatically run on pull requests and pushes to `main` and `dev` branches via [.github/workflows/pytest.yml](../.github/workflows/pytest.yml).

**CI Test Flow**:
1. PostgreSQL 16 container is started
2. Python dependencies are installed
3. Unit tests are run (`tests/unit/`)
4. Integration tests are run (`tests/integration/`)
5. Pipeline tests are run (`tests/pipeline/`)
6. Coverage report is generated and uploaded to Codecov

**Local CI Simulation**:

To check what CI will run before pushing:

```bash
# Set up test database as CI does
export DB_HOST=localhost DB_PORT=5432 DB_NAME=test_db DB_USER=test_user DB_PASSWORD=test_password
export TEST_DATABASE_URL="postgresql+psycopg://test_user:test_password@localhost:5432/test_db"
export FMP_API_KEY=test_api_key SYMBOLS="AAPL,MSFT" MARKET_TZ="America/New_York" WINDOW_MINUTES=60

# Run the same commands as CI
pytest tests/unit/ -v --cov=src --cov-report=term-missing --cov-report=xml
pytest tests/integration/ -v --cov=src --cov-append --cov-report=term-missing --cov-report=xml
pytest tests/pipeline/ -v --cov=src --cov-append --cov-report=term-missing --cov-report=xml
```

## Naming Conventions

- **Test files**: `test_<module>.py` (e.g., `test_intraday_data_collection.py`)
- **Test functions**: `test_<function>_<scenario>` (e.g., `test_parse_iso_date_accepts_valid_value`)
- **Fixtures**: PascalCase (e.g., `FakeResponse`, `FakeCursor`)

## Common Issues

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: No module named 'src'` | Ensure `PYTHONPATH=src` is set or run pytest from repo root |
| `ProgrammingError: relation "..." does not exist` | Test database not initialized; check `TEST_DATABASE_URL` and database accessibility |
| `KeyError: 'DB_PORT'` | Load `.env` before running tests: `set -a && source .env && set +a` |
| `connection refused` | Test database not running; start PostgreSQL: `sudo systemctl start postgresql-16` |
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
