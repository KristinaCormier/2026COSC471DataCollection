"""
Pytest Configuration & Shared Fixtures

Purpose:
    Provide common pytest fixtures for all test modules, including database connections,
    project paths, mock helpers, and test data loaders.

Session-Scoped Fixtures (initialized once per test session):
    - project_root: Path to repository root directory
    - data_dir: Path to tests/data/ fixture directory
    - db_url: Test database URL from TEST_DATABASE_URL env var or default
    - db_engine: SQLAlchemy engine (auto-disposed after all tests)

Function-Scoped Fixtures (created fresh per test, rolled back after):
    - db_connection: Transaction-scoped connection (auto-rollback after test)
    - db_session: SQLAlchemy ORM session bound to db_connection
    - seed_rows: Helper to insert test data into tables

Utilities:
    - FakeResponse: Mock HTTP response for API call testing
    - FakeCursor: Mock database cursor for legacy tests

Usage:
    Fixtures are auto-discovered by pytest and available via function arguments:

    def test_my_feature(db_session):
        # db_session is a fresh ORM session, auto-rolled back after test
        pass

Environment:
    - PGPORT, PGHOST, PGDATABASE, PGUSER, PGPASSWORD default to safe test values
    - TEST_DATABASE_URL can override all connection settings at once
    - All database operations are wrapped in transactions for test isolation

Author: Data Collection Team
License: MIT
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, Sequence

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Ensure required env vars exist before importing modules under test.
os.environ.setdefault("PGPORT", "5432")


@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def data_dir(project_root: Path) -> Path:
    return project_root / "tests" / "data"


@pytest.fixture(scope="session")
def sample_dataframe():
    pd = pytest.importorskip("pandas")
    return pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC"],
            "close": [101.5, 98.0, 105.2],
            "volume": [1_000_000, 850_000, 1_250_000],
            "target_up": [1, 0, 1],
        }
    )


@pytest.fixture(scope="session")
def db_url() -> str:
    return os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://cdem:COSC2024@localhost:5432/cade_test",
    )


@pytest.fixture(scope="session")
def db_engine(db_url: str):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    engine = sqlalchemy.create_engine(db_url, future=True, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_connection(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    yield connection
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def db_session(db_connection):
    orm = pytest.importorskip("sqlalchemy.orm")
    Session = orm.sessionmaker(bind=db_connection, future=True)
    session = Session()
    yield session
    session.close()


@pytest.fixture(scope="function")
def seed_rows(db_session):
    """Generic seeder for tests with ad-hoc tables."""
    sqlalchemy = pytest.importorskip("sqlalchemy")

    def _seed(table: str, rows: Sequence[Mapping[str, object]]):
        if not rows:
            return
        columns = rows[0].keys()
        placeholders = ", ".join(f":{col}" for col in columns)
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        db_session.execute(sqlalchemy.text(sql), rows)
        db_session.commit()

    return _seed


class FakeResponse:
    """Mock HTTP response for API calls."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"x" if payload is not None else b""

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


# Legacy helpers kept for older tests that still use raw-DB fakes.
class FakeCursor:
    """Mock database cursor for legacy/raw-SQL tests."""

    def __init__(self, table_exists=True):
        self.queries = []
        self.params = []
        self.table_exists = table_exists
        self.executed_values = []

    def execute(self, query, params=None):
        self.queries.append(query)
        self.params.append(params)

    def executemany(self, query, params_seq):
        self.queries.append(query)
        self.executed_values = list(params_seq)

    def fetchone(self):
        return (1,) if self.table_exists else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    """Mock database connection for legacy/raw-SQL tests."""

    def __init__(self, table_exists=True, should_fail=False):
        self.cursors = []
        self.commits = 0
        self.closed = False
        self.table_exists = table_exists
        self.should_fail = should_fail
        self.committed = False

    def cursor(self):
        cur = FakeCursor(table_exists=self.table_exists)
        self.cursors.append(cur)
        return cur

    def commit(self):
        self.commits += 1
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture(scope="function")
def mock_pg_connection(monkeypatch):
    """Legacy mock psycopg connection for old tests."""
    fake_conn = FakeConnection()

    def _fake_connect(*args, **kwargs):
        return fake_conn

    monkeypatch.setattr("psycopg.connect", _fake_connect)
    return fake_conn


@pytest.fixture(scope="function")
def fake_api_response(monkeypatch):
    """Mock successful API response."""

    def mock_get(url, params=None, timeout=None):
        return FakeResponse(
            [
                {
                    "date": "2026-02-02 10:05:00",
                    "open": 150.0,
                    "high": 151.0,
                    "low": 149.5,
                    "close": 150.5,
                    "volume": 1000000,
                }
            ]
        )

    monkeypatch.setattr("requests.get", mock_get)


@pytest.fixture(scope="function")
def mock_error_log_dir(tmp_path, monkeypatch):
    """Redirect error logging to a temp directory for tests."""
    import intraday_data_collection
    from utils import logging_utils

    error_log_dir = tmp_path / "dc_error_logs"
    error_log_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(logging_utils, "ERROR_LOG_DIR", error_log_dir)
    monkeypatch.setattr(intraday_data_collection.lu, "ERROR_LOG_DIR", error_log_dir)

    return error_log_dir