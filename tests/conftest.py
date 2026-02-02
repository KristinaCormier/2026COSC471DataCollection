from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping, Sequence

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
        "postgresql+psycopg://postgres:postgres@localhost:5432/finance_test",
    )


@pytest.fixture(scope="session")
def db_engine(db_url: str):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    engine = sqlalchemy.create_engine(db_url, future=True, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_connection(db_engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
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
    """Generic seeder for tests with ad‑hoc tables."""
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


class FakeCursor:
    """Mock database cursor for testing."""
    def __init__(self, table_exists=True):
        self.queries = []
        self.params = []
        self.table_exists = table_exists
        self.executed_values = []

    def execute(self, query, params=None):
        self.queries.append(query)
        self.params.append(params)

    def fetchone(self):
        return (1,) if self.table_exists else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    """Mock database connection for testing."""
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
    """Mock psycopg2 connection for unit tests (no real DB required)."""
    # This fixture replaces psycopg2.connect with an in-memory stub so tests
    # can exercise DB-related code paths (like insert/commit/close) without
    # requiring a running Postgres instance.
    fake_conn = FakeConnection()

    def _fake_connect(*args, **kwargs):
        return fake_conn

    monkeypatch.setattr("psycopg2.connect", _fake_connect)
    return fake_conn


@pytest.fixture(scope="function")
def fake_api_response(monkeypatch):
    """Mock successful API response."""
    def mock_get(url, params=None, timeout=None):
        return FakeResponse([
            {
                "date": "2026-02-02 10:05:00",
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000
            }
        ])
    monkeypatch.setattr("requests.get", mock_get)


@pytest.fixture(scope="function")
def mock_execute_values(monkeypatch):
    """Mock execute_values to track SQL execution without real DB."""
    captured = {"rows": None, "sql": None, "called": False}

    def _fake_execute_values(cur, sql, rows, template=None):
        captured["rows"] = rows
        captured["sql"] = sql
        captured["called"] = True

    monkeypatch.setattr("psycopg2.extras.execute_values", _fake_execute_values)
    return captured
