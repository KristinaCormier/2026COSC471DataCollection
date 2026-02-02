from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
