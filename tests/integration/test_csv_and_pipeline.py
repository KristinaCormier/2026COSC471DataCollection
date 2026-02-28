"""
Integration tests exercising SQL pipelines and staging/core tables.

These tests are a Python analog of the SQL files the user provided
(`test_csv_to_stg_raw.sql`, `test_stg_to_core_5m.sql`,
`test_truncate_stg_raw.sql`).  They use the existing `db_connection`
fixture from `conftest.py` which automatically wraps each test in a
transaction that is rolled back at the end.

The tests assume the staging and core schemas/tables already exist in the
configured test database.  They read pipeline SQL from the project
sources so that any modifications to the scripts are exercised here.
"""

from __future__ import annotations

from pathlib import Path
import pytest

# helpers ------------------------------------------------------------------

def _read_sql(filename: str) -> str:
    """Return the contents of a SQL file living under `src/`.

    The path is resolved relative to the project root so the tests can be
    executed from anywhere.
    """
    # conftest provides PROJECT_ROOT via import; avoiding circular import by
    # looking up from this file's parent directories instead.
    root = Path(__file__).resolve().parents[1]
    return (root / "src" / filename).read_text()


# Suite 1: CSV → stg_raw.market_data --------------------------------------

@pytest.mark.integration
def test_csv_to_stg_raw_inserts_one_valid_row(db_connection):
    # Arrange: insert a single valid row
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume, asset_type, source, raw_payload)
        VALUES
            ('AAPL', '2024-01-01 09:30', 100, 101, 99, 100.5, 10000,
             'stock', 'CSV_bulk_load', '{}');
        """
    )

    # Assert: exactly one row exists
    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM stg_raw.market_data
         WHERE symbol = 'AAPL' AND ts = '2024-01-01 09:30'"
    ).scalar()
    assert cnt == 1


@pytest.mark.integration
def test_csv_to_stg_raw_unique_constraint_prevents_duplicate(db_connection):
    # Insert initial row
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume, asset_type, source, raw_payload)
        VALUES
            ('AAPL', '2024-01-01 09:30', 100, 101, 99, 100.5, 10000,
             'stock', 'CSV_bulk_load', '{}');
        """
    )

    # Insert duplicate with ON CONFLICT DO NOTHING to simulate idempotent loader
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume, asset_type, source, raw_payload)
        VALUES
            ('AAPL', '2024-01-01 09:30', 100, 101, 99, 100.5, 10000,
             'stock', 'CSV_bulk_load', '{}')
        ON CONFLICT DO NOTHING;
        """
    )

    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM stg_raw.market_data WHERE symbol = 'AAPL'"
    ).scalar()
    assert cnt == 1


# Suite 2: stg_raw → core_dbms.market_data_5m -----------------------------

@pytest.mark.integration
def test_stg_to_core_valid_ohlcv_moves_to_core(db_connection):
    # Put one valid OHLCV row in staging
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume,
             asset_type, source, raw_payload, ingest_time)
        VALUES
            ('MSFT', '2024-01-01 09:35', 100, 105, 99, 103, 10000,
             'stock', 'CSV_bulk_load', '{}', now());
        """
    )

    # run the pipeline SQL that migrates valid rows
    pipeline_sql = _read_sql("export_from_stg_load_to_core.sql")
    db_connection.execute(pipeline_sql)

    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM core_dbms.market_data_5m
         WHERE symbol = 'MSFT' AND ts = '2024-01-01 09:35'"
    ).scalar()
    assert cnt == 1


@pytest.mark.integration
def test_stg_to_core_invalid_ohlc_blocked(db_connection):
    # high < low is invalid
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume,
             asset_type, source, raw_payload, ingest_time)
        VALUES
            ('TSLA', '2024-01-01 09:40', 100, 90, 95, 102, 10000,
             'stock', 'CSV_bulk_load', '{}', now());
        """
    )

    pipeline_sql = _read_sql("export_from_stg_load_to_core.sql")
    db_connection.execute(pipeline_sql)

    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM core_dbms.market_data_5m WHERE symbol = 'TSLA'"
    ).scalar()
    assert cnt == 0


@pytest.mark.integration
def test_stg_to_core_fractional_volume_blocked(db_connection):
    # fractional volume should not make it to core
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume,
             asset_type, source, raw_payload, ingest_time)
        VALUES
            ('NVDA', '2024-01-01 09:45', 100, 105, 99, 103, 10000.55,
             'stock', 'CSV_bulk_load', '{}', now());
        """
    )

    pipeline_sql = _read_sql("export_from_stg_load_to_core.sql")
    db_connection.execute(pipeline_sql)

    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM core_dbms.market_data_5m WHERE symbol = 'NVDA'"
    ).scalar()
    assert cnt == 0


# Suite 3: TRUNCATE stg_raw ------------------------------------------------

@pytest.mark.integration
def test_truncate_stg_raw_empties_tables(db_connection):
    # Insert a row so we have something to truncate
    db_connection.execute(
        """
        INSERT INTO stg_raw.market_data
            (symbol, ts, open, high, low, close, volume, asset_type, source)
        VALUES
            ('AAPL', now(), 1, 1, 1, 1, 1, 'stock', 'test');
        """
    )

    truncate_sql = _read_sql("truncate_data_from_stg_raw.sql")
    db_connection.execute(truncate_sql)

    cnt = db_connection.execute(
        "SELECT COUNT(*) FROM stg_raw.market_data"
    ).scalar()
    assert cnt == 0
