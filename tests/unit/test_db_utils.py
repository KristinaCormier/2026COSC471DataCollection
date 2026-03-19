import pytest

from utils import db_utils as dbu


pytestmark = pytest.mark.unit


def test_safe_table_name_for_symbol_basic():
    # Given: A valid stock symbol in uppercase
    symbol = "AAPL"

    # When: Converting the symbol to a safe table name
    result = dbu.safe_table_name_for_symbol(symbol)

    # Then: The result should be lowercase with market schema prefix
    assert result == "market.aapl"


def test_safe_table_name_for_symbol_strips_non_alnum():
    # Given: A symbol with non-alphanumeric characters
    symbol = "^TNX"

    # When: Converting the symbol to a safe table name
    result = dbu.safe_table_name_for_symbol(symbol)

    # Then: Non-alphanumeric characters should be stripped
    assert result == "market.tnx"


def test_safe_table_name_for_symbol_invalid_raises():
    # Given: a symbol containing no valid alphanumeric characters.
    # When: converting it with `safe_table_name_for_symbol`.
    # Then: the function raises `ValueError` instead of producing a table name.

    # Given: A symbol containing only invalid characters
    invalid_symbol = "$$$"

    # When/Then: Converting the invalid symbol should raise ValueError
    with pytest.raises(ValueError):
        dbu.safe_table_name_for_symbol(invalid_symbol)


def test_build_upsert_statement_contains_expected_conflict_clause():
    # Given: the canonical staging table name.
    # When: building an UPSERT statement with `_build_upsert_statement`.
    # Then: SQL includes insert target, ON CONFLICT update behavior, and payload update fields.

    statement = dbu._build_upsert_statement("stg_raw.market_data")

    assert "INSERT INTO stg_raw.market_data" in statement
    assert "ON CONFLICT (symbol, ts) DO UPDATE" in statement
    assert "raw_payload = EXCLUDED.raw_payload" in statement


def test_build_insert_statement_contains_do_nothing_and_returning():
    # Given: the canonical staging table name.
    # When: building an INSERT-only statement with `_build_insert_statement`.
    # Then: SQL includes DO NOTHING conflict handling and a RETURNING clause.

    statement = dbu._build_insert_statement("stg_raw.market_data")

    assert "INSERT INTO stg_raw.market_data" in statement
    assert "ON CONFLICT (symbol, ts) DO NOTHING" in statement
    assert "RETURNING ts" in statement


def test_check_table_exists_passes_when_table_is_present():
    # Given: a stub connection whose cursor reports that the table exists.
    # When: calling `check_table_exists` for `stg_raw.market_data`.
    # Then: the function completes without raising.

    class Cursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchone(self):
            return (1,)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

    dbu.check_table_exists(Connection(), "stg_raw.market_data")


def test_check_table_exists_raises_when_table_missing():
    # Given: a stub connection whose cursor reports no matching table.
    # When: calling `check_table_exists`.
    # Then: a RuntimeError is raised indicating the table does not exist.

    class Cursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchone(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

    with pytest.raises(RuntimeError, match="does not exist"):
        dbu.check_table_exists(Connection(), "stg_raw.market_data")


def test_db_connect_forwards_connection_parameters(monkeypatch):
    # Given: a patched psycopg `connect` function that records keyword arguments.
    # When: invoking `db_connect` with explicit connection settings.
    # Then: all settings are forwarded unchanged and the wrapped connection is returned.

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "connection"

    monkeypatch.setattr(dbu.psycopg, "connect", fake_connect)

    conn = dbu.db_connect(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="tester",
        password="secret",
    )

    assert conn == "connection"
    assert captured == {
        "host": "localhost",
        "port": 5432,
        "dbname": "test_db",
        "user": "tester",
        "password": "secret",
    }