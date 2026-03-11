import pytest

from src import db_utils as dbu


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
    # Given: A symbol containing only invalid characters
    invalid_symbol = "$$$"

    # When/Then: Converting the invalid symbol should raise ValueError
    with pytest.raises(ValueError):
        dbu.safe_table_name_for_symbol(invalid_symbol)