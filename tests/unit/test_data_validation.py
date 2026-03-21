import pytest

from utils import data_validation as dv


pytestmark = pytest.mark.unit


def test_is_empty_recognizes_none_and_blank_strings():
    # Given: `None`, an empty string, and a whitespace-only string.
    # When: calling `is_empty` for each value.
    # Then: each value is classified as empty.

    assert dv.is_empty(None) is True
    assert dv.is_empty("") is True
    assert dv.is_empty("   ") is True


def test_is_empty_leaves_non_empty_values_as_not_empty():
    # Given: representative non-empty values (text, integer zero, boolean false).
    # When: calling `is_empty` for each value.
    # Then: each value is classified as not empty.

    assert dv.is_empty("AAPL") is False
    assert dv.is_empty(0) is False
    assert dv.is_empty(False) is False


def test_analyze_row_reports_missing_fields_and_not_all_empty():
    # Given: a row where only `close` and `volume` are blank.
    # When: analyzing required OHLCV fields with `analyze_row`.
    # Then: only those two fields are reported missing and `all_empty` is false.

    row = {
        "date": "2026-03-19 10:05:00",
        "open": "100.0",
        "high": "101.0",
        "low": "99.0",
        "close": "",
        "volume": "",
    }

    missing_fields, all_empty = dv.analyze_row(
        row,
        ("date", "open", "high", "low", "close", "volume"),
    )

    assert sorted(missing_fields) == ["close", "volume"]
    assert all_empty is False


def test_analyze_row_detects_when_all_expected_fields_are_empty():
    # Given: a row where every required field is empty or blank.
    # When: analyzing the row with `analyze_row`.
    # Then: all required fields are flagged missing and `all_empty` is true.

    row = {
        "date": "",
        "open": None,
        "high": "   ",
        "low": "",
        "close": None,
        "volume": "",
    }

    missing_fields, all_empty = dv.analyze_row(
        row,
        ("date", "open", "high", "low", "close", "volume"),
    )

    assert sorted(missing_fields) == ["close", "date", "high", "low", "open", "volume"]
    assert all_empty is True