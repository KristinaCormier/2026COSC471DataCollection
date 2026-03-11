import argparse
import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src import gather_past_data as gather


def test_parse_iso_date_accepts_valid_value():
    parsed = gather._parse_iso_date("2026-01-15")
    assert parsed == dt.date(2026, 1, 15)


def test_parse_iso_date_rejects_invalid_value():
    with pytest.raises(argparse.ArgumentTypeError):
        gather._parse_iso_date("01-15-2026")


def test_parse_args_reads_dates_and_symbol_override():
    args = gather._parse_args(
        [
            "--from-date",
            "2026-01-01",
            "--to-date",
            "2026-01-05",
            "--symbols",
            "AAPL,MSFT",
        ]
    )

    assert args.from_date == dt.date(2026, 1, 1)
    assert args.to_date == dt.date(2026, 1, 5)
    assert args.symbols == "AAPL,MSFT"


def test_validate_historical_range_rejects_out_of_order_dates():
    today = dt.date(2026, 3, 11)

    with pytest.raises(ValueError, match="on or before"):
        gather._validate_historical_range(
            dt.date(2026, 1, 10),
            dt.date(2026, 1, 9),
            today,
        )


def test_validate_historical_range_rejects_today_or_future():
    today = dt.date(2026, 3, 11)

    with pytest.raises(ValueError, match="earlier than today"):
        gather._validate_historical_range(
            dt.date(2026, 3, 10),
            dt.date(2026, 3, 11),
            today,
        )


def test_compute_historical_window_spans_full_days():
    tz = ZoneInfo("America/New_York")

    start, end = gather._compute_historical_window(
        dt.date(2026, 1, 1),
        dt.date(2026, 1, 2),
        tz,
    )

    assert start == dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
    assert end == dt.datetime(2026, 1, 2, 23, 59, 59, tzinfo=tz)
