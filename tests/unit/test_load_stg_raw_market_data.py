import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import load_stg_raw_market_data as mod


def test_normalize_headers_accepts_any_order_and_case():
    headers = ["Close", "Volume", "Date", "Open", "High", "Low"]
    header_map = mod._normalize_headers(headers)

    assert header_map["date"] == "Date"
    assert header_map["open"] == "Open"
    assert header_map["high"] == "High"
    assert header_map["low"] == "Low"
    assert header_map["close"] == "Close"
    assert header_map["volume"] == "Volume"


def test_normalize_headers_raises_when_required_columns_missing():
    with pytest.raises(ValueError) as exc_info:
        mod._normalize_headers(["date", "open", "high", "low", "close"])

    assert "missing required CSV columns" in str(exc_info.value)
    assert "volume" in str(exc_info.value)


def test_parse_csv_timestamp_interprets_naive_as_market_tz():
    market_tz = ZoneInfo("America/New_York")
    parsed = mod._parse_csv_timestamp("2026-03-12 09:35:00", market_tz)

    assert parsed.tzinfo is not None
    assert parsed.tzinfo.utcoffset(parsed) == market_tz.utcoffset(parsed)
    assert parsed.hour == 9
    assert parsed.minute == 35


def test_parse_csv_timestamp_converts_utc_suffix_z():
    market_tz = ZoneInfo("America/New_York")
    parsed = mod._parse_csv_timestamp("2026-03-12T14:35:00Z", market_tz)

    assert parsed.tzinfo is not None
    assert parsed.hour == 10
    assert parsed.minute == 35


def test_build_payload_parses_numbers_and_keeps_raw_payload():
    market_tz = ZoneInfo("America/New_York")
    header_map = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }
    row = {
        "date": "2026-03-12 09:35:00",
        "open": "100.10",
        "high": "101.20",
        "low": "99.80",
        "close": "100.75",
        "volume": "12345",
    }

    payload = mod._build_payload(
        row=row,
        header_map=header_map,
        symbol="AAPL",
        source="CSV_bulk_load",
        asset_type="stock",
        market_tz=market_tz,
    )

    assert payload["symbol"] == "AAPL"
    assert payload["source"] == "CSV_bulk_load"
    assert payload["asset_type"] == "stock"
    assert payload["open"] == Decimal("100.10")
    assert payload["high"] == Decimal("101.20")
    assert payload["low"] == Decimal("99.80")
    assert payload["close"] == Decimal("100.75")
    assert payload["volume"] == Decimal("12345")
    assert isinstance(payload["ts"], dt.datetime)
    assert payload["raw_payload"] == row


def test_read_csv_payloads_can_skip_invalid_rows(tmp_path):
    csv_file = tmp_path / "AAPL.csv"
    csv_file.write_text(
        "date,open,high,low,close,volume\n"
        "2026-03-12 09:35:00,100,101,99,100.5,1000\n"
        "bad-date,100,101,99,100.5,1000\n",
        encoding="utf-8",
    )

    payloads, warnings = mod._read_csv_payloads(
        csv_path=csv_file,
        source="CSV_bulk_load",
        asset_type="stock",
        market_tz=ZoneInfo("America/New_York"),
        skip_invalid_rows=True,
    )

    assert len(payloads) == 1
    assert len(warnings) == 1
    assert "bad-date" in warnings[0]
