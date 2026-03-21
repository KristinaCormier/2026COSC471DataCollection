"""
Unit Tests for gather_past_data Module

Purpose:
    Test argument parsing, date validation, and window computation for the historical
    backfill CLI without requiring a database or API connection.

Scope:
    - CLI argument parsing (--from-date, --to-date, --symbols)
    - Date validation (no future dates, from-date <= to-date)
    - Time window computation for day ranges
    - No external API calls or database operations

Author: Data Collection Team
License: MIT
"""

import argparse
import datetime as dt
import runpy
import sys
from zoneinfo import ZoneInfo

import pytest

import gather_past_data as gather


@pytest.fixture(autouse=True)
def _disable_dotenv(monkeypatch):
    monkeypatch.setattr(gather, "load_dotenv", lambda: None)


def test_parse_iso_date_accepts_valid_value():
    # Given: a valid ISO-8601 date string.
    # When: parsing it with `_parse_iso_date`.
    # Then: a matching `datetime.date` object is returned.

    parsed = gather._parse_iso_date("2026-01-15")
    assert parsed == dt.date(2026, 1, 15)


def test_parse_iso_date_rejects_invalid_value():
    # Given: a non-ISO date string format.
    # When: parsing with `_parse_iso_date`.
    # Then: `argparse.ArgumentTypeError` is raised.

    with pytest.raises(argparse.ArgumentTypeError):
        gather._parse_iso_date("01-15-2026")


def test_parse_args_reads_dates_and_symbol_override():
    # Given: CLI arguments with explicit from/to dates and a symbol override list.
    # When: parsing with `_parse_args`.
    # Then: parsed namespace contains expected date objects and symbols string.

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
    # Given: a historical range where `from_date` is later than `to_date`.
    # When: validating range bounds with `_validate_historical_range`.
    # Then: a ValueError is raised for invalid ordering.

    today = dt.date(2026, 3, 11)

    with pytest.raises(ValueError, match="on or before"):
        gather._validate_historical_range(
            dt.date(2026, 1, 10),
            dt.date(2026, 1, 9),
            today,
        )


def test_validate_historical_range_rejects_today_or_future():
    # Given: a range whose end date is today/future relative to the supplied `today` value.
    # When: validating with `_validate_historical_range`.
    # Then: a ValueError is raised to enforce historical-only backfill windows.

    today = dt.date(2026, 3, 11)

    with pytest.raises(ValueError, match="earlier than today"):
        gather._validate_historical_range(
            dt.date(2026, 3, 10),
            dt.date(2026, 3, 11),
            today,
        )


def test_compute_historical_window_spans_full_days():
    # Given: two date boundaries and a market timezone.
    # When: computing the backfill datetime window.
    # Then: start is midnight of from-date and end is 23:59:59 of to-date.

    tz = ZoneInfo("America/New_York")

    start, end = gather._compute_historical_window(
        dt.date(2026, 1, 1),
        dt.date(2026, 1, 2),
        tz,
    )

    assert start == dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
    assert end == dt.datetime(2026, 1, 2, 23, 59, 59, tzinfo=tz)


def test_main_exits_when_symbols_override_has_no_values(monkeypatch, capsys):
    # Given: CLI symbols argument that resolves to an empty list after trimming.
    # When: running `gather.main`.
    # Then: the CLI exits with code 1 and prints a no-valid-symbols error.

    with pytest.raises(SystemExit) as exc_info:
        gather.main(["--from-date", "2020-01-01", "--to-date", "2020-01-02", "--symbols", " , "])

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "no valid symbols" in captured.err


def test_main_exits_on_invalid_historical_range(monkeypatch, capsys):
    # Given: CLI dates where from-date is after to-date.
    # When: executing `gather.main`.
    # Then: it exits with code 1 and emits an invalid-range message.

    monkeypatch.setenv("FMP_API_KEY", "test-key")

    with pytest.raises(SystemExit) as exc_info:
        gather.main(["--from-date", "2020-01-03", "--to-date", "2020-01-02"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "invalid historical range" in captured.err


def test_main_exits_when_api_key_missing(monkeypatch, capsys):
    # Given: missing `FMP_API_KEY` in environment.
    # When: running the backfill entrypoint.
    # Then: execution stops with `SystemExit(1)` and an API-key error.

    monkeypatch.delenv("FMP_API_KEY", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        gather.main(["--from-date", "2020-01-01", "--to-date", "2020-01-02"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "API key is missing" in captured.err


def test_main_logs_and_exits_when_database_connect_fails(monkeypatch, capsys):
    # Given: database engine creation patched to raise a runtime connection failure.
    # When: calling `gather.main`.
    # Then: it logs CONNECT failure details and exits with code 2.

    logged_errors = []

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setattr(gather, "get_engine", lambda *args: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(gather.lu, "log_db_error", lambda **kwargs: logged_errors.append(kwargs))

    with pytest.raises(SystemExit) as exc_info:
        gather.main(["--from-date", "2020-01-01", "--to-date", "2020-01-02"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "cannot connect to database" in captured.err
    assert logged_errors
    assert logged_errors[0]["operation"] == "CONNECT"


def test_main_processes_symbols_handles_symbol_error_and_close_warning(monkeypatch, capsys):
    # Given: two symbols where one API request fails and session close raises.
    # When: running `gather.main` across both symbols.
    # Then: processing continues for valid symbol, errors are surfaced, and close warning is printed.

    class FakeSession:
        def close(self):
            raise RuntimeError("close failed")

    fake_session = FakeSession()
    insert_calls = []

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("SYMBOLS", "AAPL,MSFT")
    monkeypatch.setattr(gather, "get_engine", lambda *args: object())
    monkeypatch.setattr(gather, "init_db", lambda engine: None)
    monkeypatch.setattr(gather, "get_session_factory", lambda engine: (lambda: fake_session))

    def fake_fetch(symbol, start, end, api_key, tz):
        if symbol == "MSFT":
            raise RuntimeError("API failure")
        return [{"date": "2020-01-01 09:30:00"}, {"date": "2020-01-01 09:35:00"}]

    monkeypatch.setattr(gather, "_fetch_api_data", fake_fetch)
    monkeypatch.setattr(gather, "_construct_source_url", lambda *args: "https://example")
    monkeypatch.setattr(gather, "_process_data_batch", lambda *args: [{"ts": "row"}])

    def fake_insert(session, table_name, rows, symbol, tz):
        insert_calls.append((table_name, len(rows), symbol))
        return 3

    monkeypatch.setattr(gather, "_insert_batch", fake_insert)

    gather.main(["--from-date", "2020-01-01", "--to-date", "2020-01-02"])

    captured = capsys.readouterr()
    assert "API returned rows:" in captured.out
    assert "[done] total rows ingested: 3" in captured.out
    assert "[error] MSFT: API failure" in captured.err
    assert "failed to close database session" in captured.err
    assert insert_calls == [(gather.STAGING_TABLE_NAME, 1, "AAPL")]


def test_main_prints_no_bars_when_processed_rows_empty(monkeypatch, capsys):
    # Given: mocked fetch/process steps that produce zero rows.
    # When: running the CLI main flow.
    # Then: output reports no bars for window and zero total rows ingested.

    class FakeSession:
        def close(self):
            return None

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setattr(gather, "get_engine", lambda *args: object())
    monkeypatch.setattr(gather, "init_db", lambda engine: None)
    monkeypatch.setattr(gather, "get_session_factory", lambda engine: (lambda: FakeSession()))
    monkeypatch.setattr(gather, "_fetch_api_data", lambda *args: [])
    monkeypatch.setattr(gather, "_construct_source_url", lambda *args: "https://example")
    monkeypatch.setattr(gather, "_process_data_batch", lambda *args: [])
    monkeypatch.setattr(gather, "_insert_batch", lambda *args: 0)

    gather.main(["--from-date", "2020-01-01", "--to-date", "2020-01-02"])

    captured = capsys.readouterr()
    assert "(no 5 minute bars in this window)" in captured.out
    assert "[done] total rows ingested: 0" in captured.out


def test_script_entrypoint_executes_main(monkeypatch):
    # Given: script-style argv and missing API key.
    # When: executing module via `runpy` as `__main__`.
    # Then: entrypoint path calls `main` and exits with status code 1.

    monkeypatch.setenv("FMP_API_KEY", "")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "gather_past_data.py",
            "--from-date",
            "2020-01-01",
            "--to-date",
            "2020-01-02",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("gather_past_data", run_name="__main__")

    assert exc_info.value.code == 1
