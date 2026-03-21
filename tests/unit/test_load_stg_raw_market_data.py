import datetime as dt
import runpy
import sys
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import historical_csv_data_load as mod


def test_normalize_headers_accepts_any_order_and_case():
    # Given: mixed-case OHLCV headers in a non-standard column order.
    # When: _normalize_headers builds the canonical lowercase-to-original mapping.
    # Then: each required canonical key resolves to its original CSV header name.

    headers = ["Close", "Volume", "Date", "Open", "High", "Low"]
    header_map = mod._normalize_headers(headers)

    assert header_map["date"] == "Date"
    assert header_map["open"] == "Open"
    assert header_map["high"] == "High"
    assert header_map["low"] == "Low"
    assert header_map["close"] == "Close"
    assert header_map["volume"] == "Volume"


def test_normalize_headers_raises_when_required_columns_missing():
    # Given: a header list that omits the required volume column.
    # When: _normalize_headers validates required CSV columns.
    # Then: it raises ValueError and names the missing required field.

    with pytest.raises(ValueError) as exc_info:
        mod._normalize_headers(["date", "open", "high", "low", "close"])

    assert "missing required CSV columns" in str(exc_info.value)
    assert "volume" in str(exc_info.value)


def test_parse_csv_timestamp_interprets_naive_as_market_tz():
    # Given: a naive timestamp string and the configured market timezone.
    # When: _parse_csv_timestamp parses the value.
    # Then: the returned datetime is timezone-aware and stays at local market wall time.

    market_tz = ZoneInfo("America/New_York")
    parsed = mod._parse_csv_timestamp("2026-03-12 09:35:00", market_tz)

    assert parsed.tzinfo is not None
    assert parsed.tzinfo.utcoffset(parsed) == market_tz.utcoffset(parsed)
    assert parsed.hour == 9
    assert parsed.minute == 35


def test_parse_csv_timestamp_converts_utc_suffix_z():
    # Given: an ISO timestamp with a UTC 'Z' suffix.
    # When: _parse_csv_timestamp converts it into market timezone.
    # Then: the local hour/minute match expected New York trading time.

    market_tz = ZoneInfo("America/New_York")
    parsed = mod._parse_csv_timestamp("2026-03-12T14:35:00Z", market_tz)

    assert parsed.tzinfo is not None
    assert parsed.hour == 10
    assert parsed.minute == 35


def test_parse_csv_timestamp_accepts_date_only_format():
    # Given: a date-only CSV value without a clock component.
    # When: _parse_csv_timestamp parses that date in market timezone.
    # Then: it defaults to midnight while preserving timezone awareness.

    market_tz = ZoneInfo("America/New_York")
    parsed = mod._parse_csv_timestamp("2026-03-12", market_tz)

    assert parsed.tzinfo is not None
    assert parsed.hour == 0
    assert parsed.minute == 0


def test_parse_decimal_raises_for_non_numeric_values():
    # Given: a non-numeric text value in a numeric CSV field.
    # When: _parse_decimal attempts Decimal conversion.
    # Then: it raises ValueError indicating the field is not numeric.

    with pytest.raises(ValueError) as exc_info:
        mod._parse_decimal("not-a-number", "close")

    assert "not numeric" in str(exc_info.value)


def test_build_payload_parses_numbers_and_keeps_raw_payload():
    # Given: a valid CSV row plus symbol/source metadata.
    # When: _build_payload constructs a staging payload dictionary.
    # Then: numeric strings become Decimals, ts is parsed, and raw_payload is preserved.

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
    # Given: a CSV containing one valid row and one row with an invalid date.
    # When: _read_csv_payloads runs with skip_invalid_rows=True.
    # Then: it returns only the valid payload and emits a warning for the bad line.

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


def test_read_csv_payloads_raises_when_required_column_is_missing(tmp_path):
    # Given: a CSV file that lacks the required volume column.
    # When: _read_csv_payloads validates headers before row parsing.
    # Then: it raises ValueError describing the missing required column.

    csv_file = tmp_path / "MSFT.csv"
    csv_file.write_text(
        "date,open,high,low,close\n"
        "2026-03-12 09:35:00,100,101,99,100.5\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        mod._read_csv_payloads(
            csv_path=csv_file,
            source="CSV_bulk_load",
            asset_type="stock",
            market_tz=ZoneInfo("America/New_York"),
            skip_invalid_rows=False,
        )

    assert "missing required CSV columns" in str(exc_info.value)
    assert "volume" in str(exc_info.value)


def test_parse_args_requires_csv_dir(monkeypatch):
    # Given: no CSV_PATH environment fallback and no --csv-dir argument.
    # When: _parse_args parses an empty argv list.
    # Then: argparse exits because csv_dir is required.

    monkeypatch.delenv("CSV_PATH", raising=False)

    with pytest.raises(SystemExit):
        mod._parse_args([])


def test_build_runtime_engine_uses_database_url(monkeypatch):
    # Given: DATABASE_URL is configured and create_engine is mocked.
    # When: _build_runtime_engine creates the SQLAlchemy engine.
    # Then: it forwards DATABASE_URL and expected engine options to create_engine.

    captured = {}

    def fake_create_engine(database_url, future=True, pool_pre_ping=True):
        captured["database_url"] = database_url
        captured["future"] = future
        captured["pool_pre_ping"] = pool_pre_ping
        return "engine-from-url"

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost/db")
    monkeypatch.setattr(mod, "create_engine", fake_create_engine)

    engine = mod._build_runtime_engine()

    assert engine == "engine-from-url"
    assert captured["database_url"] == "postgresql+psycopg://u:p@localhost/db"
    assert captured["future"] is True
    assert captured["pool_pre_ping"] is True


def test_parse_decimal_raises_for_empty_values():
    # Given: an empty string in a required numeric column.
    # When: _parse_decimal validates the input value.
    # Then: it raises ValueError indicating that the field is empty.

    with pytest.raises(ValueError) as exc_info:
        mod._parse_decimal("", "close")

    assert "is empty" in str(exc_info.value)


def test_parse_csv_timestamp_raises_for_empty_value():
    # Given: an empty timestamp field from CSV input.
    # When: _parse_csv_timestamp validates and parses the value.
    # Then: it raises ValueError noting the date field is empty.

    with pytest.raises(ValueError) as exc_info:
        mod._parse_csv_timestamp("", ZoneInfo("America/New_York"))

    assert "field 'date' is empty" in str(exc_info.value)


def test_parse_csv_timestamp_raises_for_invalid_format():
    # Given: a malformed timestamp string that cannot be parsed.
    # When: _parse_csv_timestamp attempts datetime conversion.
    # Then: it raises ValueError with an invalid-format message.

    with pytest.raises(ValueError) as exc_info:
        mod._parse_csv_timestamp("not-a-timestamp", ZoneInfo("America/New_York"))

    assert "invalid format" in str(exc_info.value)


def test_read_csv_payloads_raises_when_header_missing(tmp_path):
    # Given: an empty CSV file with no header row.
    # When: _read_csv_payloads initializes DictReader and validates headers.
    # Then: it raises ValueError indicating no header row was found.

    csv_file = tmp_path / "AAPL.csv"
    csv_file.write_text("", encoding="utf-8")

    with pytest.raises(ValueError) as exc_info:
        mod._read_csv_payloads(
            csv_path=csv_file,
            source="CSV_bulk_load",
            asset_type="stock",
            market_tz=ZoneInfo("America/New_York"),
            skip_invalid_rows=False,
        )

    assert "no header row" in str(exc_info.value)


def test_read_csv_payloads_raises_invalid_row_when_not_skipping(tmp_path):
    # Given: a CSV whose first data row has an invalid timestamp.
    # When: _read_csv_payloads runs with skip_invalid_rows=False.
    # Then: it raises immediately and includes file name plus line number context.

    csv_file = tmp_path / "AAPL.csv"
    csv_file.write_text(
        "date,open,high,low,close,volume\n"
        "bad-date,100,101,99,100.5,1000\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        mod._read_csv_payloads(
            csv_path=csv_file,
            source="CSV_bulk_load",
            asset_type="stock",
            market_tz=ZoneInfo("America/New_York"),
            skip_invalid_rows=False,
        )

    assert "AAPL.csv:2" in str(exc_info.value)


def test_upsert_payloads_returns_zero_for_empty_payloads():
    # Given: no payloads to persist.
    # When: _upsert_payloads is called with an empty list.
    # Then: it returns 0 and does not attempt any session execute call.

    class FakeSession:
        def execute(self, stmt):
            raise AssertionError("execute should not be called for empty payloads")

    assert mod._upsert_payloads(FakeSession(), []) == 0


def test_upsert_payloads_falls_back_when_conflict_constraint_missing(capsys):
    # Given: an ON CONFLICT write path that fails due to missing unique constraint.
    # When: _upsert_payloads catches that specific conflict-constraint database error.
    # Then: it rolls back, retries with insert-only behavior, commits, and warns on stderr.

    class FakeSession:
        def __init__(self):
            self.execute_calls = 0
            self.rollback_calls = 0
            self.commit_calls = 0

        def execute(self, stmt):
            self.execute_calls += 1
            if self.execute_calls == 1:
                raise RuntimeError("No unique or exclusion constraint matching the ON CONFLICT specification")
            return None

        def rollback(self):
            self.rollback_calls += 1

        def commit(self):
            self.commit_calls += 1

    payloads = [
        {
            "symbol": "AAPL",
            "ts": dt.datetime(2026, 3, 12, 9, 35, tzinfo=ZoneInfo("America/New_York")),
            "open": Decimal("100"),
            "high": Decimal("101"),
            "low": Decimal("99"),
            "close": Decimal("100.5"),
            "volume": Decimal("1000"),
            "asset_type": "stock",
            "source": "CSV_bulk_load",
            "raw_payload": {"date": "2026-03-12 09:35:00"},
        }
    ]

    session = FakeSession()
    inserted = mod._upsert_payloads(session, payloads)

    captured = capsys.readouterr()
    assert inserted == 1
    assert session.rollback_calls == 1
    assert session.commit_calls == 1
    assert "falling back to INSERT-only" in captured.err


def test_upsert_payloads_reraises_unexpected_database_errors():
    # Given: a session that raises an unexpected runtime database failure.
    # When: _upsert_payloads executes the write statement.
    # Then: the original RuntimeError is re-raised instead of being swallowed.

    class FakeSession:
        def execute(self, stmt):
            raise RuntimeError("db exploded")

        def rollback(self):
            return None

        def commit(self):
            raise AssertionError("commit should not be called on failure")

    payloads = [
        {
            "symbol": "AAPL",
            "ts": dt.datetime(2026, 3, 12, 9, 35, tzinfo=ZoneInfo("America/New_York")),
            "open": Decimal("100"),
            "high": Decimal("101"),
            "low": Decimal("99"),
            "close": Decimal("100.5"),
            "volume": Decimal("1000"),
            "asset_type": "stock",
            "source": "CSV_bulk_load",
            "raw_payload": {"date": "2026-03-12 09:35:00"},
        }
    ]

    with pytest.raises(RuntimeError, match="db exploded"):
        mod._upsert_payloads(FakeSession(), payloads)


def test_main_returns_one_when_csv_dir_missing(tmp_path):
    # Given: a --csv-dir path that does not exist.
    # When: main validates the input directory.
    # Then: it returns exit code 1 to indicate argument/path failure.

    missing_dir = tmp_path / "missing"

    result = mod.main(["--csv-dir", str(missing_dir)])

    assert result == 1


def test_main_returns_one_when_no_files_match(tmp_path):
    # Given: an existing CSV directory that contains no matching CSV files.
    # When: main scans the directory for input files.
    # Then: it returns exit code 1 because there is nothing to process.

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()

    result = mod.main(["--csv-dir", str(csv_dir)])

    assert result == 1


def test_main_returns_two_when_database_connect_fails(tmp_path, monkeypatch):
    # Given: at least one CSV file exists but engine.connect raises an exception.
    # When: main performs the upfront database connectivity check.
    # Then: it returns exit code 2 and still disposes the engine.

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "AAPL.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")

    class FakeEngine:
        def __init__(self):
            self.disposed = False

        def connect(self):
            raise RuntimeError("connect failed")

        def dispose(self):
            self.disposed = True

    engine = FakeEngine()
    monkeypatch.setattr(mod, "_build_runtime_engine", lambda: engine)
    monkeypatch.setattr(mod, "get_session_factory", lambda built_engine: object())

    result = mod.main(["--csv-dir", str(csv_dir)])

    assert result == 2
    assert engine.disposed is True


def test_main_dry_run_processes_rows_and_warnings(tmp_path, monkeypatch, capsys):
    # Given: dry-run mode with mocked parsed payloads and one row warning.
    # When: main processes the file without performing database writes.
    # Then: it returns 0, prints load summary, emits warnings, and disposes the engine.

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "AAPL.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")

    class FakeEngine:
        def __init__(self):
            self.disposed = False

        def connect(self):
            class _Conn:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Conn()

        def dispose(self):
            self.disposed = True

    engine = FakeEngine()
    monkeypatch.setattr(mod, "_build_runtime_engine", lambda: engine)
    monkeypatch.setattr(mod, "get_session_factory", lambda built_engine: object())
    monkeypatch.setattr(
        mod,
        "_read_csv_payloads",
        lambda **kwargs: ([{"symbol": "AAPL"}, {"symbol": "AAPL"}], ["AAPL.csv:3: bad row"]),
    )

    result = mod.main(["--csv-dir", str(csv_dir), "--dry-run"])

    captured = capsys.readouterr()
    assert result == 0
    assert "Loaded 2 rows from AAPL.csv" in captured.out
    assert "rows_loaded=2, warnings=1, failed_files=0" in captured.out
    assert "[warning] AAPL.csv:3: bad row" in captured.err
    assert engine.disposed is True


def test_main_non_dry_run_continues_on_error_when_skip_invalid_rows(tmp_path, monkeypatch):
    # Given: two files where the first fails parsing and --skip-invalid-rows is enabled.
    # When: main iterates through files in non-dry-run ingest mode.
    # Then: it continues to the second file, tracks both file attempts, and exits non-zero.

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "A_BAD.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")
    (csv_dir / "B_GOOD.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")

    class FakeEngine:
        def connect(self):
            class _Conn:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Conn()

        def dispose(self):
            return None

    class FakeSessionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    processed = []

    def fake_read_csv_payloads(**kwargs):
        processed.append(kwargs["csv_path"].name)
        if kwargs["csv_path"].name == "A_BAD.csv":
            raise RuntimeError("bad file")
        return ([{"symbol": "GOOD"}], [])

    monkeypatch.setattr(mod, "_build_runtime_engine", lambda: FakeEngine())
    monkeypatch.setattr(mod, "get_session_factory", lambda built_engine: (lambda: FakeSessionContext()))
    monkeypatch.setattr(mod, "_read_csv_payloads", fake_read_csv_payloads)
    monkeypatch.setattr(mod, "_upsert_payloads", lambda session, payloads: len(payloads))

    result = mod.main(["--csv-dir", str(csv_dir), "--skip-invalid-rows"])

    assert result == 1
    assert processed == ["A_BAD.csv", "B_GOOD.csv"]


def test_main_non_dry_run_breaks_on_first_error_without_skip(tmp_path, monkeypatch):
    # Given: two candidate files but parsing fails on the first one.
    # When: main runs without --skip-invalid-rows.
    # Then: it stops immediately after the first failure and returns exit code 1.

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "A_BAD.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")
    (csv_dir / "B_GOOD.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")

    class FakeEngine:
        def connect(self):
            class _Conn:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Conn()

        def dispose(self):
            return None

    processed = []

    def fake_read_csv_payloads(**kwargs):
        processed.append(kwargs["csv_path"].name)
        raise RuntimeError("bad file")

    monkeypatch.setattr(mod, "_build_runtime_engine", lambda: FakeEngine())
    monkeypatch.setattr(mod, "get_session_factory", lambda built_engine: object())
    monkeypatch.setattr(mod, "_read_csv_payloads", fake_read_csv_payloads)

    result = mod.main(["--csv-dir", str(csv_dir)])

    assert result == 1
    assert processed == ["A_BAD.csv"]


def test_script_entrypoint_invokes_main_and_exits(monkeypatch, tmp_path):
    # Given: the module is executed through its __main__ entrypoint with a missing csv dir.
    # When: runpy runs historical_csv_data_load as a script.
    # Then: the script calls sys.exit with code 1 from main.

    missing_dir = tmp_path / "missing"
    monkeypatch.setattr(sys, "argv", ["historical_csv_data_load.py", "--csv-dir", str(missing_dir)])

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("historical_csv_data_load", run_name="__main__")

    assert exc_info.value.code == 1
