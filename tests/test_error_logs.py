import sys
from pathlib import Path
import datetime as dt
from zoneinfo import ZoneInfo

# --- make src/ importable ---
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import auto_data_collection as mod  # src/test_error_logs.py


def _setup_isolated_cwd(tmp_path, monkeypatch):
    """Run in a temp working dir so the test doesn't touch real ./logs."""
    monkeypatch.chdir(tmp_path)
    # log_fetch_csv uses global TZ inside the module; ensure it exists.
    mod.TZ = ZoneInfo("America/New_York")
    return mod.TZ


def _read_lines(tmp_path):
    log_file = tmp_path / "logs" / "fetch_data_log.csv"
    return log_file.read_text(encoding="utf-8").splitlines()


def test_log_fetch_csv_creates_file_and_writes_header_and_row(tmp_path, monkeypatch):
    TZ = _setup_isolated_cwd(tmp_path, monkeypatch)

    start = dt.datetime(2026, 2, 3, 9, 0, tzinfo=TZ)
    end   = dt.datetime(2026, 2, 3, 9, 55, tzinfo=TZ)

    mod.log_fetch_csv(
        symbol="AAPL",
        start=start,
        end=end,
        day_from="2026-02-03",
        day_to="2026-02-03",
        api_rows=200,
        filtered_rows=12,
        csv_path="logs/fetch_data_log.csv",
    )

    lines = _read_lines(tmp_path)
    assert len(lines) == 2  # header + 1 row
    assert lines[0] == "run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows"
    assert ",AAPL," in lines[1]
    assert lines[1].endswith(",200,12")


def test_log_fetch_csv_appends_second_row_without_duplicate_header(tmp_path, monkeypatch):
    TZ = _setup_isolated_cwd(tmp_path, monkeypatch)

    start = dt.datetime(2026, 2, 3, 10, 0, tzinfo=TZ)
    end   = dt.datetime(2026, 2, 3, 10, 55, tzinfo=TZ)

    mod.log_fetch_csv(
        symbol="MSFT",
        start=start,
        end=end,
        day_from="2026-02-03",
        day_to="2026-02-03",
        api_rows=150,
        filtered_rows=10,
        csv_path="logs/fetch_data_log.csv",
    )
    mod.log_fetch_csv(
        symbol="MSFT",
        start=start,
        end=end,
        day_from="2026-02-03",
        day_to="2026-02-03",
        api_rows=151,
        filtered_rows=11,
        csv_path="logs/fetch_data_log.csv",
    )

    lines = _read_lines(tmp_path)
    header = "run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows"
    assert lines.count(header) == 1
    assert len(lines) == 3  # header + 2 rows
    assert lines[1].endswith(",150,10")
    assert lines[2].endswith(",151,11")


def test_log_fetch_csv_creates_logs_directory_if_missing(tmp_path, monkeypatch):
    TZ = _setup_isolated_cwd(tmp_path, monkeypatch)

    assert not (tmp_path / "logs").exists()

    start = dt.datetime(2026, 2, 3, 11, 0, tzinfo=TZ)
    end   = dt.datetime(2026, 2, 3, 11, 5, tzinfo=TZ)

    mod.log_fetch_csv(
        symbol="TSLA",
        start=start,
        end=end,
        day_from="2026-02-03",
        day_to="2026-02-03",
        api_rows=1,
        filtered_rows=1,
        csv_path="logs/fetch_data_log.csv",
    )

    assert (tmp_path / "logs").is_dir()
    assert (tmp_path / "logs" / "fetch_data_log.csv").exists()


def test_log_fetch_csv_run_ts_market_is_iso_and_has_tz_offset(tmp_path, monkeypatch):
    TZ = _setup_isolated_cwd(tmp_path, monkeypatch)

    start = dt.datetime(2026, 2, 3, 12, 0, tzinfo=TZ)
    end   = dt.datetime(2026, 2, 3, 12, 5, tzinfo=TZ)

    mod.log_fetch_csv(
        symbol="NVDA",
        start=start,
        end=end,
        day_from="2026-02-03",
        day_to="2026-02-03",
        api_rows=2,
        filtered_rows=2,
        csv_path="logs/fetch_data_log.csv",
    )

    row = _read_lines(tmp_path)[1]
    run_ts_market = row.split(",")[0]

    parsed = dt.datetime.fromisoformat(run_ts_market)
    assert parsed.tzinfo is not None
