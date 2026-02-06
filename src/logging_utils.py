from __future__ import annotations
import datetime as dt
from pathlib import Path


def log_fetch_csv(symbol: str, start: dt.datetime, end: dt.datetime, day_from: str, day_to: str,
                  api_rows: int, filtered_rows: int, csv_path: str, tz: dt.tzinfo):
    """
    Appends a single-line fetch log to a CSV file.
    Creates the file + header if it doesn't exist.
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "fetch_data_log.csv"

    header = "run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows\n"
    run_ts = dt.datetime.now(tz).isoformat()

    line = f"{run_ts},{symbol},{start.isoformat()},{end.isoformat()},{day_from},{day_to},{api_rows},{filtered_rows}\n"

    if not log_file.exists():
        log_file.write_text(header, encoding="utf-8")
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line)

    # optional console line so you can see where it went
    print(f"fetch log appended: {log_file}")