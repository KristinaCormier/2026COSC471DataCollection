"""
Application logging utilities for stock data collection.
Handles CSV-based logging of fetch operations and data quality metrics.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path


def log_fetch_csv(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime,
    day_from: str,
    day_to: str,
    api_rows: int,
    filtered_rows: int,
    csv_path: str,
    tz: dt.tzinfo,
) -> None:
    """
    Append a fetch operation log entry to a CSV file.
    
    Creates the file and header if it doesn't exist.
    
    Args:
        symbol: Stock symbol
        start: Window start timestamp
        end: Window end timestamp
        day_from: API query start date
        day_to: API query end date
        api_rows: Number of rows returned by API
        filtered_rows: Number of rows after filtering
        csv_path: Path to the log file
        tz: Timezone for run timestamp
    """
    log_file = Path(csv_path)
    log_dir = log_file.parent
    if str(log_dir):
        log_dir.mkdir(parents=True, exist_ok=True)

    header = "run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows\n"
    run_ts = dt.datetime.now(tz).isoformat()

    line = f"{run_ts},{symbol},{start.isoformat()},{end.isoformat()},{day_from},{day_to},{api_rows},{filtered_rows}\n"

    if not log_file.exists():
        log_file.write_text(header, encoding="utf-8")
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line)

    print(f"fetch log appended: {log_file}")
