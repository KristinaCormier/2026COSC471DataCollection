"""
Time manipulation and formatting utilities for stock data collection.
Handles timezone operations, timestamp parsing, and window calculations.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo


def current_hour(now: dt.datetime) -> dt.datetime:
    """Return the top of the current hour (minute, second, microsecond set to 0)."""
    return now.replace(minute=0, second=0, microsecond=0)


def parse_api_time(s: str, tz: ZoneInfo) -> dt.datetime:
    """Parse API timestamp string and attach timezone."""
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)


def ymd(d: dt.date) -> str:
    """Format date as YYYY-MM-DD string."""
    return d.strftime("%Y-%m-%d")


def align_to_5_minute(ts: dt.datetime) -> dt.datetime:
    """Align timestamp down to the nearest 5-minute boundary."""
    ts = ts.replace(second=0, microsecond=0)
    return ts - dt.timedelta(minutes=ts.minute % 5)


def infer_timestamp(
    last_ts: dt.datetime | None,
    now_local: dt.datetime | None,
    reason_prefix: str,
    tz: ZoneInfo,
) -> tuple[dt.datetime, str]:
    """
    Infer a missing timestamp using previous row or wall clock time.
    
    Args:
        last_ts: Previous row's timestamp (if available)
        now_local: Current wall clock time (if available)
        reason_prefix: Prefix for the reason string
        tz: Timezone to use for wall clock fallback
    
    Returns:
        Tuple of (inferred_timestamp, reason_string)
    """
    if last_ts is not None:
        return last_ts + dt.timedelta(minutes=5), f"{reason_prefix}_prev_row"
    if now_local is None:
        now_local = dt.datetime.now(tz)
    return align_to_5_minute(now_local), f"{reason_prefix}_wall_clock"


def compute_window(now_local: dt.datetime, window_min: int) -> tuple[dt.datetime, dt.datetime]:
    """
    Compute the time window for data collection.
    
    Returns (start, end) where:
      - start is top of the hour
      - end is aligned down to nearest 5-minute boundary
      - end is capped by window_min
      - end is always > start (at least 5 minutes)
    """
    start = current_hour(now_local)
    end = now_local.replace(second=0, microsecond=0)
    end = end - dt.timedelta(minutes=end.minute % 5)
    end = min(end, start + dt.timedelta(minutes=window_min))
    if end < start + dt.timedelta(minutes=5):
        end = start + dt.timedelta(minutes=5)
    return start, end
