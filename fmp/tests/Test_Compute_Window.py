
import datetime as dt
import pytest
from zoneinfo import ZoneInfo

# Import the collector module.
# If your repo uses a different path/name, adjust this.
# Option 1 (recommended): make scripts a package by adding scripts/__init__.py then:
#   from scripts import Auto_Data_Collection as collector
#
# Option 2: simple import if running from repo root and python can find it:
import scripts.Auto_Data_Collection as collector

# Test 5. Test for Compute window
# Test compute window returns a start and end time aligned to 5-minute boundaries and within the specified boundary

def test_compute_window_aligns_to_5_minute_boundary_and_is_valid():
    tz = ZoneInfo("America/New_York")
    # 15:27:42 should align end to 15:25:00
    now = dt.datetime(2026, 1, 26, 15, 27, 42, tzinfo=tz)

    start, end = collector.compute_window(now, window_min=60)

    assert start.minute == 0 and start.second == 0 and start.microsecond == 0
    assert end.second == 0 and end.microsecond == 0
    assert end.minute % 5 == 0
    assert end > start
    assert (end - start) <= dt.timedelta(minutes=60)
