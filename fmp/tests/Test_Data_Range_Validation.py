
import datetime as dt
import pytest
from zoneinfo import ZoneInfo

# Import the collector module.
# If your repo uses a different path/name, adjust this.
# Option 1 (recommended): make scripts a package by adding scripts/__init__.py the$
#   from scripts import Auto_Data_Collection as collector
#
# Option 2: simple import if running from repo root and python can find it:
import scripts.Auto_Data_Collection as collector

#Test 10 Vallidation rules for expected data ranges. US-10 Sub-Issue: #29
#Validation rules for expected data ranges and formats (prices > 0, valid timestamps)

def test_validate_bar_rejects_non_positive_close():
    tz = ZoneInfo("America/New_York")
    ts = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)

    ok, reason = collector.validate_bar(ts, 1, 2, 1, 0, 100)
    assert not ok
    assert reason == "close_not_positive"

