
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


#Test 2. Test for safe table symbol strips.
# Non- alphanumeric characters should be stripped from symbols.

def test_safe_table_name_for_symbol_strips_non_alnum():
    # Example of symbols like indexes: ^TNX, ^IRX, etc.
    assert collector.safe_table_name_for_symbol("^TNX") == "market.tnx"
