import datetime as dt
from decimal import Decimal

import pytest

from src.model.models import MarketData
from utils import scheduled_pipeline as sp


pytestmark = pytest.mark.unit


def test_json_safe_handles_decimal_datetime_dict_and_list():
    # Given: a nested payload containing Decimal values and datetimes.
    # When: normalizing the payload with `_json_safe`.
    # Then: numeric/date-like values are converted to JSON-safe string representations recursively.

    ts = dt.datetime(2026, 3, 19, 10, 5, tzinfo=dt.timezone.utc)
    payload = {
        "price": Decimal("101.25"),
        "ts": ts,
        "nested": {"value": Decimal("3.14")},
        "series": [Decimal("1"), ts],
    }

    converted = sp._json_safe(payload)

    assert converted["price"] == "101.25"
    assert converted["ts"] == ts.isoformat()
    assert converted["nested"]["value"] == "3.14"
    assert converted["series"] == ["1", ts.isoformat()]


def test_serialize_market_data_row_includes_expected_fields():
    # Given: a populated `MarketData` ORM row.
    # When: serializing it with `serialize_market_data_row`.
    # Then: the output dictionary includes expected fields and converted values used by logging/export flows.

    row = MarketData(
        ingest_id=42,
        symbol="AAPL",
        ts=dt.datetime(2026, 3, 19, 10, 5, tzinfo=dt.timezone.utc),
        open=Decimal("100.0"),
        high=Decimal("101.0"),
        low=Decimal("99.5"),
        close=Decimal("100.5"),
        volume=Decimal("1200"),
        asset_type="stock",
        source="fmp",
        ingest_time=dt.datetime(2026, 3, 19, 10, 6, tzinfo=dt.timezone.utc),
        raw_payload={"bars": [1, 2, 3]},
    )

    serialized = sp.serialize_market_data_row(row)

    assert serialized["ingest_id"] == 42
    assert serialized["symbol"] == "AAPL"
    assert serialized["open"] == "100.0"
    assert serialized["volume"] == "1200"
    assert serialized["asset_type"] == "stock"
    assert serialized["source"] == "fmp"
    assert serialized["raw_payload"] == {"bars": [1, 2, 3]}