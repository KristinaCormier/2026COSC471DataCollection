import datetime as dt
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import intraday_data_collection as collector
from src.model.models import MarketData


def test_insert_batch_inserts_rows():
    """Test that _insert_batch calls session.execute and session.commit correctly."""
    mock_session = MagicMock()
    tz = ZoneInfo("America/New_York")

    rows = [
        MarketData(
            symbol="AAPL",
            ts=dt.datetime(2026, 1, 26, 10, 5, tzinfo=tz),
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=5000.0,
            asset_type="stock",
            source="FMP_intraday",
            raw_payload={"date": "2026-01-26 10:05:00"},
        ),
        MarketData(
            symbol="AAPL",
            ts=dt.datetime(2026, 1, 26, 10, 10, tzinfo=tz),
            open=101.0,
            high=102.0,
            low=100.0,
            close=101.5,
            volume=6000.0,
            asset_type="stock",
            source="FMP_intraday",
            raw_payload={"date": "2026-01-26 10:10:00"},
        ),
    ]

    inserted = collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, rows, "AAPL", tz)

    assert mock_session.execute.called
    assert mock_session.commit.called
    assert inserted == 2


def test_insert_batch_upserts_existing_row():
    """Test that _insert_batch can be called multiple times with same data."""
    mock_session = MagicMock()
    tz = ZoneInfo("America/New_York")
    ts = dt.datetime(2026, 1, 26, 10, 5, tzinfo=tz)

    first = [
        MarketData(
            symbol="AAPL",
            ts=ts,
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=5000.0,
            asset_type="stock",
            source="FMP_intraday",
            raw_payload={"version": 1},
        )
    ]

    second = [
        MarketData(
            symbol="AAPL",
            ts=ts,
            open=110.0,
            high=111.0,
            low=109.5,
            close=110.5,
            volume=9000.0,
            asset_type="stock",
            source="FMP_intraday",
            raw_payload={"version": 2},
        )
    ]

    result1 = collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, first, "AAPL", tz)
    result2 = collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, second, "AAPL", tz)

    assert mock_session.execute.call_count == 2
    assert mock_session.commit.call_count == 2
    assert result1 == 1
    assert result2 == 1


def test_insert_batch_falls_back_to_insert_when_upsert_key_missing():
    """Test fallback path when ON CONFLICT cannot find a matching unique key."""
    mock_session = MagicMock()
    tz = ZoneInfo("America/New_York")

    rows = [
        MarketData(
            symbol="AAPL",
            ts=dt.datetime(2026, 1, 26, 10, 5, tzinfo=tz),
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=5000.0,
            asset_type="stock",
            source="FMP_intraday",
            raw_payload={"date": "2026-01-26 10:05:00"},
        )
    ]

    mock_session.execute.side_effect = [
        RuntimeError(
            "there is no unique or exclusion constraint matching the ON CONFLICT specification"
        ),
        None,
    ]

    inserted = collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, rows, "AAPL", tz)

    assert inserted == 1
    assert mock_session.execute.call_count == 2
    assert mock_session.rollback.call_count == 1
    assert mock_session.commit.call_count == 1
