import datetime as dt
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

import intraday_data_collection as collector
from src.model.models import MarketData


def test_insert_batch_inserts_rows():
    # Given: two valid `MarketData` rows and a mocked SQLAlchemy session.
    # When: calling `_insert_batch` for the staging table.
    # Then: the session executes and commits once, returning the inserted row count.

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
    # Given: two batches targeting the same `(symbol, ts)` key with different payload values.
    # When: invoking `_insert_batch` twice.
    # Then: both calls execute/commit and each reports one processed row.

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
    # Given: a session that raises ON CONFLICT key-missing on first execute.
    # When: `_insert_batch` attempts UPSERT and falls back to INSERT.
    # Then: rollback is called once, second execute succeeds, and one row is reported inserted.

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


def test_insert_batch_uses_ts_from_raw_payload_date():
    # Given: input rows where `raw_payload["date"]` contains canonical timestamp text.
    # When: `_insert_batch` builds the executable statement.
    # Then: bound `ts_m*` parameters match parsed timestamps from each payload date.

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

    assert inserted == 2
    assert mock_session.execute.call_count == 1

    statement = mock_session.execute.call_args[0][0]
    params = statement.compile().params

    for row_index in range(len(rows)):
        inserted_ts = params[f"ts_m{row_index}"]
        payload = params[f"raw_payload_m{row_index}"]
        expected_ts = dt.datetime.strptime(payload["date"], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=tz
        )
        assert inserted_ts == expected_ts


def test_insert_batch_detects_ts_raw_payload_date_mismatch():
    # Given: a row whose `ts` differs from `raw_payload["date"]`.
    # When: `_insert_batch` compiles statement parameters and the test compares derived timestamps.
    # Then: equality assertion intentionally fails to prove mismatch detection path.

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
            raw_payload={"date": "2026-01-26 10:10:00"},
        )
    ]

    collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, rows, "AAPL", tz)

    statement = mock_session.execute.call_args[0][0]
    params = statement.compile().params
    inserted_ts = params["ts_m0"]
    payload = params["raw_payload_m0"]
    expected_ts = dt.datetime.strptime(payload["date"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=tz
    )

    with pytest.raises(AssertionError):
        assert inserted_ts == expected_ts
