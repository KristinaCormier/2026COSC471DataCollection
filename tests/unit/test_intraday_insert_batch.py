import datetime as dt
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

import intraday_data_collection as collector
from src.model.models import MarketData


def _row(ts: dt.datetime) -> MarketData:
    return MarketData(
        symbol="AAPL",
        ts=ts,
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=5000.0,
        asset_type="stock",
        source="FMP_intraday",
        raw_payload={"date": ts.strftime("%Y-%m-%d %H:%M:%S")},
    )


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


def test_insert_batch_logs_and_raises_when_conflict_target_missing(monkeypatch):
    # Given: a session that raises ON CONFLICT key-missing on execute.
    # When: `_insert_batch` executes the insert.
    # Then: rollback is called, INSERT error is logged, and the exception is re-raised.

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

    mock_session.execute.side_effect = RuntimeError(
        "there is no unique or exclusion constraint matching the ON CONFLICT specification"
    )

    logged = []
    monkeypatch.setattr(collector.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    with pytest.raises(RuntimeError, match="no unique or exclusion constraint"):
        collector._insert_batch(mock_session, collector.STAGING_TABLE_NAME, rows, "AAPL", tz)

    assert mock_session.execute.call_count == 1
    assert mock_session.rollback.call_count == 1
    assert mock_session.commit.call_count == 0
    assert logged
    assert logged[0]["operation"] == "INSERT"


def test_insert_batch_uses_ts_from_raw_payload_date():
    # Given: input rows where `raw_payload["date"]` contains canonical timestamp text.
    # When: `_insert_batch` builds the executable statement.
    # Then: each statement uses row `ts`/`raw_payload` values as provided.

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
    assert mock_session.execute.call_count == len(rows)

    for row, call in zip(rows, mock_session.execute.call_args_list):
        statement = call[0][0]
        params = statement.compile().params
        assert params["ts"] == row.ts
        assert params["raw_payload"] == row.raw_payload


def test_insert_batch_detects_ts_raw_payload_date_mismatch():
    # Given: a row whose `ts` differs from `raw_payload["date"]`.
    # When: `_insert_batch` compiles statement parameters and the test compares derived timestamps.
    # Then: inserted `ts` remains independent of payload `date` text.

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
    inserted_ts = params["ts"]
    payload = params["raw_payload"]
    expected_ts = dt.datetime.strptime(payload["date"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=tz
    )

    with pytest.raises(AssertionError):
        assert inserted_ts == expected_ts


def test_insert_batch_stop_on_conflict_stops_at_first_existing_row(monkeypatch):
    # Given: rows processed newest-first where the second row conflicts.
    # When: inserting with `stop_on_conflict=True`.
    # Then: insertion stops at first conflict and returns only inserted rows before conflict.

    mock_session = MagicMock()
    tz = ZoneInfo("America/New_York")

    rows = [
        _row(dt.datetime(2026, 1, 26, 10, 15, tzinfo=tz)),
        _row(dt.datetime(2026, 1, 26, 10, 10, tzinfo=tz)),
        _row(dt.datetime(2026, 1, 26, 10, 5, tzinfo=tz)),
    ]

    first_result = MagicMock()
    first_result.rowcount = 1
    second_result = MagicMock()
    second_result.rowcount = 0
    mock_session.execute.side_effect = [first_result, second_result]

    logged = []
    monkeypatch.setattr(collector.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    inserted = collector._insert_batch(
        mock_session,
        collector.STAGING_TABLE_NAME,
        rows,
        "AAPL",
        tz,
        stop_on_conflict=True,
    )

    assert inserted == 1
    assert mock_session.execute.call_count == 2
    assert mock_session.commit.call_count == 2
    assert logged
    assert logged[0]["operation"] == "INSERT_STOP_ON_CONFLICT"


def test_insert_batch_stop_on_conflict_logs_and_raises_insert_error(monkeypatch):
    # Given: row-level insert raises an exception in stop-on-conflict mode.
    # When: `_insert_batch` is called.
    # Then: rollback occurs, INSERT error is logged, and the exception is re-raised.

    mock_session = MagicMock()
    tz = ZoneInfo("America/New_York")
    rows = [_row(dt.datetime(2026, 1, 26, 10, 15, tzinfo=tz))]

    mock_session.execute.side_effect = RuntimeError("db exploded")

    logged = []
    monkeypatch.setattr(collector.lu, "log_db_error", lambda **kwargs: logged.append(kwargs))

    with pytest.raises(RuntimeError, match="db exploded"):
        collector._insert_batch(
            mock_session,
            collector.STAGING_TABLE_NAME,
            rows,
            "AAPL",
            tz,
            stop_on_conflict=True,
        )

    assert mock_session.rollback.call_count == 1
    assert logged
    assert logged[0]["operation"] == "INSERT"
