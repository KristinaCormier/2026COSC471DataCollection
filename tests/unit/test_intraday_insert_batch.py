import datetime as dt
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src import intraday_data_collection as collector
from models import Base, MarketData


def _create_all_required_schemas(db_engine):
    with db_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg_raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg_transform"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core_dbms"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS operation_logs"))


def test_insert_batch_inserts_rows(db_engine, db_session):
    _create_all_required_schemas(db_engine)
    Base.metadata.create_all(db_engine)

    db_session.query(MarketData).delete()
    db_session.commit()

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

    inserted = collector._insert_batch(db_session, rows, "AAPL")

    assert inserted == 2

    saved = (
        db_session.query(MarketData)
        .filter(MarketData.symbol == "AAPL")
        .order_by(MarketData.ts.asc())
        .all()
    )

    assert len(saved) == 2
    assert saved[0].ts.minute == 5
    assert saved[1].ts.minute == 10
    assert saved[0].close == Decimal("100.500000")
    assert saved[1].volume == Decimal("6000.0000")


def test_insert_batch_upserts_existing_row(db_engine, db_session):
    _create_all_required_schemas(db_engine)
    Base.metadata.create_all(db_engine)

    db_session.query(MarketData).delete()
    db_session.commit()

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

    collector._insert_batch(db_session, first, "AAPL")
    collector._insert_batch(db_session, second, "AAPL")

    saved = (
        db_session.query(MarketData)
        .filter(
            MarketData.symbol == "AAPL",
            MarketData.ts == ts,
            MarketData.source == "FMP_intraday",
        )
        .one()
    )

    assert saved.open == Decimal("110.000000")
    assert saved.high == Decimal("111.000000")
    assert saved.low == Decimal("109.500000")
    assert saved.close == Decimal("110.500000")
    assert saved.volume == Decimal("9000.0000")