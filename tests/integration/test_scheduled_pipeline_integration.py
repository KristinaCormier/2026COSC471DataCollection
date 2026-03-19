import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from src.model.models import (
    DataQualityError,
    DedupConflict,
    IngestError,
    MarketData,
    MarketData5m,
    PipelineWatermark,
)
from utils.scheduled_pipeline import (
    EXPORT_PIPELINE_NAME,
    clear_staging_tables,
    export_staging_to_core,
)


pytestmark = [pytest.mark.integration, pytest.mark.postgres_only]


def _market_row(
    symbol: str,
    ts: dt.datetime,
    close: Decimal | None,
    ingest_time: dt.datetime,
    raw_payload: dict,
) -> MarketData:
    return MarketData(
        symbol=symbol,
        ts=ts,
        open=Decimal("100.0"),
        high=Decimal("101.0"),
        low=Decimal("99.0"),
        close=close,
        volume=Decimal("1200"),
        asset_type="stock",
        source="fmp",
        ingest_time=ingest_time,
        raw_payload=raw_payload,
    )


def test_export_staging_to_core_dedupes_rows_and_logs_quality_issues(db_session):
    # Given: staged rows containing one valid record and one quality-invalid record (`close=None`).
    # When: running `export_staging_to_core` against the integration database session.
    # Then: only the valid row is exported, quality errors are logged, and watermark is updated.

    valid_ts = dt.datetime(2026, 3, 19, 14, 5, tzinfo=dt.timezone.utc)

    valid_row = _market_row(
        symbol="AAPL",
        ts=valid_ts,
        close=Decimal("111.0"),
        ingest_time=dt.datetime(2026, 3, 19, 14, 7, tzinfo=dt.timezone.utc),
        raw_payload={"row": "newer", "bars": [1, 2]},
    )
    invalid_row = _market_row(
        symbol="MSFT",
        ts=dt.datetime(2026, 3, 19, 14, 10, tzinfo=dt.timezone.utc),
        close=None,
        ingest_time=dt.datetime(2026, 3, 19, 14, 11, tzinfo=dt.timezone.utc),
        raw_payload={"row": "invalid"},
    )

    db_session.add_all([valid_row, invalid_row])
    db_session.commit()

    summary = export_staging_to_core(db_session)
    db_session.commit()

    assert summary.processed_rows == 2
    assert summary.duplicate_rows == 0
    assert summary.quality_error_rows == 1
    assert summary.exported_rows == 1

    core_rows = db_session.execute(select(MarketData5m)).scalars().all()
    assert len(core_rows) == 1
    assert core_rows[0].symbol == "AAPL"
    assert core_rows[0].ts == valid_ts
    assert core_rows[0].close == Decimal("111.0")

    duplicate_logs = db_session.execute(select(DedupConflict)).scalars().all()
    assert len(duplicate_logs) == 0

    quality_logs = db_session.execute(select(DataQualityError)).scalars().all()
    assert len(quality_logs) == 1
    assert quality_logs[0].symbol == "MSFT"
    assert quality_logs[0].error_type == "invalid_price"

    watermark = db_session.execute(
        select(PipelineWatermark).where(PipelineWatermark.pipeline_name == EXPORT_PIPELINE_NAME)
    ).scalar_one()
    assert watermark.status == "success"
    assert watermark.last_processed_ts == valid_ts


def test_export_staging_to_core_writes_success_watermark_when_no_rows(db_session):
    # Given: an empty staging table.
    # When: running `export_staging_to_core`.
    # Then: zero rows are processed/exported and a success watermark with null timestamp is recorded.

    summary = export_staging_to_core(db_session)
    db_session.commit()

    assert summary.processed_rows == 0
    assert summary.duplicate_rows == 0
    assert summary.quality_error_rows == 0
    assert summary.exported_rows == 0

    watermark = db_session.execute(
        select(PipelineWatermark).where(PipelineWatermark.pipeline_name == EXPORT_PIPELINE_NAME)
    ).scalar_one()
    assert watermark.status == "success"
    assert watermark.last_processed_ts is None


def test_clear_staging_tables_removes_market_data_and_ingest_errors(db_session):
    # Given: staging tables populated with one market row and one ingest error row.
    # When: executing `clear_staging_tables`.
    # Then: both staging tables are emptied and truncated row count reflects both deletions.

    ts = dt.datetime(2026, 3, 19, 15, 0, tzinfo=dt.timezone.utc)
    db_session.add(
        _market_row(
            symbol="AAPL",
            ts=ts,
            close=Decimal("100.5"),
            ingest_time=dt.datetime(2026, 3, 19, 15, 1, tzinfo=dt.timezone.utc),
            raw_payload={"row": "cleanup"},
        )
    )
    db_session.add(
        IngestError(
            symbol="AAPL",
            ts=ts,
            asset_type="stock",
            source="fmp",
            error_type="LoadError",
            error_detail="test",
            raw_payload={"reason": "cleanup"},
        )
    )
    db_session.commit()

    summary = clear_staging_tables(db_session)
    db_session.commit()

    assert summary.truncated_rows == 2
    assert db_session.scalar(select(func.count()).select_from(MarketData)) == 0
    assert db_session.scalar(select(func.count()).select_from(IngestError)) == 0