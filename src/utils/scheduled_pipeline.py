from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Iterator, Sequence

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from model.models import (
    DataQualityError,
    DedupConflict,
    IngestError,
    MarketData,
    MarketData5m,
    PipelineWatermark,
)


STOCK_ASSET_TYPE: Final = "stock"
EXPORT_PIPELINE_NAME: Final = "export_stg_to_core"
MAX_POSTGRESQL_BIND_PARAMS: Final = 65535
MARKET_DATA_5M_INSERT_COLUMNS: Final = 9
EXPORT_UPSERT_BATCH_SIZE: Final = MAX_POSTGRESQL_BIND_PARAMS // MARKET_DATA_5M_INSERT_COLUMNS


@dataclass(frozen=True)
class PipelineSummary:
    processed_rows: int = 0
    duplicate_rows: int = 0
    quality_error_rows: int = 0
    exported_rows: int = 0
    truncated_rows: int = 0


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def serialize_market_data_row(row: MarketData) -> dict[str, Any]:
    return {
        "ingest_id": row.ingest_id,
        "symbol": row.symbol,
        "ts": _json_safe(row.ts),
        "open": _json_safe(row.open),
        "high": _json_safe(row.high),
        "low": _json_safe(row.low),
        "close": _json_safe(row.close),
        "volume": _json_safe(row.volume),
        "asset_type": row.asset_type,
        "source": row.source,
        "ingest_time": _json_safe(row.ingest_time),
        "raw_payload": _json_safe(row.raw_payload),
    }


def classify_quality_issue(row: MarketData) -> str | None:
    if row.close is None or row.close <= 0:
        return "invalid_price"
    if row.open is None or row.high is None or row.low is None:
        return "incomplete_ohlc"
    if row.volume is None or row.volume < 0:
        return "invalid_volume"
    if row.ts is None:
        return "missing_timestamp"
    return None


def dedupe_staging_rows(rows: Sequence[MarketData]) -> tuple[list[MarketData], list[DedupConflict]]:
    winners: list[MarketData] = []
    duplicate_logs: list[DedupConflict] = []
    winning_rows: dict[tuple[str | None, dt.datetime | None], MarketData] = {}

    for row in rows:
        key = (row.symbol, row.ts)
        winner = winning_rows.get(key)
        if winner is None:
            winning_rows[key] = row
            winners.append(row)
            continue

        duplicate_logs.append(
            DedupConflict(
                symbol=row.symbol,
                ts=row.ts,
                existing_row=serialize_market_data_row(winner),
                incoming_row=serialize_market_data_row(row),
                resolution="discarded_duplicate_in_staging",
            )
        )

    return winners, duplicate_logs


def build_export_payloads(
    rows: Sequence[MarketData],
) -> tuple[list[dict[str, Any]], list[DataQualityError]]:
    export_payloads: list[dict[str, Any]] = []
    quality_errors: list[DataQualityError] = []

    for row in rows:
        quality_issue = classify_quality_issue(row)
        if quality_issue is not None:
            quality_errors.append(
                DataQualityError(
                    symbol=row.symbol,
                    ts=row.ts,
                    error_type=quality_issue,
                    error_detail=json.dumps(
                        {
                            "quality_issue": quality_issue,
                            "row": serialize_market_data_row(row),
                        },
                        sort_keys=True,
                    ),
                )
            )
            continue

        export_payloads.append(
            {
                "symbol": row.symbol,
                "ts": row.ts,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": int(row.volume) if row.volume is not None else None,
                "asset_type": row.asset_type,
                "source": row.source,
            }
        )

    return export_payloads, quality_errors


def iter_batches(items: Sequence[dict[str, Any]], batch_size: int) -> Iterator[Sequence[dict[str, Any]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def export_staging_to_core(session: Session) -> PipelineSummary:
    staging_rows = session.execute(
        select(MarketData)
        .where(MarketData.asset_type == STOCK_ASSET_TYPE)
        .order_by(
            MarketData.symbol.asc(),
            MarketData.ts.asc(),
            MarketData.ingest_time.desc().nulls_last(),
            MarketData.ingest_id.desc(),
        )
    ).scalars().all()

    deduped_rows, duplicate_logs = dedupe_staging_rows(staging_rows)
    if duplicate_logs:
        session.add_all(duplicate_logs)

    export_payloads, quality_errors = build_export_payloads(deduped_rows)
    if quality_errors:
        session.add_all(quality_errors)

    if export_payloads:
        for payload_batch in iter_batches(export_payloads, EXPORT_UPSERT_BATCH_SIZE):
            insert_stmt = insert(MarketData5m).values(payload_batch)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[MarketData5m.symbol, MarketData5m.ts],
                set_={
                    "open": insert_stmt.excluded.open,
                    "high": insert_stmt.excluded.high,
                    "low": insert_stmt.excluded.low,
                    "close": insert_stmt.excluded.close,
                    "volume": insert_stmt.excluded.volume,
                    "asset_type": insert_stmt.excluded.asset_type,
                    "source": insert_stmt.excluded.source,
                },
            )
            session.execute(upsert_stmt)

    last_processed_ts = max((payload["ts"] for payload in export_payloads), default=None)
    watermark_stmt = insert(PipelineWatermark).values(
        pipeline_name=EXPORT_PIPELINE_NAME,
        last_processed_ts=last_processed_ts,
        status="success",
    )
    session.execute(
        watermark_stmt.on_conflict_do_update(
            index_elements=[PipelineWatermark.pipeline_name],
            set_={
                "last_processed_ts": watermark_stmt.excluded.last_processed_ts,
                "status": watermark_stmt.excluded.status,
                "updated_at": func.now(),
            },
        )
    )

    return PipelineSummary(
        processed_rows=len(staging_rows),
        duplicate_rows=len(duplicate_logs),
        quality_error_rows=len(quality_errors),
        exported_rows=len(export_payloads),
    )


def clear_staging_tables(session: Session) -> PipelineSummary:
    market_data_rows = session.scalar(select(func.count()).select_from(MarketData)) or 0
    ingest_error_rows = session.scalar(select(func.count()).select_from(IngestError)) or 0

    # Use PostgreSQL TRUNCATE for fast, true table truncation and sequence reset.
    session.execute(
        text(
            "TRUNCATE TABLE stg_raw.ingest_errors, stg_raw.market_data RESTART IDENTITY"
        )
    )

    return PipelineSummary(truncated_rows=market_data_rows + ingest_error_rows)