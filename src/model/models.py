"""
SQLAlchemy ORM Models for Data Pipeline

Purpose:
    Define declarative ORM models for all database tables across the three-layer schema:
    - stg_raw: Staging layer for raw API ingest
    - core_dbms: Core warehouse layer for validated, deduplicated data
    - operation_logs: Audit and error tracking

Tables:
    Staging Layer (stg_raw):
        - MarketData: Raw 5-minute bars from API (symbol, ts, OHLCV, raw JSON)
        - IngestError: Failed API calls, validation errors

    Core Layer (core_dbms):
        - MarketData5m: Quality-checked, deduplicated 5-minute bars

    Operations Layer (operation_logs):
        - AuthorityConflict: Source conflicts (for multi-source scenarios)
        - BackupLog: Backup/restore event history
        - CastError: Type conversion failures
        - DeduplicationConflict: Rows discarded as duplicates
        - DataQualityError: Rows rejected for data quality issues
        - PipelineLog: Script execution status and results

Design:
    - All tables use timezone-aware DateTime columns (UTC stored, but TZ aware in Python objects)
    - Unique constraints are field-specific (e.g., UNIQUE(symbol, ts) on market_data)
    - Foreign keys are not enforced to keep staging layer flexible
    - JSONB columns preserve raw API payload for debugging

Author: Data Collection Team
License: MIT
"""

from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Numeric,
    Integer,
    Date,
    Index,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Keep a single module object regardless of whether callers import
# `model.models` (runtime path) or `src.model.models` (package path).
if __name__ == "model.models":
    sys.modules.setdefault("src.model.models", sys.modules[__name__])
elif __name__ == "src.model.models":
    sys.modules.setdefault("model.models", sys.modules[__name__])

class Base(DeclarativeBase):
    pass


class MarketData(Base):
    __tablename__ = "market_data"

    __table_args__ = (
        UniqueConstraint("symbol", "ts", name="unique_symbol_ts_source"),
        Index("idx_stg_raw_symbol_ts", "symbol", "ts"),
        Index("idx_stg_raw_ingest_time", "ingest_time"),
        {"schema": "stg_raw"},
    )

    ingest_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    open: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6)
    )

    high: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6)
    )

    low: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6)
    )

    close: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6)
    )

    volume: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(20, 4)
    )

    asset_type: Mapped[Optional[str]] = mapped_column(Text)

    source: Mapped[Optional[str]] = mapped_column(Text)

    ingest_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )

    raw_payload: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)

   
class IngestError(Base):
    __tablename__ = "ingest_errors"
    __table_args__ = {"schema": "stg_raw"}

    error_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    ingest_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    asset_type: Mapped[Optional[str]] = mapped_column(Text)

    source: Mapped[Optional[str]] = mapped_column(Text)

    error_type: Mapped[Optional[str]] = mapped_column(Text)

    error_detail: Mapped[Optional[str]] = mapped_column(Text)

    raw_payload: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class MarketData5m(Base):
    __tablename__ = "market_data_5m"
    __table_args__ = (
        UniqueConstraint("symbol", "ts", name="unique_symbol_ts"),
        {"schema": "core_dbms"},
    )

    market_data_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[str] = mapped_column(Text, nullable=False)

    ts: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)

    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    asset_type: Mapped[str] = mapped_column(Text, nullable=False)

    source: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class AuthorityConflict(Base):
    __tablename__ = "authority_conflicts"
    __table_args__ = {"schema": "operation_logs"}

    conflict_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    source_a: Mapped[Optional[str]] = mapped_column(Text)

    source_b: Mapped[Optional[str]] = mapped_column(Text)

    preferred_source: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class BackupLog(Base):
    __tablename__ = "backup_logs"
    __table_args__ = {"schema": "operation_logs"}

    backup_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    backup_type: Mapped[Optional[str]] = mapped_column(Text)

    file_path: Mapped[Optional[str]] = mapped_column(Text)

    status: Mapped[Optional[str]] = mapped_column(Text)

    started_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    completed_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class CastError(Base):
    __tablename__ = "cast_errors"
    __table_args__ = {"schema": "operation_logs"}

    error_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    column_name: Mapped[Optional[str]] = mapped_column(Text)

    raw_value: Mapped[Optional[str]] = mapped_column(Text)

    target_type: Mapped[Optional[str]] = mapped_column(Text)

    error_detail: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class ConcurrencyIssue(Base):
    __tablename__ = "concurrency_issues"
    __table_args__ = {"schema": "operation_logs"}

    issue_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    table_name: Mapped[Optional[str]] = mapped_column(Text)

    lock_type: Mapped[Optional[str]] = mapped_column(Text)

    pid: Mapped[Optional[int]] = mapped_column(Integer)

    details: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class DataQualityError(Base):
    __tablename__ = "data_quality_errors"
    __table_args__ = {"schema": "operation_logs"}

    error_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    error_type: Mapped[Optional[str]] = mapped_column(Text)

    error_detail: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class DedupConflict(Base):
    __tablename__ = "dedup_conflicts"
    __table_args__ = {"schema": "operation_logs"}

    conflict_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    existing_row: Mapped[Optional[dict]] = mapped_column(JSONB)

    incoming_row: Mapped[Optional[dict]] = mapped_column(JSONB)

    resolution: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class IngestionLog(Base):
    __tablename__ = "ingestion_log"
    __table_args__ = {"schema": "operation_logs"}

    log_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    start_date: Mapped[Optional[dt.date]] = mapped_column(Date)

    end_date: Mapped[Optional[dt.date]] = mapped_column(Date)

    rows_loaded: Mapped[Optional[int]] = mapped_column(Integer)

    status: Mapped[Optional[str]] = mapped_column(Text)

    error_msg: Mapped[Optional[str]] = mapped_column(Text)

    logged_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class PipelineLog(Base):
    __tablename__ = "pipeline_logs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'success', 'failed', 'warning')",
            name="chk_pipeline_logs_status",
        ),
        Index("idx_pipeline_logs_stage_time", "pipeline_stage", text("created_at DESC")),
        {"schema": "operation_logs"},
    )

    log_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    pipeline_stage: Mapped[str] = mapped_column(Text, nullable=False)

    status: Mapped[str] = mapped_column(Text, nullable=False)

    message: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


class PipelineWatermark(Base):
    __tablename__ = "pipeline_watermarks"
    __table_args__ = {"schema": "operation_logs"}

    pipeline_name: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
    )

    last_processed_ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    status: Mapped[Optional[str]] = mapped_column(Text)

    updated_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class UpsertFailure(Base):
    __tablename__ = "upsert_failures"
    __table_args__ = {"schema": "operation_logs"}

    failure_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    table_name: Mapped[Optional[str]] = mapped_column(Text)

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    error_detail: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"),
    )


class TransformMarketData(Base):
    __tablename__ = "market_data"
    __table_args__ = {"schema": "stg_transform"}

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)

    ts: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )

    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6))

    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6))

    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6))

    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6))

    volume: Mapped[Optional[int]] = mapped_column(BigInteger)

    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))


class TransformError(Base):
    __tablename__ = "transform_errors"
    __table_args__ = {"schema": "stg_transform"}

    error_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True
    )

    symbol: Mapped[Optional[str]] = mapped_column(Text)

    ts: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    error_type: Mapped[Optional[str]] = mapped_column(Text)

    error_detail: Mapped[Optional[str]] = mapped_column(Text)

    log_time: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()")
    )