"""baseline schema

Revision ID: 20260312_0001
Revises:
Create Date: 2026-03-12 00:01:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260312_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS core_dbms")
    op.execute("CREATE SCHEMA IF NOT EXISTS operation_logs")
    op.execute("CREATE SCHEMA IF NOT EXISTS stg_raw")
    op.execute("CREATE SCHEMA IF NOT EXISTS stg_transform")

    op.create_table(
        "market_data",
        sa.Column("ingest_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("close", sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.Numeric(20, 4), nullable=True),
        sa.Column("asset_type", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("ingest_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("ingest_id"),
        sa.UniqueConstraint("symbol", "ts", name="unique_symbol_ts_source"),
        schema="stg_raw",
    )
    op.create_index(
        "idx_stg_raw_symbol_ts",
        "market_data",
        ["symbol", "ts"],
        unique=False,
        schema="stg_raw",
    )
    op.create_index(
        "idx_stg_raw_ingest_time",
        "market_data",
        ["ingest_time"],
        unique=False,
        schema="stg_raw",
    )

    op.create_table(
        "ingest_errors",
        sa.Column("error_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ingest_id", sa.BigInteger(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("asset_type", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("error_id"),
        schema="stg_raw",
    )

    op.create_table(
        "market_data_5m",
        sa.Column("market_data_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("close", sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("asset_type", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("market_data_id"),
        sa.UniqueConstraint("symbol", "ts", name="unique_symbol_ts"),
        schema="core_dbms",
    )

    op.create_table(
        "authority_conflicts",
        sa.Column("conflict_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_a", sa.Text(), nullable=True),
        sa.Column("source_b", sa.Text(), nullable=True),
        sa.Column("preferred_source", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("conflict_id"),
        schema="operation_logs",
    )
    op.create_table(
        "backup_logs",
        sa.Column("backup_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("backup_type", sa.Text(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("backup_id"),
        schema="operation_logs",
    )
    op.create_table(
        "cast_errors",
        sa.Column("error_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("column_name", sa.Text(), nullable=True),
        sa.Column("raw_value", sa.Text(), nullable=True),
        sa.Column("target_type", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("error_id"),
        schema="operation_logs",
    )
    op.create_table(
        "concurrency_issues",
        sa.Column("issue_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("table_name", sa.Text(), nullable=True),
        sa.Column("lock_type", sa.Text(), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("issue_id"),
        schema="operation_logs",
    )
    op.create_table(
        "data_quality_errors",
        sa.Column("error_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("error_id"),
        schema="operation_logs",
    )
    op.create_table(
        "dedup_conflicts",
        sa.Column("conflict_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("existing_row", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("incoming_row", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("conflict_id"),
        schema="operation_logs",
    )
    op.create_table(
        "ingestion_log",
        sa.Column("log_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("rows_loaded", sa.Integer(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("error_msg", sa.Text(), nullable=True),
        sa.Column("logged_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("log_id"),
        schema="operation_logs",
    )
    op.create_table(
        "pipeline_logs",
        sa.Column("log_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("pipeline_stage", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("log_id"),
        schema="operation_logs",
    )
    op.create_check_constraint(
        "chk_pipeline_logs_status",
        "pipeline_logs",
        "status IN ('running', 'success', 'failed', 'warning')",
        schema="operation_logs",
    )
    op.execute(
        "CREATE INDEX idx_pipeline_logs_stage_time "
        "ON operation_logs.pipeline_logs (pipeline_stage, created_at DESC)"
    )
    op.create_table(
        "pipeline_watermarks",
        sa.Column("pipeline_name", sa.Text(), nullable=False),
        sa.Column("last_processed_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("pipeline_name"),
        schema="operation_logs",
    )
    op.create_table(
        "upsert_failures",
        sa.Column("failure_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("table_name", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("failure_id"),
        schema="operation_logs",
    )

    op.create_table(
        "market_data",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("close", sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("vwap", sa.Numeric(20, 6), nullable=True),
        sa.PrimaryKeyConstraint("symbol", "ts"),
        schema="stg_transform",
    )
    op.create_table(
        "transform_errors",
        sa.Column("error_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("log_time", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("error_id"),
        schema="stg_transform",
    )


def downgrade() -> None:
    op.drop_table("transform_errors", schema="stg_transform")
    op.drop_table("market_data", schema="stg_transform")

    op.drop_table("upsert_failures", schema="operation_logs")
    op.drop_table("pipeline_watermarks", schema="operation_logs")
    op.execute("DROP INDEX IF EXISTS operation_logs.idx_pipeline_logs_stage_time")
    op.drop_constraint("chk_pipeline_logs_status", "pipeline_logs", schema="operation_logs", type_="check")
    op.drop_table("pipeline_logs", schema="operation_logs")
    op.drop_table("ingestion_log", schema="operation_logs")
    op.drop_table("dedup_conflicts", schema="operation_logs")
    op.drop_table("data_quality_errors", schema="operation_logs")
    op.drop_table("concurrency_issues", schema="operation_logs")
    op.drop_table("cast_errors", schema="operation_logs")
    op.drop_table("backup_logs", schema="operation_logs")
    op.drop_table("authority_conflicts", schema="operation_logs")

    op.drop_table("market_data_5m", schema="core_dbms")

    op.drop_table("ingest_errors", schema="stg_raw")
    op.drop_index("idx_stg_raw_ingest_time", table_name="market_data", schema="stg_raw")
    op.drop_index("idx_stg_raw_symbol_ts", table_name="market_data", schema="stg_raw")
    op.drop_table("market_data", schema="stg_raw")

    op.execute("DROP SCHEMA IF EXISTS stg_transform")
    op.execute("DROP SCHEMA IF EXISTS operation_logs")
    op.execute("DROP SCHEMA IF EXISTS core_dbms")
    op.execute("DROP SCHEMA IF EXISTS stg_raw")