import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB

from model.models import (
    DedupConflict,
    MarketData,
    MarketData5m,
    PipelineLog,
    TransformMarketData,
)


pytestmark = [pytest.mark.unit, pytest.mark.postgres_only]


def test_market_data_includes_staging_indexes():
    index_names = {index.name for index in MarketData.__table__.indexes}
    assert "idx_stg_raw_symbol_ts" in index_names
    assert "idx_stg_raw_ingest_time" in index_names


def test_market_data_5m_ohlc_columns_are_not_nullable():
    table = MarketData5m.__table__
    assert table.c.open.nullable is False
    assert table.c.high.nullable is False
    assert table.c.low.nullable is False
    assert table.c.close.nullable is False


def test_pipeline_log_has_status_check_constraint_and_index():
    table = PipelineLog.__table__

    check_constraints = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    ]
    names = {constraint.name for constraint in check_constraints}
    assert "chk_pipeline_logs_status" in names

    status_constraint = next(
        constraint
        for constraint in check_constraints
        if constraint.name == "chk_pipeline_logs_status"
    )
    sql_text = str(status_constraint.sqltext)
    assert "running" in sql_text
    assert "success" in sql_text
    assert "failed" in sql_text
    assert "warning" in sql_text

    index_names = {index.name for index in table.indexes}
    assert "idx_pipeline_logs_stage_time" in index_names


def test_dedup_conflict_rows_use_jsonb_columns():
    table = DedupConflict.__table__
    assert isinstance(table.c.existing_row.type, JSONB)
    assert isinstance(table.c.incoming_row.type, JSONB)


def test_transform_market_data_model_matches_composite_primary_key_shape():
    table = TransformMarketData.__table__
    assert table.schema == "stg_transform"
    assert table.name == "market_data"

    primary_key_columns = [column.name for column in table.primary_key.columns]
    assert primary_key_columns == ["symbol", "ts"]
