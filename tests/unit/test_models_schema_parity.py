import pytest
import ast
from pathlib import Path
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB

from src.model.models import (
    DedupConflict,
    MarketData,
    MarketData5m,
    PipelineLog,
    TransformMarketData,
)


pytestmark = [pytest.mark.unit, pytest.mark.postgres_only]


def test_market_data_includes_staging_indexes():
    # Given: the `MarketData` ORM table definition.
    # When: collecting declared index names from its SQLAlchemy metadata.
    # Then: both staging indexes (`symbol, ts` and `ingest_time`) are present.

    index_names = {index.name for index in MarketData.__table__.indexes}
    assert "idx_stg_raw_symbol_ts" in index_names
    assert "idx_stg_raw_ingest_time" in index_names


def test_market_data_5m_ohlcv_columns_are_nullable_for_lossless_transfer():
    # Given: the `MarketData5m` ORM table metadata.
    # When: inspecting nullability on OHLCV columns.
    # Then: the fields are nullable so rows are not dropped between layers.

    table = MarketData5m.__table__
    assert table.c.open.nullable is True
    assert table.c.high.nullable is True
    assert table.c.low.nullable is True
    assert table.c.close.nullable is True
    assert table.c.volume.nullable is True


def test_market_data_5m_migration_and_model_nullability_align():
    # Given: baseline Alembic migration source and ORM model table metadata.
    # When: reading migration AST for `core_dbms.market_data_5m` column nullability.
    # Then: migration and model agree on nullability for critical OHLCV fields.

    migration_path = (
        Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "20260312_0001_baseline_schema.py"
    )
    tree = ast.parse(migration_path.read_text())

    captured_columns = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "create_table":
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        if node.args[0].value != "market_data_5m":
            continue

        schema_kw = next((kw for kw in node.keywords if kw.arg == "schema"), None)
        if schema_kw is None:
            continue
        if not isinstance(schema_kw.value, ast.Constant):
            continue
        if schema_kw.value.value != "core_dbms":
            continue

        for arg in node.args[1:]:
            if not isinstance(arg, ast.Call):
                continue
            if not isinstance(arg.func, ast.Attribute):
                continue
            if arg.func.attr != "Column":
                continue
            if not arg.args or not isinstance(arg.args[0], ast.Constant):
                continue

            col_name = arg.args[0].value
            nullable_kw = next((kw for kw in arg.keywords if kw.arg == "nullable"), None)
            if nullable_kw and isinstance(nullable_kw.value, ast.Constant):
                captured_columns[col_name] = nullable_kw.value.value

    model_table = MarketData5m.__table__
    columns_to_check = ["open", "high", "low", "close", "volume"]

    for column_name in columns_to_check:
        assert captured_columns[column_name] == model_table.c[column_name].nullable


def test_pipeline_log_has_status_check_constraint_and_index():
    # Given: the `PipelineLog` ORM table metadata.
    # When: reading check constraints and indexes defined on that table.
    # Then: the allowed status constraint and stage/time index both exist.

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
    # Given: the `DedupConflict` ORM table definition.
    # When: inspecting the SQLAlchemy types for `existing_row` and `incoming_row`.
    # Then: both columns are typed as PostgreSQL JSONB.

    table = DedupConflict.__table__
    assert isinstance(table.c.existing_row.type, JSONB)
    assert isinstance(table.c.incoming_row.type, JSONB)


def test_transform_market_data_model_matches_composite_primary_key_shape():
    # Given: the `TransformMarketData` ORM table metadata.
    # When: checking schema/table identity and primary key columns.
    # Then: it maps to `stg_transform.market_data` with composite key `(symbol, ts)`.

    table = TransformMarketData.__table__
    assert table.schema == "stg_transform"
    assert table.name == "market_data"

    primary_key_columns = [column.name for column in table.primary_key.columns]
    assert primary_key_columns == ["symbol", "ts"]
