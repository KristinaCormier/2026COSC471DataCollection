import pytest

from model import orm_db as mod


pytestmark = pytest.mark.unit


def test_build_postgres_url_constructs_expected_connection_string():
    # Given: explicit host, port, database, username, and password values.
    # When: building a connection URL with `build_postgres_url`.
    # Then: the returned URL string exactly matches the expected SQLAlchemy psycopg format.

    url = mod.build_postgres_url(
        host="db.example.local",
        port=5432,
        database="market_data",
        user="collector",
        password="secret",
    )

    assert url == "postgresql+psycopg://collector:secret@db.example.local:5432/market_data"


def test_get_engine_calls_create_engine_with_expected_arguments(monkeypatch):
    # Given: a patched `create_engine` that captures call arguments.
    # When: requesting an engine with `get_engine`.
    # Then: `create_engine` receives the computed URL and `future=True` and its return value is propagated.

    captured = {}

    def fake_create_engine(url, future):
        captured["url"] = url
        captured["future"] = future
        return "fake-engine"

    monkeypatch.setattr(mod, "create_engine", fake_create_engine)

    engine = mod.get_engine("localhost", 5432, "test_db", "tester", "pw")

    assert engine == "fake-engine"
    assert captured["url"] == "postgresql+psycopg://tester:pw@localhost:5432/test_db"
    assert captured["future"] is True


def test_get_session_factory_uses_expected_sqlalchemy_defaults(monkeypatch):
    # Given: a patched `sessionmaker` that records keyword arguments.
    # When: creating a session factory with `get_session_factory`.
    # Then: it binds the provided engine and uses the expected SQLAlchemy defaults.

    captured = {}

    def fake_sessionmaker(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "fake-session-factory"

    monkeypatch.setattr(mod, "sessionmaker", fake_sessionmaker)

    factory = mod.get_session_factory("engine-object")

    assert factory == "fake-session-factory"
    assert captured["kwargs"]["bind"] == "engine-object"
    assert captured["kwargs"]["autoflush"] is False
    assert captured["kwargs"]["autocommit"] is False
    assert captured["kwargs"]["future"] is True


def test_ensure_schemas_emits_create_schema_for_all_required_schemas():
    # Given: a fake engine/connection that records executed SQL.
    # When: running `ensure_schemas`.
    # Then: a CREATE SCHEMA statement is executed for every schema listed in `SCHEMA_NAMES`.

    class FakeConn:
        def __init__(self):
            self.executed = []

        def execute(self, stmt):
            self.executed.append(str(stmt))

    class BeginContext:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self._conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def __init__(self):
            self.conn = FakeConn()

        def begin(self):
            return BeginContext(self.conn)

    engine = FakeEngine()

    mod.ensure_schemas(engine)

    assert len(engine.conn.executed) == len(mod.SCHEMA_NAMES)
    for schema_name in mod.SCHEMA_NAMES:
        expected_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        assert any(expected_sql in stmt for stmt in engine.conn.executed)


def test_init_db_runs_schema_creation_before_metadata_create_all(monkeypatch):
    # Given: patched `ensure_schemas` and `metadata.create_all` functions that capture call order.
    # When: initializing the database with `init_db`.
    # Then: schema creation runs before table metadata creation.

    call_order = []

    def fake_ensure_schemas(engine):
        call_order.append("ensure_schemas")

    def fake_create_all(engine):
        call_order.append("create_all")

    monkeypatch.setattr(mod, "ensure_schemas", fake_ensure_schemas)
    monkeypatch.setattr(mod.Base.metadata, "create_all", fake_create_all)

    mod.init_db("engine")

    assert call_order == ["ensure_schemas", "create_all"]