from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from model.models import Base


def build_postgres_url(host: str, port: int, database: str, user: str, password: str) -> str:
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def get_engine(host: str, port: int, database: str, user: str, password: str):
    return create_engine(
        build_postgres_url(host, port, database, user, password),
        future=True,
    )


def get_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core_dbms"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS operation_logs"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg_raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg_transform"))

    Base.metadata.create_all(engine)