"""
Database Connection & Initialization

Purpose:
    Manage SQLAlchemy engine creation, session factory setup, and schema initialization.
    Ensures all required schemas (stg_raw, core_dbms, operation_logs) and tables exist
    before any ORM operations.

Functions:
    - build_postgres_url(): Construct a PostgreSQL connection string
    - get_engine(): Create a SQLAlchemy engine with connection pooling
    - get_session_factory(): Create a session factory for ORM operations
    - init_db(): Create all schemas and tables if they don't exist

Design:
    - Schemas are created in init_db() before tables to ensure clean initialization
    - All ORM models are defined in models.py
    - The engine is configured with future=True for SQLAlchemy 2.0 compatibility
    - Sessions are configured with autoflush=False and autocommit=False for explicit control

Author: Data Collection Team
License: MIT
"""

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