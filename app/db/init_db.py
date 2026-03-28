"""Database initialization helpers."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from app.models.db_models import Base


def initialize_database(engine: Engine) -> None:
    """Create all tables if they do not already exist."""

    Base.metadata.create_all(bind=engine)
