"""SQLAlchemy persistence models for caches and query records."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    """Return a naive UTC timestamp suitable for SQLite storage."""

    return datetime.utcnow()


class Base(DeclarativeBase):
    """Declarative SQLAlchemy base."""


class NormalizedQueryRecord(Base):
    """Audit trail for normalized search queries."""

    __tablename__ = "normalized_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_key: Mapped[str] = mapped_column(String(255), index=True)
    raw_text: Mapped[str] = mapped_column(Text)
    normalized_text: Mapped[str] = mapped_column(Text)
    ndc: Mapped[str | None] = mapped_column(String(32), nullable=True)
    requested_route: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_dose_form: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_strength: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class RxNormSuggestionCacheRecord(Base):
    """Cached RxNorm autocomplete suggestions."""

    __tablename__ = "rxnorm_suggestions_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    query_text: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(64))
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)


class ProductSearchCacheRecord(Base):
    """Cached expanded product search results."""

    __tablename__ = "product_search_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    query_text: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(64))
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)


class ProductDetailCacheRecord(Base):
    """Cached product details by SETID."""

    __tablename__ = "product_detail_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    setid: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(64))
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)
