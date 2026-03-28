"""Persistence repository for application caches."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import session_scope
from app.domain.models import DrugQuery
from app.models.db_models import (
    NormalizedQueryRecord,
    ProductDetailCacheRecord,
    ProductSearchCacheRecord,
    RxNormSuggestionCacheRecord,
)


class CacheRepository:
    """Read and write cached payloads in SQLite."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def record_normalized_query(self, query_key: str, query: DrugQuery) -> None:
        """Persist normalized query metadata for later inspection."""

        with session_scope(self._session_factory) as session:
            session.add(
                NormalizedQueryRecord(
                    query_key=query_key,
                    raw_text=query.raw_text,
                    normalized_text=query.normalized_text,
                    ndc=query.ndc,
                    requested_route=query.requested_route,
                    requested_dose_form=query.requested_dose_form,
                    requested_strength=query.requested_strength,
                )
            )

    def get_search_cache(self, query_key: str) -> ProductSearchCacheRecord | None:
        """Return cached search results when present."""

        with session_scope(self._session_factory) as session:
            return session.scalar(
                select(ProductSearchCacheRecord).where(ProductSearchCacheRecord.query_key == query_key)
            )

    def save_search_cache(
        self,
        query_key: str,
        query_text: str,
        payload_json: str,
        source: str,
        fetched_at: datetime,
        expires_at: datetime,
    ) -> None:
        """Insert or update cached search results."""

        with session_scope(self._session_factory) as session:
            record = session.scalar(
                select(ProductSearchCacheRecord).where(ProductSearchCacheRecord.query_key == query_key)
            )
            if record is None:
                session.add(
                    ProductSearchCacheRecord(
                        query_key=query_key,
                        query_text=query_text,
                        payload_json=payload_json,
                        source=source,
                        fetched_at=fetched_at,
                        expires_at=expires_at,
                    )
                )
                return

            record.query_text = query_text
            record.payload_json = payload_json
            record.source = source
            record.fetched_at = fetched_at
            record.expires_at = expires_at

    def get_product_cache(self, setid: str) -> ProductDetailCacheRecord | None:
        """Return cached product detail when present."""

        with session_scope(self._session_factory) as session:
            return session.scalar(
                select(ProductDetailCacheRecord).where(ProductDetailCacheRecord.setid == setid)
            )

    def save_product_cache(
        self,
        setid: str,
        payload_json: str,
        source: str,
        fetched_at: datetime,
        expires_at: datetime,
    ) -> None:
        """Insert or update cached product detail."""

        with session_scope(self._session_factory) as session:
            record = session.scalar(
                select(ProductDetailCacheRecord).where(ProductDetailCacheRecord.setid == setid)
            )
            if record is None:
                session.add(
                    ProductDetailCacheRecord(
                        setid=setid,
                        payload_json=payload_json,
                        source=source,
                        fetched_at=fetched_at,
                        expires_at=expires_at,
                    )
                )
                return

            record.payload_json = payload_json
            record.source = source
            record.fetched_at = fetched_at
            record.expires_at = expires_at

    def get_rxnorm_cache(self, query_key: str) -> RxNormSuggestionCacheRecord | None:
        """Return cached RxNorm suggestions when present."""

        with session_scope(self._session_factory) as session:
            return session.scalar(
                select(RxNormSuggestionCacheRecord).where(RxNormSuggestionCacheRecord.query_key == query_key)
            )

    def save_rxnorm_cache(
        self,
        query_key: str,
        query_text: str,
        payload_json: str,
        source: str,
        fetched_at: datetime,
        expires_at: datetime,
    ) -> None:
        """Insert or update cached RxNorm suggestions."""

        with session_scope(self._session_factory) as session:
            record = session.scalar(
                select(RxNormSuggestionCacheRecord).where(RxNormSuggestionCacheRecord.query_key == query_key)
            )
            if record is None:
                session.add(
                    RxNormSuggestionCacheRecord(
                        query_key=query_key,
                        query_text=query_text,
                        payload_json=payload_json,
                        source=source,
                        fetched_at=fetched_at,
                        expires_at=expires_at,
                    )
                )
                return

            record.query_text = query_text
            record.payload_json = payload_json
            record.source = source
            record.fetched_at = fetched_at
            record.expires_at = expires_at
