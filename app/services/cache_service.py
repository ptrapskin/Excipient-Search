"""Cache orchestration service."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pydantic import TypeAdapter

from app.domain.enums import DataSource
from app.domain.models import (
    CachedProduct,
    CachedRxNormSuggestion,
    CachedSearch,
    DrugQuery,
    ProductDetail,
    ProductSearchResult,
    RxNormSuggestion,
)
from app.repositories.cache_repository import CacheRepository


class CacheService:
    """Application-facing cache wrapper around the SQLite repository."""

    def __init__(self, repository: CacheRepository, ttl_seconds: int) -> None:
        self._repository = repository
        self._ttl = ttl_seconds
        self._search_adapter = TypeAdapter(list[ProductSearchResult])
        self._suggest_adapter = TypeAdapter(list[RxNormSuggestion])

    async def record_normalized_query(self, query_key: str, query: DrugQuery) -> None:
        """Persist normalized query metadata."""

        self._repository.record_normalized_query(query_key=query_key, query=query)

    async def get_search_results(self, query_key: str) -> CachedSearch | None:
        """Return non-expired cached search results."""

        record = self._repository.get_search_cache(query_key)
        if record is None or self._is_expired(record.expires_at):
            return None
        results = self._search_adapter.validate_json(record.payload_json)
        return CachedSearch(
            query_key=record.query_key,
            query_text=record.query_text,
            results=results,
            source=DataSource(record.source),
            fetched_at=record.fetched_at.replace(tzinfo=timezone.utc),
            expires_at=record.expires_at.replace(tzinfo=timezone.utc),
        )

    async def set_search_results(
        self,
        query_key: str,
        query_text: str,
        results: list[ProductSearchResult],
        source: DataSource,
    ) -> CachedSearch:
        """Persist search results in the cache."""

        fetched_at = datetime.now(timezone.utc)
        expires_at = fetched_at + timedelta(seconds=self._ttl)
        self._repository.save_search_cache(
            query_key=query_key,
            query_text=query_text,
            payload_json=self._search_adapter.dump_json(results).decode("utf-8"),
            source=source.value,
            fetched_at=fetched_at.replace(tzinfo=None),
            expires_at=expires_at.replace(tzinfo=None),
        )
        return CachedSearch(
            query_key=query_key,
            query_text=query_text,
            results=results,
            source=source,
            fetched_at=fetched_at,
            expires_at=expires_at,
        )

    async def get_product_detail(self, setid: str) -> CachedProduct | None:
        """Return non-expired cached product details."""

        record = self._repository.get_product_cache(setid)
        if record is None or self._is_expired(record.expires_at):
            return None
        product = ProductDetail.model_validate_json(record.payload_json)
        return CachedProduct(
            setid=record.setid,
            product=product,
            source=DataSource(record.source),
            fetched_at=record.fetched_at.replace(tzinfo=timezone.utc),
            expires_at=record.expires_at.replace(tzinfo=timezone.utc),
        )

    async def set_product_detail(self, product: ProductDetail, source: DataSource) -> CachedProduct:
        """Persist a product detail in the cache."""

        fetched_at = datetime.now(timezone.utc)
        expires_at = fetched_at + timedelta(seconds=self._ttl)
        self._repository.save_product_cache(
            setid=product.setid,
            payload_json=product.model_dump_json(),
            source=source.value,
            fetched_at=fetched_at.replace(tzinfo=None),
            expires_at=expires_at.replace(tzinfo=None),
        )
        return CachedProduct(
            setid=product.setid,
            product=product,
            source=source,
            fetched_at=fetched_at,
            expires_at=expires_at,
        )

    async def get_rxnorm_suggestions(self, query_key: str) -> CachedRxNormSuggestion | None:
        """Return non-expired cached RxNorm suggestions."""

        record = self._repository.get_rxnorm_cache(query_key)
        if record is None or self._is_expired(record.expires_at):
            return None
        suggestions = self._suggest_adapter.validate_json(record.payload_json)
        return CachedRxNormSuggestion(
            query_key=record.query_key,
            query_text=record.query_text,
            suggestions=suggestions,
            source=DataSource(record.source),
            fetched_at=record.fetched_at.replace(tzinfo=timezone.utc),
            expires_at=record.expires_at.replace(tzinfo=timezone.utc),
        )

    async def set_rxnorm_suggestions(
        self,
        query_key: str,
        query_text: str,
        suggestions: list[RxNormSuggestion],
        source: DataSource,
    ) -> CachedRxNormSuggestion:
        """Persist RxNorm suggestions in the cache."""

        fetched_at = datetime.now(timezone.utc)
        expires_at = fetched_at + timedelta(seconds=self._ttl)
        self._repository.save_rxnorm_cache(
            query_key=query_key,
            query_text=query_text,
            payload_json=self._suggest_adapter.dump_json(suggestions).decode("utf-8"),
            source=source.value,
            fetched_at=fetched_at.replace(tzinfo=None),
            expires_at=expires_at.replace(tzinfo=None),
        )
        return CachedRxNormSuggestion(
            query_key=query_key,
            query_text=query_text,
            suggestions=suggestions,
            source=source,
            fetched_at=fetched_at,
            expires_at=expires_at,
        )

    def _is_expired(self, expires_at: datetime) -> bool:
        """Return whether a cache record has expired."""

        return expires_at <= datetime.utcnow()
