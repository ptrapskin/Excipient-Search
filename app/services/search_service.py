"""Primary product search workflow."""

from __future__ import annotations

import asyncio

from app.domain.enums import DataSource
from app.domain.models import DrugQuery, ExcipientFilter, ProductComparisonRow, ProductDetail, ProductSearchResult
from app.repositories.dailymed_api import DailyMedAPIError, dedupe_search_results
from app.services.cache_service import CacheService
from app.services.excipient_filter import ExcipientFilterService
from app.services.normalize_query import build_query_key, normalize_query


class SearchService:
    """Orchestrate product search: resolver + expander pipeline with DailyMed as source of truth."""

    def __init__(
        self,
        cache_service: CacheService,
        dailymed_repository,
        product_expander,
        rxnorm_resolver,
        excipient_filter_service: ExcipientFilterService,
    ) -> None:
        self._cache_service = cache_service
        self._dailymed_repository = dailymed_repository
        self._product_expander = product_expander
        self._rxnorm_resolver = rxnorm_resolver
        self._excipient_filter_service = excipient_filter_service

    async def search(self, raw_query: str) -> tuple[DrugQuery, list[ProductSearchResult], bool]:
        """Search via RxNorm resolver → product expander → DailyMed SPL lookup."""

        query = normalize_query(raw_query)
        query_key = build_query_key(query)
        await self._cache_service.record_normalized_query(query_key=query_key, query=query)

        if not query.normalized_text and not query.ndc:
            raise ValueError("Please enter a medication name or NDC.")

        concepts = await self._rxnorm_resolver.resolve(query)
        results = await self._product_expander.expand_from_concepts(query, concepts)

        # When a brand name is searched, also search by generic ingredient names so that
        # e.g. "Keppra" returns levetiracetam products (not just Keppra-branded ones).
        # Use search_spls (always hits local + live API) so generic products not yet cached are found.
        for ingredient_name in self._extract_ingredient_names(concepts, query.normalized_text):
            ingredient_query = DrugQuery(
                raw_text=ingredient_name,
                normalized_text=ingredient_name,
                ndc=query.ndc,
                requested_route=query.requested_route,
                requested_dose_form=query.requested_dose_form,
                requested_strength=query.requested_strength,
            )
            try:
                extra = await self._dailymed_repository.search_spls(ingredient_query)
                results = dedupe_search_results([*results, *extra])
            except DailyMedAPIError:
                pass

        await self._cache_service.set_search_results(
            query_key=query_key,
            query_text=query.normalized_text,
            results=results,
            source=DataSource.DAILMED_LIVE,
        )
        return query, results, False

    @staticmethod
    def _extract_ingredient_names(concepts: list, query_text: str) -> list[str]:
        """Return ingredient names from IN/PIN concepts that differ from the user's query text."""

        query_lower = query_text.casefold()
        seen: set[str] = set()
        names: list[str] = []
        for concept in concepts:
            if concept.tty in ("IN", "PIN"):
                name = concept.name.strip()
                key = name.casefold()
                if key != query_lower and key not in seen:
                    seen.add(key)
                    names.append(name)
        return names

    async def search_with_excipients(
        self,
        raw_query: str,
        include_terms: str | None = None,
        exclude_terms: str | None = None,
    ) -> tuple[DrugQuery, list[ProductSearchResult], list[ProductComparisonRow], ExcipientFilter, list[ProductComparisonRow], bool]:
        """Search SPLs and build a multi-product excipient comparison view."""

        query, results, cached = await self.search(raw_query)
        excipient_filter = self._excipient_filter_service.build_filter(
            include=include_terms,
            exclude=exclude_terms,
        )
        comparison_rows, results = await self._build_comparison_rows(results, excipient_filter)
        matching_rows = self._excipient_filter_service.filter_rows(comparison_rows, excipient_filter)
        return query, results, comparison_rows, excipient_filter, matching_rows, cached

    async def get_product_detail(self, setid: str, ndc: str | None = None) -> tuple[ProductDetail, bool]:
        """Retrieve a single product detail, optionally scoped to a specific NDC variant."""

        if ndc:
            details = await self._dailymed_repository.get_all_product_details(setid)
            for detail in details:
                if ndc in detail.ndcs:
                    return detail, False
            if details:
                return details[0], False

        cached = await self._cache_service.get_product_detail(setid)
        if cached is not None:
            return cached.product, True
        detail = await self._dailymed_repository.get_product_detail(setid)
        await self._cache_service.set_product_detail(detail, source=DataSource.DAILMED_LIVE)
        return detail, False

    async def _build_comparison_rows(
        self,
        results: list[ProductSearchResult],
        excipient_filter: ExcipientFilter,
    ) -> list[ProductComparisonRow]:
        """Fetch all product variants per SPL and build one comparison row each."""

        semaphore = asyncio.Semaphore(4)

        human_setids: set[str] = set()

        async def build_one(result: ProductSearchResult) -> list[ProductComparisonRow]:
            if not result.setid:
                return []
            try:
                async with semaphore:
                    details = await self._dailymed_repository.get_all_product_details(result.setid)
            except DailyMedAPIError:
                return []
            rows = []
            for detail in details:
                if detail.product_type and "HUMAN" not in detail.product_type.upper():
                    continue
                human_setids.add(result.setid)
                rows.append(self._excipient_filter_service.build_comparison_row(
                    result=result,
                    detail=detail,
                    excipient_filter=excipient_filter,
                ))
            return rows

        groups = await asyncio.gather(*(build_one(result) for result in results))
        all_rows = [row for group in groups for row in group]
        # Remove any results whose SPL contained no human-labelled products
        filtered_results = [r for r in results if not r.setid or r.setid in human_setids]
        return all_rows, filtered_results
