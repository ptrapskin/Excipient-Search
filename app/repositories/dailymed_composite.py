"""Composite DailyMed repository: local cache first, live API fallback."""

from __future__ import annotations

from app.domain.models import DrugQuery, ProductCandidate, ProductDetail, ProductSearchResult
from app.repositories.dailymed_api import DailyMedAPIError, dedupe_search_results
from app.repositories.dailymed_zip import DailyMedZipRepository
from app.repositories.dailymed_repository import DailyMedRepository


class CompositeDailyMedRepository:
    """Search the local SPL cache first; fall back to and populate from the live API."""

    def __init__(
        self,
        local_repository: DailyMedZipRepository,
        api_repository: DailyMedRepository,
        result_limit: int = 100,
    ) -> None:
        self._local_repository = local_repository
        self._api_repository = api_repository
        self._result_limit = result_limit

    async def find_products(self, query: DrugQuery, concepts: list) -> list[ProductCandidate]:
        """Return product candidates, preferring local cache and falling back to the live API."""

        local = await self._local_repository.find_products(query, concepts)
        if local:
            return local[: self._result_limit]
        api = await self._api_repository.find_products(query, concepts)
        self._local_repository.save_products(api)
        return api[: self._result_limit]

    async def search_spls(self, query: DrugQuery) -> list[ProductSearchResult]:
        """Return all matching SPLs, merging local cache with live API results."""

        local_results = await self._local_repository.search_spls(query)
        try:
            api_results = await self._api_repository.search_spls(query)
        except DailyMedAPIError:
            api_results = []
        merged = dedupe_search_results([*local_results, *api_results])
        if merged:
            self._local_repository.save_products(self._to_candidates(merged))
        return merged

    async def get_spl(self, setid: str) -> dict:
        """Prefer local SPL XML, then fall back to live API."""

        local_spl = await self._local_repository.get_spl(setid)
        if local_spl:
            return local_spl
        spl = await self._api_repository.get_spl(setid)
        if spl:
            self._local_repository.save_spl(setid, spl)
        return spl

    async def get_inactive_ingredients(self, setid: str) -> list[dict]:
        """Prefer locally parsed inactive ingredients, then fall back to live."""

        local_ingredients = await self._local_repository.get_inactive_ingredients(setid)
        if local_ingredients:
            return local_ingredients
        spl = await self.get_spl(setid)
        if spl:
            return await self._local_repository.get_inactive_ingredients(setid)
        return await self._api_repository.get_inactive_ingredients(setid)

    async def get_product_detail(self, setid: str) -> ProductDetail:
        """Prefer local SPL XML, then fall back to live API."""

        try:
            return await self._local_repository.get_product_detail(setid)
        except FileNotFoundError:
            pass
        detail = await self._api_repository.get_product_detail(setid)
        spl = await self._api_repository.get_spl(setid)
        if spl:
            self._local_repository.save_spl(setid, spl)
        return detail

    async def get_all_product_details(self, setid: str) -> list[ProductDetail]:
        """Return one ProductDetail per product subject, preferring local SPL."""

        try:
            return await self._local_repository.get_all_product_details(setid)
        except FileNotFoundError:
            pass
        spl = await self._api_repository.get_spl(setid)
        if spl:
            self._local_repository.save_spl(setid, spl)
        return await self._api_repository.get_all_product_details(setid)

    def _to_candidates(self, results: list[ProductSearchResult]):
        """Convert search results to the format expected by the local index writer."""

        from app.domain.models import ProductCandidate
        candidates = []
        for r in results:
            candidates.append(ProductCandidate(
                rxcui=r.rxcui,
                setid=r.setid,
                ndc=r.ndcs[0] if r.ndcs else None,
                product_name=r.product_name,
                labeler=r.labeler,
                dosage_form=r.dosage_form,
                route=r.route,
                source=str(r.source),
            ))
        return candidates
