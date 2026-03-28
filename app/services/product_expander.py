"""Product candidate expansion helpers."""

from __future__ import annotations

from app.domain.models import DrugQuery, ProductCandidate, ProductSearchResult, RxNormConcept
from app.repositories.dailymed_repository import DailyMedRepository


class ProductExpander:
    """Convert repository product candidates into UI-facing search results."""

    def __init__(self, repository: DailyMedRepository, limit: int = 100) -> None:
        self._repository = repository
        self._limit = limit

    async def expand_from_concepts(
        self,
        query: DrugQuery,
        concepts: list[RxNormConcept],
    ) -> list[ProductSearchResult]:
        """Expand resolved concepts into product search results."""

        candidates = await self._repository.find_products(query, concepts)
        return self._to_search_results(candidates[: self._limit])

    def _to_search_results(self, candidates: list[ProductCandidate]) -> list[ProductSearchResult]:
        """Convert internal product candidates into UI-facing search results."""

        results: list[ProductSearchResult] = []
        for candidate in candidates:
            results.append(
                ProductSearchResult(
                    product_name=candidate.product_name,
                    setid=candidate.setid,
                    ndcs=[candidate.ndc] if candidate.ndc else [],
                    route=candidate.route,
                    dosage_form=candidate.dosage_form,
                    labeler=candidate.labeler,
                    rxcui=candidate.rxcui,
                )
            )
        return results
