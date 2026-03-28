from datetime import datetime, timedelta, timezone

import pytest

from app.domain.enums import DataSource
from app.domain.models import CachedRxNormSuggestion, DrugQuery, RxNormConcept, RxNormSuggestion
from app.services.rxnorm_resolver import RxNormResolver


class FakeCacheService:
    def __init__(self, cached: CachedRxNormSuggestion | None = None) -> None:
        self.cached = cached
        self.saved: list[RxNormSuggestion] = []

    async def get_rxnorm_suggestions(self, query_key: str) -> CachedRxNormSuggestion | None:
        return self.cached

    async def set_rxnorm_suggestions(self, query_key, query_text, suggestions, source):
        self.saved = suggestions
        return CachedRxNormSuggestion(
            query_key=query_key,
            query_text=query_text,
            suggestions=suggestions,
            source=source,
            fetched_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )


class FakeRxNormRepository:
    def __init__(
        self,
        suggestions: list[RxNormSuggestion] | dict[str, list[RxNormSuggestion]],
        candidates: list[RxNormConcept] | None = None,
    ) -> None:
        self.suggestions = suggestions
        self.candidates = candidates or []
        self.calls = 0
        self.resolve_calls = 0
        self.suggest_queries: list[str] = []

    async def suggest(self, query_text: str, limit: int) -> list[RxNormSuggestion]:
        self.calls += 1
        self.suggest_queries.append(query_text)
        if isinstance(self.suggestions, dict):
            return self.suggestions.get(query_text, [])[:limit]
        return self.suggestions[:limit]

    async def resolve_candidates(self, query_text: str, limit: int) -> list[RxNormConcept]:
        self.resolve_calls += 1
        return self.candidates[:limit]


@pytest.mark.asyncio
async def test_rxnorm_resolver_returns_cached_suggestions():
    cached = CachedRxNormSuggestion(
        query_key="key",
        query_text="metformin",
        suggestions=[RxNormSuggestion(display_name="Metformin 500 MG Oral Tablet", rxcui="1", tty="SCD")],
        source=DataSource.SQLITE_CACHE,
        fetched_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    cache = FakeCacheService(cached=cached)
    repository = FakeRxNormRepository([])
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    suggestions, cached_flag = await resolver.suggest("metformin")

    assert cached_flag is True
    assert len(suggestions) == 1
    assert repository.calls == 0


@pytest.mark.asyncio
async def test_rxnorm_resolver_uses_live_repository_when_cache_misses():
    cache = FakeCacheService()
    repository = FakeRxNormRepository(
        [
            RxNormSuggestion(display_name="Metformin 500 MG Oral Tablet", rxcui="1", tty="SCD"),
            RxNormSuggestion(display_name="Metformin", rxcui="2", tty="IN"),
        ]
    )
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    suggestions, cached_flag = await resolver.suggest("metformin")

    assert cached_flag is False
    assert len(suggestions) == 2
    assert repository.calls >= 1
    assert "metformin" in repository.suggest_queries
    assert {suggestion.rxcui for suggestion in cache.saved} == {"1", "2"}


@pytest.mark.asyncio
async def test_rxnorm_resolver_expands_abbreviations_for_dropdown_suggestions():
    cache = FakeCacheService()
    repository = FakeRxNormRepository(
        {
            "acet oral suspension": [
                RxNormSuggestion(
                    display_name="acetaminophen 160 MG/5 mL Oral Suspension",
                    rxcui="1",
                    tty="SCD",
                    score=20.0,
                ),
                RxNormSuggestion(
                    display_name="acetaminophen 650 MG Oral Tablet",
                    rxcui="2",
                    tty="SCD",
                    score=40.0,
                ),
            ],
            "acet": [
                RxNormSuggestion(
                    display_name="acetaminophen 160 MG/5 mL Oral Suspension",
                    rxcui="1",
                    tty="SCD",
                    score=15.0,
                ),
            ],
        }
    )
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    suggestions, cached_flag = await resolver.suggest("acet oral susp")

    assert cached_flag is False
    assert suggestions[0].display_name == "acetaminophen 160 MG/5 mL Oral Suspension"
    assert "acet oral suspension" in repository.suggest_queries
    assert "acet" in repository.suggest_queries


@pytest.mark.asyncio
async def test_rxnorm_resolver_filters_irrelevant_short_prefix_suggestions():
    cache = FakeCacheService()
    repository = FakeRxNormRepository(
        [
            RxNormSuggestion(
                display_name="ethanol 0.7 mL/mL Topical Solution",
                rxcui="1",
                tty="SCD",
                score=95.0,
            ),
            RxNormSuggestion(
                display_name="hydrogen peroxide 5 MG/mL Topical Solution",
                rxcui="2",
                tty="SCD",
                score=90.0,
            ),
            RxNormSuggestion(
                display_name="levetiracetam 100 MG/mL Oral Solution",
                rxcui="3",
                tty="SCD",
                score=45.0,
            ),
            RxNormSuggestion(
                display_name="levothyroxine sodium 25 MCG Oral Tablet",
                rxcui="4",
                tty="SCD",
                score=40.0,
            ),
        ]
    )
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    suggestions, cached_flag = await resolver.suggest("lev")

    assert cached_flag is False
    assert [suggestion.rxcui for suggestion in suggestions] == ["3", "4"]


@pytest.mark.asyncio
async def test_rxnorm_resolver_ignores_too_short_queries():
    cache = FakeCacheService()
    repository = FakeRxNormRepository([RxNormSuggestion(display_name="Unused")])
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    suggestions, cached_flag = await resolver.suggest("a")

    assert suggestions == []
    assert cached_flag is False
    assert repository.calls == 0


@pytest.mark.asyncio
async def test_rxnorm_resolver_prefers_oral_liquid_match_and_keeps_all_candidates():
    cache = FakeCacheService()
    repository = FakeRxNormRepository(
        suggestions=[],
        candidates=[
            RxNormConcept(
                rxcui="1",
                name="amoxicillin 400 MG/5 mL Oral Suspension",
                tty="SCD",
                score=50.0,
                source="rxnorm_api",
            ),
            RxNormConcept(
                rxcui="2",
                name="amoxicillin 400 MG Injection",
                tty="SCD",
                score=60.0,
                source="rxnorm_api",
            ),
            RxNormConcept(
                rxcui="3",
                name="amoxicillin",
                tty="IN",
                score=20.0,
                source="rxnorm_api",
            ),
        ],
    )
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)
    query = DrugQuery(
        raw_text="amoxicillin 400 mg/5 mL suspension",
        normalized_text="amoxicillin 400 mg/5 mL suspension",
        requested_dose_form="SUSPENSION",
        requested_strength="400 mg/5 mL",
    )

    concepts = await resolver.resolve(query)

    assert [concept.rxcui for concept in concepts] == ["1", "3", "2"]
    assert len(concepts) == 3
    assert repository.resolve_calls == 1


@pytest.mark.asyncio
async def test_rxnorm_resolver_exact_match_beats_broader_brand_grouping():
    cache = FakeCacheService()
    repository = FakeRxNormRepository(
        suggestions=[],
        candidates=[
            RxNormConcept(
                rxcui="10",
                name="Keppra",
                tty="SBD",
                score=15.0,
                source="rxnorm_api",
            ),
            RxNormConcept(
                rxcui="11",
                name="Keppra Oral Product",
                tty="SCDG",
                score=25.0,
                source="rxnorm_api",
            ),
        ],
    )
    resolver = RxNormResolver(cache, repository, suggestion_limit=8, candidate_limit=16)

    concepts = await resolver.resolve("Keppra")

    assert concepts[0].rxcui == "10"
    assert concepts[0].tty == "SBD"
