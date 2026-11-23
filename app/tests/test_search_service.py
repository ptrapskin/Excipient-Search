from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.config import Settings
from app.domain.enums import DataSource
from app.domain.models import CachedSearch, DrugQuery, IngredientEntry, ProductComparisonRow, ProductDetail, ProductSearchResult, RxNormConcept
from app.services.excipient_filter import ExcipientFilterService
from app.repositories.dailymed_api import DailyMedApiRepository
from app.services.parsing_service import ParsingService
from app.services.search_service import SearchService


class FakeCacheService:
    def __init__(self, cached: CachedSearch | None = None) -> None:
        self.cached = cached
        self.saved_results: list[ProductSearchResult] = []
        self.recorded_query_key: str | None = None
        self.cached_products: dict[str, ProductDetail] = {}

    async def record_normalized_query(self, query_key: str, query: DrugQuery) -> None:
        self.recorded_query_key = query_key

    async def get_search_results(self, query_key: str) -> CachedSearch | None:
        return self.cached

    async def set_search_results(self, query_key, query_text, results, source):
        self.saved_results = results
        return CachedSearch(
            query_key=query_key,
            query_text=query_text,
            results=results,
            source=source,
            fetched_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    async def get_product_detail(self, setid: str):
        product = self.cached_products.get(setid)
        if product is None:
            return None
        return type("CachedProduct", (), {"product": product})()

    async def set_product_detail(self, product: ProductDetail, source):
        self.cached_products[product.setid] = product
        return type("CachedProduct", (), {"product": product})()


class FakeDailyMedRepository:
    def __init__(self, search_results: dict[str, list[ProductSearchResult]] | None = None) -> None:
        self.detail_calls = 0
        self.search_results = search_results or {}
        self.searched: list[str] = []

    async def search_spls(self, query: DrugQuery) -> list[ProductSearchResult]:
        self.searched.append(query.normalized_text)
        return self.search_results.get(query.normalized_text, [])

    async def get_all_product_details(self, setid: str):
        self.detail_calls += 1
        return [ProductDetail(
            product_name=f"Product {setid}",
            setid=setid,
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            labeler="Labeler",
            active_ingredients=[
                IngredientEntry(
                    raw_name="amoxicillin 400 mg/5 mL",
                    display_name="amoxicillin",
                    normalized_name="Amoxicillin",
                    strength="400 mg/5 mL",
                    role="active",
                    source_type="xml_structured",
                    confidence="medium",
                )
            ],
            inactive_ingredients_raw="sucrose",
            inactive_ingredients=[
                IngredientEntry(
                    raw_name="sucrose",
                    display_name="sucrose",
                    normalized_name="Sucrose",
                    role="inactive",
                    confidence="medium",
                )
            ],
            spl_source="https://example.test/spl.xml",
            fetched_at=datetime.now(timezone.utc),
        )]


class FakeRxNormResolver:
    def __init__(self, concepts: list[RxNormConcept] | None = None) -> None:
        self.concepts = concepts or []
        self.calls = 0

    async def resolve(self, query: DrugQuery) -> list[RxNormConcept]:
        self.calls += 1
        return self.concepts


@pytest.mark.asyncio
async def test_search_service_ignores_cached_search_results_and_hits_live_workflow():
    cached = CachedSearch(
        query_key="key",
        query_text="metformin",
        results=[ProductSearchResult(product_name="Cached Product", setid="abc")],
        source=DataSource.SQLITE_CACHE,
        fetched_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    cache = FakeCacheService(cached=cached)
    repository = FakeDailyMedRepository({"metformin": [ProductSearchResult(product_name="Live Product", setid="live-1")]})
    resolver = FakeRxNormResolver()
    service = SearchService(cache, repository, resolver, ExcipientFilterService())

    query, results, cached_flag = await service.search("metformin")

    assert query.normalized_text == "metformin"
    assert cached_flag is False
    assert [r.product_name for r in results] == ["Live Product"]
    assert repository.detail_calls == 0
    assert resolver.calls == 1


@pytest.mark.asyncio
async def test_search_service_caches_live_results():
    cache = FakeCacheService()
    repository = FakeDailyMedRepository({"metformin": [ProductSearchResult(product_name="Live Product", setid="abc")]})
    service = SearchService(cache, repository, FakeRxNormResolver(), ExcipientFilterService())

    await service.search("metformin")

    assert [r.product_name for r in cache.saved_results] == ["Live Product"]


@pytest.mark.asyncio
async def test_search_service_adds_products_for_rxnorm_ingredient_aliases():
    cache = FakeCacheService()
    repository = FakeDailyMedRepository({
        "keppra": [ProductSearchResult(product_name="Keppra", setid="brand")],
        "levetiracetam": [ProductSearchResult(product_name="Levetiracetam", setid="generic")],
    })
    resolver = FakeRxNormResolver([
        RxNormConcept(rxcui="1", name="levetiracetam", tty="IN", score=100.0, source="rxnorm_api"),
        RxNormConcept(rxcui="2", name="Keppra 500 MG Oral Tablet", tty="SBD", score=90.0, source="rxnorm_api"),
    ])
    service = SearchService(cache, repository, resolver, ExcipientFilterService())

    _, results, _ = await service.search("keppra")

    assert repository.searched == ["keppra", "levetiracetam"]
    assert {r.setid for r in results} == {"brand", "generic"}


@pytest.mark.asyncio
async def test_search_service_builds_filtered_excipient_comparison():
    cache = FakeCacheService()
    repository = FakeDailyMedRepository({"amoxicillin": [
        ProductSearchResult(product_name="Product A", setid="set-a"),
        ProductSearchResult(product_name="Product B", setid="set-b"),
    ]})
    resolver = FakeRxNormResolver()

    async def fake_all_details(setid: str):
        repository.detail_calls += 1
        if setid == "set-a":
            ingredients = [
                IngredientEntry(
                    raw_name="red dye 40",
                    display_name="red dye 40",
                    normalized_name="Red Dye 40",
                    role="inactive",
                    confidence="medium",
                ),
                IngredientEntry(
                    raw_name="sucrose",
                    display_name="sucrose",
                    normalized_name="Sucrose",
                    role="inactive",
                    confidence="medium",
                ),
            ]
        else:
            ingredients = [
                IngredientEntry(
                    raw_name="sucrose",
                    display_name="sucrose",
                    normalized_name="Sucrose",
                    role="inactive",
                    confidence="medium",
                ),
            ]
        return [ProductDetail(
            product_name=f"Product {setid}",
            setid=setid,
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            labeler="Labeler",
            active_ingredients=[
                IngredientEntry(
                    raw_name="amoxicillin 400 mg/5 mL",
                    display_name="amoxicillin",
                    normalized_name="Amoxicillin",
                    strength="400 mg/5 mL",
                    role="active",
                    source_type="xml_structured",
                    confidence="medium",
                )
            ],
            inactive_ingredients_raw="; ".join(ingredient.raw_name for ingredient in ingredients),
            inactive_ingredients=ingredients,
            spl_source="https://example.test/spl.xml",
            fetched_at=datetime.now(timezone.utc),
        )]

    repository.get_all_product_details = fake_all_details
    service = SearchService(cache, repository, resolver, ExcipientFilterService())

    query, results, comparison_rows, excipient_filter, matching_rows, cached_flag, unavailable = (
        await service.search_with_excipients("amoxicillin", exclude_terms="red dye")
    )

    assert query.normalized_text == "amoxicillin"
    assert cached_flag is False
    assert unavailable == 0
    assert len(results) == 2
    assert len(comparison_rows) == 2
    assert excipient_filter.exclude_terms == ["red dye"]
    assert [row.setid for row in matching_rows] == ["set-b"]


@pytest.mark.asyncio
async def test_dailymed_search_parses_live_json():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/spls.json")
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "title": "METFORMIN HYDROCHLORIDE TABLET [ACME LABS]",
                        "setid": "set-123",
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://example.org") as client:
        settings = Settings(dailymed_base_url="https://example.org")
        repository = DailyMedApiRepository(settings, client, ParsingService())
        query = DrugQuery(raw_text="metformin", normalized_text="metformin")

        results = await repository.search_products(query)

    assert len(results) == 1
    assert results[0].product_name == "METFORMIN HYDROCHLORIDE TABLET"
    assert results[0].labeler == "ACME LABS"
    assert results[0].setid == "set-123"
