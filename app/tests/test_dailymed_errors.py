import asyncio

import httpx
import pytest

from app.domain.models import DrugQuery, ProductSearchResult
from app.repositories import http_client
from app.repositories.dailymed_api import DailyMedAPIError
from app.repositories.dailymed_composite import CompositeDailyMedRepository
from app.repositories.http_client import get_with_retry


def _client(statuses: list[int], headers: dict | None = None) -> tuple[httpx.AsyncClient, list[int]]:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(status)
        return httpx.Response(status, headers=headers or {}, json={})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), calls


@pytest.fixture
def sleeps(monkeypatch):
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr(http_client.asyncio, "sleep", fake_sleep)
    return recorded


def test_retry_waits_for_retry_after_on_rate_limit(sleeps):
    client, calls = _client([429, 200], headers={"Retry-After": "2"})

    response = asyncio.run(get_with_retry(client, "https://example.test", error_cls=DailyMedAPIError))

    assert response.status_code == 200
    assert calls == [429, 200]
    assert sleeps == [2.0]


def test_retry_backs_off_and_caps_wait(sleeps):
    client, calls = _client([503], headers={"Retry-After": "120"})

    with pytest.raises(DailyMedAPIError):
        asyncio.run(get_with_retry(client, "https://example.test", retries=2, error_cls=DailyMedAPIError))

    assert calls == [503, 503, 503]
    assert sleeps == [http_client.MAX_RETRY_DELAY_SECONDS] * 2


def test_retry_does_not_repeat_client_errors(sleeps):
    client, calls = _client([400])

    with pytest.raises(DailyMedAPIError):
        asyncio.run(get_with_retry(client, "https://example.test", error_cls=DailyMedAPIError))

    assert calls == [400]
    assert sleeps == []


class _Local:
    def __init__(self, results):
        self.results = results
        self.saved = []

    async def search_spls(self, query):
        return self.results

    def save_products(self, products):
        self.saved.extend(products)


class _FailingApi:
    async def search_spls(self, query):
        raise DailyMedAPIError("DailyMed request failed")


def test_composite_search_raises_when_live_fails_and_nothing_cached():
    repo = CompositeDailyMedRepository(local_repository=_Local([]), api_repository=_FailingApi())

    with pytest.raises(DailyMedAPIError):
        asyncio.run(repo.search_spls(DrugQuery(raw_text="x", normalized_text="x")))


def test_composite_search_returns_cache_when_live_fails():
    cached = [ProductSearchResult(product_name="Cached Product", setid="set-1")]
    repo = CompositeDailyMedRepository(local_repository=_Local(cached), api_repository=_FailingApi())

    results = asyncio.run(repo.search_spls(DrugQuery(raw_text="x", normalized_text="x")))

    assert [r.product_name for r in results] == ["Cached Product"]


def test_label_fetches_stop_at_time_budget():
    from datetime import datetime, timezone

    from app.domain.models import ExcipientFilter, ProductDetail
    from app.services.excipient_filter import ExcipientFilterService
    from app.services.search_service import SearchService

    class SlowRepo:
        async def get_all_product_details(self, setid):
            if setid == "slow":
                await asyncio.sleep(5)
            return [ProductDetail(product_name=setid, setid=setid, spl_source="test", fetched_at=datetime.now(timezone.utc))]

    service = SearchService(None, SlowRepo(), None, ExcipientFilterService())
    results = [ProductSearchResult(product_name=s, setid=s) for s in ("fast", "slow")]

    async def run():
        deadline = asyncio.get_running_loop().time() + 0.2
        return await service._build_comparison_rows(results, ExcipientFilter(), deadline)

    rows, kept, unavailable = asyncio.run(run())

    assert [r.product_name for r in rows] == ["fast"]
    assert unavailable == 1
