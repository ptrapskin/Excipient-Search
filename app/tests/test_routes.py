import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.domain.enums import DataSource
from app.domain.models import DrugQuery, ExcipientFilter, IngredientEntry, ProductComparisonRow, ProductDetail, ProductSearchResult, RxNormSuggestion
from app.main import create_app
from app.repositories.dailymed_api import DailyMedAPIError


@pytest.fixture
def client(tmp_path: Path):
    db_path = tmp_path / "route-tests.db"
    os.environ["EXCIPIENT_SEARCH_DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    # Keep route tests off the real product index; they fake the search service.
    os.environ["EXCIPIENT_SEARCH_PRODUCT_INDEX_PATH"] = str(tmp_path / "no-product-index.db")
    get_settings.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    os.environ.pop("EXCIPIENT_SEARCH_DATABASE_URL", None)
    os.environ.pop("EXCIPIENT_SEARCH_PRODUCT_INDEX_PATH", None)
    get_settings.cache_clear()


def test_search_page_renders(client):
    response = client.get("/")

    assert response.status_code == 200
    assert "Search DailyMed Products" in response.text


def test_results_page_shows_empty_state(client):
    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        return (
            DrugQuery(raw_text=raw_query, normalized_text=raw_query),
            [],
            [],
            ExcipientFilter(),
            [],
            False,
            0,
        )

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients
    response = client.get("/search", params={"q": "metformin"})

    assert response.status_code == 200
    assert "No DailyMed products matched this search." in response.text


def test_results_page_omits_excipient_comparison_section(client):
    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        query = DrugQuery(raw_text=raw_query, normalized_text=raw_query)
        result = ProductSearchResult(
            product_name="Amoxicillin Suspension",
            setid="set-1",
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            labeler="Labeler",
            source=DataSource.DAILMED_LIVE,
        )
        row = ProductComparisonRow(
            product_name="Amoxicillin Suspension",
            setid="set-1",
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            dailymed_strength="400 mg/5 mL",
            labeler="Labeler",
            inactive_ingredients=[
                IngredientEntry(
                    raw_name="sucrose",
                    display_name="sucrose",
                    normalized_name="Sucrose",
                    role="inactive",
                    confidence="medium",
                )
            ],
            matched_include_terms=["sucrose"],
            matched_exclude_terms=[],
            matches_filter=True,
            source=DataSource.DAILMED_LIVE,
        )
        return query, [result], [row], ExcipientFilter(include_terms=["sucrose"]), [row], False, 0

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients
    response = client.get("/search", params={"q": "amoxicillin", "include": "sucrose"})

    assert response.status_code == 200
    assert "Matching Products" in response.text
    assert "Excipient Comparison" not in response.text
    assert "Strength" in response.text
    assert "400 mg/5 mL" in response.text
    assert "Filter by form:" in response.text
    assert 'data-sort-key="product"' in response.text


def test_rxnorm_api_route_returns_suggestions(client):
    async def fake_suggest(query: str):
        return (
            [RxNormSuggestion(display_name="Metformin 500 MG Oral Tablet", rxcui="1", tty="SCD")],
            False,
        )

    client.app.state.container.rxnorm_resolver.suggest = fake_suggest
    response = client.get("/api/rxnorm/suggest", params={"q": "metformin"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["suggestions"][0]["display_name"] == "Metformin 500 MG Oral Tablet"


def test_product_api_route_returns_detail(client):
    async def fake_detail(setid: str):
        return (
            ProductDetail(
                product_name="Example Product",
                setid=setid,
                ndcs=["12345-678-90"],
                route="ORAL",
                dosage_form="TABLET",
                labeler="Example Labeler",
                active_ingredients=[
                    IngredientEntry(
                        raw_name="metformin hydrochloride 500 mg",
                        display_name="metformin hydrochloride",
                        normalized_name="Metformin Hydrochloride",
                        strength="500 mg",
                        role="active",
                        source_type="xml_structured",
                        confidence="medium",
                    )
                ],
                inactive_ingredients_raw="lactose monohydrate",
                inactive_ingredients=[
                    IngredientEntry(
                        raw_name="lactose monohydrate",
                        display_name="lactose monohydrate",
                        normalized_name="Lactose Monohydrate",
                        role="inactive",
                        confidence="medium",
                    )
                ],
                spl_source="https://example.org/spl.xml",
                fetched_at=datetime.now(timezone.utc),
            ),
            False,
        )

    client.app.state.container.search_service.get_product_detail = fake_detail
    response = client.get("/api/products/set-123")

    assert response.status_code == 200
    payload = response.json()
    assert payload["product"]["setid"] == "set-123"
    assert payload["product"]["inactive_ingredients"][0]["raw_name"] == "lactose monohydrate"


def test_search_api_route_returns_filtered_matches(client):
    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        query = DrugQuery(raw_text=raw_query, normalized_text=raw_query)
        result = ProductSearchResult(
            product_name="Amoxicillin Suspension",
            setid="set-1",
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            labeler="Labeler",
            source=DataSource.DAILMED_LIVE,
        )
        row = ProductComparisonRow(
            product_name="Amoxicillin Suspension",
            setid="set-1",
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="SUSPENSION",
            labeler="Labeler",
            inactive_ingredients=[
                IngredientEntry(
                    raw_name="sucrose",
                    display_name="sucrose",
                    normalized_name="Sucrose",
                    role="inactive",
                    confidence="medium",
                )
            ],
            matched_exclude_terms=[],
            matches_filter=True,
            source=DataSource.DAILMED_LIVE,
        )
        return query, [result], [row], ExcipientFilter(exclude_terms=["red dye"]), [row], False, 0

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients
    response = client.get("/api/search", params={"q": "amoxicillin", "exclude": "red dye"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["filter"]["exclude_terms"] == ["red dye"]
    assert payload["matching_products"][0]["product_name"] == "Amoxicillin Suspension"


def test_gluten_screener_page_renders_without_query(client):
    response = client.get("/gluten-screener")

    assert response.status_code == 200
    assert "Gluten Excipient Screener" in response.text
    assert "How this screen works" in response.text


def test_gluten_screener_ranks_products_by_gluten_concern(client):
    def make_row(name: str, setid: str, inactive: list[str]) -> ProductComparisonRow:
        return ProductComparisonRow(
            product_name=name,
            setid=setid,
            ndcs=["12345-678-90"],
            route="ORAL",
            dosage_form="TABLET",
            labeler="Labeler",
            inactive_ingredients=[IngredientEntry(raw_name=i, display_name=i) for i in inactive],
            source=DataSource.DAILMED_LIVE,
        )

    rows = [
        make_row("Alpha Corn Tablet", "set-1", ["STARCH, CORN"]),
        make_row("Beta Plain Starch Tablet", "set-2", ["STARCH"]),
        make_row("Gamma Wheat Tablet", "set-3", ["STARCH, WHEAT"]),
    ]

    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        return DrugQuery(raw_text=raw_query, normalized_text=raw_query), [], rows, ExcipientFilter(), rows, False, 0

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients
    response = client.get("/gluten-screener", params={"q": "example"})

    assert response.status_code == 200
    text = response.text
    assert text.index("Gamma Wheat Tablet") < text.index("Beta Plain Starch Tablet") < text.index("Alpha Corn Tablet")
    assert "Verify starch source" in text
    assert 'data-default-sort="gluten"' in text


def test_results_page_warns_when_labels_could_not_load(client):
    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        return DrugQuery(raw_text=raw_query, normalized_text=raw_query), [], [], ExcipientFilter(), [], False, 3

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients

    for path in ("/search", "/gluten-screener"):
        response = client.get(path, params={"q": "example"})
        assert response.status_code == 200
        assert "3 labels could not be loaded from DailyMed in time" in response.text


def test_gluten_screener_reports_dailymed_outage(client):
    async def fake_search_with_excipients(raw_query: str, include_terms=None, exclude_terms=None):
        raise DailyMedAPIError("DailyMed request failed")

    client.app.state.container.search_service.search_with_excipients = fake_search_with_excipients
    response = client.get("/gluten-screener", params={"q": "example"})

    assert response.status_code == 502
    assert "DailyMed is temporarily unavailable" in response.text
    assert "No DailyMed products matched" not in response.text


def test_disposal_pages_are_separate(client):
    meds = client.get("/dispose-meds")
    sharps = client.get("/dispose-sharps")

    assert meds.status_code == 200
    assert "Safe Medication Disposal" in meds.text
    assert "How to use a drop box" in meds.text
    assert sharps.status_code == 200
    assert "Safe Sharps Disposal" in sharps.text
    assert "What counts as sharps" in sharps.text
    assert 'href="/dispose-sharps"' in meds.text
