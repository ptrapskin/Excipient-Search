import pytest

from app.domain.models import DrugQuery, ProductCandidate, RxNormConcept
from app.services.product_expander import ProductExpander


class FakeDailyMedRepository:
    def __init__(self, candidates: list[ProductCandidate]) -> None:
        self.candidates = candidates
        self.calls = 0

    async def find_products(self, query: DrugQuery, rxnorm: list[RxNormConcept]) -> list[ProductCandidate]:
        self.calls += 1
        return self.candidates


@pytest.mark.asyncio
async def test_product_expander_converts_candidates_to_search_results():
    repository = FakeDailyMedRepository(
        [
            ProductCandidate(
                rxcui="123",
                setid="set-123",
                ndc="11111-111-11",
                product_name="Metformin 500 MG Oral Tablet",
                labeler="ACME",
                dosage_form="TABLET",
                route="ORAL",
                source="dailymed_api",
            )
        ]
    )
    expander = ProductExpander(repository, limit=10)
    query = DrugQuery(raw_text="metformin", normalized_text="metformin")
    concepts = [
        RxNormConcept(rxcui="123", name="Metformin 500 MG Oral Tablet", tty="SCD", score=99.0, source="rxnorm_api")
    ]

    results = await expander.expand_from_concepts(query, concepts)

    assert repository.calls == 1
    assert results[0].product_name == "Metformin 500 MG Oral Tablet"
    assert results[0].setid == "set-123"
    assert results[0].ndcs == ["11111-111-11"]
