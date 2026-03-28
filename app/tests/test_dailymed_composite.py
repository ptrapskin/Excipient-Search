from pathlib import Path

import pytest

from app.domain.models import DrugQuery, ProductCandidate, ProductDetail
from app.repositories.dailymed_composite import CompositeDailyMedRepository
from app.repositories.dailymed_zip import DailyMedZipRepository
from app.services.parsing_service import ParsingService


class FakeApiRepository:
    def __init__(self) -> None:
        self.find_calls = 0
        self.spl_calls = 0

    async def find_products(self, query, rxnorm):
        self.find_calls += 1
        return [
            ProductCandidate(
                rxcui="123",
                setid="set-123",
                ndc="11111-111-11",
                product_name="Metformin Live Product",
                labeler="Live Labeler",
                dosage_form="TABLET",
                route="ORAL",
                source="dailymed_api",
            )
        ]

    async def get_spl(self, setid: str) -> dict:
        self.spl_calls += 1
        return {
            "setid": setid,
            "xml_text": """
            <document xmlns="urn:hl7-org:v3">
                <title>Live Product</title>
                <author>
                    <assignedEntity>
                        <representedOrganization><name>Live Labeler</name></representedOrganization>
                    </assignedEntity>
                </author>
                <routeCode displayName="ORAL" />
                <manufacturedProduct><manufacturedMedicine><formCode displayName="TABLET" /></manufacturedMedicine></manufacturedProduct>
                <section><title>Inactive Ingredients</title><text><paragraph>lactose</paragraph></text></section>
            </document>
            """,
            "source": "dailymed_api",
        }

    async def get_inactive_ingredients(self, setid: str) -> list[dict]:
        return [{"raw_name": "lactose", "normalized_name": "Lactose", "unii": None, "confidence": "medium"}]

    async def get_product_detail(self, setid: str) -> ProductDetail:
        raise AssertionError("Composite test should use get_spl cache path instead")


@pytest.mark.asyncio
async def test_composite_repository_caches_live_product_candidates_locally(tmp_path: Path):
    local_repository = DailyMedZipRepository(tmp_path, ParsingService())
    api_repository = FakeApiRepository()
    repository = CompositeDailyMedRepository(local_repository, api_repository, result_limit=1)

    query = DrugQuery(raw_text="metformin", normalized_text="metformin")
    first = await repository.find_products(query, [])
    second = await repository.find_products(query, [])

    assert first[0].product_name == "Metformin Live Product"
    assert second[0].product_name == "Metformin Live Product"
    assert api_repository.find_calls == 1


@pytest.mark.asyncio
async def test_composite_repository_caches_spl_locally(tmp_path: Path):
    local_repository = DailyMedZipRepository(tmp_path, ParsingService())
    api_repository = FakeApiRepository()
    repository = CompositeDailyMedRepository(local_repository, api_repository, result_limit=10)

    first = await repository.get_spl("set-123")
    second = await repository.get_spl("set-123")

    assert first["setid"] == "set-123"
    assert second["setid"] == "set-123"
    assert api_repository.spl_calls == 1
