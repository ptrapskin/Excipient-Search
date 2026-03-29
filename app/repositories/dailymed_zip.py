"""Local DailyMed ZIP/index repository and cache."""

from __future__ import annotations

import json
import re
from pathlib import Path

from app.domain.enums import DataSource
from app.domain.models import DrugQuery, ProductCandidate, ProductDetail, ProductSearchResult
from app.repositories.dailymed_api import (
    build_all_product_details_from_xml,
    build_product_detail_from_xml,
    build_product_inactive_ingredient_dicts,
)
from app.services.parsing_service import ParsingService


class DailyMedZipRepository:
    """Local file-backed repository for future ZIP/index support and live result caching."""

    def __init__(self, data_root: Path, parsing_service: ParsingService) -> None:
        self._data_root = data_root
        self._parsing_service = parsing_service
        self._index_dir = self._data_root / "local_index"
        self._index_path = self._index_dir / "product_candidates.json"
        self._spl_dir = self._index_dir / "spl"

    async def search_spls(self, query: DrugQuery) -> list[ProductSearchResult]:
        """Search the local index for SPL records matching the query."""

        records = self._load_index()
        if not records:
            return []

        results: list[ProductSearchResult] = []
        seen: set[str] = set()
        query_text = query.normalized_text.casefold()

        for record in records:
            product_name = str(record.get("product_name") or "").strip()
            labeler = str(record.get("labeler") or "").strip() or None
            dosage_form = str(record.get("dosage_form") or "").strip() or None
            route = str(record.get("route") or "").strip() or None
            rxcui = str(record.get("rxcui") or "").strip() or None
            ndc = str(record.get("ndc") or "").strip() or None
            setid = str(record.get("setid") or "").strip() or None

            # Match by NDC or drug name text
            if query.ndc:
                if ndc != query.ndc:
                    continue
            elif not query_text or query_text not in product_name.casefold():
                continue

            # Apply dose form and route filters
            if query.requested_dose_form and dosage_form:
                if query.requested_dose_form.casefold() not in dosage_form.casefold():
                    continue
            if query.requested_route and route:
                if query.requested_route.casefold() not in route.casefold():
                    continue

            key = setid or ndc or product_name.casefold()
            if key in seen:
                continue
            seen.add(key)

            results.append(
                ProductSearchResult(
                    product_name=product_name or "Unknown product",
                    setid=setid,
                    ndcs=[ndc] if ndc else [],
                    route=route,
                    dosage_form=dosage_form,
                    labeler=labeler,
                    rxcui=rxcui,
                    source=DataSource.DAILMED_LIVE,
                )
            )
        return results

    async def find_products(self, query: DrugQuery, concepts: list) -> list[ProductCandidate]:
        """Return locally cached product candidates matching the query."""

        results = await self.search_spls(query)
        return [
            ProductCandidate(
                rxcui=result.rxcui,
                setid=result.setid,
                ndc=result.ndcs[0] if result.ndcs else None,
                product_name=result.product_name,
                labeler=result.labeler,
                dosage_form=result.dosage_form,
                route=result.route,
                source=str(result.source),
            )
            for result in results
        ]

    async def get_spl(self, setid: str) -> dict:
        """Return locally cached SPL XML when available."""

        spl_path = self._spl_dir / f"{setid}.xml"
        if not spl_path.exists():
            return {}
        return {
            "setid": setid,
            "xml_text": spl_path.read_text(encoding="utf-8"),
            "source": "dailymed_zip",
        }

    async def get_inactive_ingredients(self, setid: str) -> list[dict]:
        """Return inactive ingredient dictionaries derived from local SPL XML."""

        spl = await self.get_spl(setid)
        xml_text = spl.get("xml_text")
        if not xml_text:
            return []
        return build_product_inactive_ingredient_dicts(xml_text, self._parsing_service)

    async def get_product_detail(self, setid: str) -> ProductDetail:
        """Build a product detail from local SPL XML."""

        spl = await self.get_spl(setid)
        xml_text = spl.get("xml_text")
        if not xml_text:
            raise FileNotFoundError(setid)
        return build_product_detail_from_xml(
            xml_text=xml_text,
            setid=setid,
            base_url="local-cache",
            parsing_service=self._parsing_service,
        )

    async def get_all_product_details(self, setid: str) -> list[ProductDetail]:
        """Return one ProductDetail per product subject in the local SPL."""

        spl = await self.get_spl(setid)
        xml_text = spl.get("xml_text")
        if not xml_text:
            raise FileNotFoundError(setid)
        return build_all_product_details_from_xml(
            xml_text=xml_text,
            setid=setid,
            base_url="local-cache",
            parsing_service=self._parsing_service,
        )

    def save_products(self, products: list[ProductCandidate]) -> None:
        """Persist merged product candidates into the local index."""

        try:
            self._index_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return  # read-only filesystem (e.g. Vercel deployment)
        existing = self._load_index()
        merged: dict[str, dict] = {}

        for record in existing:
            key = self._candidate_key(
                setid=str(record.get("setid") or "").strip() or None,
                ndc=str(record.get("ndc") or "").strip() or None,
                product_name=str(record.get("product_name") or "").strip(),
            )
            if key:
                merged[key] = record

        for product in products:
            key = self._candidate_key(product.setid, product.ndc, product.product_name)
            if not key:
                continue
            merged[key] = {
                "rxcui": product.rxcui,
                "setid": product.setid,
                "ndc": product.ndc,
                "product_name": product.product_name,
                "labeler": product.labeler,
                "dosage_form": product.dosage_form,
                "route": product.route,
                "source": product.source,
            }

        self._index_path.write_text(
            json.dumps(list(merged.values()), indent=2),
            encoding="utf-8",
        )

    def save_spl(self, setid: str, spl_payload: dict) -> None:
        """Persist raw SPL XML in the local cache."""

        xml_text = str(spl_payload.get("xml_text") or "")
        if not xml_text:
            return
        try:
            self._spl_dir.mkdir(parents=True, exist_ok=True)
            (self._spl_dir / f"{setid}.xml").write_text(xml_text, encoding="utf-8")
        except OSError:
            return  # read-only filesystem (e.g. Vercel deployment)

    def _load_index(self) -> list[dict]:
        """Load local index records when present."""

        if not self._index_path.exists():
            return []
        try:
            payload = json.loads(self._index_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
        return payload if isinstance(payload, list) else []

    def _candidate_key(
        self,
        setid: str | None,
        ndc: str | None,
        product_name: str,
    ) -> str | None:
        """Build a stable storage key for a product candidate."""

        if setid:
            return f"setid:{setid}"
        if ndc:
            return f"ndc:{ndc}"
        cleaned_name = re.sub(r"\s+", " ", product_name).strip().casefold()
        return f"name:{cleaned_name}" if cleaned_name else None
