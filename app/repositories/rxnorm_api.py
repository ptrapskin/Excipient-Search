"""Repository for live RxNorm API access."""

from __future__ import annotations

import asyncio
import logging

import httpx

from app.config import Settings
from app.domain.models import RxNormConcept, RxNormSuggestion
from app.repositories.http_client import get_with_retry

logger = logging.getLogger(__name__)

USEFUL_TTYS = {"SCD", "SBD", "SCDG", "IN", "PIN", "MIN"}


class RxNormAPIError(RuntimeError):
    """Raised when RxNorm requests fail."""


class RxNormApiRepository:
    """Small client for RxNorm autocomplete and concept lookup."""

    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self._settings = settings
        self._client = client

    async def resolve_candidates(self, query_text: str, limit: int) -> list[RxNormConcept]:
        """Resolve RxNorm candidates using exact-ish and broader drug lookups."""

        exact_ids = await self._find_rxcuis_by_name(query_text)
        exact_concepts = await self._fetch_concepts(exact_ids)
        drug_concepts = await self._get_drug_candidates(query_text)
        approximate_concepts = await self._get_approximate_candidates(query_text, limit=limit)

        merged: dict[str, RxNormConcept] = {}
        for concept in [*exact_concepts.values(), *drug_concepts, *approximate_concepts]:
            if concept.tty not in USEFUL_TTYS:
                continue
            existing = merged.get(concept.rxcui)
            if existing is None or concept.score > existing.score:
                merged[concept.rxcui] = concept

        # Brand-name searches (only SBD results, no ingredient concepts) need the generic
        # ingredient name so DailyMed can return all formulations, not just branded ones.
        has_ingredient = any(c.tty in ("IN", "PIN") for c in merged.values())
        if not has_ingredient:
            sbd_rxcuis = [c.rxcui for c in merged.values() if c.tty == "SBD"]
            if sbd_rxcuis:
                ingredient_concepts = await self._get_related_ingredients(sbd_rxcuis[:3])
                for concept in ingredient_concepts:
                    if concept.rxcui not in merged:
                        merged[concept.rxcui] = concept

        return list(merged.values())

    async def suggest(self, query_text: str, limit: int) -> list[RxNormSuggestion]:
        """Return RxNorm autocomplete suggestions for a user query."""

        approximate_concepts = await self._get_approximate_candidates(query_text, limit=limit)
        drug_concepts = await self._get_drug_candidates(query_text)

        merged: dict[str, RxNormConcept] = {}
        for concept in [*approximate_concepts, *drug_concepts]:
            if concept.tty not in USEFUL_TTYS:
                continue
            existing = merged.get(concept.rxcui)
            if existing is None or concept.score > existing.score:
                merged[concept.rxcui] = concept

        suggestions: list[RxNormSuggestion] = []
        for concept in merged.values():
            suggestions.append(
                RxNormSuggestion(
                    display_name=concept.name,
                    rxcui=concept.rxcui,
                    tty=concept.tty,
                    score=concept.score,
                )
            )
        return suggestions[:limit]

    async def get_concept(self, rxcui: str) -> RxNormConcept | None:
        """Retrieve concept properties for a known RxCUI."""

        payload = await self._fetch_json(f"/rxcui/{rxcui}/properties.json")
        properties = payload.get("properties") or {}
        if not properties:
            return None
        return RxNormConcept(
            rxcui=str(properties.get("rxcui") or rxcui),
            name=str(properties.get("name") or "").strip(),
            tty=str(properties.get("tty") or "").strip() or "",
            score=0.0,
            source="rxnorm_api",
        )

    async def _fetch_concepts(self, rxcuis: list[str]) -> dict[str, RxNormConcept]:
        """Fetch RxNorm concept properties concurrently."""

        async def load_one(rxcui: str) -> tuple[str, RxNormConcept | None]:
            try:
                return rxcui, await self.get_concept(rxcui)
            except RxNormAPIError:
                logger.warning("Failed to enrich RxNorm concept %s", rxcui)
                return rxcui, None

        pairs = await asyncio.gather(*(load_one(rxcui) for rxcui in rxcuis))
        return {rxcui: concept for rxcui, concept in pairs if concept is not None}

    async def _find_rxcuis_by_name(self, query_text: str) -> list[str]:
        """Find exact-ish RxNorm identifiers for a normalized string."""

        payload = await self._fetch_json(
            "/rxcui.json",
            params={"name": query_text, "search": 2},
        )
        id_group = payload.get("idGroup", {})
        ids = id_group.get("rxnormId", [])
        if isinstance(ids, str):
            return [ids]
        return [str(value).strip() for value in ids if str(value).strip()]

    async def _get_drug_candidates(self, query_text: str) -> list[RxNormConcept]:
        """Return concept properties from the RxNorm drugs endpoint."""

        payload = await self._fetch_json("/drugs.json", params={"name": query_text})
        concepts: list[RxNormConcept] = []
        drug_group = payload.get("drugGroup", {})
        concept_groups = drug_group.get("conceptGroup", [])
        if isinstance(concept_groups, dict):
            concept_groups = [concept_groups]

        for group in concept_groups:
            tty = str(group.get("tty") or "").strip()
            concept_properties = group.get("conceptProperties", [])
            if isinstance(concept_properties, dict):
                concept_properties = [concept_properties]
            for concept in concept_properties:
                rxcui = str(concept.get("rxcui") or "").strip()
                name = str(concept.get("name") or "").strip()
                concept_tty = str(concept.get("tty") or tty).strip()
                if not rxcui or not name or not concept_tty:
                    continue
                concepts.append(
                    RxNormConcept(
                        rxcui=rxcui,
                        name=name,
                        tty=concept_tty,
                        score=5.0,
                        source="rxnorm_api",
                    )
                )
        return concepts

    async def _get_approximate_candidates(self, query_text: str, limit: int) -> list[RxNormConcept]:
        """Return approximate match candidates enriched with concept properties."""

        payload = await self._fetch_json(
            "/approximateTerm.json",
            params={"term": query_text, "maxEntries": limit, "option": 1},
        )
        group = payload.get("approximateGroup", {})
        raw_candidates = group.get("candidate", [])
        if isinstance(raw_candidates, dict):
            raw_candidates = [raw_candidates]

        candidates_by_rxcui: dict[str, dict] = {}
        for candidate in raw_candidates:
            rxcui = str(candidate.get("rxcui", "")).strip()
            if not rxcui or rxcui in candidates_by_rxcui:
                continue
            candidates_by_rxcui[rxcui] = candidate

        concepts = await self._fetch_concepts(list(candidates_by_rxcui))
        resolved: list[RxNormConcept] = []
        for rxcui, candidate in candidates_by_rxcui.items():
            concept = concepts.get(rxcui)
            name = concept.name if concept is not None else str(candidate.get("name") or "").strip()
            tty = concept.tty if concept is not None else ""
            score_value = candidate.get("score")
            score = float(score_value) if score_value not in (None, "") else 0.0
            if not name or not tty:
                continue
            resolved.append(
                RxNormConcept(
                    rxcui=rxcui,
                    name=name,
                    tty=tty,
                    score=score,
                    source="rxnorm_api",
                )
            )
        return resolved

    async def _get_related_ingredients(self, sbd_rxcuis: list[str]) -> list[RxNormConcept]:
        """Return IN-type ingredient concepts related to the given SBD RxCUIs."""

        seen: set[str] = set()
        results: list[RxNormConcept] = []
        for rxcui in sbd_rxcuis:
            try:
                payload = await self._fetch_json(
                    f"/rxcui/{rxcui}/related.json",
                    params={"tty": "IN"},
                )
                groups = payload.get("relatedGroup", {}).get("conceptGroup", [])
                if isinstance(groups, dict):
                    groups = [groups]
                for group in groups:
                    props = group.get("conceptProperties", [])
                    if isinstance(props, dict):
                        props = [props]
                    for prop in props:
                        related_rxcui = str(prop.get("rxcui") or "").strip()
                        name = str(prop.get("name") or "").strip()
                        if related_rxcui and name and related_rxcui not in seen:
                            seen.add(related_rxcui)
                            results.append(
                                RxNormConcept(
                                    rxcui=related_rxcui,
                                    name=name,
                                    tty="IN",
                                    score=0.0,
                                    source="rxnorm_api",
                                )
                            )
            except RxNormAPIError:
                logger.warning("Failed to fetch related ingredients for rxcui %s", rxcui)
        return results

    async def _fetch_json(self, path: str, params: dict | None = None) -> dict:
        """Fetch JSON with lightweight retry handling."""

        response = await get_with_retry(
            self._client,
            url=f"{self._settings.rxnorm_base_url}{path}",
            params=params,
            retries=self._settings.http_retries,
            error_cls=RxNormAPIError,
            label="RxNorm",
        )
        try:
            return response.json()
        except ValueError as exc:
            raise RxNormAPIError("RxNorm returned invalid JSON") from exc
