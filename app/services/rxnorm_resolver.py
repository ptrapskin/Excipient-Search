"""RxNorm suggestion and resolution service."""

from __future__ import annotations

import hashlib
import re

from app.domain.enums import DataSource
from app.domain.models import DrugQuery, RxNormConcept, RxNormSuggestion
from app.repositories.rxnorm_api import RxNormApiRepository
from app.services.cache_service import CacheService
from app.services.normalize_query import normalize_query

PREFERRED_TTY_SCORES = {
    "SCD": 40.0,
    "SBD": 38.0,
    "SCDG": 28.0,
    "PIN": 18.0,
    "MIN": 16.0,
    "IN": 14.0,
}

ORAL_LIQUID_FORMS = {"SOLUTION", "SUSPENSION", "SYRUP"}
INJECTION_MARKERS = ("inject", "intraven", "intramuscular", "subcutaneous", "iv")
NON_ORAL_ROUTE_MARKERS = ("topical", "ophthalmic", "otic", "nasal", "dermal", "transdermal")
STOPWORDS = {
    "oral",
    "tablet",
    "capsule",
    "solution",
    "suspension",
    "syrup",
    "injectable",
    "injection",
    "ophthalmic",
    "topical",
    "otic",
    "nasal",
    "mg",
    "mcg",
    "g",
    "kg",
    "ml",
    "m",
    "l",
}

SUGGESTION_ABBREVIATIONS = {
    "cap": "capsule",
    "caps": "capsule",
    "inj": "injection",
    "liq": "liquid",
    "ophth": "ophthalmic",
    "po": "oral",
    "sol": "solution",
    "soln": "solution",
    "susp": "suspension",
    "syr": "syrup",
    "tab": "tablet",
    "tabs": "tablet",
    "top": "topical",
}


class RxNormResolver:
    """Resolve lightweight RxNorm suggestions for the UI."""

    def __init__(
        self,
        cache_service: CacheService,
        repository: RxNormApiRepository,
        suggestion_limit: int,
        candidate_limit: int,
    ) -> None:
        self._cache_service = cache_service
        self._repository = repository
        self._suggestion_limit = suggestion_limit
        self._candidate_limit = candidate_limit

    async def suggest(self, query_text: str) -> tuple[list[RxNormSuggestion], bool]:
        """Return cached or live RxNorm suggestions."""

        normalized = " ".join(query_text.strip().split())
        if len(normalized) < 2:
            return [], False

        query_key = hashlib.sha256(normalized.casefold().encode("utf-8")).hexdigest()
        cached = await self._cache_service.get_rxnorm_suggestions(query_key)
        if cached is not None:
            return cached.suggestions, True

        suggestion_query = self._expand_suggestion_query(normalized)
        suggestion_context = normalize_query(suggestion_query)
        variants = self._build_suggestion_variants(suggestion_query)

        merged: dict[str, RxNormSuggestion] = {}
        for variant in variants:
            live_suggestions = await self._repository.suggest(variant, limit=self._suggestion_limit)
            for suggestion in live_suggestions:
                key = suggestion.rxcui or suggestion.display_name.casefold()
                scored = self._rank_suggestion(suggestion_context, suggestion)
                existing = merged.get(key)
                if existing is None or (scored.score or 0.0) > (existing.score or 0.0):
                    merged[key] = scored

        suggestions = sorted(
            merged.values(),
            key=lambda item: item.score or 0.0,
            reverse=True,
        )
        suggestions = [
            suggestion
            for suggestion in suggestions
            if self._should_keep_suggestion(suggestion_context, suggestion)
        ][: self._suggestion_limit]
        await self._cache_service.set_rxnorm_suggestions(
            query_key=query_key,
            query_text=normalized,
            suggestions=suggestions,
            source=DataSource.RXNORM_LIVE,
        )
        return suggestions, False

    async def resolve(self, query: DrugQuery | str) -> list[RxNormConcept]:
        """Resolve a query into ranked RxNorm concept candidates."""

        drug_query = normalize_query(query) if isinstance(query, str) else query
        if len(drug_query.normalized_text) < 2:
            return []

        candidates = await self._repository.resolve_candidates(
            drug_query.normalized_text,
            limit=self._candidate_limit,
        )
        ranked = [self._rank_candidate(drug_query, candidate) for candidate in candidates]
        ranked.sort(key=lambda candidate: candidate.score, reverse=True)
        return ranked

    def _rank_suggestion(self, query: DrugQuery, suggestion: RxNormSuggestion) -> RxNormSuggestion:
        """Score a suggestion for shorthand-friendly autocomplete behavior."""

        candidate_name = suggestion.display_name.casefold()
        query_text = query.normalized_text.casefold()
        query_tokens = self._meaningful_tokens(query.normalized_text)
        candidate_tokens = self._candidate_tokens(suggestion.display_name)

        score = suggestion.score or 0.0

        if candidate_name == query_text:
            score += 120.0
        elif candidate_name.startswith(query_text):
            score += 50.0
        elif query_text in candidate_name:
            score += 25.0

        if query_tokens:
            prefix_matches = sum(
                1
                for token in query_tokens
                if any(candidate_token.startswith(token) for candidate_token in candidate_tokens)
            )
            score += (prefix_matches / len(query_tokens)) * 35.0
            if prefix_matches == len(query_tokens):
                score += 25.0

        if query.requested_route and query.requested_route.casefold() in candidate_name:
            score += 15.0
        if query.requested_dose_form and query.requested_dose_form.casefold() in candidate_name:
            score += 22.0

        return RxNormSuggestion(
            display_name=suggestion.display_name,
            rxcui=suggestion.rxcui,
            tty=suggestion.tty,
            score=score,
        )

    def _should_keep_suggestion(self, query: DrugQuery, suggestion: RxNormSuggestion) -> bool:
        """Filter out obviously irrelevant autocomplete suggestions."""

        query_tokens = self._ordered_meaningful_tokens(query.normalized_text)
        if not query_tokens:
            return True

        candidate_tokens = self._candidate_tokens(suggestion.display_name)
        query_text = query.normalized_text.casefold()
        candidate_name = suggestion.display_name.casefold()

        if len(query_tokens) == 1 and len(query_tokens[0]) <= 4:
            token = query_tokens[0]
            return candidate_name.startswith(token) or any(
                candidate_token.startswith(token) for candidate_token in candidate_tokens
            )

        matched_tokens = sum(
            1
            for token in query_tokens
            if candidate_name.startswith(token)
            or any(candidate_token.startswith(token) for candidate_token in candidate_tokens)
            or token in candidate_name
        )
        return matched_tokens > 0 or query_text in candidate_name

    def _rank_candidate(self, query: DrugQuery, candidate: RxNormConcept) -> RxNormConcept:
        """Apply workflow-aware ranking heuristics to a candidate concept."""

        candidate_name = candidate.name.casefold()
        query_text = query.normalized_text.casefold()
        query_tokens = self._meaningful_tokens(query.normalized_text)
        candidate_tokens = self._meaningful_tokens(candidate.name)

        score = candidate.score
        score += PREFERRED_TTY_SCORES.get(candidate.tty, -50.0)

        if candidate_name == query_text:
            score += 120.0
        elif candidate_name.startswith(query_text):
            score += 60.0
        elif query_text in candidate_name:
            score += 35.0

        if query.requested_strength:
            if query.requested_strength.casefold() in candidate_name:
                score += 30.0
            elif candidate.tty in {"SCD", "SBD"}:
                score -= 12.0

        shared_tokens = query_tokens & candidate_tokens
        if query_tokens:
            score += (len(shared_tokens) / len(query_tokens)) * 25.0
            if not shared_tokens:
                score -= 70.0

        oral_liquid_requested = (
            query.requested_route == "ORAL"
            or query.requested_dose_form in ORAL_LIQUID_FORMS
        )
        if oral_liquid_requested:
            if "oral" in candidate_name:
                score += 18.0
            if query.requested_dose_form and query.requested_dose_form.casefold() in candidate_name:
                score += 24.0
            if any(marker in candidate_name for marker in NON_ORAL_ROUTE_MARKERS):
                score -= 40.0
            if any(marker in candidate_name for marker in INJECTION_MARKERS):
                score -= 35.0

        injectable_requested = query.requested_route in {
            "INJECTABLE",
            "INJECTION",
            "INTRAVENOUS",
            "INTRAMUSCULAR",
            "SUBCUTANEOUS",
        }
        if not injectable_requested and any(marker in candidate_name for marker in INJECTION_MARKERS):
            score -= 35.0

        return RxNormConcept(
            rxcui=candidate.rxcui,
            name=candidate.name,
            tty=candidate.tty,
            score=score,
            source=candidate.source,
        )

    def _meaningful_tokens(self, value: str) -> set[str]:
        """Return normalized tokens suitable for concept matching."""

        tokens = set(re.findall(r"[a-z0-9]+", value.casefold()))
        filtered = {
            token
            for token in tokens
            if token not in STOPWORDS and not token.isdigit()
        }
        return filtered

    def _candidate_tokens(self, value: str) -> set[str]:
        """Return candidate tokens for prefix-style autocomplete matching."""

        return set(re.findall(r"[a-z0-9]+", value.casefold()))

    def _expand_suggestion_query(self, query_text: str) -> str:
        """Expand common user abbreviations before hitting RxNorm suggest."""

        tokens = []
        for token in re.findall(r"[a-z0-9]+", query_text.casefold()):
            tokens.append(SUGGESTION_ABBREVIATIONS.get(token, token))
        return " ".join(tokens)

    def _build_suggestion_variants(self, query_text: str) -> list[str]:
        """Build a small set of RxNorm suggestion queries for abbreviated typing."""

        ordered_tokens = self._ordered_meaningful_tokens(query_text)
        variants: list[str] = []
        for candidate in (
            query_text,
            " ".join(ordered_tokens),
            ordered_tokens[0] if ordered_tokens else "",
        ):
            cleaned = " ".join(candidate.split()).strip()
            if cleaned and cleaned not in variants:
                variants.append(cleaned)
        return variants

    def _ordered_meaningful_tokens(self, value: str) -> list[str]:
        """Return meaningful tokens in input order for suggestion fallback queries."""

        ordered: list[str] = []
        for token in re.findall(r"[a-z0-9]+", value.casefold()):
            if token in STOPWORDS or token.isdigit() or token in ordered:
                continue
            ordered.append(token)
        return ordered
