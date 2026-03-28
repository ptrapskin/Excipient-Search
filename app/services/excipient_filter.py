"""Excipient comparison and filtering helpers."""

from __future__ import annotations

import re

from app.domain.models import ExcipientFilter, IngredientEntry, ProductComparisonRow, ProductDetail, ProductSearchResult
from app.services.excipient_matching import normalize_excipient_text, term_matches_ingredient

TERM_SPLIT_PATTERN = re.compile(r"[,\n;]+")


def parse_filter_terms(value: str | None) -> list[str]:
    """Parse a comma or semicolon separated filter string into normalized terms."""

    if not value:
        return []
    terms = []
    seen: set[str] = set()
    for part in TERM_SPLIT_PATTERN.split(value):
        cleaned = " ".join(part.strip().split())
        canonical = normalize_excipient_text(cleaned) if cleaned else ""
        if not cleaned or canonical in seen:
            continue
        seen.add(canonical)
        terms.append(cleaned)
    return terms


class ExcipientFilterService:
    """Build comparison rows and apply include/exclude excipient filtering."""

    def build_filter(self, include: str | None = None, exclude: str | None = None) -> ExcipientFilter:
        """Create a normalized excipient filter object."""

        return ExcipientFilter(
            include_terms=parse_filter_terms(include),
            exclude_terms=parse_filter_terms(exclude),
        )

    def build_comparison_row(
        self,
        result: ProductSearchResult,
        detail: ProductDetail,
        excipient_filter: ExcipientFilter,
    ) -> ProductComparisonRow:
        """Create one comparison row and annotate filter matches."""

        matched_include = self._match_terms(excipient_filter.include_terms, detail.inactive_ingredients)
        matched_exclude = self._match_terms(excipient_filter.exclude_terms, detail.inactive_ingredients)

        matches_filter = True
        if excipient_filter.include_terms:
            matches_filter = bool(matched_include)
        if matched_exclude:
            matches_filter = False

        return ProductComparisonRow(
            product_name=detail.product_name or result.product_name,
            setid=result.setid,
            ndcs=detail.ndcs or result.ndcs,
            route=detail.route or result.route,
            dosage_form=detail.dosage_form or result.dosage_form,
            dailymed_strength=self._build_strength_summary(detail),
            labeler=detail.labeler or result.labeler,
            rxcui=result.rxcui,
            inactive_ingredients=detail.inactive_ingredients,
            inactive_ingredients_raw=detail.inactive_ingredients_raw,
            matched_include_terms=matched_include,
            matched_exclude_terms=matched_exclude,
            matches_filter=matches_filter,
            source=result.source,
        )

    def filter_rows(
        self,
        rows: list[ProductComparisonRow],
        excipient_filter: ExcipientFilter,
    ) -> list[ProductComparisonRow]:
        """Return rows that satisfy the requested excipient filter."""

        if not excipient_filter.include_terms and not excipient_filter.exclude_terms:
            return rows
        return [row for row in rows if row.matches_filter]

    def _build_strength_summary(self, detail: ProductDetail) -> str | None:
        """Return a display-friendly strength summary from DailyMed active ingredients."""

        strengths: list[str] = []
        seen: set[str] = set()
        for ingredient in detail.active_ingredients:
            strength = " ".join((ingredient.strength or "").split()).strip()
            if not strength:
                continue
            key = strength.casefold()
            if key in seen:
                continue
            seen.add(key)
            strengths.append(strength)
        if not strengths:
            return None
        return ", ".join(strengths)

    def _match_terms(self, terms: list[str], haystack: list[IngredientEntry]) -> list[str]:
        """Return requested terms present in the product's excipient list."""

        matched: list[str] = []
        for term in terms:
            if any(term_matches_ingredient(term, ingredient) for ingredient in haystack):
                matched.append(term)
        return matched
