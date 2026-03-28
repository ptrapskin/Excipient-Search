"""Query normalization helpers."""

from __future__ import annotations

import hashlib
import re

from app.domain.models import DrugQuery

NDC_PATTERN = re.compile(
    r"\b\d{4,5}-\d{3,4}-\d{1,2}\b|\b\d{10,11}\b"
)
UNIT_PATTERN = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>mcg|μg|ug|mg|g|kg|ml|mL|l|L|%)\b",
    re.IGNORECASE,
)
STRENGTH_PATTERN = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:mcg|mg|g|kg|mL|L|%)"
    r"(?:\s*/\s*\d+(?:\.\d+)?\s*(?:mcg|mg|g|kg|mL|L|%))?\b",
    re.IGNORECASE,
)
NON_TEXT_PUNCTUATION_PATTERN = re.compile(r"[^\w\s/\-%.]+")
NON_NDC_HYPHEN_PATTERN = re.compile(r"(?<!\d)-(?!\d)|(?<!\d)-(?=\d)|(?<=\d)-(?!\d)")
MULTISPACE_PATTERN = re.compile(r"\s+")

UNIT_MAP = {
    "μg": "mcg",
    "ug": "mcg",
    "mcg": "mcg",
    "mg": "mg",
    "g": "g",
    "kg": "kg",
    "ml": "mL",
    "l": "L",
    "%": "%",
}

ROUTE_HINTS = {
    "oral": "ORAL",
    "topical": "TOPICAL",
    "nasal": "NASAL",
    "ophthalmic": "OPHTHALMIC",
    "otic": "OTIC",
    "injectable": "INJECTABLE",
    "injection": "INJECTION",
    "intravenous": "INTRAVENOUS",
    "intramuscular": "INTRAMUSCULAR",
    "subcutaneous": "SUBCUTANEOUS",
}

DOSE_FORM_HINTS = {
    "tablet": "TABLET",
    "capsule": "CAPSULE",
    "solution": "SOLUTION",
    "suspension": "SUSPENSION",
    "syrup": "SYRUP",
    "cream": "CREAM",
    "ointment": "OINTMENT",
    "spray": "SPRAY",
    "gel": "GEL",
}


def normalize_query(raw_text: str) -> DrugQuery:
    """Trim and lightly structure a user-entered query."""

    trimmed = raw_text.strip()
    ndc_match = NDC_PATTERN.search(trimmed)
    normalized_text = _normalize_text(trimmed)
    lowered = normalized_text.casefold()
    strength_match = STRENGTH_PATTERN.search(normalized_text)

    requested_route = next((value for key, value in ROUTE_HINTS.items() if key in lowered), None)
    requested_dose_form = next((value for key, value in DOSE_FORM_HINTS.items() if key in lowered), None)

    return DrugQuery(
        raw_text=raw_text,
        normalized_text=normalized_text,
        ndc=_normalize_ndc(ndc_match.group(0)) if ndc_match else None,
        requested_route=requested_route,
        requested_dose_form=requested_dose_form,
        requested_strength=strength_match.group(0) if strength_match else None,
    )


def build_query_key(query: DrugQuery) -> str:
    """Build a stable cache key from a normalized query."""

    pieces = [
        query.normalized_text.casefold(),
        query.ndc or "",
        query.requested_route or "",
        query.requested_dose_form or "",
        query.requested_strength or "",
    ]
    digest = hashlib.sha256("|".join(pieces).encode("utf-8")).hexdigest()
    return digest


def _normalize_text(value: str) -> str:
    """Strip punctuation, standardize spacing, and normalize common units."""

    if not value:
        return ""

    normalized = value.replace("_", " ")
    normalized = NON_TEXT_PUNCTUATION_PATTERN.sub(" ", normalized)
    normalized = NON_NDC_HYPHEN_PATTERN.sub(" ", normalized)
    normalized = re.sub(r"\s*/\s*", "/", normalized)
    normalized = UNIT_PATTERN.sub(_replace_unit, normalized)
    normalized = MULTISPACE_PATTERN.sub(" ", normalized)
    return normalized.strip(" -/.,")


def _replace_unit(match: re.Match[str]) -> str:
    """Return canonical spacing and unit casing for numeric units."""

    value = match.group("value")
    unit = UNIT_MAP[match.group("unit").casefold()]
    return f"{value} {unit}"


def _normalize_ndc(value: str) -> str:
    """Normalize NDC formatting while preserving digits and valid hyphens."""

    compact = MULTISPACE_PATTERN.sub("", value)
    return compact.strip()
