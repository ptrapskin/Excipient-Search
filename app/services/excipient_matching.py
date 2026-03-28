"""Excipient-specific normalization and matching helpers."""

from __future__ import annotations

import re

from app.domain.models import IngredientEntry

MULTISPACE_PATTERN = re.compile(r"\s+")
PUNCTUATION_PATTERN = re.compile(r"[^\w\s]")
NO_PATTERN = re.compile(r"\b(?:number|no)\.?\b")

CANONICAL_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bfd\s*(?:&|and)\s*c\b", re.IGNORECASE), "fdc"),
    (re.compile(r"\bd\s*(?:&|and)\s*c\b", re.IGNORECASE), "dc"),
    (re.compile(r"\bglycerol\b", re.IGNORECASE), "glycerin"),
    (re.compile(r"\banhydrous glycerin\b", re.IGNORECASE), "glycerin"),
    (re.compile(r"\bpolyethylene glycol\b", re.IGNORECASE), "peg"),
    (re.compile(r"\bmacrogol\b", re.IGNORECASE), "peg"),
    (re.compile(r"\bd[\s-]+sorbitol\b", re.IGNORECASE), "sorbitol"),
    (re.compile(r"\bd[\s-]+mannitol\b", re.IGNORECASE), "mannitol"),
    (re.compile(r"\bnon[\s-]*crystallizing sorbitol solution\b", re.IGNORECASE), "sorbitol solution"),
    (re.compile(r"\bhydrogenated maltose syrup\b", re.IGNORECASE), "maltitol"),
    (re.compile(r"\bmaltitol syrup\b", re.IGNORECASE), "maltitol"),
    (re.compile(r"\bmaltitol solution\b", re.IGNORECASE), "maltitol"),
    (re.compile(r"\blactitol monohydrate\b", re.IGNORECASE), "lactitol"),
    (re.compile(r"\bisomaltitol\b", re.IGNORECASE), "isomalt"),
    (re.compile(r"\bpolyethylene glycol\b", re.IGNORECASE), "peg"),
    (re.compile(r"\bpropylene glycol solution\b", re.IGNORECASE), "propylene glycol"),
)

REMOVABLE_PHRASES: tuple[str, ...] = (
    " aluminum lake",
    " monohydrate",
    " anhydrous",
    " solution",
    " syrup",
    " liquid",
)

DYE_PATTERN = re.compile(
    r"^(?:(fdc|dc)\s+)?(red|yellow|blue|green|orange|violet)\s+(?:dye\s+)?no\s+([a-z0-9]+)(?:\s+aluminum\s+lake)?$",
    re.IGNORECASE,
)
UNII_PATTERN = re.compile(r"^[A-Z0-9]{10}$")


def normalize_excipient_text(text: str | None) -> str:
    """Return a canonical low-noise representation for matching."""

    if not text:
        return ""

    normalized = text.casefold()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("#", " no ")
    normalized = normalized.replace("_", " ")
    normalized = normalized.replace("-", " ")
    normalized = NO_PATTERN.sub(" no ", normalized)
    normalized = PUNCTUATION_PATTERN.sub(" ", normalized)
    normalized = MULTISPACE_PATTERN.sub(" ", normalized).strip()

    for pattern, replacement in CANONICAL_REPLACEMENTS:
        normalized = pattern.sub(replacement, normalized)

    normalized = MULTISPACE_PATTERN.sub(" ", normalized).strip()
    return normalized


def build_match_keys(
    text: str | None,
    *,
    unii: str | None = None,
    broaden: bool = False,
) -> set[str]:
    """Return canonical keys that should be considered equivalent for matching."""

    normalized = normalize_excipient_text(text)
    keys: set[str] = set()
    if normalized:
        keys.add(normalized)

        for phrase in REMOVABLE_PHRASES:
            if phrase in normalized:
                stripped = normalized.replace(phrase, " ")
                stripped = MULTISPACE_PATTERN.sub(" ", stripped).strip()
                if stripped:
                    keys.add(stripped)

        if broaden:
            if normalized.startswith("peg "):
                keys.add("peg")
            if normalized.startswith("sorbitol solution"):
                keys.add("sorbitol")

        dye_match = DYE_PATTERN.match(normalized)
        if dye_match:
            _family, color, number = dye_match.groups()
            keys.add(f"{color} no {number}")
            keys.add(f"{color} dye {number}")
            keys.add(f"{color} dye")

    if unii:
        keys.add(unii.upper())
    if text:
        collapsed = re.sub(r"\s+", "", text).upper()
        if UNII_PATTERN.fullmatch(collapsed):
            keys.add(collapsed)
    return keys


def ingredient_match_keys(ingredient: IngredientEntry) -> set[str]:
    """Build match keys for one parsed ingredient entry."""

    keys: set[str] = set()
    for value in (ingredient.raw_name, ingredient.display_name, ingredient.normalized_name):
        keys.update(build_match_keys(value, broaden=True))
    if ingredient.unii:
        keys.add(ingredient.unii.upper())
    return {key for key in keys if key}


def term_matches_ingredient(term: str, ingredient: IngredientEntry) -> bool:
    """Return whether a user-entered excipient term matches an ingredient entry."""

    term_keys = build_match_keys(term)
    ingredient_keys = ingredient_match_keys(ingredient)
    if not term_keys or not ingredient_keys:
        return False

    for term_key in term_keys:
        for ingredient_key in ingredient_keys:
            if term_key == ingredient_key:
                return True
            if term_key in ingredient_key:
                return True
    return False
