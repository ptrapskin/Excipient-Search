"""Sugar alcohol excipient matching against a canonical concept map.

Matching is done on normalized text (lowercase, whitespace-collapsed,
punctuation-stripped) so that "Sorbitol Solution 70%" still matches the
"sorbitol solution" alias.

All aliases within each concept are ordered longest-first so the most
specific variant is recorded as the matched term in the audit trail.

Returns a list of MatchedExcipient, one per canonical concept found.
Duplicate canonical matches from multiple ingredient entries are collapsed.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .models import IngredientEntry, MatchedExcipient
from .utils import normalize_text

# ---------------------------------------------------------------------------
# Canonical sugar alcohol concept map
# aliases are checked longest-first within each concept.
# ---------------------------------------------------------------------------

SUGAR_ALCOHOL_CONCEPTS: dict[str, dict] = {
    "sorbitol": {
        "category": "high",
        "aliases": [
            "noncrystallizing sorbitol solution",
            "non-crystallizing sorbitol solution",
            "non crystallizing sorbitol solution",
            "sorbitol solution",
            "d-sorbitol",
            "glucitol",
            "sorbitol",
        ],
    },
    "mannitol": {
        "category": "high",
        "aliases": [
            "d-mannitol",
            "mannitol",
        ],
    },
    "xylitol": {
        "category": "moderate",
        "aliases": ["xylitol"],
    },
    "maltitol": {
        "category": "moderate",
        "aliases": [
            "maltitol solution",
            "maltitol syrup",
            "maltitol",
        ],
    },
    "lactitol": {
        "category": "moderate",
        "aliases": [
            "lactitol monohydrate",
            "lactitol",
        ],
    },
    "isomalt": {
        "category": "moderate",
        "aliases": ["isomalt"],
    },
}

# Pre-normalise aliases once at import time.
_NORMALISED_CONCEPTS: dict[str, dict] = {
    canonical: {
        "category": data["category"],
        "aliases": [normalize_text(a) for a in data["aliases"]],
    }
    for canonical, data in SUGAR_ALCOHOL_CONCEPTS.items()
}


def match_excipients(ingredients: list[IngredientEntry]) -> list[MatchedExcipient]:
    """Match ingredient entries against the sugar alcohol concept map.

    Each canonical concept contributes at most one MatchedExcipient (the first
    ingredient entry and alias that trigger a match).

    Returns MatchedExcipient objects ordered by the concept map insertion order.
    """
    results: list[MatchedExcipient] = []
    for canonical, data in _NORMALISED_CONCEPTS.items():
        category = data["category"]
        aliases = data["aliases"]
        for ing in ingredients:
            norm = normalize_text(ing.raw_name)
            for alias in aliases:   # longest-first
                if alias in norm:
                    results.append(MatchedExcipient(
                        raw_name=ing.raw_name,
                        normalized_name=ing.normalized_name,
                        canonical_name=canonical,
                        category=category,
                        unii=ing.unii,
                    ))
                    break  # one match per ingredient entry for this canonical
            else:
                continue
            break  # found this canonical — move to next concept
    return results
