"""Gluten exposure screening for DailyMed product excipient lists.

Applies the Celiac Disease Foundation's guidance on gluten in medications
(celiac.org, "Gluten in Medicine, Vitamins & Supplements"):

- Starch is the only excipient likely to carry a measurable amount of gluten.
- Plain "starch", pregelatinized starch, and sodium starch glycolate can be
  derived from any source (including wheat), so the source must be verified
  unless the label names it.
- Starch with a named non-wheat source (corn, potato, tapioca, ...) is not a
  gluten concern.
- Wheat starch, or any other wheat/barley/rye-derived ingredient, should be
  avoided.
- Sugar alcohols (polyols) contain no gluten even when wheat-derived.
- Olmesartan contains no gluten but can cause a celiac-like enteropathy.

Like ``osmotic_filter``, the module is free of I/O so it can be reused by any
route or offline script.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

from app.domain.models import IngredientEntry

FindingCategory = Literal["gluten_source", "unspecified_starch", "oat", "non_wheat_starch", "polyol"]
GlutenTier = Literal["avoid", "verify", "low", "unknown"]

# Sort order used by the screener table (most concerning first).
TIER_ORDER: dict[str, int] = {"avoid": 1, "verify": 2, "unknown": 3, "low": 4}

TIER_LABELS: dict[str, str] = {
    "avoid": "Gluten source listed",
    "verify": "Verify starch source",
    "unknown": "No ingredient list",
    "low": "Unlikely to contain gluten",
}

# ---------------------------------------------------------------------------
# Ingredient patterns
# ---------------------------------------------------------------------------
# Polyols are checked first: the guidance notes that a "wheat-derived"
# ingredient is usually one of these, and they contain no gluten. Hydrogenated
# starch hydrolysate is a polyol despite containing the word "starch".
_POLYOL_PATTERN = re.compile(
    r"\b(mannitol|sorbitol|sorbitan|maltitol|xylitol|lactitol|erythritol|isomalt|"
    r"hydrogenated starch hydrolysates?)\b",
    re.IGNORECASE,
)

# Gluten-containing grains. "malt" is matched as a whole word so maltitol,
# maltodextrin, maltose, and maltol do not trigger it.
_GLUTEN_GRAIN_PATTERN = re.compile(
    r"\b(wheat|triticum|gluten|barley|hordeum|rye|secale|triticale|spelt|semolina|durum|malt)\b",
    re.IGNORECASE,
)

# "Gluten-free" style wording on a label is not a gluten source.
_GLUTEN_FREE_PATTERN = re.compile(r"\bgluten[\s-]*free\b", re.IGNORECASE)

# Oats are not named in the guidance but are commonly cross-contaminated with
# wheat, so they are flagged for verification rather than avoidance.
_OAT_PATTERN = re.compile(r"\b(oats?|oatmeal|avena)\b", re.IGNORECASE)

_STARCH_PATTERN = re.compile(r"\bstarch", re.IGNORECASE)

_NON_WHEAT_STARCH_SOURCES = re.compile(
    r"\b(corn|maize|zea mays|potato|solanum|tapioca|cassava|manihot|rice|oryza|"
    r"pea|pisum|sago|arrowroot)\b",
    re.IGNORECASE,
)

_PREGELATINIZED_PATTERN = re.compile(r"pre-?gelatini[sz]ed", re.IGNORECASE)
_SODIUM_STARCH_GLYCOLATE_PATTERN = re.compile(r"sodium starch glycolate", re.IGNORECASE)

OLMESARTAN_NOTE = (
    "Olmesartan contains no gluten but can cause severe celiac-like enteropathy. "
    "The Celiac Disease Foundation advises avoiding it."
)


@dataclass
class GlutenFinding:
    """One ingredient that is relevant to the gluten screen."""

    ingredient: str
    category: FindingCategory
    note: str


@dataclass
class GlutenAssessment:
    """Gluten screen result for one product."""

    tier: GlutenTier
    findings: list[GlutenFinding] = field(default_factory=list)
    olmesartan: bool = False

    @property
    def label(self) -> str:
        return TIER_LABELS[self.tier]

    @property
    def sort_key(self) -> str:
        return f"{TIER_ORDER[self.tier]} {self.tier}"

    @property
    def flagged(self) -> list[GlutenFinding]:
        """Findings that drive the tier (excludes reassuring notes)."""

        return [f for f in self.findings if f.category in ("gluten_source", "unspecified_starch", "oat")]

    @property
    def reassuring(self) -> list[GlutenFinding]:
        return [f for f in self.findings if f.category in ("non_wheat_starch", "polyol")]


def _ingredient_text(ingredient: IngredientEntry) -> str:
    return (ingredient.display_name or ingredient.raw_name or "").strip()


def classify_ingredient(name: str) -> GlutenFinding | None:
    """Classify one inactive ingredient name, or return None if it is not relevant."""

    if not name:
        return None

    if _POLYOL_PATTERN.search(name):
        return GlutenFinding(
            ingredient=name,
            category="polyol",
            note="Sugar alcohol; contains no gluten even when wheat-derived.",
        )

    grain_text = _GLUTEN_FREE_PATTERN.sub("", name)
    grain = _GLUTEN_GRAIN_PATTERN.search(grain_text)
    if grain:
        if _STARCH_PATTERN.search(name):
            note = "Wheat starch is the main gluten risk in medications. Avoid."
        else:
            note = f"Derived from a gluten-containing grain ({grain.group(1).lower()}). Avoid unless the manufacturer confirms it is gluten-free."
        return GlutenFinding(ingredient=name, category="gluten_source", note=note)

    if _STARCH_PATTERN.search(name):
        source = _NON_WHEAT_STARCH_SOURCES.search(name)
        if source:
            return GlutenFinding(
                ingredient=name,
                category="non_wheat_starch",
                note=f"Starch source listed as {source.group(1).lower()}; not a gluten source.",
            )
        if _SODIUM_STARCH_GLYCOLATE_PATTERN.search(name):
            note = "Usually potato-derived, occasionally corn, but can come from any starch. Ask the manufacturer for the source."
        elif _PREGELATINIZED_PATTERN.search(name):
            note = "Usually corn-derived but can come from any starch. Ask the manufacturer for the source."
        else:
            note = "Source not stated. Unlike foods, drug starch is not required to be corn. Ask the manufacturer for the source."
        return GlutenFinding(ingredient=name, category="unspecified_starch", note=note)

    if _OAT_PATTERN.search(name):
        return GlutenFinding(
            ingredient=name,
            category="oat",
            note="Oats are often cross-contaminated with wheat. Confirm the product is gluten-free.",
        )

    return None


def assess_product(
    inactive_ingredients: list[IngredientEntry],
    active_ingredients: list[IngredientEntry] | None = None,
) -> GlutenAssessment:
    """Assess a product's gluten risk from its inactive (and active) ingredients."""

    findings: list[GlutenFinding] = []
    seen: set[str] = set()
    for ingredient in inactive_ingredients:
        name = _ingredient_text(ingredient)
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        finding = classify_ingredient(name)
        if finding is not None:
            findings.append(finding)

    olmesartan = any(
        "olmesartan" in _ingredient_text(ingredient).casefold()
        for ingredient in (active_ingredients or [])
    )

    categories = {f.category for f in findings}
    if "gluten_source" in categories:
        tier: GlutenTier = "avoid"
    elif categories & {"unspecified_starch", "oat"}:
        tier = "verify"
    elif not inactive_ingredients:
        tier = "unknown"
    else:
        tier = "low"

    return GlutenAssessment(tier=tier, findings=findings, olmesartan=olmesartan)
