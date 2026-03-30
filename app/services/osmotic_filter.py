"""Clinical filtering pipeline for oral/enteral liquid sugar alcohol exposure screening.

Classifies DailyMed SPL product records by dosage form, route, and sugar alcohol
content into concern tiers (HIGH / MODERATE / REVIEW / EXCLUDED) suitable for
enteral/jejunal intolerance review.

Usage
-----
Call ``evaluate(dosage_form, route, inactive_ingredients)`` which returns a
``FilterDecision`` carrying the full classification, matched evidence, and tier.

The module is intentionally free of I/O and async; it can be used from both the
offline build script and the live web-app analysis path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.domain.models import IngredientEntry

# ---------------------------------------------------------------------------
# Form classification keywords
# ---------------------------------------------------------------------------
# Checked in order; exclusions are applied before inclusions, so a product
# whose dosage_form is "CAPSULE, LIQUID FILLED" is excluded even though it
# contains the word "liquid".
#
# Within each category the tuples are ordered longest-match-first so the most
# specific keyword is recorded in the audit trail.
# ---------------------------------------------------------------------------

# These form substrings disqualify a record regardless of route.
_EXCLUDED_FORM_KEYWORDS: tuple[str, ...] = (
    # Specific multi-word forms first so they are caught before their substrings
    "capsule, liquid filled",
    "tablet, film coated",
    "tablet, coated",
    # Generic solid/non-oral forms
    "capsule",
    "tablet",
    "kit",
    # Non-oral/non-enteral route descriptors that appear in form names
    "injection",
    "injectable",
    "infusion",
    "ophthalmic",
    "otic",
    "nasal",
    "topical",
    "cutaneous",
    "vaginal",
    "rectal",
    "inhalation",
    "nebulizer",
    "irrigation",
)

# Strong oral/enteral liquid forms — may pass with a blank route.
# "oral solution" / "oral suspension" / "oral drops" listed before their
# shorter equivalents so the audit trail records the more specific match.
_STRONG_FORM_KEYWORDS: tuple[str, ...] = (
    "oral solution",
    "oral suspension",
    "oral drops",
    "solution",
    "suspension",
    "syrup",
    "elixir",
    "concentrate",
)

# Ambiguous liquid forms — only pass when route is explicitly oral/enteral.
# "drops" is listed after "oral drops" in the strong list; when we reach here
# we already know the form did not match "oral drops".
_AMBIGUOUS_FORM_KEYWORDS: tuple[str, ...] = (
    "emulsion",
    "liquid",
    "drops",
)

FormClass = Literal["strong", "ambiguous", "excluded", "non_liquid"]

# ---------------------------------------------------------------------------
# Route classification keywords
# ---------------------------------------------------------------------------

_ORAL_ROUTE_KEYWORDS: tuple[str, ...] = (
    "oral",
    "sublingual",
    "buccal",
    "oropharyngeal",
    "enteral",
    "nasogastric",
    "gastric",
)

# Any route that matches one of these is excluded.  Routes not matching either
# list are also treated as excluded (conservative: unrecognized ≠ oral).
_EXCLUDED_ROUTE_KEYWORDS: tuple[str, ...] = (
    "intravenous",
    "intramuscular",
    "subcutaneous",
    "intradermal",
    "intrathecal",
    "intravitreal",
    "intravesical",
    "intraarticular",
    "injection",
    "injectable",
    "infusion",
    "ophthalmic",
    "otic",
    "nasal",
    "topical",
    "cutaneous",
    "vaginal",
    "rectal",
    "inhalation",
    "nebulizer",
    "irrigation",
    "transdermal",
    "percutaneous",
)

RouteClass = Literal["oral", "excluded", "blank"]

# ---------------------------------------------------------------------------
# Sugar alcohol concept map
# ---------------------------------------------------------------------------
# Maps canonical name → list of alias substrings (case-insensitive).
# Aliases within each entry are ordered longest-first so the most specific
# variant (e.g. "noncrystallizing sorbitol solution") is recorded in the
# matched_term field rather than the generic "sorbitol".
# ---------------------------------------------------------------------------

SUGAR_ALCOHOL_CONCEPTS: dict[str, list[str]] = {
    "sorbitol": [
        "noncrystallizing sorbitol solution",
        "non-crystallizing sorbitol solution",
        "non crystallizing sorbitol solution",
        "sorbitol solution",
        "d-sorbitol",
        "glucitol",
        "sorbitol",
    ],
    "mannitol": [
        "d-mannitol",
        "mannitol",
    ],
    "xylitol": [
        "xylitol",
    ],
    "maltitol": [
        "maltitol solution",
        "maltitol syrup",
        "maltitol",
    ],
    "lactitol": [
        "lactitol monohydrate",
        "lactitol",
    ],
    "isomalt": [
        "isomalt",
    ],
    "glycerin": [
        "2-propanol, 1,3-dihydroxy-",
        "1,2,3-trihydroxypropane",
        "synthetic glycerine",
        "1,2,3-propanetriol",
        "vegetable glycerin",
        "vegetable glycerol",
        "synthetic glycerol",
        "trihydroxypropane",
        "glycerin [hsdb]",
        "glycerin [usp]",
        "glycerin [jan]",
        "glycerol [inn]",
        "fema no. 2525",
        "glycerine",
        "fema 2525",
        "glycerol",
        "glycerin",
        "e-422",
        "e422",
    ],
    "polyethylene glycol": [
        "polyethylene glycol",
        "macrogol",
    ],
}

_HIGH_SUGAR_ALCOHOLS: frozenset[str] = frozenset(["sorbitol", "mannitol"])
_MODERATE_SUGAR_ALCOHOLS: frozenset[str] = frozenset(["xylitol", "maltitol", "lactitol", "isomalt"])

ConcernTier = Literal["high", "moderate", "review", "excluded"]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class SugarAlcoholMatch:
    """One sugar alcohol concept identified in a product's inactive ingredients."""

    canonical_name: str   # normalised concept name, e.g. "sorbitol"
    matched_term: str     # alias substring that triggered the match
    risk_level: str       # "high" or "moderate"


@dataclass
class FilterDecision:
    """Complete classification for one SPL product record.

    Carries the tier decision plus all evidence that produced it so every
    included or excluded record can be audited without re-running the pipeline.
    """

    form_class: FormClass
    route_class: RouteClass

    # Form / route evidence
    included_form_match: str | None        # keyword that qualified the form
    excluded_form_match: str | None        # keyword that disqualified the form
    included_route_match: str | None       # keyword that qualified the route
    excluded_route_match: str | None       # keyword that disqualified the route

    # Sugar alcohol evidence
    matched_sugar_alcohols: list[SugarAlcoholMatch] = field(default_factory=list)

    # Decision
    concern_tier: ConcernTier = "excluded"
    inclusion_reasons: list[str] = field(default_factory=list)
    exclusion_reasons: list[str] = field(default_factory=list)
    review_reason: str | None = None


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

def classify_form(dosage_form: str | None) -> tuple[FormClass, str | None, str | None]:
    """Classify a dosage form string into FormClass.

    Returns:
        (form_class, included_keyword, excluded_keyword)

    Exclusion keywords are checked before inclusion keywords, so "CAPSULE,
    LIQUID FILLED" is excluded even though it contains "liquid".
    """
    if not dosage_form:
        return "non_liquid", None, None

    fl = dosage_form.casefold()

    for kw in _EXCLUDED_FORM_KEYWORDS:
        if kw in fl:
            return "excluded", None, kw

    for kw in _STRONG_FORM_KEYWORDS:
        if kw in fl:
            return "strong", kw, None

    for kw in _AMBIGUOUS_FORM_KEYWORDS:
        if kw in fl:
            return "ambiguous", kw, None

    return "non_liquid", None, None


def classify_route(route: str | None) -> tuple[RouteClass, str | None, str | None]:
    """Classify a route string into RouteClass.

    Returns:
        (route_class, included_keyword, excluded_keyword)

    Any route that is present but not in the oral/enteral allowlist is treated
    as excluded — a conservative default that avoids false-positive oral matches
    for unrecognised routes such as TRANSDERMAL or PERCUTANEOUS.
    """
    if not route:
        return "blank", None, None

    rl = route.casefold()

    for kw in _EXCLUDED_ROUTE_KEYWORDS:
        if kw in rl:
            return "excluded", None, kw

    for kw in _ORAL_ROUTE_KEYWORDS:
        if kw in rl:
            return "oral", kw, None

    # Route present but not recognised as oral — treat conservatively.
    return "excluded", None, route


def match_sugar_alcohols(ingredients: list[IngredientEntry]) -> list[SugarAlcoholMatch]:
    """Match ingredient entries against the sugar alcohol concept map.

    For each canonical name the aliases are checked longest-first, so the most
    specific variant (e.g. "noncrystallizing sorbitol solution") is recorded as
    matched_term rather than the generic "sorbitol".

    Returns one SugarAlcoholMatch per canonical name found; duplicate canonical
    names from multiple ingredient entries are collapsed to one.
    """
    results: list[SugarAlcoholMatch] = []
    for canonical, aliases in SUGAR_ALCOHOL_CONCEPTS.items():
        for ing in ingredients:
            ing_text = (ing.display_name or ing.raw_name or "").casefold()
            for alias in aliases:          # already ordered longest-first
                if alias in ing_text:
                    risk = "high" if canonical in _HIGH_SUGAR_ALCOHOLS else "moderate"
                    results.append(SugarAlcoholMatch(
                        canonical_name=canonical,
                        matched_term=alias,
                        risk_level=risk,
                    ))
                    break
            else:
                continue
            break   # found this canonical for at least one ingredient
    return results


def assign_concern_tier(
    form_class: FormClass,
    route_class: RouteClass,
    sugar_alcohols: list[SugarAlcoholMatch],
) -> tuple[ConcernTier, str | None]:
    """Derive the concern tier and an optional review reason.

    Tier rules
    ----------
    EXCLUDED : form is non-liquid or explicitly excluded, route is excluded,
               or no sugar alcohols found.
    REVIEW   : ambiguous dosage form (liquid / drops / emulsion) with no
               confirmed oral/enteral route — insufficient evidence to assign
               a clinical risk level.
    HIGH     : strong oral-liquid form; oral/enteral route (or blank route with
               strong form); contains sorbitol or mannitol.
    MODERATE : same form/route criteria as HIGH but contains only lower-risk
               sugar alcohols (xylitol, maltitol, lactitol, isomalt).

    Returns:
        (tier, review_reason)
    """
    if form_class in ("excluded", "non_liquid"):
        return "excluded", None

    if not sugar_alcohols:
        return "excluded", None

    if route_class == "excluded":
        return "excluded", None

    # Ambiguous form with no confirmed oral route — send to REVIEW.
    if form_class == "ambiguous" and route_class == "blank":
        return "review", "Ambiguous dosage form with no recorded route"

    # Strong form (blank or oral route) OR ambiguous form (oral route confirmed).
    has_high = any(sa.risk_level == "high" for sa in sugar_alcohols)
    return ("high" if has_high else "moderate"), None


# ---------------------------------------------------------------------------
# Primary entry point
# ---------------------------------------------------------------------------

def evaluate(
    dosage_form: str | None,
    route: str | None,
    inactive_ingredients: list[IngredientEntry],
) -> FilterDecision:
    """Run the full filter pipeline for one product record.

    This is the single entry-point used by both the offline build script and the
    live web-app analysis path, ensuring they apply identical logic.

    Parameters
    ----------
    dosage_form:
        Raw dosage form string from the SPL label (e.g. "ORAL SOLUTION").
    route:
        Raw route string from the SPL label (e.g. "ORAL"), or None if absent.
    inactive_ingredients:
        Parsed IngredientEntry list from the SPL inactive-ingredients section.

    Returns
    -------
    FilterDecision
        Full classification including tier, matched evidence, and audit fields.
    """
    form_class, inc_form, exc_form = classify_form(dosage_form)
    route_class, inc_route, exc_route = classify_route(route)
    sugar_alcohols = match_sugar_alcohols(inactive_ingredients)
    tier, review_reason = assign_concern_tier(form_class, route_class, sugar_alcohols)

    inclusion_reasons: list[str] = []
    exclusion_reasons: list[str] = []

    if inc_form:
        inclusion_reasons.append(f"form:{inc_form}")
    if exc_form:
        exclusion_reasons.append(f"form:{exc_form}")
    if inc_route:
        inclusion_reasons.append(f"route:{inc_route}")
    if exc_route:
        exclusion_reasons.append(f"route:{exc_route}")
    if sugar_alcohols:
        inclusion_reasons.append(
            "sugar_alcohols:" + ",".join(sa.canonical_name for sa in sugar_alcohols)
        )
    if not sugar_alcohols and tier == "excluded":
        exclusion_reasons.append("no_sugar_alcohol_found")

    return FilterDecision(
        form_class=form_class,
        route_class=route_class,
        included_form_match=inc_form,
        excluded_form_match=exc_form,
        included_route_match=inc_route,
        excluded_route_match=exc_route,
        matched_sugar_alcohols=sugar_alcohols,
        concern_tier=tier,
        inclusion_reasons=inclusion_reasons,
        exclusion_reasons=exclusion_reasons,
        review_reason=review_reason,
    )
