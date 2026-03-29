"""Dosage form and route classification for oral/enteral liquid product filtering.

All functions are pure (no I/O, no side effects).
The clinical rationale for each keyword list is documented inline.
"""
from __future__ import annotations

from typing import Literal

from .models import FilterDecision

FormClass = Literal["strong", "ambiguous", "excluded", "non_liquid"]
RouteClass = Literal["oral", "excluded", "blank"]

# ---------------------------------------------------------------------------
# Dosage form keyword lists
# ---------------------------------------------------------------------------
# IMPORTANT: exclusions are checked BEFORE inclusions so that
# "CAPSULE, LIQUID FILLED" is excluded even though it contains "liquid".
# Within each tuple, longer strings must precede shorter substrings so the
# most specific match is recorded in the audit trail.
# ---------------------------------------------------------------------------

_EXCLUDED_FORM_KEYWORDS: tuple[str, ...] = (
    # Multi-word forms that would otherwise match inclusion keywords
    "capsule, liquid filled",
    "tablet, film coated",
    "tablet, coated",
    # Solid oral forms — not a liquid excipient exposure pathway
    "capsule",
    "tablet",
    "kit",
    # Non-oral/non-enteral route descriptors that appear inside form names
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

# Strong oral-liquid forms: may pass even with a blank route because the
# dosage form name alone is strong evidence of oral/enteral administration.
_STRONG_FORM_KEYWORDS: tuple[str, ...] = (
    "oral solution",     # must precede "solution"
    "oral suspension",   # must precede "suspension"
    "oral drops",        # must precede "drops"
    "solution",
    "suspension",
    "syrup",
    "elixir",
    "concentrate",
)

# Ambiguous liquid forms: require an explicit oral/enteral route to be included;
# with a blank route they are sent to the REVIEW bucket.
_AMBIGUOUS_FORM_KEYWORDS: tuple[str, ...] = (
    "emulsion",
    "liquid",
    "drops",   # "oral drops" is caught above; bare "drops" could be ophthalmic
)

# ---------------------------------------------------------------------------
# Route keyword lists
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

# Any route matching an exclusion keyword is excluded.
# Any route that is present but matches NEITHER list is also excluded
# (conservative: unrecognised route ≠ oral).
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

# ---------------------------------------------------------------------------
# Classifier functions
# ---------------------------------------------------------------------------

def classify_form(dosage_form: str | None) -> tuple[FormClass, str | None, str | None]:
    """Classify a dosage form string.

    Returns:
        (form_class, included_keyword, excluded_keyword)

    Exclusion keywords are checked first so multi-word forms like
    "CAPSULE, LIQUID FILLED" are excluded before "liquid" is seen.
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
    """Classify a route string.

    Returns:
        (route_class, included_keyword, excluded_keyword)

    Any route that is present but not in the oral/enteral allowlist is treated
    as excluded — a conservative default.
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

    # Route present but unrecognised — treat conservatively as non-oral.
    return "excluded", None, route


def make_filter_decision(
    dosage_form: str | None,
    route: str | None,
) -> FilterDecision:
    """Return a FilterDecision for the given form and route.

    should_process is True when the record might pass — i.e. the form is
    liquid (strong or ambiguous) and the route is not explicitly excluded.
    should_process is False when the record is a hard exclude and excipient
    matching can be skipped entirely.
    """
    form_class, inc_form, exc_form = classify_form(dosage_form)
    route_class, inc_route, exc_route = classify_route(route)

    should_process = (
        form_class in ("strong", "ambiguous")
        and route_class != "excluded"
    )

    return FilterDecision(
        form_class=form_class,
        route_class=route_class,
        included_form_match=inc_form,
        excluded_form_match=exc_form,
        included_route_match=inc_route,
        excluded_route_match=exc_route,
        should_process=should_process,
    )


def make_filter_decision_broad(
    dosage_form: str | None,
    route: str | None,
) -> FilterDecision:
    """Broad recall mode: same form/route exclusions, but explicitly documents
    the logic for the broad recall QA path.

    should_process is True whenever form is strong OR ambiguous AND route is
    not explicitly excluded.  This is identical to make_filter_decision but is
    provided as a distinct entry point so that callers can clearly signal they
    are operating in broad-recall mode.

    In broad recall mode main.py does NOT exclude records with 0 sugar alcohol
    matches — every record that passes form/route is written to
    broad_recall_products.csv regardless of excipient hit.
    """
    form_class, inc_form, exc_form = classify_form(dosage_form)
    route_class, inc_route, exc_route = classify_route(route)

    should_process = (
        form_class in ("strong", "ambiguous")
        and route_class != "excluded"
    )

    return FilterDecision(
        form_class=form_class,
        route_class=route_class,
        included_form_match=inc_form,
        excluded_form_match=exc_form,
        included_route_match=inc_route,
        excluded_route_match=exc_route,
        should_process=should_process,
    )
