"""Concern tier assignment based on filter decision and matched excipients.

Tiers:
    high      – strong oral-liquid form, oral/enteral route or blank+strong,
                contains sorbitol or mannitol.
    moderate  – same form/route criteria, contains only lower-risk sugar alcohols
                (xylitol, maltitol, lactitol, isomalt).
    review    – ambiguous liquid form with no confirmed oral route.
    excluded  – excluded form, excluded route, or no sugar alcohol match.

All functions are pure (no I/O).
"""
from __future__ import annotations

from .models import FilterDecision, MatchedExcipient

_HIGH_CANONICALS: frozenset[str] = frozenset(["sorbitol", "mannitol"])


def assign_concern_tier(
    decision: FilterDecision,
    matched: list[MatchedExcipient],
) -> tuple[str, str | None]:
    """Derive the concern tier and an optional review reason.

    Returns:
        (tier, review_reason)  where review_reason is None unless tier == "review"
    """
    if not decision.should_process:
        return "excluded", None

    if not matched:
        return "excluded", None

    # Ambiguous form with no confirmed oral route → REVIEW.
    if decision.form_class == "ambiguous" and decision.route_class == "blank":
        return "review", "Ambiguous dosage form with no recorded administration route"

    # At this point: strong form (blank or oral route)  OR  ambiguous + oral route.
    has_high = any(m.canonical_name in _HIGH_CANONICALS for m in matched)
    return ("high" if has_high else "moderate"), None
