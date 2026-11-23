"""NDC format conversion shared by the product index export and search.

Labels list NDCs in one of three 10-digit hyphenated layouts (4-4-2, 5-3-2,
5-4-1), while billing systems and many people use the 11-digit 5-4-2 form, with
or without hyphens. Converting both sides to 11 digits lets any of these match.
Product-level codes (labeler-product, no package) convert to 9 digits.
"""

from __future__ import annotations

import re

_SEGMENT_WIDTHS = (5, 4, 2)


def to_ndc11(ndc: str) -> str | None:
    """Convert a hyphenated NDC to its 11-digit (or 9-digit product) form."""

    parts = [p for p in ndc.strip().split("-")]
    if len(parts) not in (2, 3) or not all(p.isdigit() for p in parts):
        digits = re.sub(r"\D", "", ndc)
        return digits if len(digits) == 11 else None
    if any(len(p) > w for p, w in zip(parts, _SEGMENT_WIDTHS)):
        return None
    return "".join(p.zfill(w) for p, w in zip(parts, _SEGMENT_WIDTHS))


def ndc11_list(ndcs: str | None) -> str:
    """Space-separated 11-digit forms of a semicolon-separated NDC list."""

    codes = (to_ndc11(part) for part in (ndcs or "").split(";") if part.strip())
    return " ".join(code for code in codes if code)


def ndc_search_keys(query_ndc: str) -> list[str]:
    """11-digit (or 9-digit) keys a typed NDC could correspond to."""

    if "-" in query_ndc:
        key = to_ndc11(query_ndc)
        return [key] if key else []
    digits = re.sub(r"\D", "", query_ndc)
    if len(digits) == 11:
        return [digits]
    if len(digits) == 10:
        # Unhyphenated 10-digit codes are ambiguous; try each label layout.
        return sorted({
            "0" + digits,                         # 4-4-2
            digits[:5] + "0" + digits[5:],        # 5-3-2
            digits[:9] + "0" + digits[9:],        # 5-4-1
        })
    return []
