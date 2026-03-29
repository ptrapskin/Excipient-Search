"""Read-only access to the excipient_finder SQLite database (excipients.db).

Used by the web app to serve the Sugar Alcohol Risk page without re-running
the ingestion pipeline.  All queries are read-only and synchronous; callers
should wrap them in asyncio.to_thread() inside async route handlers.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Data models (web-layer views over the DB rows)
# ---------------------------------------------------------------------------

@dataclass
class ExcipientProduct:
    product_name: str
    setid: str
    labeler: str | None
    dosage_form: str | None
    route: str | None
    active_ingredient: str       # primary active ingredient (display)
    strength: str | None         # e.g. "10 g/15 mL" or None for older DB rows
    sugar_alcohols: list[str]    # canonical names e.g. ["sorbitol"]
    sugar_alcohol_uniis: list[str]  # parallel UNII list (empty string when absent)
    concern_tier: str            # "high" | "moderate" | "review"
    ndcs: list[str]


@dataclass
class ExcipientGroup:
    """Products sharing a primary active ingredient."""

    ingredient_name: str
    products: list[ExcipientProduct] = field(default_factory=list)

    @property
    def product_count(self) -> int:
        return len(self.products)

    @property
    def high_count(self) -> int:
        return sum(1 for p in self.products if p.concern_tier == "high")

    @property
    def moderate_count(self) -> int:
        return sum(1 for p in self.products if p.concern_tier == "moderate")


@dataclass
class ExcipientIndexStats:
    total_high: int
    total_moderate: int
    total_review: int
    last_processed: str | None   # ISO datetime string from DB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _primary_active(active_ingredients_raw: str | None) -> str:
    """Return a display-ready primary active ingredient name."""
    if not active_ingredients_raw:
        return "Unknown"
    first = active_ingredients_raw.split(";")[0].strip()
    return first.title() if first else "Unknown"


def _split_semicolon(value: str | None) -> list[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(";") if s.strip()]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names present in a table."""
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

def get_stats(db_path: Path) -> ExcipientIndexStats | None:
    """Return tier counts and last-processed timestamp, or None if DB absent."""
    if not db_path.exists():
        return None

    with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            row["concern_tier"]: row["cnt"]
            for row in conn.execute(
                "SELECT concern_tier, COUNT(*) as cnt FROM products "
                "WHERE inclusion_decision = 'included' GROUP BY concern_tier"
            )
        }
        last = conn.execute(
            "SELECT processed_at FROM products ORDER BY processed_at DESC LIMIT 1"
        ).fetchone()

    return ExcipientIndexStats(
        total_high=counts.get("high", 0),
        total_moderate=counts.get("moderate", 0),
        total_review=counts.get("review", 0),
        last_processed=last["processed_at"] if last else None,
    )


def get_sugar_alcohol_counts(db_path: Path) -> tuple[dict[str, int], int]:
    """Return ({canonical_name: product_count}, multiple_count) for the index."""
    if not db_path.exists():
        return {}, 0
    counts: dict[str, int] = {}
    multiple = 0
    with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True) as conn:
        rows = conn.execute(
            "SELECT matched_sugar_alcohols FROM products WHERE inclusion_decision = 'included'"
        ).fetchall()
    for (val,) in rows:
        if val:
            parts = [s.strip() for s in val.split(";") if s.strip()]
            if len(parts) > 1:
                multiple += 1
            for sa in parts:
                counts[sa] = counts.get(sa, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1])), multiple


def get_groups(
    db_path: Path,
    sugar_alcohol: str | None = None,  # None or "all" = every included product
) -> tuple[list[ExcipientGroup], int]:
    """Return (groups sorted by ingredient name, total product count).

    Parameters
    ----------
    sugar_alcohol:
        Canonical sugar alcohol name to filter by, or None / "all" for every included product.
    """
    if not db_path.exists():
        return [], 0

    where_clauses = ["inclusion_decision = 'included'"]
    params: list[str] = []

    if sugar_alcohol == "multiple":
        where_clauses.append("matched_sugar_alcohols LIKE '%; %'")
    elif sugar_alcohol and sugar_alcohol != "all":
        where_clauses.append("('; ' || matched_sugar_alcohols || '; ') LIKE ?")
        params.append(f"%; {sugar_alcohol}; %")

    where = "WHERE " + " AND ".join(where_clauses)

    with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = _table_columns(conn, "products")
        # Optional columns added after the initial schema — select with fallback NULL.
        strength_col = "active_strength" if "active_strength" in cols else "NULL AS active_strength"
        sa_unii_col = "matched_sugar_alcohol_uniis" if "matched_sugar_alcohol_uniis" in cols else "NULL AS matched_sugar_alcohol_uniis"
        rows = conn.execute(
            f"""
            SELECT spl_setid, product_name, labeler, dosage_form, route,
                   active_ingredients_raw, {strength_col},
                   matched_sugar_alcohols, {sa_unii_col},
                   concern_tier, ndcs
            FROM   products
            {where}
            ORDER BY active_ingredients_raw, product_name
            """,
            params,
        ).fetchall()

    groups: dict[str, ExcipientGroup] = {}
    for row in rows:
        active = _primary_active(row["active_ingredients_raw"])
        key = active.casefold()
        if key not in groups:
            groups[key] = ExcipientGroup(ingredient_name=active)
        groups[key].products.append(ExcipientProduct(
            product_name=row["product_name"] or "Unknown",
            setid=row["spl_setid"],
            labeler=row["labeler"],
            dosage_form=row["dosage_form"],
            route=row["route"],
            active_ingredient=active,
            strength=row["active_strength"],
            sugar_alcohols=_split_semicolon(row["matched_sugar_alcohols"]),
            sugar_alcohol_uniis=_split_semicolon(row["matched_sugar_alcohol_uniis"]) if row["matched_sugar_alcohol_uniis"] else [],
            concern_tier=row["concern_tier"],
            ndcs=_split_semicolon(row["ndcs"]),
        ))

    sorted_groups = sorted(groups.values(), key=lambda g: g.ingredient_name.casefold())
    total = sum(g.product_count for g in sorted_groups)
    return sorted_groups, total


@dataclass
class LocalProductDetail:
    """Product detail built from local excipients.db — used as API fallback."""

    product_name: str
    setid: str
    labeler: str | None
    dosage_form: str | None
    route: str | None
    ndcs: list[str]
    active_ingredients_raw: str | None      # semicolon-joined names
    active_strength: str | None             # semicolon-joined strengths
    active_ingredients_unii: str | None     # semicolon-joined UNIIs
    inactive_ingredients_raw: str | None    # semicolon-joined names
    inactive_ingredients_unii: str | None   # semicolon-joined UNIIs aligned with above
    matched_sugar_alcohols: list[str]       # canonical names
    matched_sugar_alcohol_uniis: list[str]  # parallel UNII list
    concern_tier: str


def get_product_by_setid(db_path: Path, setid: str) -> LocalProductDetail | None:
    """Return local product detail for a setid, or None if not in the local index."""
    if not db_path.exists():
        return None

    with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = _table_columns(conn, "products")
        strength_col = "active_strength" if "active_strength" in cols else "NULL AS active_strength"
        unii_col = "active_ingredients_unii" if "active_ingredients_unii" in cols else "NULL AS active_ingredients_unii"
        sa_unii_col = "matched_sugar_alcohol_uniis" if "matched_sugar_alcohol_uniis" in cols else "NULL AS matched_sugar_alcohol_uniis"
        inact_unii_col = "inactive_ingredients_unii" if "inactive_ingredients_unii" in cols else "NULL AS inactive_ingredients_unii"
        row = conn.execute(
            f"""
            SELECT spl_setid, product_name, labeler, dosage_form, route, ndcs,
                   active_ingredients_raw, {strength_col}, {unii_col},
                   inactive_ingredients_raw, {inact_unii_col},
                   matched_sugar_alcohols, {sa_unii_col}, concern_tier
            FROM   products
            WHERE  spl_setid = ? AND inclusion_decision = 'included'
            LIMIT  1
            """,
            (setid,),
        ).fetchone()

    if row is None:
        return None

    return LocalProductDetail(
        product_name=row["product_name"] or "Unknown",
        setid=row["spl_setid"],
        labeler=row["labeler"],
        dosage_form=row["dosage_form"],
        route=row["route"],
        ndcs=_split_semicolon(row["ndcs"]),
        active_ingredients_raw=row["active_ingredients_raw"],
        active_strength=row["active_strength"],
        active_ingredients_unii=row["active_ingredients_unii"],
        inactive_ingredients_raw=row["inactive_ingredients_raw"],
        inactive_ingredients_unii=row["inactive_ingredients_unii"],
        matched_sugar_alcohols=_split_semicolon(row["matched_sugar_alcohols"]),
        matched_sugar_alcohol_uniis=_split_semicolon(row["matched_sugar_alcohol_uniis"]) if row["matched_sugar_alcohol_uniis"] else [],
        concern_tier=row["concern_tier"],
    )
