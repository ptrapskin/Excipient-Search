"""Read-only search over the full product index (product_index.db).

The index is built offline by the ``excipient_finder`` pipeline from the
DailyMed bulk downloads and holds every human drug product's ingredients across
all dosage forms. Searching it avoids one live DailyMed request per label.
Queries are synchronous; wrap calls in ``asyncio.to_thread()`` from async code.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from app.domain.enums import DataSource
from app.domain.models import DrugQuery, IngredientEntry, ProductDetail, ProductSearchResult
from app.services.ndc import ndc_search_keys

logger = logging.getLogger(__name__)

# Upper bound on rows returned for one search, to keep very broad queries fast.
MAX_RESULTS = 3000

_HAYSTACK = (
    "search_text || ' ' || lower(coalesce(dosage_form, '')) || ' ' || "
    "lower(coalesce(route, '')) || ' ' || lower(coalesce(active_strength, ''))"
)


def _split(value: str | None) -> list[str]:
    return [part.strip() for part in (value or "").split(";") if part.strip()]


def _like(term: str) -> str:
    escaped = term.casefold().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _parse_timestamp(value: str | None) -> datetime:
    try:
        return datetime.fromisoformat(value) if value else datetime.now(timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def resolve_index_path(db_path: Path, unpack_dir: Path | None = None) -> Path:
    """Return a usable index path, unpacking ``<db_path>.gz`` when only that exists.

    The committed index is gzipped to stay under GitHub's file size limit. It is
    unpacked once into a writable temp directory (the deployed code directory is
    read-only), named by the archive's size and mtime so a new deploy unpacks fresh.
    Returns *db_path* unchanged when neither file exists; the index is then unavailable.
    """

    gz_path = db_path.with_name(db_path.name + ".gz")
    if db_path.exists() or not gz_path.exists():
        return db_path

    stat = gz_path.stat()
    target = (unpack_dir or Path(tempfile.gettempdir())) / f"{db_path.stem}-{stat.st_size}-{stat.st_mtime_ns}.db"
    if target.exists():
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f"{target.name}.{os.getpid()}.part")
    logger.info("Unpacking product index %s -> %s", gz_path, target)
    with gzip.open(gz_path, "rb") as src, partial.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    partial.replace(target)
    return target


class ProductIndexRepository:
    """Search the offline product index by name, active ingredient, or NDC."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._ndc11: bool | None = None

    @property
    def available(self) -> bool:
        """True when the index file exists and has products."""

        if not self._db_path.exists():
            return False
        try:
            with self._connect() as conn:
                return conn.execute("SELECT 1 FROM indexed_products LIMIT 1").fetchone() is not None
        except sqlite3.Error:
            return False

    def built_at(self) -> str | None:
        """Timestamp of the most recently indexed product."""

        with self._connect() as conn:
            row = conn.execute("SELECT max(processed_at) FROM indexed_products").fetchone()
        return row[0] if row else None

    def search(
        self,
        query: DrugQuery,
        alias_names: list[str] | None = None,
    ) -> list[tuple[ProductSearchResult, ProductDetail]]:
        """Return products matching the query terms, an alias name, or the query NDC."""

        clauses: list[str] = []
        params: list[str] = []

        if query.ndc:
            clauses.append("ndcs LIKE ? ESCAPE '\\'")
            params.append(_like(query.ndc))
            if self._has_ndc11():
                for key in ndc_search_keys(query.ndc):
                    clauses.append("ndc11 LIKE ?")
                    params.append(f"%{key}%")
        else:
            terms = query.normalized_text.split()
            if terms:
                clauses.append("(" + " AND ".join(f"{_HAYSTACK} LIKE ? ESCAPE '\\'" for _ in terms) + ")")
                params.extend(_like(term) for term in terms)
            for name in alias_names or []:
                clauses.append("search_text LIKE ? ESCAPE '\\'")
                params.append(_like(name))

        if not clauses:
            return []

        sql = (
            "SELECT spl_setid, product_name, labeler, dosage_form, route, ndcs, "
            "active_ingredients_raw, active_strength, inactive_ingredients_raw, "
            "product_type, processed_at "
            f"FROM indexed_products WHERE {' OR '.join(clauses)} "
            f"ORDER BY product_name COLLATE NOCASE LIMIT {MAX_RESULTS}"
        )
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_models(row) for row in rows]

    def _has_ndc11(self) -> bool:
        """True when the index carries the 11-digit NDC column (added at export)."""

        if self._ndc11 is None:
            with self._connect() as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(indexed_products)")}
            self._ndc11 = "ndc11" in columns
        return self._ndc11

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self._db_path.as_posix()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _to_models(row: sqlite3.Row) -> tuple[ProductSearchResult, ProductDetail]:
        ndcs = _split(row["ndcs"])
        strengths = _split(row["active_strength"])
        active = [
            IngredientEntry(
                raw_name=name,
                display_name=name,
                strength=strengths[i] if i < len(strengths) else None,
                role="active",
            )
            for i, name in enumerate(_split(row["active_ingredients_raw"]))
        ]
        inactive = [
            IngredientEntry(raw_name=name, display_name=name, role="inactive")
            for name in _split(row["inactive_ingredients_raw"])
        ]
        result = ProductSearchResult(
            product_name=row["product_name"] or "",
            setid=row["spl_setid"],
            ndcs=ndcs,
            route=row["route"],
            dosage_form=row["dosage_form"],
            labeler=row["labeler"],
            source=DataSource.LOCAL_INDEX,
        )
        detail = ProductDetail(
            product_name=row["product_name"] or "",
            setid=row["spl_setid"],
            ndcs=ndcs,
            route=row["route"],
            dosage_form=row["dosage_form"],
            labeler=row["labeler"],
            product_type=row["product_type"],
            active_ingredients=active,
            inactive_ingredients_raw=row["inactive_ingredients_raw"],
            inactive_ingredients=inactive,
            spl_source="product_index",
            fetched_at=_parse_timestamp(row["processed_at"]),
        )
        return result, detail
