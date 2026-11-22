"""Full product index: every human drug product's ingredients, all dosage forms.

Written alongside excipients.db by the ingestion pipeline so the web app's
Excipient Search and Gluten Excipient Screener can search locally instead of
calling the live DailyMed API for every label. Kept in its own database file so
its size can be managed separately from the liquid-only screener data.

The full index is too large to commit, so ``export_compressed`` writes a slim,
gzipped copy (product_index.db.gz) that is committed and deployed instead.
"""
from __future__ import annotations

import gzip
import shutil
import sqlite3
from pathlib import Path

from excipient_finder.models import SplRecord
from excipient_finder.utils import utc_now_str


def init_index_db(db_path: Path) -> sqlite3.Connection:
    """Create the index database and table if they do not exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS indexed_products (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            spl_setid                TEXT NOT NULL,
            product_name             TEXT,
            labeler                  TEXT,
            dosage_form              TEXT,
            route                    TEXT,
            ndcs                     TEXT,
            active_ingredients_raw   TEXT,
            active_strength          TEXT,
            inactive_ingredients_raw TEXT,
            product_type             TEXT,
            search_text              TEXT NOT NULL,
            source_file              TEXT,
            processed_at             TEXT,
            UNIQUE(spl_setid, product_name, ndcs)
        );
        CREATE INDEX IF NOT EXISTS idx_indexed_products_setid
            ON indexed_products (spl_setid);
    """)
    conn.commit()
    return conn


def clear_index(conn: sqlite3.Connection) -> None:
    """Wipe the index at the start of a clean (non-resume) rebuild."""
    conn.execute("DELETE FROM indexed_products")
    conn.commit()


def build_search_text(rec: SplRecord) -> str:
    """Lowercased product and active ingredient names used for name searches."""
    return " | ".join(
        part.casefold() for part in (rec.product_name, rec.active_ingredients_raw) if part
    )


def insert_index_record(conn: sqlite3.Connection, rec: SplRecord) -> None:
    """Insert or replace one product. Later labels for the same product win."""
    conn.execute(
        """
        INSERT OR REPLACE INTO indexed_products (
            spl_setid, product_name, labeler, dosage_form, route, ndcs,
            active_ingredients_raw, active_strength, inactive_ingredients_raw,
            product_type, search_text, source_file, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rec.setid,
            rec.product_name,
            rec.labeler,
            rec.dosage_form,
            rec.route,
            "; ".join(rec.ndcs),
            rec.active_ingredients_raw,
            rec.active_strength,
            rec.inactive_ingredients_raw,
            rec.product_type,
            build_search_text(rec),
            rec.source_file,
            utc_now_str(),
        ),
    )


# Columns the web app reads; build bookkeeping and the uniqueness index are dropped.
_EXPORT_COLUMNS = (
    "spl_setid, product_name, labeler, dosage_form, route, ndcs, "
    "active_ingredients_raw, active_strength, inactive_ingredients_raw, "
    "product_type, search_text, processed_at"
)


def export_compressed(index_db_path: Path, gz_path: Path) -> Path:
    """Write a slim copy of the index without indexes, then gzip it to *gz_path*."""
    slim_path = gz_path.with_name(gz_path.name + ".slim.tmp")
    slim_path.unlink(missing_ok=True)
    conn = sqlite3.connect(str(slim_path))
    try:
        conn.execute("ATTACH DATABASE ? AS src", (str(index_db_path),))
        conn.execute(
            f"CREATE TABLE indexed_products AS SELECT {_EXPORT_COLUMNS} "
            "FROM src.indexed_products ORDER BY product_name COLLATE NOCASE"
        )
        conn.commit()
        conn.execute("DETACH DATABASE src")
        conn.execute("VACUUM")
    finally:
        conn.close()

    tmp_gz = gz_path.with_name(gz_path.name + ".tmp")
    with slim_path.open("rb") as src, gzip.open(tmp_gz, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    tmp_gz.replace(gz_path)
    slim_path.unlink()
    return gz_path
