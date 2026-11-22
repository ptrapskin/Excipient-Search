"""Full product index: every human drug product's ingredients, all dosage forms.

Written alongside excipients.db by the ingestion pipeline so the web app's
Excipient Search and Gluten Excipient Screener can search locally instead of
calling the live DailyMed API for every label. Kept in its own database file so
its size can be managed separately from the liquid-only screener data.
"""
from __future__ import annotations

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
