"""SQLite database setup and write helpers for the excipient_finder pipeline.

Schema
------
products           – one row per retained product subject (HIGH / MODERATE / REVIEW)
matched_excipients – one row per matched sugar alcohol per product
processing_log     – one row per outer-zip processing event

Writes are batched (commit every BATCH_SIZE rows) for throughput.
"""
from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

from .models import FilterDecision, MatchedExcipient, ProductOutputRow, SplRecord
from .utils import normalize_text, utc_now_str

BATCH_SIZE = 500


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create the database file and tables if they do not exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _create_tables(conn)
    _create_qa_tables(conn)
    _migrate(conn)
    conn.commit()
    return conn


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            spl_setid               TEXT NOT NULL,
            product_name            TEXT,
            labeler                 TEXT,
            dosage_form             TEXT,
            normalized_form         TEXT,
            form_class              TEXT,
            route                   TEXT,
            normalized_route        TEXT,
            route_class             TEXT,
            ndcs                    TEXT,
            active_ingredients_raw  TEXT,
            active_strength         TEXT,
            active_ingredients_unii TEXT,
            concern_tier            TEXT NOT NULL,
            inclusion_decision      TEXT NOT NULL,
            review_reason           TEXT,
            included_form_match     TEXT,
            excluded_form_match     TEXT,
            included_route_match    TEXT,
            excluded_route_match    TEXT,
            inactive_ingredients_raw TEXT,
            inactive_ingredients_unii TEXT,
            matched_sugar_alcohols  TEXT,
            matched_sugar_alcohol_terms TEXT,
            matched_sugar_alcohol_uniis TEXT,
            source_file             TEXT,
            processed_at            TEXT
        );

        CREATE TABLE IF NOT EXISTS matched_excipients (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            spl_setid       TEXT NOT NULL,
            raw_name        TEXT,
            normalized_name TEXT,
            canonical_name  TEXT,
            category        TEXT,
            unii            TEXT
        );

        CREATE TABLE IF NOT EXISTS processing_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file     TEXT NOT NULL,
            status          TEXT NOT NULL,
            message         TEXT,
            processed_at    TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_products_setid
            ON products (spl_setid);
        CREATE INDEX IF NOT EXISTS idx_products_tier
            ON products (concern_tier);
        CREATE INDEX IF NOT EXISTS idx_excipients_setid
            ON matched_excipients (spl_setid);
        CREATE INDEX IF NOT EXISTS idx_log_source
            ON processing_log (source_file);
    """)

    # Staging table for liquid oral products with no sugar alcohols.
    # Populated during the main pass; joined against SA products in
    # promote_alternatives() to identify SA-free alternatives.
    # Cleared at the start of each run and again after promotion.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS liquid_candidates (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            spl_setid               TEXT NOT NULL,
            product_name            TEXT,
            labeler                 TEXT,
            dosage_form             TEXT,
            normalized_form         TEXT,
            form_class              TEXT,
            route                   TEXT,
            normalized_route        TEXT,
            route_class             TEXT,
            ndcs                    TEXT,
            active_ingredients_raw  TEXT,
            active_strength         TEXT,
            active_ingredients_unii TEXT,
            inactive_ingredients_raw TEXT,
            inactive_ingredients_unii TEXT,
            source_file             TEXT,
            processed_at            TEXT,
            UNIQUE(spl_setid)
        );
        CREATE INDEX IF NOT EXISTS idx_candidates_setid
            ON liquid_candidates (spl_setid);
        CREATE INDEX IF NOT EXISTS idx_candidates_unii
            ON liquid_candidates (active_ingredients_unii);
    """)


def _create_qa_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS qa_funnel_summary (
            key   TEXT NOT NULL,
            value INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS qa_parse_failures (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file      TEXT,
            xml_member_name  TEXT,
            error_type       TEXT,
            error_message    TEXT,
            processed_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS qa_excluded_audit (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            spl_setid                TEXT,
            product_name             TEXT,
            dosage_form              TEXT,
            route                    TEXT,
            form_class               TEXT,
            route_class              TEXT,
            exclusion_reason         TEXT,
            review_reason            TEXT,
            matched_sugar_alcohol_terms TEXT,
            source_file              TEXT,
            processed_at             TEXT
        );
    """)


def _migrate(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial schema (idempotent)."""
    prod_cols = {row[1] for row in conn.execute("PRAGMA table_info(products)")}
    if "active_strength" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN active_strength TEXT")
    if "active_ingredients_unii" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN active_ingredients_unii TEXT")
    if "matched_sugar_alcohol_uniis" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN matched_sugar_alcohol_uniis TEXT")
    if "inactive_ingredients_unii" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN inactive_ingredients_unii TEXT")
    if "alternative_sugar_alcohols" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN alternative_sugar_alcohols TEXT")

    # Deduplicate products by spl_setid (keep highest id = most recent), then
    # enforce uniqueness so INSERT OR REPLACE / INSERT OR IGNORE work correctly.
    conn.execute(
        "DELETE FROM products WHERE id NOT IN "
        "(SELECT MAX(id) FROM products GROUP BY spl_setid)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_products_setid ON products (spl_setid)"
    )

    exc_cols = {row[1] for row in conn.execute("PRAGMA table_info(matched_excipients)")}
    if "unii" not in exc_cols:
        conn.execute("ALTER TABLE matched_excipients ADD COLUMN unii TEXT")


# ---------------------------------------------------------------------------
# Product writing
# ---------------------------------------------------------------------------

def insert_product(conn: sqlite3.Connection, row: ProductOutputRow) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO products (
            spl_setid, product_name, labeler, dosage_form, normalized_form,
            form_class, route, normalized_route, route_class, ndcs,
            active_ingredients_raw, active_strength, active_ingredients_unii,
            concern_tier, inclusion_decision, review_reason,
            included_form_match, excluded_form_match,
            included_route_match, excluded_route_match,
            inactive_ingredients_raw, inactive_ingredients_unii, matched_sugar_alcohols,
            matched_sugar_alcohol_terms, matched_sugar_alcohol_uniis,
            source_file, processed_at
        ) VALUES (
            :spl_setid, :product_name, :labeler, :dosage_form, :normalized_form,
            :form_class, :route, :normalized_route, :route_class, :ndcs,
            :active_ingredients_raw, :active_strength, :active_ingredients_unii,
            :concern_tier, :inclusion_decision, :review_reason,
            :included_form_match, :excluded_form_match,
            :included_route_match, :excluded_route_match,
            :inactive_ingredients_raw, :inactive_ingredients_unii, :matched_sugar_alcohols,
            :matched_sugar_alcohol_terms, :matched_sugar_alcohol_uniis,
            :source_file, :processed_at
        )
        """,
        {
            "spl_setid": row.spl_setid,
            "product_name": row.product_name,
            "labeler": row.labeler,
            "dosage_form": row.dosage_form,
            "normalized_form": row.normalized_form,
            "form_class": row.form_class,
            "route": row.route,
            "normalized_route": row.normalized_route,
            "route_class": row.route_class,
            "ndcs": row.ndcs,
            "active_ingredients_raw": row.active_ingredients_raw,
            "active_strength": row.active_strength,
            "active_ingredients_unii": row.active_ingredients_unii,
            "concern_tier": row.concern_tier,
            "inclusion_decision": row.inclusion_decision,
            "review_reason": row.review_reason,
            "included_form_match": row.included_form_match,
            "excluded_form_match": row.excluded_form_match,
            "included_route_match": row.included_route_match,
            "excluded_route_match": row.excluded_route_match,
            "inactive_ingredients_raw": row.inactive_ingredients_raw,
            "inactive_ingredients_unii": row.inactive_ingredients_unii,
            "matched_sugar_alcohols": row.matched_sugar_alcohols,
            "matched_sugar_alcohol_terms": row.matched_sugar_alcohol_terms,
            "matched_sugar_alcohol_uniis": row.matched_sugar_alcohol_uniis,
            "source_file": row.source_file,
            "processed_at": row.processed_at,
        },
    )


def insert_excipients(
    conn: sqlite3.Connection,
    setid: str,
    excipients: list[MatchedExcipient],
) -> None:
    conn.executemany(
        """
        INSERT INTO matched_excipients (spl_setid, raw_name, normalized_name, canonical_name, category, unii)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (setid, e.raw_name, e.normalized_name, e.canonical_name, e.category, e.unii)
            for e in excipients
        ],
    )


# ---------------------------------------------------------------------------
# Liquid candidate staging (SA-free alternative pipeline)
# ---------------------------------------------------------------------------

def clear_liquid_candidates(conn: sqlite3.Connection) -> None:
    """Clear the staging table at the start of a fresh pipeline run."""
    conn.execute("DELETE FROM liquid_candidates")
    conn.commit()


def insert_liquid_candidate(
    conn: sqlite3.Connection,
    rec: SplRecord,
    decision: FilterDecision,
) -> None:
    """Store a liquid oral product with no sugar alcohols as a candidate for SA-free alternative promotion."""
    conn.execute(
        """
        INSERT OR IGNORE INTO liquid_candidates (
            spl_setid, product_name, labeler, dosage_form, normalized_form,
            form_class, route, normalized_route, route_class, ndcs,
            active_ingredients_raw, active_strength, active_ingredients_unii,
            inactive_ingredients_raw, inactive_ingredients_unii,
            source_file, processed_at
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?
        )
        """,
        (
            rec.setid, rec.product_name, rec.labeler, rec.dosage_form,
            normalize_text(rec.dosage_form or ""),
            decision.form_class, rec.route, normalize_text(rec.route or ""),
            decision.route_class, "; ".join(rec.ndcs),
            rec.active_ingredients_raw, rec.active_strength, rec.active_ingredients_unii,
            rec.inactive_ingredients_raw,
            "; ".join(e.unii or "" for e in rec.inactive_ingredient_entries),
            rec.source_file, utc_now_str(),
        ),
    )


def promote_alternatives(conn: sqlite3.Connection) -> int:
    """Join liquid_candidates against SA products on active ingredient UNII.

    For each candidate that shares a UNII with at least one SA product, insert
    a row into products with concern_tier='alternative' and
    alternative_sugar_alcohols set to the canonical SA names from the matched
    SA products.  Returns the number of alternative rows inserted.
    """
    # Build UNII → set of canonical SA names from confirmed SA products
    sa_rows = conn.execute(
        """
        SELECT active_ingredients_unii, matched_sugar_alcohols
        FROM   products
        WHERE  concern_tier IN ('high', 'moderate', 'review')
          AND  active_ingredients_unii IS NOT NULL
          AND  active_ingredients_unii != ''
        """
    ).fetchall()

    unii_to_sas: dict[str, set[str]] = {}
    for unii_str, sa_str in sa_rows:
        for unii in (unii_str or "").split(";"):
            unii = unii.strip()
            if unii:
                for sa in (sa_str or "").split(";"):
                    sa = sa.strip()
                    if sa:
                        unii_to_sas.setdefault(unii, set()).add(sa)

    if not unii_to_sas:
        conn.execute("DELETE FROM liquid_candidates")
        conn.commit()
        return 0

    # Build a set of (product_name_lower, labeler_lower) already identified as SA-containing
    # so we never promote the same product as a sugar-alcohol-free alternative
    # (can happen when different label versions of the same product have different inactive ingredient lists)
    sa_product_keys: set[tuple[str, str]] = {
        ((name or "").lower(), (lab or "").lower())
        for name, lab in conn.execute(
            "SELECT product_name, labeler FROM products WHERE concern_tier IN ('high', 'moderate', 'review')"
        )
    }

    # Fetch candidates and find those that share a UNII with SA products
    orig_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    candidates = conn.execute("SELECT * FROM liquid_candidates").fetchall()
    conn.row_factory = orig_factory

    inserted = 0
    now = utc_now_str()
    for cand in candidates:
        # Skip if the same product+labeler is already identified as SA-containing
        cand_key = ((cand["product_name"] or "").lower(), (cand["labeler"] or "").lower())
        if cand_key in sa_product_keys:
            continue

        cand_uniis = [u.strip() for u in (cand["active_ingredients_unii"] or "").split(";") if u.strip()]
        matching_sas: set[str] = set()
        for unii in cand_uniis:
            matching_sas |= unii_to_sas.get(unii, set())
        if not matching_sas:
            continue

        alt_sa_str = "; ".join(sorted(matching_sas))
        conn.execute(
            """
            INSERT OR IGNORE INTO products (
                spl_setid, product_name, labeler, dosage_form, normalized_form,
                form_class, route, normalized_route, route_class, ndcs,
                active_ingredients_raw, active_strength, active_ingredients_unii,
                concern_tier, inclusion_decision,
                inactive_ingredients_raw, inactive_ingredients_unii,
                matched_sugar_alcohols, matched_sugar_alcohol_terms, matched_sugar_alcohol_uniis,
                alternative_sugar_alcohols, source_file, processed_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                'alternative', 'included',
                ?, ?,
                '', '', '',
                ?, ?, ?
            )
            """,
            (
                cand["spl_setid"], cand["product_name"], cand["labeler"],
                cand["dosage_form"], cand["normalized_form"],
                cand["form_class"], cand["route"], cand["normalized_route"],
                cand["route_class"], cand["ndcs"],
                cand["active_ingredients_raw"], cand["active_strength"],
                cand["active_ingredients_unii"],
                cand["inactive_ingredients_raw"], cand["inactive_ingredients_unii"],
                alt_sa_str, cand["source_file"], now,
            ),
        )
        inserted += 1

    conn.execute("DELETE FROM liquid_candidates")
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Processing log
# ---------------------------------------------------------------------------

def log_file_start(conn: sqlite3.Connection, source_file: str) -> None:
    conn.execute(
        "INSERT INTO processing_log (source_file, status, message, processed_at) VALUES (?, ?, ?, ?)",
        (source_file, "started", None, utc_now_str()),
    )
    conn.commit()


def log_file_success(
    conn: sqlite3.Connection,
    source_file: str,
    counts: dict[str, int],
) -> None:
    msg = ", ".join(f"{k}={v}" for k, v in counts.items())
    conn.execute(
        "INSERT INTO processing_log (source_file, status, message, processed_at) VALUES (?, ?, ?, ?)",
        (source_file, "success", msg, utc_now_str()),
    )
    conn.commit()


def log_file_failure(
    conn: sqlite3.Connection,
    source_file: str,
    error: str,
) -> None:
    conn.execute(
        "INSERT INTO processing_log (source_file, status, message, processed_at) VALUES (?, ?, ?, ?)",
        (source_file, "failed", error, utc_now_str()),
    )
    conn.commit()


def is_already_processed(conn: sqlite3.Connection, source_file: str) -> bool:
    """Return True if source_file has a 'success' entry in processing_log."""
    row = conn.execute(
        "SELECT 1 FROM processing_log WHERE source_file = ? AND status = 'success' LIMIT 1",
        (source_file,),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

_CSV_FIELDS = [
    "spl_setid", "product_name", "labeler", "dosage_form", "form_class",
    "route", "route_class", "ndcs", "active_ingredients_raw", "concern_tier",
    "inclusion_decision", "review_reason",
    "included_form_match", "excluded_form_match",
    "included_route_match", "excluded_route_match",
    "inactive_ingredients_raw", "matched_sugar_alcohols",
    "matched_sugar_alcohol_terms", "source_file", "processed_at",
]


def write_csvs(
    conn: sqlite3.Connection,
    csv_dir: Path,
    *,
    write_excluded_debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Export products table to tier-specific CSV files."""
    csv_dir.mkdir(parents=True, exist_ok=True)

    tier_files = {
        "high":     csv_dir / "final_products_of_concern.csv",
        "moderate": csv_dir / "moderate_products_of_concern.csv",
        "review":   csv_dir / "review_products.csv",
    }
    if write_excluded_debug:
        tier_files["excluded"] = csv_dir / "excluded_products_debug.csv"

    for tier, path in tier_files.items():
        rows = conn.execute(
            f"SELECT {', '.join(_CSV_FIELDS)} FROM products WHERE concern_tier = ?",
            (tier,),
        ).fetchall()
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(_CSV_FIELDS)
            writer.writerows(rows)
        if logger:
            logger.info("  Wrote %d rows -> %s", len(rows), path.name)


def get_tier_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT concern_tier, COUNT(*) FROM products GROUP BY concern_tier"
    ).fetchall()
    return {tier: count for tier, count in rows}


# ---------------------------------------------------------------------------
# QA write helpers
# ---------------------------------------------------------------------------

def write_funnel_to_db(conn: sqlite3.Connection, counts_dict: dict) -> None:
    """Clear and rewrite qa_funnel_summary from a plain dict of {key: int}."""
    conn.execute("DELETE FROM qa_funnel_summary")
    conn.executemany(
        "INSERT INTO qa_funnel_summary (key, value) VALUES (?, ?)",
        list(counts_dict.items()),
    )
    conn.commit()


def insert_parse_failure_to_db(conn: sqlite3.Connection, failure_dict: dict) -> None:
    """Insert a single parse-failure record to qa_parse_failures."""
    conn.execute(
        """
        INSERT INTO qa_parse_failures
            (source_file, xml_member_name, error_type, error_message, processed_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            failure_dict.get("source_file", ""),
            failure_dict.get("xml_member_name", ""),
            failure_dict.get("error_type", ""),
            failure_dict.get("error_message", ""),
            failure_dict.get("processed_at", ""),
        ),
    )


def insert_qa_audit_record(
    conn: sqlite3.Connection,
    spl_setid: str,
    product_name: str,
    dosage_form: str | None,
    route: str | None,
    form_class: str,
    route_class: str,
    exclusion_reason: str,
    review_reason: str | None,
    matched_terms: str,
    source_file: str,
    processed_at: str,
) -> None:
    """Insert one row to qa_excluded_audit."""
    conn.execute(
        """
        INSERT INTO qa_excluded_audit (
            spl_setid, product_name, dosage_form, route,
            form_class, route_class, exclusion_reason, review_reason,
            matched_sugar_alcohol_terms, source_file, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            spl_setid, product_name, dosage_form, route,
            form_class, route_class, exclusion_reason, review_reason,
            matched_terms, source_file, processed_at,
        ),
    )
