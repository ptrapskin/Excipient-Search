"""Quality-assurance helpers for the excipient_finder pipeline.

Provides:
  - FunnelCounts dataclass for tracking processing-funnel metrics.
  - CSV report writers (funnel summary, parse failures, excipient/form/route
    breakdowns, random QA samples).
  - Static QA test runners for the matcher, form classifier, and route
    classifier.
  - Known-positive validation against a reference CSV.
"""
from __future__ import annotations

import csv
import logging
import random
import sqlite3
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

from .excipient_matcher import SUGAR_ALCOHOL_CONCEPTS
from .filters import classify_form, classify_route
from .models import IngredientEntry
from .utils import normalize_text, utc_now_str


# ---------------------------------------------------------------------------
# FunnelCounts
# ---------------------------------------------------------------------------

@dataclass
class FunnelCounts:
    # --- XML / parse level ---
    total_xml_files: int = 0
    parse_successes: int = 0
    parse_failures: int = 0

    # --- Record level ---
    total_records: int = 0
    non_human_skipped: int = 0

    # --- Form classification ---
    with_dosage_form: int = 0
    strong_liquid_form: int = 0
    ambiguous_form: int = 0
    excluded_form: int = 0
    non_liquid_form: int = 0

    # --- Route classification ---
    with_route: int = 0
    allowed_route: int = 0
    excluded_route: int = 0
    blank_route: int = 0

    # --- Excipient matching ---
    with_inactive_ingredients: int = 0
    with_sugar_alcohol_hit: int = 0

    # --- Final tier ---
    final_high: int = 0
    final_moderate: int = 0
    final_review: int = 0
    final_excluded: int = 0

    def add(self, other: "FunnelCounts") -> None:
        """Add all numeric fields from *other* into self (in-place)."""
        for f in fields(self):
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))

    def log_summary(self, logger: logging.Logger) -> None:
        """Log a nicely formatted funnel summary."""
        logger.info("=" * 60)
        logger.info("PROCESSING FUNNEL SUMMARY")
        logger.info("  XML files yielded      : %d", self.total_xml_files)
        logger.info("  Parse successes        : %d", self.parse_successes)
        logger.info("  Parse failures         : %d", self.parse_failures)
        logger.info("  Total records          : %d", self.total_records)
        logger.info("  Non-human skipped      : %d", self.non_human_skipped)
        logger.info("  With dosage form       : %d", self.with_dosage_form)
        logger.info("    Strong liquid form   : %d", self.strong_liquid_form)
        logger.info("    Ambiguous form       : %d", self.ambiguous_form)
        logger.info("    Excluded form        : %d", self.excluded_form)
        logger.info("    Non-liquid form      : %d", self.non_liquid_form)
        logger.info("  With route             : %d", self.with_route)
        logger.info("    Allowed route        : %d", self.allowed_route)
        logger.info("    Excluded route       : %d", self.excluded_route)
        logger.info("    Blank route          : %d", self.blank_route)
        logger.info("  With inactive ingreds  : %d", self.with_inactive_ingredients)
        logger.info("  With sugar alcohol hit : %d", self.with_sugar_alcohol_hit)
        logger.info("  Final HIGH             : %d", self.final_high)
        logger.info("  Final MODERATE         : %d", self.final_moderate)
        logger.info("  Final REVIEW           : %d", self.final_review)
        logger.info("  Final EXCLUDED         : %d", self.final_excluded)
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Internal CSV helper
# ---------------------------------------------------------------------------

def _write_csv(path: Path, header: list[str], rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def write_funnel_summary(counts: FunnelCounts, path: Path) -> None:
    """Write a two-column stage/count CSV from a FunnelCounts."""
    rows = [(f.name, getattr(counts, f.name)) for f in fields(counts)]
    _write_csv(path, ["stage", "count"], rows)


def write_parse_failures_csv(failures_list: list[dict], path: Path) -> None:
    """Write parse-failure records to CSV."""
    header = ["source_file", "xml_member_name", "error_type", "error_message", "processed_at"]
    rows = [
        [
            d.get("source_file", ""),
            d.get("xml_member_name", ""),
            d.get("error_type", ""),
            d.get("error_message", ""),
            d.get("processed_at", ""),
        ]
        for d in failures_list
    ]
    _write_csv(path, header, rows)


def write_excipient_summary(
    conn: sqlite3.Connection,
    path: Path,
    logger: logging.Logger | None = None,
) -> None:
    """Count included products per SUGAR_ALCOHOL_CONCEPTS key using LIKE query."""
    header = ["canonical_name", "product_count"]
    rows = []
    for canonical in SUGAR_ALCOHOL_CONCEPTS:
        count = conn.execute(
            "SELECT COUNT(*) FROM products "
            "WHERE inclusion_decision = 'included' "
            "AND matched_sugar_alcohols LIKE ?",
            (f"%{canonical}%",),
        ).fetchone()[0]
        rows.append((canonical, count))
    _write_csv(path, header, rows)
    if logger:
        logger.info("  Wrote excipient summary -> %s", path.name)


def write_form_summary(
    conn: sqlite3.Connection,
    path: Path,
    logger: logging.Logger | None = None,
) -> None:
    """GROUP BY form_class, concern_tier COUNT(*) for all products."""
    header = ["form_class", "concern_tier", "count"]
    rows = conn.execute(
        "SELECT form_class, concern_tier, COUNT(*) "
        "FROM products "
        "GROUP BY form_class, concern_tier "
        "ORDER BY form_class, concern_tier"
    ).fetchall()
    _write_csv(path, header, rows)
    if logger:
        logger.info("  Wrote form summary -> %s", path.name)


def write_route_summary(
    conn: sqlite3.Connection,
    path: Path,
    logger: logging.Logger | None = None,
) -> None:
    """GROUP BY route_class, concern_tier COUNT(*) for all products."""
    header = ["route_class", "concern_tier", "count"]
    rows = conn.execute(
        "SELECT route_class, concern_tier, COUNT(*) "
        "FROM products "
        "GROUP BY route_class, concern_tier "
        "ORDER BY route_class, concern_tier"
    ).fetchall()
    _write_csv(path, header, rows)
    if logger:
        logger.info("  Wrote route summary -> %s", path.name)


_SAMPLE_FIELDS = [
    "spl_setid", "product_name", "labeler", "dosage_form", "form_class",
    "route", "route_class", "concern_tier", "inclusion_decision",
    "review_reason", "matched_sugar_alcohols", "matched_sugar_alcohol_terms",
    "inactive_ingredients_raw", "source_file", "processed_at",
]


def write_qa_samples(
    conn: sqlite3.Connection,
    csv_dir: Path,
    sample_size: int = 25,
    logger: logging.Logger | None = None,
) -> None:
    """Write random samples for included/review/excluded tiers (3 CSV files)."""
    csv_dir.mkdir(parents=True, exist_ok=True)
    tiers = {
        "included": ("included", csv_dir / "qa_sample_included.csv"),
        "review":   ("review",   csv_dir / "qa_sample_review.csv"),
        "excluded": ("excluded", csv_dir / "qa_sample_excluded.csv"),
    }
    for label, (tier_val, out_path) in tiers.items():
        # Fetch all matching rows then sample in Python to avoid SQLite RANDOM() ordering issues
        all_rows = conn.execute(
            f"SELECT {', '.join(_SAMPLE_FIELDS)} FROM products "
            "WHERE concern_tier = ? OR inclusion_decision = ?",
            (tier_val, tier_val),
        ).fetchall()
        sample = random.sample(all_rows, min(sample_size, len(all_rows)))
        _write_csv(out_path, _SAMPLE_FIELDS, sample)
        if logger:
            logger.info(
                "  QA sample %s: %d rows sampled from %d -> %s",
                label, len(sample), len(all_rows), out_path.name,
            )


# ---------------------------------------------------------------------------
# Static QA test helpers
# ---------------------------------------------------------------------------

def run_matcher_qa(
    logger: logging.Logger,
    csv_path: Path | None = None,
) -> bool:
    """Test sugar alcohol alias matching.

    Returns True if all cases pass.
    """
    from .excipient_matcher import match_excipients

    positives = [
        "SORBITOL",
        "Sorbitol solution",
        "NONCRYSTALLIZING SORBITOL SOLUTION",
        "non-crystallizing sorbitol solution",
        "mannitol",
        "D-Mannitol",
        "xylitol",
        "maltitol",
        "Maltitol Solution",
        "lactitol",
        "lactitol monohydrate",
        "isomalt",
    ]
    negatives = [
        "sorbitan monostearate",
        "sucralose",
        "glycerin",
        "cellulose",
        "sodium benzoate",
    ]

    all_pass = True
    csv_rows: list[list[str]] = []

    for inp in positives:
        entry = IngredientEntry(raw_name=inp, normalized_name=normalize_text(inp))
        matched = match_excipients([entry])
        got = matched[0].canonical_name if matched else ""
        passed = bool(matched)
        if not passed:
            logger.warning("MATCHER QA FAIL [positive]: %r -> no match", inp)
            all_pass = False
        csv_rows.append(["positive", inp, "any_match", got, "PASS" if passed else "FAIL"])

    for inp in negatives:
        entry = IngredientEntry(raw_name=inp, normalized_name=normalize_text(inp))
        matched = match_excipients([entry])
        got = matched[0].canonical_name if matched else ""
        passed = not bool(matched)
        if not passed:
            logger.warning("MATCHER QA FAIL [negative]: %r -> matched %r", inp, got)
            all_pass = False
        csv_rows.append(["negative", inp, "no_match", got, "PASS" if passed else "FAIL"])

    if all_pass:
        logger.info("Matcher QA: all %d cases passed.", len(csv_rows))
    else:
        logger.warning("Matcher QA: SOME CASES FAILED — see log above.")

    if csv_path is not None:
        _write_csv(
            csv_path,
            ["case_type", "input", "expected", "got", "result"],
            csv_rows,
        )

    return all_pass


def run_form_qa(
    logger: logging.Logger,
    csv_path: Path | None = None,
) -> bool:
    """Test classify_form() against known inputs.

    Returns True if all cases pass.
    """
    cases: list[tuple[str, str]] = [
        # (input, expected_class)
        ("SOLUTION",          "strong"),
        ("SUSPENSION",        "strong"),
        ("SYRUP",             "strong"),
        ("ELIXIR",            "strong"),
        ("CONCENTRATE",       "strong"),
        ("ORAL DROPS",        "strong"),
        ("ORAL SOLUTION",     "strong"),
        ("ORAL SUSPENSION",   "strong"),
        ("LIQUID",            "ambiguous"),
        ("DROPS",             "ambiguous"),
        ("EMULSION",          "ambiguous"),
        ("CAPSULE",           "excluded"),
        ("CAPSULE LIQUID FILLED", "excluded"),
        ("TABLET",            "excluded"),
        ("TABLET FILM COATED","excluded"),
        ("INJECTION",         "excluded"),
        ("OPHTHALMIC SOLUTION","excluded"),
        ("TOPICAL SOLUTION",  "excluded"),
        ("GEL",               "non_liquid"),
        ("POWDER",            "non_liquid"),
        ("CREAM",             "non_liquid"),
    ]

    all_pass = True
    csv_rows: list[list[str]] = []

    for inp, expected in cases:
        got_class, inc_kw, exc_kw = classify_form(inp)
        passed = got_class == expected
        if not passed:
            logger.warning(
                "FORM QA FAIL: %r -> expected=%r got=%r", inp, expected, got_class
            )
            all_pass = False
        csv_rows.append([inp, expected, got_class, inc_kw or "", exc_kw or "", "PASS" if passed else "FAIL"])

    if all_pass:
        logger.info("Form QA: all %d cases passed.", len(cases))
    else:
        logger.warning("Form QA: SOME CASES FAILED — see log above.")

    if csv_path is not None:
        _write_csv(
            csv_path,
            ["input", "expected", "got", "inc_keyword", "exc_keyword", "result"],
            csv_rows,
        )

    return all_pass


def run_route_qa(
    logger: logging.Logger,
    csv_path: Path | None = None,
) -> bool:
    """Test classify_route() against known inputs.

    Returns True if all cases pass.
    """
    cases: list[tuple[str | None, str]] = [
        # (input, expected_class)
        ("ORAL",           "oral"),
        ("ENTERAL",        "oral"),
        ("GASTRIC",        "oral"),
        ("NASOGASTRIC",    "oral"),
        ("BUCCAL",         "oral"),
        ("SUBLINGUAL",     "oral"),
        ("OROPHARYNGEAL",  "oral"),
        ("TOPICAL",        "excluded"),
        ("OPHTHALMIC",     "excluded"),
        ("NASAL",          "excluded"),
        ("RECTAL",         "excluded"),
        ("INHALATION",     "excluded"),
        ("INTRAVENOUS",    "excluded"),
        ("INTRAMUSCULAR",  "excluded"),
        (None,             "blank"),
        ("",               "blank"),
    ]

    all_pass = True
    csv_rows: list[list[str]] = []

    for inp, expected in cases:
        got_class, inc_kw, exc_kw = classify_route(inp)
        passed = got_class == expected
        if not passed:
            logger.warning(
                "ROUTE QA FAIL: %r -> expected=%r got=%r", inp, expected, got_class
            )
            all_pass = False
        csv_rows.append([
            str(inp) if inp is not None else "",
            expected, got_class,
            inc_kw or "", exc_kw or "",
            "PASS" if passed else "FAIL",
        ])

    if all_pass:
        logger.info("Route QA: all %d cases passed.", len(cases))
    else:
        logger.warning("Route QA: SOME CASES FAILED — see log above.")

    if csv_path is not None:
        _write_csv(
            csv_path,
            ["input", "expected", "got", "inc_keyword", "exc_keyword", "result"],
            csv_rows,
        )

    return all_pass


# ---------------------------------------------------------------------------
# Known-positive validation
# ---------------------------------------------------------------------------

def validate_known_positives(
    conn: sqlite3.Connection,
    positives_path: Path,
    output_path: Path,
    logger: logging.Logger,
) -> None:
    """Validate a set of known-positive records against the products table.

    The input CSV must have columns:
        expected_product_name, expected_setid (optional), expected_excipient,
        expected_decision, notes

    Outcomes per row:
        PASS            – product found, excipient matched, decision matches
        MISSING         – product not found in DB at all
        FAIL_EXCIPIENT  – product found but expected excipient not present
        FAIL_TIER       – product found, excipient present, but tier/decision wrong
    """
    if not positives_path.exists():
        logger.warning(
            "Known-positives file not found, skipping validation: %s", positives_path
        )
        return

    header = [
        "expected_product_name", "expected_setid", "expected_excipient",
        "expected_decision", "notes", "outcome", "found_setid",
        "found_decision", "found_excipients",
    ]
    out_rows: list[list[str]] = []

    with open(positives_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            exp_name = row.get("expected_product_name", "").strip()
            exp_setid = row.get("expected_setid", "").strip()
            exp_excipient = row.get("expected_excipient", "").strip()
            exp_decision = row.get("expected_decision", "").strip()
            notes = row.get("notes", "").strip()

            # Try to find the product
            if exp_setid:
                db_rows = conn.execute(
                    "SELECT spl_setid, concern_tier, inclusion_decision, "
                    "matched_sugar_alcohols, product_name "
                    "FROM products WHERE spl_setid = ?",
                    (exp_setid,),
                ).fetchall()
            else:
                # Fuzzy name match using normalize_text
                norm_name = normalize_text(exp_name)
                all_names = conn.execute(
                    "SELECT spl_setid, concern_tier, inclusion_decision, "
                    "matched_sugar_alcohols, product_name FROM products"
                ).fetchall()
                db_rows = [
                    r for r in all_names
                    if normalize_text(r[4] or "") == norm_name
                ]

            if not db_rows:
                outcome = "MISSING"
                out_rows.append([
                    exp_name, exp_setid, exp_excipient, exp_decision,
                    notes, outcome, "", "", "",
                ])
                logger.warning("Known-positive MISSING: %r", exp_name or exp_setid)
                continue

            # Take the first matching record
            found = db_rows[0]
            found_setid = found[0]
            found_tier = found[1]
            found_inclusion = found[2]
            found_excipients = found[3] or ""
            found_decision = found_inclusion  # "included" or "excluded"

            # Check excipient
            if exp_excipient:
                norm_exc = normalize_text(exp_excipient)
                excipient_present = any(
                    normalize_text(e.strip()) == norm_exc
                    for e in found_excipients.split(";")
                    if e.strip()
                )
                if not excipient_present:
                    # Also check partial match (excipient name appears in the string)
                    excipient_present = norm_exc in normalize_text(found_excipients)
            else:
                excipient_present = True

            # Check decision/tier
            if exp_decision:
                decision_match = (
                    normalize_text(exp_decision) == normalize_text(found_decision)
                    or normalize_text(exp_decision) == normalize_text(found_tier)
                )
            else:
                decision_match = True

            if not excipient_present:
                outcome = "FAIL_EXCIPIENT"
                logger.warning(
                    "Known-positive FAIL_EXCIPIENT: %r — expected %r in [%s]",
                    exp_name or exp_setid, exp_excipient, found_excipients,
                )
            elif not decision_match:
                outcome = "FAIL_TIER"
                logger.warning(
                    "Known-positive FAIL_TIER: %r — expected %r got %r/%r",
                    exp_name or exp_setid, exp_decision, found_inclusion, found_tier,
                )
            else:
                outcome = "PASS"

            out_rows.append([
                exp_name, exp_setid, exp_excipient, exp_decision,
                notes, outcome, found_setid, found_decision, found_excipients,
            ])

    _write_csv(output_path, header, out_rows)
    passes = sum(1 for r in out_rows if r[5] == "PASS")
    logger.info(
        "Known-positive validation: %d/%d PASS -> %s",
        passes, len(out_rows), output_path.name,
    )
