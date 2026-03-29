"""CLI entrypoint for the excipient_finder data-ingestion pipeline.

Usage:
    python -m excipient_finder.main --input-root "C:/Data/DailyMed"
    python -m excipient_finder.main --input-root "C:/Data/DailyMed" --debug --resume
    python -m excipient_finder.main --input-root "C:/Data/DailyMed" --limit 5 --write-excluded-debug
    python -m excipient_finder.main --input-root "C:/Data/DailyMed" --write-qa-reports --write-qa-samples

Pipeline per outer zip:
    1.  Check --resume: skip if already logged as 'success'.
    2.  Iterate inner zips → yield (setid, xml_text, inner_entry).
    3.  Parse XML → list[SplRecord].
    4.  For each SplRecord:
        a. Skip non-human product types.
        b. Apply form/route filter (FilterDecision).
        c. If should_process is False: record as 'excluded' if --write-excluded-debug.
        d. Match excipients.
        e. Assign concern tier.
        f. Build ProductOutputRow and write to DB (batch commit every BATCH_SIZE).
    5.  Log success with tier counts.
    6.  After all zips: write CSVs, QA reports, funnel summary.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from excipient_finder.config import Config, DEFAULT_OUTPUT_ROOT
from excipient_finder.db import (
    BATCH_SIZE,
    get_tier_counts,
    init_db,
    insert_excipients,
    insert_parse_failure_to_db,
    insert_product,
    insert_qa_audit_record,
    is_already_processed,
    log_file_failure,
    log_file_start,
    log_file_success,
    write_csvs,
    write_funnel_to_db,
)
from excipient_finder.excipient_matcher import match_excipients
from excipient_finder.filters import make_filter_decision, make_filter_decision_broad
from excipient_finder.models import ProductOutputRow
from excipient_finder.qa import (
    FunnelCounts,
    run_form_qa,
    run_matcher_qa,
    run_route_qa,
    validate_known_positives,
    write_excipient_summary,
    write_form_summary,
    write_funnel_summary,
    write_parse_failures_csv,
    write_qa_samples,
    write_route_summary,
)
from excipient_finder.tiering import assign_concern_tier
from excipient_finder.utils import normalize_text, setup_logging, utc_now_str
from excipient_finder.xml_parser import parse_spl_subjects
from excipient_finder.zip_reader import iter_outer_zips, iter_spl_xmls

# Broad-recall CSV columns
_BROAD_RECALL_HEADER = [
    "spl_setid", "product_name", "labeler", "dosage_form", "route",
    "form_class", "route_class", "matched_sugar_alcohols",
    "matched_sugar_alcohol_terms", "concern_tier", "in_strict_output",
    "source_file",
]


def build_output_row(
    rec,
    decision,
    matched,
    tier: str,
    review_reason: str | None,
    inclusion_decision: str,
) -> ProductOutputRow:
    """Construct a ProductOutputRow from pipeline components."""
    return ProductOutputRow(
        spl_setid=rec.setid,
        product_name=rec.product_name,
        labeler=rec.labeler,
        dosage_form=rec.dosage_form,
        normalized_form=normalize_text(rec.dosage_form or ""),
        form_class=decision.form_class,
        route=rec.route,
        normalized_route=normalize_text(rec.route or ""),
        route_class=decision.route_class,
        ndcs="; ".join(rec.ndcs),
        active_ingredients_raw=rec.active_ingredients_raw,
        active_strength=rec.active_strength,
        active_ingredients_unii=rec.active_ingredients_unii,
        concern_tier=tier,
        inclusion_decision=inclusion_decision,
        review_reason=review_reason,
        included_form_match=decision.included_form_match,
        excluded_form_match=decision.excluded_form_match,
        included_route_match=decision.included_route_match,
        excluded_route_match=decision.excluded_route_match,
        inactive_ingredients_raw=rec.inactive_ingredients_raw,
        inactive_ingredients_unii="; ".join(e.unii or "" for e in rec.inactive_ingredient_entries),
        matched_sugar_alcohols="; ".join(m.canonical_name for m in matched),
        matched_sugar_alcohol_terms="; ".join(m.raw_name for m in matched),
        matched_sugar_alcohol_uniis="; ".join(m.unii or "" for m in matched),
        source_file=rec.source_file,
        processed_at=utc_now_str(),
        matched_excipient_list=matched,
    )


def process_outer_zip(
    zip_path: Path,
    conn,
    cfg: Config,
    logger: logging.Logger,
) -> tuple[dict, FunnelCounts, list, list]:
    """Process one outer zip file.

    Returns:
        (counts_dict, funnel, broad_recall_rows, parse_failures)
    """
    counts: dict[str, int] = {
        "high": 0, "moderate": 0, "review": 0, "excluded": 0,
        "spls": 0, "parse_errors": 0,
    }
    funnel = FunnelCounts()
    broad_recall_rows: list[dict] = []
    parse_failures: list[dict] = []
    pending = 0
    seen_setids: set[str] = set()

    for setid, xml_text, xml_source in iter_spl_xmls(zip_path, logger, parse_failures=parse_failures):
        if setid in seen_setids:
            continue
        seen_setids.add(setid)

        funnel.total_xml_files += 1

        records = parse_spl_subjects(xml_text, setid, zip_path.name)
        if not records:
            counts["parse_errors"] += 1
            funnel.parse_failures += 1
            # Record a parse error in QA audit if enabled
            if cfg.write_qa_reports:
                parse_failures.append({
                    "source_file": zip_path.name,
                    "xml_member_name": xml_source,
                    "error_type": "parse_error",
                    "error_message": "parse_spl_subjects returned empty",
                    "processed_at": utc_now_str(),
                })
            continue

        funnel.parse_successes += 1
        funnel.total_records += len(records)
        counts["spls"] += 1

        for rec in records:
            # Skip non-human product types (veterinary, animal feed, etc.)
            if rec.product_type and "HUMAN" not in rec.product_type.upper():
                funnel.non_human_skipped += 1
                if cfg.write_qa_reports:
                    insert_qa_audit_record(
                        conn,
                        spl_setid=rec.setid,
                        product_name=rec.product_name,
                        dosage_form=rec.dosage_form,
                        route=rec.route,
                        form_class="",
                        route_class="",
                        exclusion_reason="non_human_product",
                        review_reason=None,
                        matched_terms="",
                        source_file=rec.source_file,
                        processed_at=utc_now_str(),
                    )
                continue

            # Funnel: form classification
            if rec.dosage_form:
                funnel.with_dosage_form += 1
            if rec.route:
                funnel.with_route += 1

            decision = make_filter_decision(rec.dosage_form, rec.route)

            # Track form class counts
            if decision.form_class == "strong":
                funnel.strong_liquid_form += 1
            elif decision.form_class == "ambiguous":
                funnel.ambiguous_form += 1
            elif decision.form_class == "excluded":
                funnel.excluded_form += 1
            else:  # non_liquid
                funnel.non_liquid_form += 1

            # Track route class counts
            if decision.route_class == "oral":
                funnel.allowed_route += 1
            elif decision.route_class == "excluded":
                funnel.excluded_route += 1
            else:  # blank
                funnel.blank_route += 1

            if not decision.should_process:
                counts["excluded"] += 1
                funnel.final_excluded += 1

                if cfg.write_excluded_debug:
                    row = build_output_row(
                        rec, decision, [], "excluded", None, "excluded"
                    )
                    insert_product(conn, row)
                    pending += 1

                if cfg.write_qa_reports:
                    if decision.form_class in ("excluded", "non_liquid"):
                        exclusion_reason = "excluded_form"
                    else:
                        exclusion_reason = "excluded_route"
                    insert_qa_audit_record(
                        conn,
                        spl_setid=rec.setid,
                        product_name=rec.product_name,
                        dosage_form=rec.dosage_form,
                        route=rec.route,
                        form_class=decision.form_class,
                        route_class=decision.route_class,
                        exclusion_reason=exclusion_reason,
                        review_reason=None,
                        matched_terms="",
                        source_file=rec.source_file,
                        processed_at=utc_now_str(),
                    )
                continue

            # Only do excipient matching if form/route pass
            if rec.inactive_ingredient_entries:
                funnel.with_inactive_ingredients += 1

            matched = match_excipients(rec.inactive_ingredient_entries)

            if matched:
                funnel.with_sugar_alcohol_hit += 1

            tier, review_reason = assign_concern_tier(decision, matched)

            # Broad recall: capture every record that passed form/route filter
            if cfg.broad_recall:
                broad_recall_rows.append({
                    "spl_setid": rec.setid,
                    "product_name": rec.product_name,
                    "labeler": rec.labeler or "",
                    "dosage_form": rec.dosage_form or "",
                    "route": rec.route or "",
                    "form_class": decision.form_class,
                    "route_class": decision.route_class,
                    "matched_sugar_alcohols": "; ".join(m.canonical_name for m in matched),
                    "matched_sugar_alcohol_terms": "; ".join(m.raw_name for m in matched),
                    "concern_tier": tier,
                    "in_strict_output": str(tier != "excluded"),
                    "source_file": rec.source_file,
                })

            if tier == "excluded":
                counts["excluded"] += 1
                funnel.final_excluded += 1

                if cfg.write_excluded_debug:
                    row = build_output_row(
                        rec, decision, matched, "excluded", None, "excluded"
                    )
                    insert_product(conn, row)
                    pending += 1

                if cfg.write_qa_reports:
                    insert_qa_audit_record(
                        conn,
                        spl_setid=rec.setid,
                        product_name=rec.product_name,
                        dosage_form=rec.dosage_form,
                        route=rec.route,
                        form_class=decision.form_class,
                        route_class=decision.route_class,
                        exclusion_reason="no_sugar_alcohol_match",
                        review_reason=None,
                        matched_terms="; ".join(m.raw_name for m in matched),
                        source_file=rec.source_file,
                        processed_at=utc_now_str(),
                    )
                continue

            inclusion = "included"
            counts[tier] = counts.get(tier, 0) + 1

            if tier == "high":
                funnel.final_high += 1
            elif tier == "moderate":
                funnel.final_moderate += 1
            elif tier == "review":
                funnel.final_review += 1

            row = build_output_row(rec, decision, matched, tier, review_reason, inclusion)
            insert_product(conn, row)
            if matched:
                insert_excipients(conn, rec.setid, matched)
            pending += 1

        if pending >= BATCH_SIZE:
            conn.commit()
            pending = 0

    if pending:
        conn.commit()

    return counts, funnel, broad_recall_rows, parse_failures


def run(cfg: Config) -> None:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    cfg.output_root.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(cfg.log_dir, debug=cfg.debug)
    logger.info("=" * 60)
    logger.info("excipient_finder pipeline starting")
    logger.info("  input_root      : %s", cfg.input_root)
    logger.info("  output_root     : %s", cfg.output_root)
    logger.info("  db_path         : %s", cfg.db_path)
    logger.info("  resume          : %s", cfg.resume)
    logger.info("  debug           : %s", cfg.debug)
    logger.info("  broad_recall    : %s", cfg.broad_recall)
    logger.info("  write_qa_reports: %s", cfg.write_qa_reports)
    logger.info("  write_qa_samples: %s", cfg.write_qa_samples)
    logger.info("=" * 60)

    conn = init_db(cfg.db_path)

    outer_zips = list(iter_outer_zips(cfg.input_root))
    logger.info("Found %d outer ZIP file(s) under %s", len(outer_zips), cfg.input_root)

    if cfg.limit:
        outer_zips = outer_zips[: cfg.limit]
        logger.info("Limiting to %d ZIP file(s)", cfg.limit)

    total_counts: dict[str, int] = {
        "high": 0, "moderate": 0, "review": 0, "excluded": 0,
        "spls": 0, "parse_errors": 0,
    }
    total_funnel = FunnelCounts()
    all_parse_failures: list[dict] = []
    all_broad_recall: list[dict] = []

    for i, zip_path in enumerate(outer_zips, 1):
        logger.info("[%d/%d] %s", i, len(outer_zips), zip_path.name)

        if cfg.resume and is_already_processed(conn, zip_path.name):
            logger.info("  -> skipping (already processed successfully)")
            continue

        log_file_start(conn, zip_path.name)
        try:
            counts, zip_funnel, broad_rows, zip_failures = process_outer_zip(
                zip_path, conn, cfg, logger
            )
        except KeyboardInterrupt:
            logger.warning("Interrupted by user. Partial results saved.")
            log_file_failure(conn, zip_path.name, "Interrupted by user")
            break
        except Exception as exc:
            logger.error("  FAILED: %s", exc, exc_info=True)
            log_file_failure(conn, zip_path.name, str(exc))
            continue

        log_file_success(conn, zip_path.name, counts)
        logger.info(
            "  -> SPLs=%d  high=%d  moderate=%d  review=%d  excluded=%d  parse_errors=%d",
            counts["spls"], counts["high"], counts["moderate"],
            counts["review"], counts["excluded"], counts["parse_errors"],
        )
        for k in total_counts:
            total_counts[k] += counts.get(k, 0)

        total_funnel.add(zip_funnel)
        all_parse_failures.extend(zip_failures)
        all_broad_recall.extend(broad_rows)

        # Flush parse failures to DB as we go (if QA reporting enabled)
        if cfg.write_qa_reports:
            for failure in zip_failures:
                insert_parse_failure_to_db(conn, failure)
            conn.commit()

    # -----------------------------------------------------------------------
    # Post-processing: QA reports, CSV exports, funnel summary
    # -----------------------------------------------------------------------

    # Always log and write funnel summary
    total_funnel.log_summary(logger)
    cfg.qa_dir.mkdir(parents=True, exist_ok=True)
    write_funnel_summary(total_funnel, cfg.qa_dir / "qa_funnel_summary.csv")
    write_funnel_to_db(conn, dataclasses.asdict(total_funnel))

    # Always write parse failures CSV
    write_parse_failures_csv(all_parse_failures, cfg.qa_dir / "qa_parse_failures.csv")

    # Always run static QA tests and log results
    logger.info("Running static QA tests...")
    qa_matcher_path = cfg.qa_dir / "qa_matcher_results.csv" if cfg.write_qa_reports else None
    qa_form_path = cfg.qa_dir / "qa_form_results.csv" if cfg.write_qa_reports else None
    qa_route_path = cfg.qa_dir / "qa_route_results.csv" if cfg.write_qa_reports else None

    matcher_ok = run_matcher_qa(logger, csv_path=qa_matcher_path)
    form_ok = run_form_qa(logger, csv_path=qa_form_path)
    route_ok = run_route_qa(logger, csv_path=qa_route_path)

    if not (matcher_ok and form_ok and route_ok):
        logger.warning("One or more static QA tests FAILED — review logs above.")

    # Extended QA reports (DB-backed summaries)
    if cfg.write_qa_reports:
        logger.info("Writing extended QA reports...")
        write_excipient_summary(
            conn, cfg.qa_dir / "qa_excipient_summary.csv", logger=logger
        )
        write_form_summary(
            conn, cfg.qa_dir / "qa_form_summary.csv", logger=logger
        )
        write_route_summary(
            conn, cfg.qa_dir / "qa_route_summary.csv", logger=logger
        )

    # QA samples
    if cfg.write_qa_samples:
        logger.info("Writing QA samples...")
        write_qa_samples(
            conn,
            cfg.qa_dir,
            sample_size=cfg.qa_sample_size,
            logger=logger,
        )

    # Broad recall output
    if cfg.broad_recall and all_broad_recall:
        broad_path = cfg.csv_dir / "broad_recall_products.csv"
        broad_path.parent.mkdir(parents=True, exist_ok=True)
        with open(broad_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_BROAD_RECALL_HEADER, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_broad_recall)
        logger.info(
            "  Broad recall: wrote %d rows -> %s",
            len(all_broad_recall), broad_path.name,
        )

    # Known-positive validation
    if cfg.known_positives_path:
        validate_known_positives(
            conn,
            cfg.known_positives_path,
            cfg.qa_dir / "qa_known_positives_validation.csv",
            logger,
        )

    # Final summary
    logger.info("=" * 60)
    logger.info("Pipeline complete.")
    logger.info("  Total SPLs     : %d", total_counts["spls"])
    logger.info("  HIGH           : %d", total_counts["high"])
    logger.info("  MODERATE       : %d", total_counts["moderate"])
    logger.info("  REVIEW         : %d", total_counts["review"])
    logger.info("  Excluded       : %d", total_counts["excluded"])
    logger.info("  Parse errors   : %d", total_counts["parse_errors"])

    logger.info("Writing CSV exports...")
    write_csvs(
        conn,
        cfg.csv_dir,
        write_excluded_debug=cfg.write_excluded_debug,
        logger=logger,
    )

    conn.close()
    logger.info("Done. DB at: %s", cfg.db_path)


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        prog="python -m excipient_finder.main",
        description="Ingest DailyMed SPL zip files and identify oral/enteral liquid "
                    "products containing sugar alcohol excipients.",
    )
    parser.add_argument(
        "--input-root",
        required=True,
        type=Path,
        help="Root directory containing DailyMed outer ZIP files.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Directory for DB, logs, and CSV output (default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N outer ZIP files (for testing).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    parser.add_argument(
        "--write-excluded-debug",
        action="store_true",
        help="Also write excluded records to the DB and excluded_products_debug.csv.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip outer ZIP files that are already logged as successfully processed.",
    )
    parser.add_argument(
        "--broad-recall",
        action="store_true",
        help="Write all form/route-passing records to broad_recall_products.csv "
             "regardless of sugar alcohol match.",
    )
    parser.add_argument(
        "--known-positives",
        type=Path,
        default=None,
        metavar="PATH",
        help="CSV of known-positive products to validate against the DB after processing.",
    )
    parser.add_argument(
        "--write-qa-samples",
        action="store_true",
        help="Write random QA samples per tier to the qa/ directory.",
    )
    parser.add_argument(
        "--write-qa-reports",
        action="store_true",
        help="Write extended QA reports (excipient/form/route summaries, audit tables) "
             "to the qa/ directory.",
    )
    parser.add_argument(
        "--qa-sample-size",
        type=int,
        default=25,
        metavar="N",
        help="Number of rows per tier for QA samples (default: 25).",
    )
    args = parser.parse_args()

    return Config(
        input_root=args.input_root,
        output_root=args.output_root,
        limit=args.limit,
        debug=args.debug,
        write_excluded_debug=args.write_excluded_debug,
        resume=args.resume,
        broad_recall=args.broad_recall,
        known_positives_path=args.known_positives,
        write_qa_samples=args.write_qa_samples,
        write_qa_reports=args.write_qa_reports,
        qa_sample_size=args.qa_sample_size,
    )


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)
