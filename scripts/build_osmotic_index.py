#!/usr/bin/env python3
"""Build the osmotic diarrhea risk index from local DailyMed SPL zip files.

Reads every SPL XML, applies the osmotic_filter pipeline, and writes:
  - app/data/osmotic_risk_index.json   — web-app index (HIGH + MODERATE + REVIEW)
  - app/data/osmotic/high_concern.csv
  - app/data/osmotic/moderate_concern.csv
  - app/data/osmotic/review.csv
  - app/data/osmotic/excluded_debug.csv  (only with --debug)

Usage
-----
    python scripts/build_osmotic_index.py
    python scripts/build_osmotic_index.py --zips D:/DailyMed/spl_zips
    python scripts/build_osmotic_index.py --debug        # also writes excluded_debug.csv

Expected runtime: 30–90 minutes depending on machine and number of zip files.
Run monthly after downloading updated zips from DailyMed.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path so app modules are importable.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from app.repositories.dailymed_api import build_all_product_details_from_xml
from app.services import osmotic_filter
from app.services.osmotic_filter import FilterDecision, SugarAlcoholMatch
from app.services.osmotic_risk_service import (
    OsmoticProduct,
    _build_strength,
    _group_products,
    _primary_active_ingredient,
)
from app.services.parsing_service import ParsingService

_DEFAULT_ZIPS = Path.home() / ".excipient_finder" / "spl_zips"
_DEFAULT_OUT = _PROJECT_ROOT / "app" / "data" / "osmotic_risk_index.json"
_DEFAULT_CSV_DIR = _PROJECT_ROOT / "app" / "data" / "osmotic"

# ---------------------------------------------------------------------------
# Build-time record (richer than OsmoticProduct; used only for CSV output)
# ---------------------------------------------------------------------------

@dataclass
class _BuildRecord:
    """One filtered SPL product record with full audit trail."""

    # Identity
    product_name: str
    setid: str
    labeler: str | None
    dosage_form: str | None
    route: str | None
    active_ingredient: str
    strength: str | None
    ndcs: list[str]

    # Classification
    form_class: str
    route_class: str
    included_form_match: str | None
    excluded_form_match: str | None
    included_route_match: str | None
    excluded_route_match: str | None

    # Sugar alcohol evidence
    matched_sugar_alcohols: list[str]        # canonical names
    matched_sugar_alcohol_terms: list[str]   # actual alias text matched

    # Tier
    concern_tier: str
    review_reason: str | None = None

    def to_osmotic_product(self) -> OsmoticProduct:
        return OsmoticProduct(
            product_name=self.product_name,
            setid=self.setid,
            active_ingredient=self.active_ingredient,
            ndcs=self.ndcs,
            labeler=self.labeler,
            dosage_form=self.dosage_form,
            route=self.route,
            strength=self.strength,
            sugar_alcohols_found=self.matched_sugar_alcohols,
            concern_tier=self.concern_tier,
        )


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

_CSV_FIELDNAMES = [
    "product_name", "setid", "labeler", "dosage_form", "route",
    "active_ingredient", "strength", "ndcs",
    "form_class", "route_class",
    "included_form_match", "excluded_form_match",
    "included_route_match", "excluded_route_match",
    "matched_sugar_alcohols", "matched_sugar_alcohol_terms",
    "concern_tier", "review_reason",
]


def _record_to_row(r: _BuildRecord) -> dict:
    return {
        "product_name": r.product_name,
        "setid": r.setid,
        "labeler": r.labeler or "",
        "dosage_form": r.dosage_form or "",
        "route": r.route or "",
        "active_ingredient": r.active_ingredient,
        "strength": r.strength or "",
        "ndcs": "; ".join(r.ndcs),
        "form_class": r.form_class,
        "route_class": r.route_class,
        "included_form_match": r.included_form_match or "",
        "excluded_form_match": r.excluded_form_match or "",
        "included_route_match": r.included_route_match or "",
        "excluded_route_match": r.excluded_route_match or "",
        "matched_sugar_alcohols": ", ".join(r.matched_sugar_alcohols),
        "matched_sugar_alcohol_terms": ", ".join(r.matched_sugar_alcohol_terms),
        "concern_tier": r.concern_tier,
        "review_reason": r.review_reason or "",
    }


def _write_csv(path: Path, records: list[_BuildRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        for r in records:
            writer.writerow(_record_to_row(r))


# ---------------------------------------------------------------------------
# Inner zip parsing helpers
# ---------------------------------------------------------------------------

def _inner_zip_entries(zf: zipfile.ZipFile) -> list[str]:
    """Return names of inner .zip entries (each wraps one SPL XML)."""
    return [name for name in zf.namelist() if name.endswith(".zip")]


def _read_xml_from_inner_zip(outer_zf: zipfile.ZipFile, inner_entry: str) -> tuple[str, str]:
    """Open an inner SPL zip and return (setid, xml_text).

    Raises ValueError if no XML is found inside.
    """
    inner_bytes = outer_zf.read(inner_entry)
    with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zf:
        xml_names = [
            n for n in inner_zf.namelist()
            if n.endswith(".xml") and not n.endswith("_indexingInstructions.xml")
        ]
        if not xml_names:
            raise ValueError("No XML found")
        xml_bytes = inner_zf.read(xml_names[0])
        xml_text = xml_bytes.decode("utf-8", errors="replace")
        setid = xml_names[0].rsplit("/", 1)[-1][:-4]
        return setid, xml_text


# ---------------------------------------------------------------------------
# Per-zip processing
# ---------------------------------------------------------------------------

def process_zip(
    zip_path: Path,
    parsing_service: ParsingService,
    *,
    include_excluded: bool = False,
) -> tuple[list[_BuildRecord], list[_BuildRecord], int]:
    """Parse every SPL in one zip.

    Returns:
        (included_records, excluded_records, total_spl_count)

    included_records: HIGH, MODERATE, and REVIEW tier products.
    excluded_records: EXCLUDED tier products (only populated when include_excluded=True).
    """
    included: list[_BuildRecord] = []
    excluded: list[_BuildRecord] = []
    seen_setids: set[str] = set()
    spl_count = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        entries = _inner_zip_entries(zf)
        total = len(entries)

        for i, entry in enumerate(entries, 1):
            if i % 1000 == 0 or i == total:
                print(
                    f"\r    {i:>6,} / {total:,} SPLs  "
                    f"({len(included)} included)",
                    end="", flush=True,
                )

            try:
                setid, xml_text = _read_xml_from_inner_zip(zf, entry)
            except Exception:
                continue

            if setid in seen_setids:
                continue
            seen_setids.add(setid)
            spl_count += 1

            try:
                details = build_all_product_details_from_xml(
                    xml_text=xml_text,
                    setid=setid,
                    base_url="local-zip",
                    parsing_service=parsing_service,
                )
            except Exception:
                continue

            for detail in details:
                if detail.product_type and "HUMAN" not in detail.product_type.upper():
                    continue

                decision: FilterDecision = osmotic_filter.evaluate(
                    detail.dosage_form,
                    detail.route,
                    detail.inactive_ingredients,
                )

                record = _BuildRecord(
                    product_name=detail.product_name,
                    setid=setid,
                    labeler=detail.labeler,
                    dosage_form=detail.dosage_form,
                    route=detail.route,
                    active_ingredient=_primary_active_ingredient(detail.active_ingredients),
                    strength=_build_strength(detail.active_ingredients),
                    ndcs=detail.ndcs,
                    form_class=decision.form_class,
                    route_class=decision.route_class,
                    included_form_match=decision.included_form_match,
                    excluded_form_match=decision.excluded_form_match,
                    included_route_match=decision.included_route_match,
                    excluded_route_match=decision.excluded_route_match,
                    matched_sugar_alcohols=[
                        sa.canonical_name for sa in decision.matched_sugar_alcohols
                    ],
                    matched_sugar_alcohol_terms=[
                        sa.matched_term for sa in decision.matched_sugar_alcohols
                    ],
                    concern_tier=decision.concern_tier,
                    review_reason=decision.review_reason,
                )

                if decision.concern_tier != "excluded":
                    included.append(record)
                elif include_excluded:
                    excluded.append(record)

    print()  # newline after progress line
    return included, excluded, spl_count


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build osmotic risk index from local DailyMed SPL zip files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--zips",
        type=Path,
        default=_DEFAULT_ZIPS,
        metavar="PATH",
        help=f"Directory containing downloaded zip files (default: {_DEFAULT_ZIPS})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=_DEFAULT_OUT,
        metavar="PATH",
        help=f"Output JSON path (default: {_DEFAULT_OUT})",
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=_DEFAULT_CSV_DIR,
        metavar="PATH",
        help=f"Directory for CSV outputs (default: {_DEFAULT_CSV_DIR})",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Also write excluded_debug.csv (large file — for pipeline audit)",
    )
    args = parser.parse_args()

    zip_files = sorted(args.zips.glob("dm_spl_release_human_*.zip"))
    if not zip_files:
        print(f"No zip files found in: {args.zips}")
        print("Run scripts/download_spl_zips.py first.")
        sys.exit(1)

    print(f"Zip files   : {len(zip_files)}")
    for zf in zip_files:
        print(f"  {zf.name}  ({zf.stat().st_size / 1e9:.1f} GB)")
    print(f"JSON output : {args.out}")
    print(f"CSV output  : {args.csv_dir}/")
    print()

    parsing_service = ParsingService()
    all_included: list[_BuildRecord] = []
    all_excluded: list[_BuildRecord] = []
    spl_source_names: list[str] = []
    total_spl_count = 0

    for zip_path in zip_files:
        print(f"Processing: {zip_path.name}")
        try:
            included, excluded, spl_count = process_zip(
                zip_path, parsing_service, include_excluded=args.debug
            )
            all_included.extend(included)
            all_excluded.extend(excluded)
            spl_source_names.append(zip_path.name)
            total_spl_count += spl_count
            print(f"  -> {len(included)} included (running total: {len(all_included)})")
        except KeyboardInterrupt:
            print("\nInterrupted — saving partial results.")
            break
        except Exception as exc:
            print(f"  ERROR processing {zip_path.name}: {exc}")

    # Tier breakdown
    high    = [r for r in all_included if r.concern_tier == "high"]
    moderate = [r for r in all_included if r.concern_tier == "moderate"]
    review  = [r for r in all_included if r.concern_tier == "review"]

    print(f"\nTier breakdown across {total_spl_count:,} SPLs processed:")
    print(f"  HIGH     : {len(high):,}")
    print(f"  MODERATE : {len(moderate):,}")
    print(f"  REVIEW   : {len(review):,}")
    print(f"  Total    : {len(all_included):,}")

    # --- Write CSVs ---
    print(f"\nWriting CSVs to {args.csv_dir}/")
    _write_csv(args.csv_dir / "high_concern.csv", high)
    _write_csv(args.csv_dir / "moderate_concern.csv", moderate)
    _write_csv(args.csv_dir / "review.csv", review)
    if args.debug and all_excluded:
        _write_csv(args.csv_dir / "excluded_debug.csv", all_excluded)
        print(f"  excluded_debug.csv  : {len(all_excluded):,} records")

    # --- Build JSON index for web app (all non-excluded tiers) ---
    print(f"\nGrouping {len(all_included)} products by active ingredient...")
    osmotic_products = [r.to_osmotic_product() for r in all_included]
    groups, total = _group_products(osmotic_products)
    print(f"  -> {len(groups)} active ingredient groups, {total} product variants")

    index = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source_files": spl_source_names,
        "spl_count": total_spl_count,
        "total": total,
        "tier_counts": {
            "high": len(high),
            "moderate": len(moderate),
            "review": len(review),
        },
        "groups": [
            {
                "ingredient_name": g.ingredient_name,
                "products": [asdict(p) for p in g.products],
            }
            for g in groups
        ],
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(index, indent=2), encoding="utf-8")
    print(f"\nJSON index saved to: {args.out}")
    print("Restart the web app to pick up the new index.")


if __name__ == "__main__":
    main()
