"""Iterate nested DailyMed ZIP archives without permanent extraction.

DailyMed bulk download structure:
    outer.zip                          (e.g. dm_spl_release_human_rx_part1.zip)
        prescription/DATE_SETID.zip   (inner zip, one per SPL)
            SETID.xml                  (SPL XML label)
            optional images

Strategy:
  - Open each outer zip with zipfile.ZipFile.
  - Iterate inner zip entries (any namelist() entry ending in .zip).
  - Open each inner zip from memory via io.BytesIO — no disk extraction.
  - Yield (setid, xml_text, inner_entry) for each XML found inside.
  - Log and skip malformed inner zips or XMLs.
  - Optionally collect parse failures into a caller-supplied list.
"""
from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from typing import Iterator

from .utils import utc_now_str


def iter_outer_zips(input_root: Path) -> Iterator[Path]:
    """Recursively find all *.zip files under input_root, sorted."""
    yield from sorted(input_root.rglob("*.zip"))


def iter_spl_xmls(
    outer_zip_path: Path,
    logger: logging.Logger,
    parse_failures: list | None = None,
) -> Iterator[tuple[str, str, str]]:
    """Open an outer DailyMed zip and yield (setid, xml_text, inner_entry) for each inner SPL.

    Skips and logs any inner zip that cannot be opened or contains no XML.
    The outer zip is never fully extracted to disk.

    If *parse_failures* is provided (a list), failure dicts are appended to it
    for every inner entry that cannot be read or opened.
    """
    try:
        outer_zf = zipfile.ZipFile(outer_zip_path, "r")
    except Exception as exc:
        logger.error("Cannot open outer zip %s: %s", outer_zip_path.name, exc)
        return

    with outer_zf:
        inner_entries = [n for n in outer_zf.namelist() if n.endswith(".zip")]
        logger.debug("  %d inner zips in %s", len(inner_entries), outer_zip_path.name)

        for inner_entry in inner_entries:
            try:
                inner_bytes = outer_zf.read(inner_entry)
            except Exception as exc:
                logger.warning("  Cannot read inner entry %s: %s", inner_entry, exc)
                if parse_failures is not None:
                    parse_failures.append({
                        "source_file": outer_zip_path.name,
                        "xml_member_name": inner_entry,
                        "error_type": "read_error",
                        "error_message": str(exc),
                        "processed_at": utc_now_str(),
                    })
                continue

            try:
                inner_zf = zipfile.ZipFile(io.BytesIO(inner_bytes))
            except Exception as exc:
                logger.warning("  Cannot open inner zip %s: %s", inner_entry, exc)
                if parse_failures is not None:
                    parse_failures.append({
                        "source_file": outer_zip_path.name,
                        "xml_member_name": inner_entry,
                        "error_type": "xml_error",
                        "error_message": str(exc),
                        "processed_at": utc_now_str(),
                    })
                continue

            with inner_zf:
                xml_names = [
                    n for n in inner_zf.namelist()
                    if n.endswith(".xml")
                    and not n.endswith("_indexingInstructions.xml")
                ]
                if not xml_names:
                    logger.debug("  No XML in inner zip %s", inner_entry)
                    continue

                for xml_name in xml_names:
                    try:
                        xml_bytes = inner_zf.read(xml_name)
                        xml_text = xml_bytes.decode("utf-8", errors="replace")
                    except Exception as exc:
                        logger.warning(
                            "  Cannot read XML %s in %s: %s",
                            xml_name, inner_entry, exc,
                        )
                        if parse_failures is not None:
                            parse_failures.append({
                                "source_file": outer_zip_path.name,
                                "xml_member_name": inner_entry,
                                "error_type": "read_error",
                                "error_message": str(exc),
                                "processed_at": utc_now_str(),
                            })
                        continue

                    # setid = XML filename without extension
                    setid = xml_name.rsplit("/", 1)[-1]
                    if setid.endswith(".xml"):
                        setid = setid[:-4]

                    yield setid, xml_text, inner_entry
