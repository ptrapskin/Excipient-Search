"""Service for fetching and diffing recent DailyMed Rx label updates."""

from __future__ import annotations

import asyncio
import difflib
import io
import logging
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, timedelta

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)

_NS = "urn:hl7-org:v3"

# LOINC codes → human-readable section names (Rx SPL sections)
_SECTION_NAMES: dict[str, str] = {
    "60561-8": "Recent Major Changes",
    "34066-1": "Boxed Warning",
    "34067-9": "Indications and Usage",
    "43678-2": "Dosage Forms and Strengths",
    "34068-7": "Dosage and Administration",
    "34070-3": "Contraindications",
    "34071-1": "Warnings and Precautions",
    "34072-9": "Precautions",
    "34073-7": "Drug Interactions",
    "34074-5": "Drug / Laboratory Test Interactions",
    "43684-0": "Use in Specific Populations",
    "34076-0": "Patient Counseling Information",
    "34083-6": "Adverse Reactions",
    "34084-4": "Drug Abuse and Dependence",
    "34085-1": "Overdosage",
    "34089-3": "Description",
    "34086-9": "Clinical Pharmacology",
    "43680-8": "Pharmacodynamics",
    "43682-4": "Pharmacokinetics",
    "34088-5": "Clinical Studies",
    "34069-5": "How Supplied / Storage and Handling",
    "42230-3": "Patient Counseling Information",
    "34093-5": "Patient Information",
}

# Rx document code — used to filter out OTC/cosmetic labels
_RX_DOC_CODE = "34391-3"

# Max concurrent ZIP download pairs
_MAX_CONCURRENT = 5

# How many version>1 candidates to process before stopping
_MAX_RECORDS = 50

# DailyMed base URL for ZIP downloads (different from API base)
_DAILYMED_WEB_BASE = "https://dailymed.nlm.nih.gov/dailymed"


@dataclass
class SectionDiff:
    section_name: str
    removed: list[str]
    added: list[str]


@dataclass
class LabelChangeRecord:
    setid: str
    product_name: str
    current_version: int
    previous_version: int
    published_date: str
    dailymed_url: str
    section_diffs: list[SectionDiff] = field(default_factory=list)
    fetch_error: str | None = None

    @property
    def has_changes(self) -> bool:
        return bool(self.section_diffs)

    @property
    def changed_section_names(self) -> list[str]:
        return [s.section_name for s in self.section_diffs]


class LabelChangesService:
    def __init__(self, http_client: httpx.AsyncClient, settings: Settings) -> None:
        self._client = http_client
        self._settings = settings

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_recent_changes(self, days: int = 7) -> list[LabelChangeRecord]:
        """Return up to _MAX_RECORDS Rx label updates from the last `days` days."""
        end = date.today()
        start = end - timedelta(days=days)
        candidates = await self._fetch_candidates(start, end)
        logger.info("Label changes: %d candidates found, processing up to %d", len(candidates), _MAX_RECORDS)

        sem = asyncio.Semaphore(_MAX_CONCURRENT)
        tasks = [self._process_candidate(c, sem) for c in candidates[:_MAX_RECORDS]]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Sort by published_date descending (already in order from API, but be explicit)
        return [r for r in results if r is not None]

    # ------------------------------------------------------------------
    # Candidate fetching
    # ------------------------------------------------------------------

    async def _fetch_candidates(self, start: date, end: date) -> list[dict]:
        """Page through DailyMed SPL list, collecting version>1 Rx label entries."""
        candidates: list[dict] = []
        page = 1

        while len(candidates) < _MAX_RECORDS:
            url = (
                f"{self._settings.dailymed_base_url}/spls.json"
                f"?published_date_start={start.strftime('%Y-%m-%d')}"
                f"&published_date_end={end.strftime('%Y-%m-%d')}"
                f"&pagesize=100&page={page}"
            )
            try:
                resp = await self._client.get(url, timeout=15)
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("Label changes list fetch failed (page %d): %s", page, exc)
                break

            data = resp.json()
            spls = data.get("data", [])
            if not spls:
                break

            for spl in spls:
                if spl.get("spl_version", 1) > 1:
                    candidates.append(spl)
                if len(candidates) >= _MAX_RECORDS:
                    break

            meta = data.get("metadata", {})
            if page >= meta.get("total_pages", 1):
                break
            page += 1

        return candidates

    # ------------------------------------------------------------------
    # Per-record processing
    # ------------------------------------------------------------------

    async def _process_candidate(self, spl: dict, sem: asyncio.Semaphore) -> LabelChangeRecord | None:
        setid = spl["setid"]
        current_version = spl["spl_version"]
        published_date = spl.get("published_date", "")
        product_name = self._clean_title(spl.get("title", "Unknown"))
        dailymed_url = f"{_DAILYMED_WEB_BASE}/drugInfo.cfm?setid={setid}"

        async with sem:
            try:
                previous_version = await self._get_previous_version(setid, current_version)
                if previous_version is None:
                    return None

                xml_current, xml_previous = await asyncio.gather(
                    self._fetch_spl_xml(setid),
                    self._fetch_spl_xml(setid, version=previous_version),
                )

                # Filter to Rx labels only
                if not self._is_rx_label(xml_current):
                    return None

                sections_current = self._extract_sections(xml_current)
                sections_previous = self._extract_sections(xml_previous)
                diffs = self._diff_sections(sections_previous, sections_current)

                return LabelChangeRecord(
                    setid=setid,
                    product_name=product_name,
                    current_version=current_version,
                    previous_version=previous_version,
                    published_date=published_date,
                    dailymed_url=dailymed_url,
                    section_diffs=diffs,
                )
            except Exception as exc:
                logger.warning("Label changes: failed to process %s: %s", setid, exc)
                return LabelChangeRecord(
                    setid=setid,
                    product_name=product_name,
                    current_version=current_version,
                    previous_version=0,
                    published_date=published_date,
                    dailymed_url=dailymed_url,
                    fetch_error=str(exc),
                )

    async def _get_previous_version(self, setid: str, current_version: int) -> int | None:
        """Return the version number immediately before current_version."""
        url = f"{self._settings.dailymed_base_url}/spls/{setid}/history.json"
        try:
            resp = await self._client.get(url, timeout=10)
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(f"history fetch failed: {exc}") from exc

        history = resp.json().get("data", {}).get("history", [])
        versions = sorted([h["spl_version"] for h in history], reverse=True)
        idx = next((i for i, v in enumerate(versions) if v == current_version), None)
        if idx is None or idx + 1 >= len(versions):
            return None
        return versions[idx + 1]

    async def _fetch_spl_xml(self, setid: str, version: int | None = None) -> str:
        """Download the ZIP for a setid (and optional version) and return the XML text."""
        url = f"{_DAILYMED_WEB_BASE}/getFile.cfm?setid={setid}&type=zip"
        if version is not None:
            url += f"&version={version}"
        try:
            resp = await self._client.get(url, timeout=30, follow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(f"ZIP fetch failed (version={version}): {exc}") from exc

        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                xml_name = next(n for n in zf.namelist() if n.endswith(".xml"))
                return zf.read(xml_name).decode("utf-8", errors="replace")
        except Exception as exc:
            raise RuntimeError(f"ZIP parse failed: {exc}") from exc

    # ------------------------------------------------------------------
    # XML parsing
    # ------------------------------------------------------------------

    def _is_rx_label(self, xml_str: str) -> bool:
        try:
            root = ET.fromstring(xml_str)
            code_el = root.find(f"{{{_NS}}}code")
            if code_el is None:
                return False
            return code_el.get("code") == _RX_DOC_CODE
        except Exception:
            return False

    def _extract_sections(self, xml_str: str) -> dict[str, list[str]]:
        """Return {section_name: [paragraph texts]} for all named sections."""
        try:
            root = ET.fromstring(xml_str)
        except Exception:
            return {}

        sections: dict[str, list[str]] = {}
        for section in root.iter(f"{{{_NS}}}section"):
            code_el = section.find(f"{{{_NS}}}code")
            if code_el is None:
                continue
            code = code_el.get("code", "")
            name = _SECTION_NAMES.get(code)
            if name is None:
                continue
            texts = self._extract_text_from_section(section)
            if texts:
                # If section seen twice, append (some labels repeat section codes)
                if name in sections:
                    sections[name].extend(texts)
                else:
                    sections[name] = texts
        return sections

    def _extract_text_from_section(self, section_el: ET.Element) -> list[str]:
        """Extract cleaned paragraph/item/td strings from a section element."""
        texts: list[str] = []
        for elem in section_el.iter():
            if elem.tag in (
                f"{{{_NS}}}paragraph",
                f"{{{_NS}}}item",
                f"{{{_NS}}}td",
            ):
                raw = "".join(elem.itertext())
                cleaned = " ".join(raw.split())
                if cleaned:
                    texts.append(cleaned)
        return texts

    # ------------------------------------------------------------------
    # Diffing
    # ------------------------------------------------------------------

    def _diff_sections(
        self,
        old: dict[str, list[str]],
        new: dict[str, list[str]],
    ) -> list[SectionDiff]:
        diffs: list[SectionDiff] = []
        # Preserve section order: Recent Major Changes first, then others
        priority = {"Recent Major Changes"}
        all_names = list(priority & (set(old) | set(new))) + [
            n for n in list(old) + [n for n in new if n not in old]
            if n not in priority
        ]
        seen: set[str] = set()
        for name in all_names:
            if name in seen:
                continue
            seen.add(name)
            old_lines = old.get(name, [])
            new_lines = new.get(name, [])
            if old_lines == new_lines:
                continue
            diff = list(difflib.ndiff(old_lines, new_lines))
            removed = [line[2:] for line in diff if line.startswith("- ")]
            added = [line[2:] for line in diff if line.startswith("+ ")]
            if removed or added:
                diffs.append(SectionDiff(
                    section_name=name,
                    removed=removed,
                    added=added,
                ))
        return diffs

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_title(title: str) -> str:
        """Strip labeler bracket suffix and apply title case."""
        if "[" in title:
            title = title[: title.rindex("[")].strip()
        # Title case but preserve common all-caps drug names reasonably
        return title.title()
