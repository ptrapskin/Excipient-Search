"""Service for fetching recent DailyMed Rx label updates filtered by Recent Major Changes."""

from __future__ import annotations

import asyncio
import difflib
import html as html_mod
import io
import logging
import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, timedelta

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)

_NS = "urn:hl7-org:v3"

# LOINC codes → human-readable section names
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

# Rx document code
_RX_DOC_CODE = "34391-3"

# Max concurrent ZIP downloads
_MAX_CONCURRENT = 5

# Max version>1 candidates collected from API
_MAX_CANDIDATES = 300

# Max records returned after filtering
_MAX_DISPLAYED = 100

# DailyMed base URL for ZIP downloads
_DAILYMED_WEB_BASE = "https://dailymed.nlm.nih.gov/dailymed"

# Match MM/YYYY dates in Recent Major Changes text
_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{4})\b")


@dataclass
class SectionDiff:
    section_name: str
    inline_html: str  # word-level inline diff with <del> and <ins>


@dataclass
class LabelChangeRecord:
    setid: str
    product_name: str
    current_version: int
    previous_version: int
    published_date: str
    dailymed_url: str
    # Rows from the Recent Major Changes table: [(section_name, date_string), ...]
    rmc_entries: list[tuple[str, str]] = field(default_factory=list)
    # Inline diffs for sections listed in rmc_entries only
    section_diffs: list[SectionDiff] = field(default_factory=list)
    fetch_error: str | None = None

    @property
    def has_changes(self) -> bool:
        return bool(self.section_diffs)


class LabelChangesService:
    def __init__(self, http_client: httpx.AsyncClient, settings: Settings) -> None:
        self._client = http_client
        self._settings = settings

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_recent_changes(self, days: int = 60) -> list[LabelChangeRecord]:
        """Return Rx labels whose Recent Major Changes section has entries within `days` days."""
        end = date.today()
        start = end - timedelta(days=days)
        candidates = await self._fetch_candidates(start, end)
        logger.info("Label changes: %d candidates, filtering by Recent Major Changes within %d days",
                    len(candidates), days)

        sem = asyncio.Semaphore(_MAX_CONCURRENT)
        tasks = [self._process_candidate(c, sem, days) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [r for r in results if r is not None][:_MAX_DISPLAYED]

    # ------------------------------------------------------------------
    # Candidate fetching
    # ------------------------------------------------------------------

    async def _fetch_candidates(self, start: date, end: date) -> list[dict]:
        """Page through DailyMed SPL list, collecting version>1 entries."""
        candidates: list[dict] = []
        page = 1

        while len(candidates) < _MAX_CANDIDATES:
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
                if len(candidates) >= _MAX_CANDIDATES:
                    break

            meta = data.get("metadata", {})
            if page >= meta.get("total_pages", 1):
                break
            page += 1

        return candidates

    # ------------------------------------------------------------------
    # Per-record processing (two-phase)
    # ------------------------------------------------------------------

    async def _process_candidate(
        self, spl: dict, sem: asyncio.Semaphore, lookback_days: int
    ) -> LabelChangeRecord | None:
        setid = spl["setid"]
        current_version = spl["spl_version"]
        published_date = spl.get("published_date", "")
        product_name = self._clean_title(spl.get("title", "Unknown"))
        dailymed_url = f"{_DAILYMED_WEB_BASE}/drugInfo.cfm?setid={setid}"

        async with sem:
            try:
                # Phase 1: current XML only — filter by Rx + recent RMC dates
                xml_current = await self._fetch_spl_xml(setid)

                if not self._is_rx_label(xml_current):
                    return None

                sections_current = self._extract_sections(xml_current)
                rmc_entries = self._extract_rmc_entries(sections_current)

                if not rmc_entries:
                    return None

                # Phase 2: fetch previous version and diff only RMC-listed sections
                previous_version = await self._get_previous_version(setid, current_version)
                if previous_version is None:
                    return None

                xml_previous = await self._fetch_spl_xml(setid, version=previous_version)
                sections_previous = self._extract_sections(xml_previous)

                rmc_section_names = {entry[0] for entry in rmc_entries}
                diffs = self._diff_sections(sections_previous, sections_current, rmc_section_names)

                return LabelChangeRecord(
                    setid=setid,
                    product_name=product_name,
                    current_version=current_version,
                    previous_version=previous_version,
                    published_date=published_date,
                    dailymed_url=dailymed_url,
                    rmc_entries=rmc_entries,
                    section_diffs=diffs,
                )

            except Exception as exc:
                logger.warning("Label changes: failed to process %s: %s", setid, exc)
                return None

    async def _get_previous_version(self, setid: str, current_version: int) -> int | None:
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
            return code_el is not None and code_el.get("code") == _RX_DOC_CODE
        except Exception:
            return False

    def _extract_sections(self, xml_str: str) -> dict[str, list[str]]:
        """Return {section_name: [text lines]} for all known LOINC sections."""
        try:
            root = ET.fromstring(xml_str)
        except Exception:
            return {}

        sections: dict[str, list[str]] = {}
        for section in root.iter(f"{{{_NS}}}section"):
            code_el = section.find(f"{{{_NS}}}code")
            if code_el is None:
                continue
            name = _SECTION_NAMES.get(code_el.get("code", ""))
            if name is None:
                continue
            texts = self._extract_text_from_section(section)
            if texts:
                if name in sections:
                    sections[name].extend(texts)
                else:
                    sections[name] = texts
        return sections

    def _extract_text_from_section(self, section_el: ET.Element) -> list[str]:
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

    def _extract_rmc_entries(self, sections: dict[str, list[str]]) -> list[tuple[str, str]]:
        """Parse Recent Major Changes table into [(section_name, date_string), ...].

        DailyMed stores the RMC table as alternating td cells: section name, then date.
        """
        lines = sections.get("Recent Major Changes", [])
        entries: list[tuple[str, str]] = []
        # Pair up consecutive lines: odd = section name, even = date
        i = 0
        while i + 1 < len(lines):
            section_name = lines[i].strip()
            date_str = lines[i + 1].strip()
            if _DATE_RE.search(date_str):
                entries.append((section_name, date_str))
                i += 2
            else:
                i += 1
        return entries

    # ------------------------------------------------------------------
    # Date filtering
    # ------------------------------------------------------------------

    def _has_recent_dates(
        self, rmc_entries: list[tuple[str, str]], lookback_days: int
    ) -> bool:
        cutoff = date.today() - timedelta(days=lookback_days)
        for _, date_str in rmc_entries:
            for m, y in _DATE_RE.findall(date_str):
                try:
                    if date(int(y), int(m), 1) >= cutoff:
                        return True
                except ValueError:
                    pass
        return False

    # ------------------------------------------------------------------
    # Diffing
    # ------------------------------------------------------------------

    def _diff_sections(
        self,
        old: dict[str, list[str]],
        new: dict[str, list[str]],
        rmc_section_names: set[str],
    ) -> list[SectionDiff]:
        """Compute inline diffs only for sections listed in Recent Major Changes."""
        diffs: list[SectionDiff] = []
        for name in new:
            if name == "Recent Major Changes":
                continue
            # Match if the section name appears inside an RMC entry
            # e.g. "Warnings and Precautions" matches "Warnings and Precautions (5.2, 5.3)"
            if not any(name in rmc_entry for rmc_entry in rmc_section_names):
                continue
            old_lines = old.get(name, [])
            new_lines = new.get(name, [])
            if old_lines == new_lines:
                continue
            inline_html = self._inline_diff(old_lines, new_lines)
            if inline_html:
                diffs.append(SectionDiff(section_name=name, inline_html=inline_html))
        return diffs

    def _inline_diff(self, old_lines: list[str], new_lines: list[str]) -> str:
        """Word-level inline diff HTML with <del> (removed) and <ins> (added) tags."""
        old_words = " ".join(old_lines).split()
        new_words = " ".join(new_lines).split()
        matcher = difflib.SequenceMatcher(None, old_words, new_words, autojunk=False)
        parts: list[str] = []
        for op, i1, i2, j1, j2 in matcher.get_opcodes():
            if op == "equal":
                parts.append(html_mod.escape(" ".join(old_words[i1:i2])))
            elif op == "delete":
                parts.append(f'<del>{html_mod.escape(" ".join(old_words[i1:i2]))}</del>')
            elif op == "insert":
                parts.append(f'<ins>{html_mod.escape(" ".join(new_words[j1:j2]))}</ins>')
            elif op == "replace":
                parts.append(
                    f'<del>{html_mod.escape(" ".join(old_words[i1:i2]))}</del>'
                    f'<ins>{html_mod.escape(" ".join(new_words[j1:j2]))}</ins>'
                )
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_title(title: str) -> str:
        if "[" in title:
            title = title[: title.rindex("[")].strip()
        return title.title()
