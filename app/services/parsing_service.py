"""Parsing helpers for DailyMed ingredient extraction."""

from __future__ import annotations

import re

from app.domain.models import IngredientEntry

UNII_PATTERN = re.compile(r"\(UNII:\s*([A-Z0-9]+)\)", re.IGNORECASE)
TRAILING_STRENGTH_PATTERN = re.compile(
    r"\s+\d+(?:\.\d+)?\s*(?:mg|mcg|g|kg|mL|ml|L|%)\b.*$",
    re.IGNORECASE,
)
STRENGTH_PATTERN = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:mg|mcg|g|kg|mL|ml|L|%)"
    r"(?:\s*/\s*\d+(?:\.\d+)?\s*(?:mg|mcg|g|kg|mL|ml|L|%))?\b",
    re.IGNORECASE,
)
SPLIT_PATTERN = re.compile(r"[;\n]+")
COMMA_SPLIT_PATTERN = re.compile(r",(?![^()]*\))")

HEADER_LINES = {
    "ingredient name",
    "strength",
    "inactive ingredients",
    "inactive ingredient",
    "active ingredients",
    "active ingredient",
    "basis of strength",
    "contains",
}

LEADING_LABEL_PATTERN = re.compile(
    r"^(?:inactive ingredients?|active ingredients?|contains)\s*:\s*",
    re.IGNORECASE,
)


class ParsingService:
    """Convert raw DailyMed text into structured ingredient records."""

    def parse_inactive_ingredients(self, raw_text: str | None) -> list[IngredientEntry]:
        """Parse a raw inactive ingredient section into narrative entries."""

        return self.parse_narrative_ingredients(
            raw_text,
            source_type="narrative_text",
            role="inactive",
        )

    def parse_narrative_ingredients(
        self,
        raw_text: str | None,
        source_type: str,
        role: str,
    ) -> list[IngredientEntry]:
        """Parse narrative ingredient text into entries with provenance."""

        entries: list[IngredientEntry] = []
        seen: set[str] = set()
        for chunk in self._split_candidates(raw_text):
            entry = self.build_entry(chunk, source_type=source_type, role=role)
            if entry is None:
                continue
            key = f"{entry.display_name or entry.raw_name}|{entry.unii or ''}|{entry.role or ''}"
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)
        return entries

    def parse_structured_ingredients(
        self,
        values: list[str],
        source_type: str,
        role: str,
    ) -> list[IngredientEntry]:
        """Parse structured XML fragments into ingredient entries."""

        entries: list[IngredientEntry] = []
        seen: set[str] = set()
        for value in values:
            entry = self.build_entry(value, source_type=source_type, role=role)
            if entry is None:
                continue
            key = f"{entry.display_name or entry.raw_name}|{entry.unii or ''}|{entry.role or ''}"
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)
        return entries

    def build_entry(
        self,
        raw_value: str,
        source_type: str,
        role: str,
        *,
        unii_override: str | None = None,
        strength_override: str | None = None,
        raw_name_override: str | None = None,
        display_name_override: str | None = None,
    ) -> IngredientEntry | None:
        """Create a structured entry from a value and explicit provenance."""

        raw_name = self._collapse_text(raw_name_override or raw_value)
        display_name = display_name_override or self._cleanup_name(raw_value)
        if not display_name:
            return None

        unii_match = UNII_PATTERN.search(raw_value)
        unii = (unii_override or (unii_match.group(1) if unii_match else None))
        strength_match = STRENGTH_PATTERN.search(raw_value)
        strength = strength_override or (strength_match.group(0) if strength_match else None)
        if unii:
            unii = unii.upper()

        if source_type == "table":
            confidence = "high"
        elif source_type == "xml_structured":
            confidence = "medium"
        else:
            confidence = "low"
        normalized_name = self._normalize_display_name(display_name)

        return IngredientEntry(
            raw_name=raw_name,
            display_name=display_name,
            normalized_name=normalized_name,
            unii=unii,
            strength=strength,
            role=role,
            source_type=source_type,
            confidence=confidence,
        )

    def extract_named_entries(self, raw_text: str | None) -> list[str]:
        """Return cleaned ingredient-like names from a raw section."""

        names: list[str] = []
        seen: set[str] = set()
        for chunk in self._split_candidates(raw_text):
            name = self._cleanup_name(chunk)
            if not name:
                continue
            lowered = name.casefold()
            if lowered in seen:
                continue
            seen.add(lowered)
            names.append(name)
        return names

    def _split_candidates(self, raw_text: str | None) -> list[str]:
        """Split raw section text into likely ingredient candidates."""

        if not raw_text:
            return []

        normalized = raw_text.replace("\r", "\n").replace("\t", "\n")
        initial_chunks = [part.strip() for part in SPLIT_PATTERN.split(normalized) if part.strip()]

        chunks: list[str] = []
        for chunk in initial_chunks:
            comma_parts = [part.strip() for part in COMMA_SPLIT_PATTERN.split(chunk) if part.strip()]
            if len(comma_parts) > 1 and "unii" not in chunk.casefold():
                chunks.extend(comma_parts)
            else:
                chunks.append(chunk)
        return chunks

    def _cleanup_name(self, value: str) -> str | None:
        """Remove headers and trailing strength text from a candidate."""

        collapsed = self._collapse_text(value).strip(" ,;:")
        if not collapsed:
            return None

        collapsed = LEADING_LABEL_PATTERN.sub("", collapsed).strip(" ,;:")
        if collapsed.casefold() in HEADER_LINES:
            return None

        without_strength = TRAILING_STRENGTH_PATTERN.sub("", collapsed).strip(" ,;:")
        without_unii = UNII_PATTERN.sub("", without_strength)
        cleaned = re.sub(r"\s+", " ", without_unii).strip(" ,;:")
        if not cleaned or cleaned.casefold() in HEADER_LINES:
            return None
        if cleaned.isdigit():
            return None
        return cleaned

    def build_table_entry(
        self,
        ingredient_value: str,
        *,
        raw_row_text: str,
        role: str,
        unii: str | None = None,
        strength: str | None = None,
    ) -> IngredientEntry | None:
        """Build a high-confidence ingredient entry from a structured table row."""

        return self.build_entry(
            ingredient_value,
            source_type="table",
            role=role,
            unii_override=unii,
            strength_override=strength,
            raw_name_override=raw_row_text,
        )

    def _collapse_text(self, value: str) -> str:
        """Collapse repeated whitespace while preserving the original token order."""

        return re.sub(r"\s+", " ", value).strip()

    def _normalize_display_name(self, raw_name: str) -> str:
        """Lightweight normalization that preserves meaningful casing."""

        if raw_name.islower():
            return raw_name.title()
        return raw_name
