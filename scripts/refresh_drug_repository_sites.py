#!/usr/bin/env python3
"""Refresh the Wisconsin Drug Repository Program site directory.

Scrapes the participant list embedded in the DHS consumer guide page,
geocodes each address with OpenStreetMap Nominatim, and writes:
  - app/data/drug_repository_sites.json

Usage
-----
    python scripts/refresh_drug_repository_sites.py

Respects Nominatim's usage policy (max 1 request/sec, identifying
User-Agent). Expected runtime: ~2-3 minutes for ~110 sites.
Re-run periodically (e.g. quarterly) since DHS updates the directory
without notice.
"""

from __future__ import annotations

import html
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SOURCE_URL = "https://www.dhs.wisconsin.gov/guide/cancer-drugrepo.htm"
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_USER_AGENT = (
    "ExcipientFinder-DrugRepositoryFinder/1.0 (contact: ptrapskinrph@gmail.com)"
)
_OUTPUT_PATH = _PROJECT_ROOT / "app" / "data" / "drug_repository_sites.json"


def fetch_source_html() -> str:
    req = urllib.request.Request(_SOURCE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def extract_section(page_html: str, section_id: str) -> str:
    match = re.search(rf'id="{section_id}">(.*?)</div>', page_html, re.S)
    return match.group(1) if match else ""


def parse_orgs(section_html: str, participation: str) -> list[dict]:
    orgs = []
    for match in re.finditer(
        r'<li data-list-item-id="[^"]*">([^<]+)<ul>(.*?)</ul></li>', section_html, re.S
    ):
        name = html.unescape(match.group(1)).strip()
        inner = match.group(2)
        phone = re.search(r"Phone:\s*([^<]+)", inner)
        addr = re.search(r"Address:\s*([^<]+)", inner)
        contact = re.search(r"Contact person:\s*([^<]+)", inner)
        if not addr:
            continue
        orgs.append(
            {
                "name": name,
                "phone": html.unescape(phone.group(1)).strip() if phone else None,
                "address": html.unescape(addr.group(1)).strip(),
                "contact": html.unescape(contact.group(1)).strip() if contact else None,
                "participation": participation,
            }
        )
    return orgs


_SUITE_PATTERN = re.compile(r"^(suite|ste|room|rm|floor|fl|unit|#)\b", re.IGNORECASE)
_INLINE_SUITE_PATTERN = re.compile(
    r"\s*[,\-]?\s*(suite|ste|room|rm|floor|fl|unit)\.?\s*[\w-]+\s*$", re.IGNORECASE
)


def _strip_suite(address: str) -> str | None:
    """Drop a trailing suite/room/floor/unit qualifier from an address line.

    Nominatim's geocoder often can't resolve suite-level detail, so falling
    back to the building-level address recovers many hits.
    """

    parts = [p.strip() for p in address.split(",")]
    for i in range(len(parts) - 1, -1, -1):
        if _SUITE_PATTERN.match(parts[i]):
            cleaned = ", ".join(parts[:i] + parts[i + 1 :])
            if cleaned != address:
                return cleaned
        inline_cleaned = _INLINE_SUITE_PATTERN.sub("", parts[i]).strip()
        if inline_cleaned and inline_cleaned != parts[i]:
            cleaned_parts = parts[:i] + [inline_cleaned] + parts[i + 1 :]
            cleaned = ", ".join(cleaned_parts)
            if cleaned != address:
                return cleaned
    return None


def _city_state_zip(address: str) -> str | None:
    """Fall back to a city/state/zip-only query as a last resort (approximate)."""

    parts = [p.strip() for p in address.split(",")]
    if len(parts) < 2:
        return None
    tail = ", ".join(parts[-2:])
    return tail if tail != address else None


def _geocode_once(address: str) -> tuple[float, float] | None:
    query = urllib.parse.urlencode({"q": address, "format": "json", "limit": 1, "countrycodes": "us"})
    req = urllib.request.Request(f"{_NOMINATIM_URL}?{query}", headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            results = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"  geocode error for {address!r}: {exc}", file=sys.stderr)
        return None
    if not results:
        return None
    return float(results[0]["lat"]), float(results[0]["lon"])


def geocode(address: str) -> tuple[float, float] | None:
    coords = _geocode_once(address)
    if coords is not None:
        return coords

    stripped = _strip_suite(address)
    if stripped is not None:
        time.sleep(1)
        print(f"  retrying without suite/room detail: {stripped!r}")
        coords = _geocode_once(stripped)
        if coords is not None:
            return coords

    fallback = _city_state_zip(address)
    if fallback is not None:
        time.sleep(1)
        print(f"  retrying with city/state/zip only (approximate): {fallback!r}")
        coords = _geocode_once(fallback)
        if coords is not None:
            return coords

    return None


def main() -> None:
    print(f"Fetching {_SOURCE_URL} ...")
    page_html = fetch_source_html()

    full_section = extract_section(page_html, "a1")
    partial_section = extract_section(page_html, "a2")
    orgs = parse_orgs(full_section, "full") + parse_orgs(partial_section, "partial")
    if not orgs:
        print("No participant entries found — DHS page structure may have changed.", file=sys.stderr)
        sys.exit(1)

    seen: set[tuple[str, str]] = set()
    deduped = []
    for org in orgs:
        key = (org["name"], org["address"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(org)
    orgs = deduped
    print(f"Parsed {len(orgs)} unique participant sites.")

    existing_by_address: dict[str, dict] = {}
    if _OUTPUT_PATH.exists():
        for site in json.loads(_OUTPUT_PATH.read_text(encoding="utf-8")):
            if site.get("lat") is not None:
                existing_by_address[site["address"]] = site

    sites = []
    skipped = []
    for i, org in enumerate(orgs, start=1):
        cached = existing_by_address.get(org["address"])
        if cached:
            org["lat"], org["lon"] = cached["lat"], cached["lon"]
            print(f"[{i}/{len(orgs)}] (cached) {org['name']}")
        else:
            coords = geocode(org["address"])
            if coords is None:
                print(f"[{i}/{len(orgs)}] FAILED to geocode: {org['name']} — {org['address']}")
                skipped.append(org)
                time.sleep(1)
                continue
            org["lat"], org["lon"] = coords
            print(f"[{i}/{len(orgs)}] {org['name']} -> {coords}")
            time.sleep(1)  # Nominatim usage policy: max 1 req/sec
        sites.append(org)

    sites.sort(key=lambda s: s["name"])
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(json.dumps(sites, indent=2), encoding="utf-8")
    print(f"\nWrote {len(sites)} sites to {_OUTPUT_PATH}")
    if skipped:
        print(f"WARNING: {len(skipped)} sites could not be geocoded and were omitted:")
        for org in skipped:
            print(f"  - {org['name']}: {org['address']}")


if __name__ == "__main__":
    main()
