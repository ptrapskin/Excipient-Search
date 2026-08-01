"""Find Wisconsin Drug Repository Program sites near a given address."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import httpx

_SITES_PATH = Path(__file__).resolve().parent.parent / "data" / "drug_repository_sites.json"
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_GEOCODER_USER_AGENT = "ExcipientFinder-DrugRepositoryFinder/1.0 (contact: ptrapskinrph@gmail.com)"

_EARTH_RADIUS_MILES = 3958.8


class AddressNotFoundError(Exception):
    """Raised when the user-supplied address cannot be geocoded."""


@dataclass(frozen=True)
class RepositorySite:
    name: str
    phone: str | None
    address: str
    contact: str | None
    participation: str
    lat: float
    lon: float


@dataclass(frozen=True)
class RepositorySiteResult:
    site: RepositorySite
    distance_miles: float


@lru_cache(maxsize=1)
def _load_sites() -> list[RepositorySite]:
    raw = json.loads(_SITES_PATH.read_text(encoding="utf-8"))
    return [
        RepositorySite(
            name=entry["name"],
            phone=entry.get("phone"),
            address=entry["address"],
            contact=entry.get("contact"),
            participation=entry.get("participation", "full"),
            lat=entry["lat"],
            lon=entry["lon"],
        )
        for entry in raw
        if entry.get("lat") is not None and entry.get("lon") is not None
    ]


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * _EARTH_RADIUS_MILES * math.asin(math.sqrt(a))


async def geocode_address(http_client: httpx.AsyncClient, address: str) -> tuple[float, float]:
    """Geocode a free-form address via OpenStreetMap Nominatim."""

    response = await http_client.get(
        _NOMINATIM_URL,
        params={"q": address, "format": "json", "limit": 1, "countrycodes": "us"},
        headers={"User-Agent": _GEOCODER_USER_AGENT},
    )
    response.raise_for_status()
    results = response.json()
    if not results:
        raise AddressNotFoundError(f"Could not find a location for '{address}'.")
    return float(results[0]["lat"]), float(results[0]["lon"])


async def find_nearest_sites(
    http_client: httpx.AsyncClient,
    address: str,
    limit: int = 10,
) -> tuple[float, float, list[RepositorySiteResult]]:
    """Geocode the address and return the nearest repository sites, sorted by distance."""

    lat, lon = await geocode_address(http_client, address)
    sites = _load_sites()
    results = sorted(
        (
            RepositorySiteResult(site=site, distance_miles=_haversine_miles(lat, lon, site.lat, site.lon))
            for site in sites
        ),
        key=lambda r: r.distance_miles,
    )
    return lat, lon, results[:limit]
