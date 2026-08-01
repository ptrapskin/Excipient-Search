"""JSON route for the Drug Repository Program site finder."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.api.dependencies import get_container
from app.services.drug_repository_service import AddressNotFoundError, find_nearest_sites

router = APIRouter(prefix="/api/drug-repository-sites", tags=["drug-repository"])


class DrugRepositorySiteOut(BaseModel):
    name: str
    phone: str | None
    address: str
    contact: str | None
    participation: str
    distance_miles: float


class DrugRepositorySearchResponse(BaseModel):
    query_lat: float
    query_lon: float
    sites: list[DrugRepositorySiteOut]


@router.get("/search", response_model=DrugRepositorySearchResponse)
async def search_nearby_sites(
    request: Request,
    address: str = Query(default="", min_length=1),
    limit: int = Query(default=10, ge=1, le=25),
) -> DrugRepositorySearchResponse:
    """Return the drug repository sites nearest to a given address."""

    address = address.strip()
    if not address:
        raise HTTPException(status_code=400, detail="Enter an address, city, or ZIP code.")

    container = get_container(request)
    try:
        lat, lon, results = await find_nearest_sites(container.http_client, address, limit=limit)
    except AddressNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return DrugRepositorySearchResponse(
        query_lat=lat,
        query_lon=lon,
        sites=[
            DrugRepositorySiteOut(
                name=r.site.name,
                phone=r.site.phone,
                address=r.site.address,
                contact=r.site.contact,
                participation=r.site.participation,
                distance_miles=round(r.distance_miles, 1),
            )
            for r in results
        ],
    )
