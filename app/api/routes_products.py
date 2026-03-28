"""JSON routes for product detail workflows."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.api.dependencies import get_container
from app.models.schemas import ProductDetailResponse
from app.repositories.dailymed_api import DailyMedAPIError

router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("/{setid}", response_model=ProductDetailResponse)
async def get_product(request: Request, setid: str) -> ProductDetailResponse:
    """Return product details as JSON."""

    container = get_container(request)
    try:
        product, cached = await container.search_service.get_product_detail(setid)
    except DailyMedAPIError as exc:
        raise HTTPException(status_code=502, detail="DailyMed product lookup failed.") from exc
    return ProductDetailResponse(product=product, cached=cached)
