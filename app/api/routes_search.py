"""JSON routes for search workflows."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.api.dependencies import get_container
from app.models.schemas import ProductSearchResponse
from app.repositories.dailymed_api import DailyMedAPIError

router = APIRouter(prefix="/api/search", tags=["search"])


@router.get("", response_model=ProductSearchResponse)
async def search_products(
    request: Request,
    q: str = Query(default=""),
    include: str = Query(default=""),
    exclude: str = Query(default=""),
) -> ProductSearchResponse:
    """Return search results as JSON."""

    container = get_container(request)
    try:
        query, results, comparison_rows, excipient_filter, matching_rows, cached = (
            await container.search_service.search_with_excipients(
                q,
                include_terms=include,
                exclude_terms=exclude,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DailyMedAPIError as exc:
        raise HTTPException(status_code=502, detail="DailyMed search failed.") from exc
    return ProductSearchResponse(
        query=query,
        results=results,
        comparison_rows=comparison_rows,
        filter=excipient_filter,
        matching_products=matching_rows,
        cached=cached,
    )
