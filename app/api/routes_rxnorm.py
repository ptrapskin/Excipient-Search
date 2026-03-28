"""JSON routes for RxNorm suggestion workflows."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.api.dependencies import get_container
from app.models.schemas import RxNormSuggestResponse

router = APIRouter(prefix="/api/rxnorm", tags=["rxnorm"])


@router.get("/suggest", response_model=RxNormSuggestResponse)
async def rxnorm_suggest(request: Request, q: str = Query(default="")) -> RxNormSuggestResponse:
    """Return RxNorm suggestions for autocomplete."""

    container = get_container(request)
    suggestions, cached = await container.rxnorm_resolver.suggest(q)
    return RxNormSuggestResponse(query=q, suggestions=suggestions, cached=cached)
