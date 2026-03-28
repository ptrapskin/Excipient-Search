"""Server-rendered page routes."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.api.dependencies import get_container
from app.domain.models import ExcipientFilter
from app.repositories.dailymed_api import DailyMedAPIError

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
async def search_page(request: Request) -> HTMLResponse:
    """Render the search page."""

    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={"query": "", "error": None},
    )


@router.get("/search", response_class=HTMLResponse)
async def search_results_page(
    request: Request,
    q: str = "",
    include: str = "",
    exclude: str = "",
) -> HTMLResponse:
    """Render search results for a query."""

    container = get_container(request)
    if not q.strip():
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "query": q,
                "normalized_query": None,
                "results": [],
                "comparison_rows": [],
                "matching_rows": [],
                "excipient_filter": ExcipientFilter(),
                "include": include,
                "exclude": exclude,
                "cached": False,
                "error": "Enter a medication name or NDC to search DailyMed.",
            },
        )

    try:
        normalized_query, results, comparison_rows, excipient_filter, matching_rows, cached = (
            await container.search_service.search_with_excipients(q, include_terms=include, exclude_terms=exclude)
        )
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "query": q,
                "normalized_query": normalized_query,
                "results": results,
                "comparison_rows": comparison_rows,
                "matching_rows": matching_rows,
                "excipient_filter": excipient_filter,
                "include": include,
                "exclude": exclude,
                "cached": cached,
                "error": None,
            },
        )
    except ValueError as exc:
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "query": q,
                "normalized_query": None,
                "results": [],
                "comparison_rows": [],
                "matching_rows": [],
                "excipient_filter": ExcipientFilter(),
                "include": include,
                "exclude": exclude,
                "cached": False,
                "error": str(exc),
            },
        )
    except DailyMedAPIError:
        logger.exception("DailyMed search failed for query %s", q)
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "query": q,
                "normalized_query": None,
                "results": [],
                "comparison_rows": [],
                "matching_rows": [],
                "excipient_filter": ExcipientFilter(),
                "include": include,
                "exclude": exclude,
                "cached": False,
                "error": "DailyMed is temporarily unavailable. Please try again shortly.",
            },
            status_code=502,
        )


@router.get("/products/{setid}", response_class=HTMLResponse)
async def product_page(request: Request, setid: str, ndc: str = "") -> HTMLResponse:
    """Render the product detail page."""

    container = get_container(request)
    try:
        product, cached = await container.search_service.get_product_detail(setid, ndc=ndc or None)
        return templates.TemplateResponse(
            request=request,
            name="product.html",
            context={"product": product, "cached": cached, "error": None},
        )
    except DailyMedAPIError:
        logger.exception("DailyMed product fetch failed for setid %s", setid)
        return templates.TemplateResponse(
            request=request,
            name="product.html",
            context={
                "product": None,
                "cached": False,
                "error": "Product details are unavailable right now.",
            },
            status_code=502,
        )
