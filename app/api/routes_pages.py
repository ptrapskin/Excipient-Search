"""Server-rendered page routes."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.api.dependencies import get_container
from app.domain.models import ExcipientFilter
from app.repositories import excipient_db
from app.repositories.dailymed_api import DailyMedAPIError
from app.services import osmotic_filter

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

# ---------------------------------------------------------------------------
# Custom Jinja filters
# ---------------------------------------------------------------------------

import re as _re

_FORMULATION_SUFFIXES = _re.compile(
    r"\b(suspension|solution|syrup|elixir|liquid|drops|emulsion|concentrate|"
    r"oral suspension|oral solution|oral drops|for suspension|for solution|"
    r"reconstituted|reconstitution|injection|injectable|infusion|"
    r"tablet|tablets|capsule|capsules|patch|cream|ointment|gel|lotion|"
    r"powder|granules|suppository|suppositories|spray|inhaler|"
    r"extended release|immediate release|delayed release|modified release|"
    r"er|xr|sr|dr|ir)\s*$",
    _re.IGNORECASE,
)


_LOWERCASE_WORDS = {"and", "or", "of", "in", "with", "for", "the", "a", "an"}


def _title_case(text: str) -> str:
    words = text.split()
    return " ".join(
        w.capitalize() if i == 0 or w.lower() not in _LOWERCASE_WORDS else w.lower()
        for i, w in enumerate(words)
    )


def _normalize_product_name(name: str | None) -> str:
    if not name:
        return ""
    cleaned = _FORMULATION_SUFFIXES.sub("", name).strip().strip(",;-").strip()
    return _title_case(cleaned) if cleaned else _title_case(name)


templates.env.filters["product_name"] = _normalize_product_name


def _datetimeformat(value: str) -> str:
    try:
        from datetime import datetime
        dt = datetime.strptime(value, "%Y-%m-%d")
        return f"{dt.strftime('%B')} {dt.day}, {dt.year}"
    except Exception:
        return value


templates.env.filters["datetimeformat"] = _datetimeformat


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
        sugar_alcohols = osmotic_filter.match_sugar_alcohols(product.inactive_ingredients) if product else []
        search_query = (product.active_ingredients[0].display_name or "") if (product and product.active_ingredients) else ""
        return templates.TemplateResponse(
            request=request,
            name="product.html",
            context={"product": product, "cached": cached, "error": None, "sugar_alcohols": sugar_alcohols, "search_query": search_query},
        )
    except DailyMedAPIError:
        logger.warning("DailyMed product fetch failed for setid %s — trying local index", setid)
        db_path = container.settings.project_root / "excipients.db"
        local = await asyncio.to_thread(excipient_db.get_product_by_setid, db_path, setid)
        if local is not None:
            return templates.TemplateResponse(
                request=request,
                name="product_local.html",
                context={"product": local, "error": None},
            )
        return templates.TemplateResponse(
            request=request,
            name="product.html",
            context={
                "product": None,
                "cached": False,
                "error": "Product details are unavailable right now (DailyMed API error).",
            },
            status_code=502,
        )


# Label Changes route disabled — sequestered for future rework
# @router.get("/label-changes", response_class=HTMLResponse)
# async def label_changes_page(request: Request) -> HTMLResponse: ...


_VALID_TIERS = {"high", "moderate", "review", "all"}


@router.get("/sugar-alcohol-risk", response_class=HTMLResponse, include_in_schema=False)
async def sugar_alcohol_risk_redirect(request: Request):
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/osmotic-excipient-screener", status_code=301)


@router.get("/osmotic-excipient-screener", response_class=HTMLResponse)
async def sugar_alcohol_risk_page(
    request: Request,
    sa: str = "all",
) -> HTMLResponse:
    """Render the sugar alcohol excipient list page from the local SQLite index."""

    container = get_container(request)
    db_path = container.settings.project_root / "excipients.db"

    stats, (groups, total), (sa_counts, multiple_count) = await asyncio.gather(
        asyncio.to_thread(excipient_db.get_stats, db_path),
        asyncio.to_thread(excipient_db.get_groups, db_path, sa),
        asyncio.to_thread(excipient_db.get_sugar_alcohol_counts, db_path),
    )

    return templates.TemplateResponse(
        request=request,
        name="sugar_alcohol_risk.html",
        context={
            "groups": groups,
            "total": total,
            "stats": stats,
            "sa_counts": sa_counts,
            "multiple_count": multiple_count,
            "selected_sa": sa,
        },
    )
