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
from app.services.osmotic_risk_service import LIQUID_DRUG_INGREDIENTS, SUGAR_ALCOHOLS, _LIQUID_LOWER

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


_OSMOTIC_BASE_CONTEXT = {
    "liquid_keywords": sorted(_LIQUID_LOWER),
    "sugar_alcohols": SUGAR_ALCOHOLS,
    "ingredient_count": len(LIQUID_DRUG_INGREDIENTS),
}


@router.get("/osmotic-risk", response_class=HTMLResponse)
async def osmotic_risk_page(request: Request, run: bool = False) -> HTMLResponse:
    """Render the osmotic diarrhea risk analysis page."""

    container = get_container(request)
    service = container.osmotic_risk_service

    # Always show pre-built index if it exists — instant load, no API calls.
    prebuilt = service.get_prebuilt_index()
    if prebuilt is not None:
        return templates.TemplateResponse(
            request=request,
            name="osmotic_risk.html",
            context={
                **_OSMOTIC_BASE_CONTEXT,
                "groups": prebuilt.groups,
                "total": prebuilt.total,
                "built_at": prebuilt.built_at,
                "source_files": prebuilt.source_files,
                "error": None,
            },
        )

    # No pre-built index — show landing page or run live analysis.
    if not run:
        return templates.TemplateResponse(
            request=request,
            name="osmotic_risk.html",
            context={
                **_OSMOTIC_BASE_CONTEXT,
                "groups": None,
                "total": 0,
                "built_at": None,
                "source_files": [],
                "error": None,
            },
        )

    try:
        groups, total = await service.run()
        return templates.TemplateResponse(
            request=request,
            name="osmotic_risk.html",
            context={
                **_OSMOTIC_BASE_CONTEXT,
                "groups": groups,
                "total": total,
                "built_at": None,
                "source_files": [],
                "error": None,
            },
        )
    except Exception:
        logger.exception("Osmotic risk analysis failed")
        return templates.TemplateResponse(
            request=request,
            name="osmotic_risk.html",
            context={
                **_OSMOTIC_BASE_CONTEXT,
                "groups": None,
                "total": 0,
                "built_at": None,
                "source_files": [],
                "error": "Analysis failed. DailyMed may be temporarily unavailable.",
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


_VALID_TIERS = {"high", "moderate", "review", "all"}


@router.get("/sugar-alcohol-risk", response_class=HTMLResponse)
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
