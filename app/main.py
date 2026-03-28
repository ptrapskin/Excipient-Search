"""FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes_pages import router as pages_router
from app.api.routes_products import router as products_router
from app.api.routes_rxnorm import router as rxnorm_router
from app.api.routes_search import router as search_router
from app.config import Settings, get_settings
from app.db.init_db import initialize_database
from app.db.session import create_session_factory, create_sqlalchemy_engine
from app.logging_config import configure_logging
from app.repositories.cache_repository import CacheRepository
from app.repositories.dailymed_api import DailyMedApiRepository
from app.repositories.dailymed_composite import CompositeDailyMedRepository
from app.repositories.rxnorm_api import RxNormApiRepository
from app.repositories.dailymed_zip import DailyMedZipRepository
from app.services.cache_service import CacheService
from app.services.excipient_filter import ExcipientFilterService
from app.services.parsing_service import ParsingService
from app.services.product_expander import ProductExpander
from app.services.rxnorm_resolver import RxNormResolver
from app.services.search_service import SearchService


@dataclass
class AppContainer:
    """Shared application services."""

    settings: Settings
    http_client: httpx.AsyncClient
    search_service: SearchService
    rxnorm_resolver: RxNormResolver


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database, HTTP clients, and service graph."""

    settings = get_settings()
    configure_logging(settings)

    engine = create_sqlalchemy_engine(settings.resolved_database_url)
    initialize_database(engine)
    session_factory = create_session_factory(engine)

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.http_timeout_seconds),
        headers={"User-Agent": "Excipient Search/1.0"},
    )
    parsing_service = ParsingService()
    cache_repository = CacheRepository(session_factory)
    cache_service = CacheService(cache_repository, ttl_seconds=settings.cache_ttl_seconds)
    local_dailymed_repository = DailyMedZipRepository(settings.project_root / "app" / "data", parsing_service)
    dailymed_api_repository = DailyMedApiRepository(settings, http_client, parsing_service)
    dailymed_repository = CompositeDailyMedRepository(
        local_repository=local_dailymed_repository,
        api_repository=dailymed_api_repository,
    )
    rxnorm_repository = RxNormApiRepository(settings, http_client)
    rxnorm_resolver = RxNormResolver(
        cache_service=cache_service,
        repository=rxnorm_repository,
        suggestion_limit=settings.rxnorm_suggestion_limit,
        candidate_limit=settings.rxnorm_candidate_limit,
    )
    excipient_filter_service = ExcipientFilterService()
    product_expander = ProductExpander(dailymed_repository, limit=settings.rxnorm_candidate_limit)
    search_service = SearchService(
        cache_service=cache_service,
        dailymed_repository=dailymed_repository,
        product_expander=product_expander,
        rxnorm_resolver=rxnorm_resolver,
        excipient_filter_service=excipient_filter_service,
    )

    app.state.container = AppContainer(
        settings=settings,
        http_client=http_client,
        search_service=search_service,
        rxnorm_resolver=rxnorm_resolver,
    )

    try:
        yield
    finally:
        await http_client.aclose()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(title="Excipient Search", lifespan=lifespan)
    app.include_router(pages_router)
    app.include_router(search_router)
    app.include_router(products_router)
    app.include_router(rxnorm_router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    return app


app = create_app()
