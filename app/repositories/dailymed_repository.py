"""DailyMed repository protocol."""

from __future__ import annotations

from typing import Protocol

from app.domain.models import DrugQuery, ProductDetail, ProductSearchResult


class DailyMedRepository(Protocol):
    """Repository contract for finding products and retrieving SPL content."""

    async def search_spls(self, query: DrugQuery) -> list[ProductSearchResult]:
        """Return all SPL product results for a query, using DailyMed as source of truth."""

    async def get_spl(self, setid: str) -> dict:
        """Return raw SPL payload data for a SETID."""

    async def get_inactive_ingredients(self, setid: str) -> list[dict]:
        """Return parsed inactive ingredient payloads for a SETID."""

    async def get_product_detail(self, setid: str) -> ProductDetail:
        """Return a normalized detailed product view."""

    async def get_all_product_details(self, setid: str) -> list[ProductDetail]:
        """Return one ProductDetail per product subject found in the SPL."""
