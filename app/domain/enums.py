"""Domain enums used across the application."""

from __future__ import annotations

from enum import Enum


class DataSource(str, Enum):
    """Known system sources for retrieved data."""

    DAILMED_LIVE = "dailymed-live"
    RXNORM_LIVE = "rxnorm-live"
    SQLITE_CACHE = "sqlite-cache"
