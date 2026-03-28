"""Shared HTTP retry utility for repository clients."""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict | None = None,
    retries: int = 2,
    error_cls: type[RuntimeError] = RuntimeError,
    label: str = "HTTP",
) -> httpx.Response:
    """GET a URL with retry handling, raising error_cls on final failure.

    Handles 404 immediately (no retry). Logs warnings on transient failures.
    """

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response
        except httpx.TimeoutException as exc:
            last_error = exc
            logger.warning("%s timeout on attempt %s for %s", label, attempt + 1, url)
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if exc.response.status_code == 404:
                raise error_cls(f"{label} resource not found") from exc
            logger.warning("%s HTTP error on attempt %s: %s", label, attempt + 1, exc)
        except httpx.HTTPError as exc:
            last_error = exc
            logger.warning("%s request failed on attempt %s: %s", label, attempt + 1, exc)
    raise error_cls(f"{label} request failed") from last_error
