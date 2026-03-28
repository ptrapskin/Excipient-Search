"""Shared FastAPI dependencies for route handlers."""

from __future__ import annotations

from fastapi import Request


def get_container(request: Request):
    """Return the application service container."""

    return request.app.state.container
