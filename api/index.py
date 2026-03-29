"""Vercel serverless entry point — exports the FastAPI ASGI app."""

import sys
from pathlib import Path

# Ensure project root is on sys.path so `app` package resolves correctly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import app  # noqa: F401  (Vercel looks for `app`)
