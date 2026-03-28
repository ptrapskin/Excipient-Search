"""Logging configuration for the application."""

from __future__ import annotations

import logging.config

from app.config import Settings


def configure_logging(settings: Settings) -> None:
    """Configure root logging for the web application."""

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                }
            },
            "root": {
                "handlers": ["console"],
                "level": settings.log_level.upper(),
            },
        }
    )
