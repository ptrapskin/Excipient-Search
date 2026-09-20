"""Application configuration."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables when present."""

    model_config = SettingsConfigDict(
        env_prefix="EXCIPIENT_SEARCH_",
        case_sensitive=False,
    )

    app_name: str = "Excipient Search"
    debug: bool = False
    database_url: str | None = None
    dailymed_base_url: str = "https://dailymed.nlm.nih.gov/dailymed/services/v2"
    rxnorm_base_url: str = "https://rxnav.nlm.nih.gov/REST"
    http_timeout_seconds: float = 20.0
    http_retries: int = 2
    cache_ttl_seconds: int = 60 * 60 * 24
    rxnorm_suggestion_limit: int = 8
    rxnorm_candidate_limit: int = 16
    log_level: str = "INFO"
    secret_key: str = Field(default="development-secret-key", repr=False)

    @property
    def project_root(self) -> Path:
        """Return the repository root."""

        return Path(__file__).resolve().parent.parent

    @property
    def sqlite_path(self) -> Path:
        """Return the default SQLite file path."""

        return self.project_root / "excipient_search.db"

    @property
    def resolved_database_url(self) -> str:
        """Return the configured database URL, defaulting to local SQLite."""

        if self.database_url:
            return self.database_url
        return f"sqlite:///{self.sqlite_path.as_posix()}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()
