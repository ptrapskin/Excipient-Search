"""Primary domain models for the application."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, Field

from app.domain.enums import DataSource


@dataclass(slots=True)
class DrugQuery:
    """Normalized representation of a user's drug search."""

    raw_text: str
    normalized_text: str
    ndc: str | None = None
    requested_route: str | None = None
    requested_dose_form: str | None = None
    requested_strength: str | None = None


class RxNormSuggestion(BaseModel):
    """Autocomplete suggestion sourced from RxNorm."""

    display_name: str
    rxcui: str | None = None
    tty: str | None = None
    score: float | None = None


@dataclass(slots=True)
class RxNormConcept:
    """Resolved RxNorm concept for future terminology refinement."""

    rxcui: str
    name: str
    tty: str
    score: float
    source: str


@dataclass(slots=True)
class ProductCandidate:
    """Expanded product candidate derived from RxNorm and DailyMed paths."""

    rxcui: str | None
    setid: str | None
    ndc: str | None
    product_name: str
    labeler: str | None
    dosage_form: str | None
    route: str | None
    source: str


class IngredientEntry(BaseModel):
    """Parsed excipient entry from a DailyMed label."""

    raw_name: str
    display_name: str | None = None
    normalized_name: str | None = None
    unii: str | None = None
    strength: str | None = None
    role: str | None = None
    source_type: str | None = None
    confidence: str | None = None


class ProductSearchResult(BaseModel):
    """Product result shown on the search results page."""

    product_name: str
    setid: str | None = None
    ndcs: list[str] = Field(default_factory=list)
    route: str | None = None
    dosage_form: str | None = None
    labeler: str | None = None
    rxcui: str | None = None
    source: DataSource = DataSource.DAILMED_LIVE


class ProductDetail(BaseModel):
    """Detailed product view built from DailyMed SPL content."""

    product_name: str
    setid: str
    ndcs: list[str] = Field(default_factory=list)
    route: str | None = None
    dosage_form: str | None = None
    labeler: str | None = None
    product_type: str | None = None
    active_ingredients: list[IngredientEntry] = Field(default_factory=list)
    inactive_ingredients_raw: str | None = None
    inactive_ingredients: list[IngredientEntry] = Field(default_factory=list)
    spl_source: str
    fetched_at: datetime


class ExcipientFilter(BaseModel):
    """Include and exclude excipient terms for product filtering."""

    include_terms: list[str] = Field(default_factory=list)
    exclude_terms: list[str] = Field(default_factory=list)


class ProductComparisonRow(BaseModel):
    """Comparison row for one product and its excipient profile."""

    product_name: str
    setid: str | None = None
    ndcs: list[str] = Field(default_factory=list)
    route: str | None = None
    dosage_form: str | None = None
    dailymed_strength: str | None = None
    labeler: str | None = None
    rxcui: str | None = None
    inactive_ingredients: list[IngredientEntry] = Field(default_factory=list)
    inactive_ingredients_raw: str | None = None
    matched_include_terms: list[str] = Field(default_factory=list)
    matched_exclude_terms: list[str] = Field(default_factory=list)
    matches_filter: bool = True
    source: DataSource = DataSource.DAILMED_LIVE


class CachedSearch(BaseModel):
    """Cached product search payload."""

    query_key: str
    query_text: str
    results: list[ProductSearchResult] = Field(default_factory=list)
    source: DataSource
    fetched_at: datetime
    expires_at: datetime


class CachedProduct(BaseModel):
    """Cached product detail payload."""

    setid: str
    product: ProductDetail
    source: DataSource
    fetched_at: datetime
    expires_at: datetime


class CachedRxNormSuggestion(BaseModel):
    """Cached RxNorm suggestion payload."""

    query_key: str
    query_text: str
    suggestions: list[RxNormSuggestion] = Field(default_factory=list)
    source: DataSource
    fetched_at: datetime
    expires_at: datetime
