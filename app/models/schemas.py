"""Transport schemas for API routes and page contexts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.domain.models import (
    DrugQuery,
    ExcipientFilter,
    ProductComparisonRow,
    ProductDetail,
    ProductSearchResult,
    RxNormSuggestion,
)


class MessageResponse(BaseModel):
    """Generic message payload."""

    message: str


class ProductSearchResponse(BaseModel):
    """JSON search response."""

    query: DrugQuery
    results: list[ProductSearchResult] = Field(default_factory=list)
    comparison_rows: list[ProductComparisonRow] = Field(default_factory=list)
    filter: ExcipientFilter = Field(default_factory=ExcipientFilter)
    matching_products: list[ProductComparisonRow] = Field(default_factory=list)
    cached: bool = False


class ProductDetailResponse(BaseModel):
    """JSON product detail response."""

    product: ProductDetail
    cached: bool = False


class RxNormSuggestResponse(BaseModel):
    """JSON autocomplete response."""

    query: str
    suggestions: list[RxNormSuggestion] = Field(default_factory=list)
    cached: bool = False
