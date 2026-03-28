# Architecture Overview

## Why the Layers Are Split

The project separates terminology resolution from product and formulation analysis.
That split is intentional:

- RxNorm is used for query assistance and future terminology normalization.
- DailyMed is used for live product, SPL, and excipient retrieval.
- Excipient truth is not inferred from RxNorm.

This prevents terminology concerns from leaking into formulation parsing and keeps the
system extensible when later phases introduce more advanced ranking and crosswalk logic.

## Layered Structure

### Query Normalization

`app/services/normalize_query.py` trims whitespace, normalizes spacing, and extracts
lightweight hints such as likely NDCs, routes, dose forms, and strengths. It has no
external API dependency.

### RxNorm Suggestion and Resolution

`app/repositories/rxnorm_api.py` handles live RxNorm HTTP access.
`app/services/rxnorm_resolver.py` adds cache-aware suggestion behavior for the UI and
ranked candidate resolution for normalized drug queries.

This layer is intentionally lightweight in version 1. It is designed to support future
candidate ranking, selected RxCUI workflows, and broader terminology normalization.

The resolver keeps multiple candidate concepts rather than collapsing immediately to a
single winner. It prioritizes `SCD` and `SBD`, keeps useful fallback types such as
`SCDG`, `IN`, `PIN`, and `MIN`, and applies workflow-aware ranking signals such as:

- exact normalized name match
- strength match
- oral liquid form boosts
- injectable penalties when not requested
- non-oral route penalties for oral liquid workflows

### Live DailyMed Retrieval

`app/repositories/dailymed_repository.py` defines the shared retrieval contract.
The concrete implementations are:

- `app/repositories/dailymed_zip.py` for local index and SPL cache access
- `app/repositories/dailymed_api.py` for live DailyMed HTTP retrieval
- `app/repositories/dailymed_composite.py` for local-first, live-fallback behavior

The live API repository owns all DailyMed HTTP calls and shared SPL parsing logic.
The composite repository searches local data first, falls back to live API when local
results are missing or insufficient, merges by SETID/NDC, and stores the resolved
product candidates and SPL XML back into the local cache.

This repository boundary is what makes a later `dailymed_zip.py` implementation
possible without rewriting the service and route layers.

### Product Search and Expansion

`app/services/search_service.py` orchestrates the full search workflow:

1. Normalize the raw query.
2. Record the normalized query.
3. Check the SQLite cache.
4. Resolve and rank RxNorm concept candidates.
5. Expand those concepts into DailyMed product candidates.
6. Fall back to plain DailyMed text search when concept expansion is insufficient.
7. Persist the expanded results.

`app/services/product_expander.py` is kept separate so future strategies can enrich
search results differently, for example with local ZIP-backed metadata joins.

The current product expansion rules are:

- `SCD` and `SBD` concepts take a product-oriented RxCUI path into DailyMed.
- `SCDG` concepts are treated as grouping fallbacks and expanded through name-based DailyMed search.
- `IN`, `PIN`, and `MIN` concepts are expanded cautiously and require route or dose-form context.

### Excipient Extraction and Parsing

`app/services/parsing_service.py` preserves raw inactive ingredient text and performs
generic lightweight parsing into `IngredientEntry` objects.

`app/services/excipient_filter.py` builds comparison rows across multiple matching
products and applies basic include/exclude excipient filtering. This is intentionally
string-based in version 1 so the architecture can later grow into:

- alias dictionaries
- category-based filters such as dyes or preservatives
- normalized excipient concepts
- UNII-backed matching

Version 1 deliberately avoids:

- sorbitol-specific rules
- excipient categorization
- clinical risk scoring
- alias or UNII inference beyond direct text extraction

That keeps the parsing layer generic and reusable for later enrichment passes.

### Persistence and Cache

SQLite is used from the start. Cache tables store:

- normalized query records
- RxNorm suggestion responses
- expanded product search results
- product detail responses

`app/repositories/cache_repository.py` handles persistence concerns.
`app/services/cache_service.py` handles payload serialization, TTL checks, and
application-facing cache models.

## Current Version and Future Direction

Version 1 is intentionally live-API-first. It calls DailyMed directly for product
search and product detail retrieval and uses RxNorm only for suggestions.

The current structure is prepared for later additions including:

- stronger RxNorm normalization pipelines
- RxCUI-guided DailyMed refinement
- local DailyMed ZIP indexing
- excipient alias dictionaries
- UNII matching
- filtering, analytics, and exports
- clinical warning and risk engines
