# Excipient Search

Excipient Search is a FastAPI web application for searching live DailyMed product
records and inspecting inactive ingredient listings for matching products and NDCs.
Version 1 uses the live DailyMed API for both search and product detail retrieval and
supports multi-product excipient comparison and basic include/exclude filtering. The
codebase stays structured for later RxNorm refinement, local ZIP indexing, alias
mapping, UNII matching, filtering, and risk logic.

## Stack

- Python 3.12+
- FastAPI
- Jinja2 templates
- SQLite
- SQLAlchemy
- Pydantic
- httpx
- pytest

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

Start the application from the repository root:

```bash
uvicorn app.main:app --reload
```

Then open `http://127.0.0.1:8000`.

## Database Initialization

The SQLite database is initialized automatically on startup. By default the app creates
`excipient_search.db` in the repository root.

To point at a different database:

```bash
set EXCIPIENT_SEARCH_DATABASE_URL=sqlite:///C:/path/to/excipient_search.db
```

## Routes

- `GET /` search page
- `GET /search?q=...` server-rendered search results
- `GET /search?q=...&include=...&exclude=...` comparison and excipient-filtered results
- `GET /products/{setid}` server-rendered product detail
- `GET /api/search?q=...&include=...&exclude=...` JSON search and comparison API
- `GET /api/products/{setid}` JSON product detail API
- `GET /api/rxnorm/suggest?q=...` RxNorm-backed suggestion API

## Tests

Run the test suite from the repository root:

```bash
pytest app/tests
```

## Project Notes

- DailyMed is the source of truth for product/SPL/excipient retrieval.
- RxNorm is used as a terminology layer for autocomplete and ranked concept resolution.
- Product expansion now uses ranked RxNorm concepts to drive DailyMed product retrieval
  before falling back to plain text DailyMed search.
- DailyMed retrieval now uses a composite repository:
  local index/SPL cache first, then live API fallback, with merged results cached back locally.
- Search results support cross-product excipient comparison and basic include/exclude
  filtering so users can identify formulations that do or do not contain requested excipients.
- Query normalization, search orchestration, parsing, repositories, caching, and UI
  routes are separated into dedicated modules for maintainability.

## Future Roadmap

- Stronger RxNorm normalization and candidate ranking
- RxCUI-based search refinement
- Local DailyMed ZIP indexing alongside the live repository
- Excipient alias and UNII mapping
- Product filtering and cross-product analytics
- Clinical warning and risk scoring layers
- Export flows for CSV or PDF
