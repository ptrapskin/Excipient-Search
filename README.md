# Excipient Finder

FastAPI web application with two tools:

- **Excipient Finder** — search live DailyMed product records, compare inactive ingredient listings across formulations, and filter by specific excipients.
- **Osmotic Excipient Screener** — pre-built index of oral/enteral liquid products containing sugar alcohol excipients (sorbitol, mannitol, xylitol, etc.) with concern tier classification and SA-free alternative identification.

## Stack

- Python 3.12+
- FastAPI + Jinja2 templates
- SQLite (`excipients.db` — main data; `excipient_search.db` — API cache)
- SQLAlchemy, Pydantic, httpx

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
uvicorn app.main:app --reload
```

Then open `http://127.0.0.1:8000`.

## Refreshing Data (Monthly)

Both tools read from `excipients.db`. Rebuild it from the latest DailyMed release with:

```bash
python -m excipient_finder.main --fetch all
```

This downloads each DailyMed ZIP (~20 GB total across 17 files) one at a time into a temp folder, processes it, deletes it immediately, and cleans up — no ZIP files remain on disk. Only `excipients.db` is updated.

To resume an interrupted run without reprocessing already-completed ZIPs:

```bash
python -m excipient_finder.main --fetch all --resume
```

DailyMed publishes updated releases monthly. The screener page shows the `processed_at` timestamp so you can tell when the data was last rebuilt.

After each run, a diff report is written to `qa/update_diff.csv` showing every product that was added, removed, or had its concern tier or sugar alcohol list change relative to the previous build.

## Routes

- `GET /` — search home
- `GET /search?q=...` — product search results
- `GET /search?q=...&include=...&exclude=...` — excipient-filtered comparison
- `GET /products/{setid}` — product detail with sugar alcohol annotation
- `GET /osmotic-excipient-screener` — osmotic excipient screener
- `GET /api/search?q=...` — JSON search API
- `GET /api/products/{setid}` — JSON product detail API
- `GET /api/rxnorm/suggest?q=...` — RxNorm autocomplete

## Architecture Notes

- **`excipients.db`** — built offline by `excipient_finder/main.py`. Contains all oral/enteral liquid products with sugar alcohol classification (`concern_tier`: high, moderate, review, alternative). Both tools read from it; the excipient finder also falls back to it when the live DailyMed API is unavailable.
- **`excipient_search.db`** — SQLAlchemy-managed cache for live DailyMed API responses and RxNorm suggestions. Auto-initialized on startup.
- **`app/data/osmotic_risk_index.json`** — not used by the web app; written by `scripts/build_osmotic_index.py` as an offline audit artifact only.
- DailyMed is the source of truth for live product/SPL/excipient retrieval. RxNorm backs autocomplete and ranked concept resolution.

## Tests

```bash
pytest app/tests
```
