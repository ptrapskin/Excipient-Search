# excipient_finder

A standalone Python data-ingestion pipeline that reads DailyMed bulk ZIP archives,
parses Structured Product Labeling (SPL) XML, identifies oral and enteral liquid
drug products, matches sugar alcohol excipients against a curated concept map,
assigns clinical concern tiers, and writes results to SQLite and CSV.

---

## Project purpose

Pediatric and neonatal patients receiving multiple oral/enteral liquid medications
may accumulate significant quantities of sugar alcohols (sorbitol, mannitol, xylitol,
maltitol, lactitol, isomalt) from inactive ingredients. These excipients are osmotically
active and can cause gastrointestinal distress, feeding intolerance, or worsen
necrotizing enterocolitis risk at high cumulative doses. This tool automates the
identification of commercially available oral liquid products that contain these
excipients so that clinicians and pharmacists can review and counsel accordingly.

---

## Folder structure

```
excipient_finder/
├── __init__.py             # Makes this a Python package
├── README.md               # This file
├── config.py               # Config dataclass with path defaults
├── models.py               # Dataclasses: SplRecord, FilterDecision, MatchedExcipient, ProductOutputRow
├── utils.py                # normalize_text(), utc_now_str(), setup_logging()
├── zip_reader.py           # Iterates nested DailyMed ZIP archives in memory
├── xml_parser.py           # Parses SPL XML into SplRecord objects (xml.etree.ElementTree)
├── filters.py              # Pure dosage-form and route classifiers
├── excipient_matcher.py    # Sugar alcohol concept map and matching logic
├── tiering.py              # Assigns HIGH / MODERATE / REVIEW / EXCLUDED tiers
├── db.py                   # SQLite schema, insert helpers, CSV export
└── main.py                 # CLI entrypoint and pipeline orchestration
```

Output files (written to `--output-root`, default `C:\Users\traps\OneDrive\Apps\Excipient Finder`):

```
excipients.db                        # SQLite database
final_products_of_concern.csv        # HIGH-tier products
moderate_products_of_concern.csv     # MODERATE-tier products
review_products.csv                  # REVIEW-tier products
excluded_products_debug.csv          # EXCLUDED products (only with --write-excluded-debug)
logs/
    run_YYYYMMDD_HHMMSS.log          # Per-run log file
```

---

## Requirements

- **Python 3.12+**
- **Standard library only** — no `pip install` required.
  - `xml.etree.ElementTree` for XML parsing
  - `zipfile` + `io.BytesIO` for in-memory ZIP handling
  - `sqlite3` for database writes
  - `csv` for CSV export
  - `argparse`, `logging`, `dataclasses`, `pathlib`, `re`, `datetime`

---

## DailyMed ZIP structure

DailyMed distributes SPL data as nested ZIP archives with the following layout:

```
dm_spl_release_human_rx_part1.zip        ← outer zip (downloaded from DailyMed)
    prescription/20060131_<UUID>.zip      ← inner zip, one per drug label
        <UUID>.xml                         ← SPL XML (the drug label)
        optional-image.jpg
```

The pipeline opens each outer zip and iterates inner `.zip` entries. Each inner zip
is opened entirely in memory using `io.BytesIO` — nothing is extracted to disk.
The SPL XML filename (without `.xml`) is used as the `setid`. Malformed inner zips
or XML files are logged as warnings and skipped; processing continues with the next entry.

---

## How to run

### Basic run

```bash
python -m excipient_finder.main --input-root "C:/Data/DailyMed"
```

### Resume a previous run (skip already-processed outer ZIPs)

```bash
python -m excipient_finder.main --input-root "C:/Data/DailyMed" --resume
```

### Test with a small sample

```bash
python -m excipient_finder.main --input-root "C:/Data/DailyMed" --limit 5 --debug
```

### Write excluded products to DB and CSV (for auditing filters)

```bash
python -m excipient_finder.main \
    --input-root "C:/Data/DailyMed" \
    --write-excluded-debug
```

### Custom output location

```bash
python -m excipient_finder.main \
    --input-root "C:/Data/DailyMed" \
    --output-root "D:/Results/ExcipientFinder"
```

### All options

| Flag | Default | Description |
|------|---------|-------------|
| `--input-root PATH` | (required) | Directory containing DailyMed outer ZIP files |
| `--output-root PATH` | `C:\Users\traps\OneDrive\Apps\Excipient Finder` | Directory for DB, logs, and CSVs |
| `--limit N` | None | Process at most N outer ZIPs (useful for testing) |
| `--debug` | False | Enable DEBUG-level logging |
| `--write-excluded-debug` | False | Also write excluded records to DB and CSV |
| `--resume` | False | Skip outer ZIPs already logged as successful |

---

## Where outputs go

| Output | Location | Description |
|--------|----------|-------------|
| `excipients.db` | `--output-root` | SQLite database with three tables |
| `final_products_of_concern.csv` | `--output-root` | HIGH-tier products |
| `moderate_products_of_concern.csv` | `--output-root` | MODERATE-tier products |
| `review_products.csv` | `--output-root` | REVIEW-tier products needing manual review |
| `excluded_products_debug.csv` | `--output-root` | Excluded products (requires `--write-excluded-debug`) |
| `logs/run_*.log` | `--output-root/logs/` | Timestamped log file per run |

### Database tables

**`products`** — one row per retained product subject

Key columns: `spl_setid`, `product_name`, `labeler`, `dosage_form`, `form_class`,
`route`, `route_class`, `ndcs` (semicolon-joined), `active_ingredients_raw`,
`concern_tier`, `inclusion_decision`, `review_reason`, `matched_sugar_alcohols`,
`matched_sugar_alcohol_terms`, `source_file`, `processed_at`

**`matched_excipients`** — one row per matched sugar alcohol per product

Columns: `spl_setid`, `raw_name`, `normalized_name`, `canonical_name`, `category`

**`processing_log`** — one row per outer-zip processing event

Columns: `source_file`, `status` (`started` | `success` | `failed`), `message`, `processed_at`

---

## Clinical filtering logic

### Step 1 — Dosage form classification

Exclusion keywords are checked before inclusion keywords so that compound forms
like "CAPSULE, LIQUID FILLED" are correctly excluded.

| Form class | Examples | Behaviour |
|------------|----------|-----------|
| `strong` | ORAL SOLUTION, SUSPENSION, SYRUP, ELIXIR, CONCENTRATE | Passes with blank or oral route |
| `ambiguous` | LIQUID, EMULSION, DROPS | Passes only with confirmed oral route; blank route → REVIEW |
| `excluded` | CAPSULE, TABLET, INJECTION, OPHTHALMIC, INHALATION | Hard-excluded; excipient matching skipped |
| `non_liquid` | (anything else) | Hard-excluded |

### Step 2 — Route classification

| Route class | Examples | Behaviour |
|-------------|----------|-----------|
| `oral` | ORAL, SUBLINGUAL, BUCCAL, ENTERAL, NASOGASTRIC | Passes |
| `excluded` | INTRAVENOUS, INTRAMUSCULAR, TOPICAL, INHALATION, OPHTHALMIC | Hard-excluded |
| `blank` | (no route recorded) | Passes for strong forms, triggers REVIEW for ambiguous forms |

Any route present but not recognized as oral is treated as excluded (conservative default).

### Step 3 — Sugar alcohol matching

Matching is performed on normalized text (lowercase, whitespace-collapsed,
punctuation-stripped). Aliases are checked longest-first within each concept so the
most specific match is recorded in the audit trail.

| Canonical name | Category | Key aliases |
|----------------|----------|-------------|
| sorbitol | high | sorbitol, sorbitol solution, noncrystallizing sorbitol solution, d-sorbitol, glucitol |
| mannitol | high | mannitol, d-mannitol |
| xylitol | moderate | xylitol |
| maltitol | moderate | maltitol, maltitol solution, maltitol syrup |
| lactitol | moderate | lactitol, lactitol monohydrate |
| isomalt | moderate | isomalt |

### Step 4 — Concern tier assignment

| Tier | Criteria |
|------|----------|
| `high` | Form passes, route passes or blank+strong, contains sorbitol or mannitol |
| `moderate` | Form passes, route passes or blank+strong, contains only moderate-category sugar alcohols |
| `review` | Ambiguous form with blank route and at least one sugar alcohol match |
| `excluded` | Hard-excluded form/route, or no sugar alcohol match after passing filters |

---

## Resume behaviour

With `--resume`, the pipeline checks the `processing_log` table for each outer ZIP
filename before processing it. If a `success` entry exists, the file is skipped.
Resume operates at the outer-ZIP level only — there is no mid-ZIP checkpoint.
If a run was interrupted mid-ZIP, re-run without `--resume` (or delete the failed
log entry) to reprocess that file.

---

## Limitations

- **Rx labels only**: DailyMed files pointed to by `--input-root` determine scope.
  OTC and animal labels are not included unless explicitly added (see below).
- **No quantity estimation**: The pipeline identifies presence of sugar alcohols but
  does not parse concentrations or volumes from label text.
- **No osmolality calculation**: Osmotic load estimation would require concentration
  data and is outside scope.
- **No web UI**: This is a batch data-ingestion tool. Outputs are SQLite + CSV files
  intended for downstream analysis in spreadsheets, R, or Python notebooks.
- **Single-pass matching**: Each canonical sugar alcohol concept contributes at most
  one `matched_excipients` row per product, even if multiple inactive ingredients
  match the same concept.

---

## Adding OTC or other DailyMed files

The pipeline is agnostic about which DailyMed ZIPs are in `--input-root`. To include
OTC products, simply download the relevant DailyMed bulk ZIPs (e.g.
`dm_spl_release_human_otc_part*.zip`) and place them alongside the Rx ZIPs in your
`--input-root` directory, or point `--input-root` at a folder that contains all of them.
The filter for `"HUMAN"` in `product_type` already handles mixed Rx/OTC content.
Veterinary labels are automatically skipped because their `product_type` does not
contain the word "HUMAN".
