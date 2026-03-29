# Osmotic Risk Filter — Design Notes

## Purpose

This pipeline identifies **oral and enteral liquid drug products** that contain
sugar alcohol excipients, specifically for enteral/jejunal intolerance review.
It is not a general-purpose excipient screen — the goal is clinical precision,
not completeness.

---

## Why `CAPSULE, LIQUID FILLED` is excluded

A liquid-filled capsule (e.g. cyclosporine soft-gel, calcitriol capsule) contains
its drug in a liquid vehicle, but that liquid is encapsulated and swallowed whole.
The excipient is not delivered as a free oral liquid.  Liquid-filled capsules
were matching the filter because their dosage form string contains the word
"liquid", but they carry negligible osmotic risk through the GI lumen and are
irrelevant to enteral feeding tube scenarios.

**Rule:** `CAPSULE, LIQUID FILLED` is in the exclusion list and is checked
*before* any liquid inclusion keyword, so "liquid" inside the form name cannot
promote it to a passing record.

---

## Why blank routes are handled differently for strong vs ambiguous forms

DailyMed SPL records vary widely in how completely the route field is populated.
Many legitimate oral products — especially older labels — carry a blank route.
Blanket rejection of blank-route records would discard thousands of real oral
syrups and solutions.

The pipeline distinguishes two cases:

| Dosage form class | Blank route | Outcome |
|---|---|---|
| **Strong** (`ORAL SOLUTION`, `SUSPENSION`, `SYRUP`, `ELIXIR`, `CONCENTRATE`, `ORAL DROPS`) | Blank | **Passes** — the dosage form name itself is strong evidence of oral/enteral use |
| **Ambiguous** (`LIQUID`, `DROPS`, `EMULSION`) | Blank | **REVIEW** — insufficient evidence; cannot rule out topical, otic, or ophthalmic use |

A product classified as REVIEW is still included in the JSON index and CSVs so
a clinician can make the final call.  It is never silently discarded.

---

## Why unrecognised routes are excluded

If a route is present but does not match any of the oral/enteral keywords
(`oral`, `sublingual`, `buccal`, `oropharyngeal`, `enteral`, `nasogastric`,
`gastric`), the product is excluded.  This is conservative by design.

Rationale: a route of `TRANSDERMAL`, `PERCUTANEOUS`, or any other unrecognised
value is almost certainly not oral.  Treating unknown routes as oral would
reintroduce injectables and topicals that do not carry the DailyMed route
exclusion keywords explicitly.

---

## Concern tier definitions

| Tier | Form | Route | Sugar alcohols |
|---|---|---|---|
| **HIGH** | Strong | Oral/enteral confirmed, or blank with strong form | Sorbitol or mannitol |
| **MODERATE** | Strong | Same as HIGH | Xylitol, maltitol, lactitol, or isomalt only |
| **REVIEW** | Ambiguous | Blank | Any sugar alcohol |
| **EXCLUDED** | Any excluded/non-liquid form, excluded route, or no sugar alcohol | — | — |

Sorbitol and mannitol are classified HIGH because they are the most common
clinically documented causes of osmotic diarrhea in liquid drug products.
The remaining sugar alcohols (xylitol, maltitol, lactitol, isomalt) are
MODERATE — they carry the same mechanistic risk but appear less frequently
at clinically significant doses in Rx liquid products.

---

## Sugar alcohol concept normalisation

The ingredient text in SPL labels is not standardised.  `sorbitol` may appear as:

- `sorbitol`
- `sorbitol solution`
- `sorbitol solution 70%`
- `noncrystallizing sorbitol solution`
- `non-crystallizing sorbitol solution`
- `non crystallizing sorbitol solution`
- `d-sorbitol`
- `glucitol`

All of these map to the canonical concept `sorbitol`.  The alias that triggered
the match is preserved in the `matched_sugar_alcohol_terms` CSV column so the
original label text is always recoverable.

Aliases within each concept are checked **longest-first** so the most specific
variant is recorded in the audit trail.

---

## Output files

| File | Contents |
|---|---|
| `app/data/osmotic_risk_index.json` | Web-app index (HIGH + MODERATE + REVIEW) |
| `app/data/osmotic/high_concern.csv` | HIGH tier products with full audit columns |
| `app/data/osmotic/moderate_concern.csv` | MODERATE tier products |
| `app/data/osmotic/review.csv` | REVIEW tier — ambiguous form, blank route |
| `app/data/osmotic/excluded_debug.csv` | EXCLUDED records (`--debug` flag only) |

---

## What this pipeline does NOT yet do

- Quantitative sugar alcohol dose estimation
- Osmolality calculations
- Discontinued/withdrawn label filtering
- OTC product coverage (OTC zip files not yet downloaded)
- Concentration-weighted risk ranking

These are deferred to a future iteration.
