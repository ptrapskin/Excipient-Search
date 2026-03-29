"""Service for identifying liquid oral/enteral drug products containing sugar alcohol excipients."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from app.domain.models import DrugQuery, IngredientEntry
from app.repositories.dailymed_api import DailyMedAPIError, DailyMedApiRepository
from app.services import osmotic_filter

# Drug ingredient names commonly formulated as oral or enteral liquids.
# DailyMed is searched for each ingredient; results are then filtered by the
# osmotic_filter pipeline before checking for sugar alcohol excipients.
LIQUID_DRUG_INGREDIENTS = [
    # Analgesics / opioids
    "acetaminophen", "ibuprofen", "oxycodone", "hydrocodone", "morphine",
    "codeine", "tramadol", "methadone", "buprenorphine", "naloxone",
    # Antibiotics
    "amoxicillin", "ampicillin", "azithromycin", "cephalexin", "cefdinir",
    "cefprozil", "cefuroxime", "clarithromycin", "clindamycin", "erythromycin",
    "metronidazole", "nitrofurantoin", "penicillin", "sulfamethoxazole",
    "trimethoprim", "doxycycline", "levofloxacin", "linezolid",
    # Antifungals / antivirals
    "fluconazole", "nystatin", "itraconazole", "valganciclovir", "acyclovir",
    "oseltamivir", "ribavirin",
    # GI
    "omeprazole", "lansoprazole", "ranitidine", "famotidine", "metoclopramide",
    "ondansetron", "lactulose", "bismuth", "simethicone", "magnesium hydroxide",
    "aluminum hydroxide", "sucralfate",
    # Respiratory / allergy / cough
    "guaifenesin", "dextromethorphan", "promethazine", "diphenhydramine",
    "hydroxyzine", "chlorpheniramine", "cetirizine", "loratadine", "fexofenadine",
    # Psychiatric
    "risperidone", "haloperidol", "aripiprazole", "lithium", "sertraline",
    "fluoxetine", "fluphenazine", "thioridazine",
    # Antiepileptics
    "levetiracetam", "carbamazepine", "oxcarbazepine", "valproic acid",
    "phenytoin", "phenobarbital", "diazepam", "lorazepam", "clonazepam",
    "zonisamide", "lacosamide",
    # Cardiovascular
    "digoxin", "captopril", "enalapril", "propranolol", "metoprolol",
    "hydralazine", "furosemide", "spironolactone", "amlodipine",
    # Vitamins / minerals
    "ferrous sulfate", "potassium chloride", "zinc sulfate", "folic acid",
    "cyanocobalamin", "vitamin d", "iron",
    # Steroids
    "prednisolone", "dexamethasone", "prednisone", "budesonide",
    "methylprednisolone", "hydrocortisone",
    # Diabetes
    "metformin", "glipizide",
    # Transplant / immunosuppression
    "tacrolimus", "cyclosporine", "mycophenolate",
    # Other
    "megestrol", "baclofen", "loperamide", "levothyroxine", "potassium iodide",
    "caffeine", "theophylline",
]

# Re-export the canonical sugar alcohol list and liquid form keywords so that
# routes_pages.py can reference them without depending on osmotic_filter directly.
SUGAR_ALCOHOLS: list[str] = list(osmotic_filter.SUGAR_ALCOHOL_CONCEPTS.keys())
_LIQUID_LOWER: frozenset[str] = frozenset(
    list(osmotic_filter._STRONG_FORM_KEYWORDS)
    + list(osmotic_filter._AMBIGUOUS_FORM_KEYWORDS)
)


@dataclass
class OsmoticProduct:
    product_name: str
    setid: str
    active_ingredient: str
    ndcs: list[str] = field(default_factory=list)
    labeler: str | None = None
    dosage_form: str | None = None
    route: str | None = None
    strength: str | None = None
    sugar_alcohols_found: list[str] = field(default_factory=list)  # canonical names
    concern_tier: str = "high"   # "high", "moderate", or "review"


@dataclass
class ActiveIngredientGroup:
    ingredient_name: str
    products: list[OsmoticProduct] = field(default_factory=list)

    @property
    def product_count(self) -> int:
        return len(self.products)


@dataclass
class OsmoticRiskIndex:
    """Pre-built index loaded from the local JSON file."""

    groups: list[ActiveIngredientGroup]
    total: int
    built_at: datetime | None = None
    source_files: list[str] = field(default_factory=list)


def load_prebuilt_index(index_path: Path) -> OsmoticRiskIndex | None:
    """Load the pre-built osmotic risk index from disk. Returns None if not present."""
    if not index_path.exists():
        return None
    try:
        data = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    groups: list[ActiveIngredientGroup] = []
    for g in data.get("groups", []):
        products = [
            OsmoticProduct(
                product_name=p["product_name"],
                setid=p["setid"],
                active_ingredient=p["active_ingredient"],
                ndcs=p.get("ndcs", []),
                labeler=p.get("labeler"),
                dosage_form=p.get("dosage_form"),
                route=p.get("route"),
                strength=p.get("strength"),
                sugar_alcohols_found=p.get("sugar_alcohols_found", []),
                concern_tier=p.get("concern_tier", "high"),
            )
            for p in g.get("products", [])
        ]
        groups.append(ActiveIngredientGroup(ingredient_name=g["ingredient_name"], products=products))

    built_at: datetime | None = None
    raw_built_at = data.get("built_at")
    if raw_built_at:
        try:
            built_at = datetime.fromisoformat(raw_built_at)
        except ValueError:
            pass

    return OsmoticRiskIndex(
        groups=groups,
        total=data.get("total", sum(len(g.products) for g in groups)),
        built_at=built_at,
        source_files=data.get("source_files", []),
    )


class OsmoticRiskService:
    """Search DailyMed for oral/enteral liquid drug products containing sugar alcohols."""

    _SEARCH_CONCURRENCY = 5
    _DETAIL_CONCURRENCY = 15

    def __init__(self, dailymed_api_repository: DailyMedApiRepository, data_root: Path) -> None:
        self._repo = dailymed_api_repository
        self._index_path = data_root / "osmotic_risk_index.json"

    def get_prebuilt_index(self) -> OsmoticRiskIndex | None:
        """Return the pre-built index if available, otherwise None."""
        return load_prebuilt_index(self._index_path)

    async def run(self) -> tuple[list[ActiveIngredientGroup], int]:
        """Search DailyMed live for oral/enteral liquid products with sugar alcohols.

        Phase 1: Search by drug ingredient name, pre-filter to products whose
                 product name suggests a liquid form.
        Phase 2: Fetch SPL details; apply osmotic_filter.evaluate() to classify
                 each product and assign a concern tier.

        Returns (groups sorted alphabetically by ingredient name, total count).
        Includes HIGH, MODERATE, and REVIEW tier products.
        """
        search_semaphore = asyncio.Semaphore(self._SEARCH_CONCURRENCY)

        async def search_ingredient(ingredient: str) -> list[str]:
            query = DrugQuery(raw_text=ingredient, normalized_text=ingredient)
            try:
                async with search_semaphore:
                    results = await self._repo.search_spls(query)
            except DailyMedAPIError:
                return []
            return [r.setid for r in results if r.setid and _name_is_liquid(r.product_name)]

        batches = await asyncio.gather(*(search_ingredient(ing) for ing in LIQUID_DRUG_INGREDIENTS))

        seen_setids: set[str] = set()
        setid_order: list[str] = []
        for batch in batches:
            for setid in batch:
                if setid not in seen_setids:
                    seen_setids.add(setid)
                    setid_order.append(setid)

        detail_semaphore = asyncio.Semaphore(self._DETAIL_CONCURRENCY)

        async def process(setid: str) -> list[OsmoticProduct]:
            try:
                async with detail_semaphore:
                    details = await self._repo.get_all_product_details(setid)
            except DailyMedAPIError:
                return []
            products = []
            for detail in details:
                if detail.product_type and "HUMAN" not in detail.product_type.upper():
                    continue
                decision = osmotic_filter.evaluate(
                    detail.dosage_form,
                    detail.route,
                    detail.inactive_ingredients,
                )
                if decision.concern_tier == "excluded":
                    continue
                products.append(OsmoticProduct(
                    product_name=detail.product_name,
                    setid=setid,
                    active_ingredient=_primary_active_ingredient(detail.active_ingredients),
                    ndcs=detail.ndcs,
                    labeler=detail.labeler,
                    dosage_form=detail.dosage_form,
                    route=detail.route,
                    strength=_build_strength(detail.active_ingredients),
                    sugar_alcohols_found=[sa.canonical_name for sa in decision.matched_sugar_alcohols],
                    concern_tier=decision.concern_tier,
                ))
            return products

        raw = await asyncio.gather(*(process(s) for s in setid_order))
        all_products = [p for group in raw for p in group]
        return _group_products(all_products)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _name_is_liquid(product_name: str) -> bool:
    """Return True if the product name contains a liquid dosage form keyword."""
    name_lower = product_name.casefold()
    return any(kw in name_lower for kw in _LIQUID_LOWER)


def _primary_active_ingredient(ingredients: list[IngredientEntry]) -> str:
    if not ingredients:
        return "Unknown"
    ing = ingredients[0]
    return ing.normalized_name or ing.display_name or ing.raw_name or "Unknown"


def _build_strength(ingredients: list[IngredientEntry]) -> str | None:
    strengths: list[str] = []
    seen: set[str] = set()
    for ing in ingredients:
        s = " ".join((ing.strength or "").split()).strip()
        if s and s.casefold() not in seen:
            seen.add(s.casefold())
            strengths.append(s)
    return ", ".join(strengths) if strengths else None


def _group_products(
    products: list[OsmoticProduct],
) -> tuple[list[ActiveIngredientGroup], int]:
    """Group products by active ingredient, sorted alphabetically."""
    groups: dict[str, ActiveIngredientGroup] = {}
    for product in products:
        key = product.active_ingredient.casefold()
        if key not in groups:
            groups[key] = ActiveIngredientGroup(ingredient_name=product.active_ingredient)
        groups[key].products.append(product)
    sorted_groups = sorted(groups.values(), key=lambda g: g.ingredient_name.casefold())
    total = sum(g.product_count for g in sorted_groups)
    return sorted_groups, total
