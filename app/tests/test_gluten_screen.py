import pytest

from app.domain.models import IngredientEntry
from app.services.gluten_screen import assess_product, classify_ingredient


def _ingredients(*names: str) -> list[IngredientEntry]:
    return [IngredientEntry(raw_name=name, display_name=name) for name in names]


@pytest.mark.parametrize(
    ("name", "category"),
    [
        ("STARCH, WHEAT", "gluten_source"),
        ("WHEAT GERM OIL", "gluten_source"),
        ("BARLEY MALT EXTRACT", "gluten_source"),
        ("STARCH, CORN", "non_wheat_starch"),
        ("CORN STARCH", "non_wheat_starch"),
        ("SODIUM STARCH GLYCOLATE TYPE A POTATO", "non_wheat_starch"),
        ("STARCH, PREGELATINIZED CORN", "non_wheat_starch"),
        ("STARCH, TAPIOCA", "non_wheat_starch"),
        ("STARCH", "unspecified_starch"),
        ("PREGELATINIZED STARCH", "unspecified_starch"),
        ("SODIUM STARCH GLYCOLATE", "unspecified_starch"),
        ("MANNITOL", "polyol"),
        ("HYDROGENATED STARCH HYDROLYSATE", "polyol"),
        ("OAT FLOUR", "oat"),
    ],
)
def test_classify_ingredient_categories(name, category):
    finding = classify_ingredient(name)

    assert finding is not None
    assert finding.category == category


@pytest.mark.parametrize("name", ["MALTODEXTRIN", "MALTOSE", "ETHYL MALTOL", "MICROCRYSTALLINE CELLULOSE", "LACTOSE MONOHYDRATE", "GLUTEN-FREE FLAVOR"])
def test_classify_ingredient_ignores_unrelated_excipients(name):
    assert classify_ingredient(name) is None


def test_assess_product_avoid_beats_verify():
    assessment = assess_product(_ingredients("STARCH", "STARCH, WHEAT", "MAGNESIUM STEARATE"))

    assert assessment.tier == "avoid"
    assert [f.ingredient for f in assessment.flagged] == ["STARCH", "STARCH, WHEAT"]


def test_assess_product_unspecified_starch_needs_verification():
    assert assess_product(_ingredients("PREGELATINIZED STARCH", "MANNITOL")).tier == "verify"


def test_assess_product_named_non_wheat_starch_is_low():
    assessment = assess_product(_ingredients("STARCH, CORN", "LACTOSE MONOHYDRATE"))

    assert assessment.tier == "low"
    assert assessment.flagged == []
    assert [f.ingredient for f in assessment.reassuring] == ["STARCH, CORN"]


def test_assess_product_without_ingredient_list_is_unknown():
    assert assess_product([]).tier == "unknown"


def test_assess_product_flags_olmesartan():
    assessment = assess_product(
        _ingredients("STARCH, CORN"),
        active_ingredients=_ingredients("OLMESARTAN MEDOXOMIL"),
    )

    assert assessment.olmesartan is True
    assert assessment.tier == "low"
