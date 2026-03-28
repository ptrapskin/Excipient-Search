from app.domain.enums import DataSource
from app.domain.models import ExcipientFilter, IngredientEntry, ProductDetail, ProductSearchResult
from app.services.excipient_filter import ExcipientFilterService, parse_filter_terms
from app.services.excipient_matching import normalize_excipient_text, term_matches_ingredient


def test_parse_filter_terms_splits_and_deduplicates():
    terms = parse_filter_terms("red dye, lactose; red dye")

    assert terms == ["red dye", "lactose"]


def test_parse_filter_terms_deduplicates_normalized_dye_variants():
    terms = parse_filter_terms("FD&C Red #40; fd and c red no 40; Red Dye")

    assert terms == ["FD&C Red #40", "Red Dye"]


def test_normalize_excipient_text_handles_dye_variants():
    assert normalize_excipient_text("FD&C Red #40") == "fdc red no 40"
    assert normalize_excipient_text("D&C Red No. 33") == "dc red no 33"


def test_term_matches_ingredient_handles_aliases_and_dye_family():
    dye = IngredientEntry(
        raw_name="FD&C Red No. 40",
        display_name="FD&C Red No. 40",
        normalized_name="FD&C Red No. 40",
        unii="WZB9127XOA",
        confidence="medium",
    )
    glycerin = IngredientEntry(
        raw_name="glycerin",
        display_name="glycerin",
        normalized_name="Glycerin",
        confidence="medium",
    )
    peg = IngredientEntry(
        raw_name="polyethylene glycol 400",
        display_name="polyethylene glycol 400",
        normalized_name="Polyethylene Glycol 400",
        confidence="medium",
    )
    lactitol = IngredientEntry(
        raw_name="lactitol monohydrate",
        display_name="lactitol monohydrate",
        normalized_name="Lactitol Monohydrate",
        confidence="medium",
    )

    assert term_matches_ingredient("red dye", dye) is True
    assert term_matches_ingredient("fd&c red #40", dye) is True
    assert term_matches_ingredient("WZB9127XOA", dye) is True
    assert term_matches_ingredient("glycerol", glycerin) is True
    assert term_matches_ingredient("peg", peg) is True
    assert term_matches_ingredient("polyethylene glycol", peg) is True
    assert term_matches_ingredient("lactitol", lactitol) is True
    assert term_matches_ingredient("peg 3350", peg) is False


def test_excipient_filter_service_marks_excluded_product():
    service = ExcipientFilterService()
    excipient_filter = ExcipientFilter(include_terms=[], exclude_terms=["red dye"])
    result = ProductSearchResult(
        product_name="Amoxicillin Suspension",
        setid="set-1",
        source=DataSource.DAILMED_LIVE,
    )
    detail = ProductDetail(
        product_name="Amoxicillin Suspension",
        setid="set-1",
        ndcs=["11111-111-11"],
        labeler="Labeler",
        active_ingredients=[],
        inactive_ingredients_raw="red dye 40; sucrose",
        inactive_ingredients=[
            IngredientEntry(raw_name="red dye 40", normalized_name="Red Dye 40", confidence="medium"),
            IngredientEntry(raw_name="sucrose", normalized_name="Sucrose", confidence="medium"),
        ],
        spl_source="https://example.test/spl.xml",
        fetched_at=detail_time(),
    )

    row = service.build_comparison_row(result, detail, excipient_filter)

    assert row.matches_filter is False
    assert row.matched_exclude_terms == ["red dye"]


def test_excipient_filter_service_builds_dailymed_strength_summary():
    service = ExcipientFilterService()
    excipient_filter = ExcipientFilter()
    result = ProductSearchResult(
        product_name="Combination Tablet",
        setid="set-2",
        source=DataSource.DAILMED_LIVE,
    )
    detail = ProductDetail(
        product_name="Combination Tablet",
        setid="set-2",
        ndcs=["22222-222-22"],
        labeler="Labeler",
        active_ingredients=[
            IngredientEntry(raw_name="drug a 5 mg", strength="5 mg", role="active", confidence="medium"),
            IngredientEntry(raw_name="drug b 500 mg", strength="500 mg", role="active", confidence="medium"),
            IngredientEntry(raw_name="drug c 5 mg", strength="5 mg", role="active", confidence="medium"),
        ],
        inactive_ingredients_raw="lactose",
        inactive_ingredients=[],
        spl_source="https://example.test/spl.xml",
        fetched_at=detail_time(),
    )

    row = service.build_comparison_row(result, detail, excipient_filter)

    assert row.dailymed_strength == "5 mg, 500 mg"


def detail_time():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)
