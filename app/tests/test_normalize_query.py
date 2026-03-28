from app.services.normalize_query import build_query_key, normalize_query


def test_normalize_query_trims_and_preserves_raw_text():
    query = normalize_query("  metformin   oral   tablet  ")

    assert query.raw_text == "  metformin   oral   tablet  "
    assert query.normalized_text == "metformin oral tablet"
    assert query.requested_route == "ORAL"
    assert query.requested_dose_form == "TABLET"


def test_normalize_query_detects_ndc_and_strength():
    query = normalize_query("0002-8215-01 amoxicillin 500 mg capsule")

    assert query.ndc == "0002-8215-01"
    assert query.requested_strength == "500 mg"
    assert query.requested_dose_form == "CAPSULE"


def test_normalize_query_strips_punctuation_and_normalizes_units():
    query = normalize_query(" amoxicillin, 400mg / 5ml suspension!!! ")

    assert query.normalized_text == "amoxicillin 400 mg/5 mL suspension"
    assert query.requested_strength == "400 mg/5 mL"
    assert query.requested_dose_form == "SUSPENSION"


def test_normalize_query_detects_route_and_dose_form_from_messy_text():
    query = normalize_query("Acyclovir ophthalmic solution")

    assert query.normalized_text == "Acyclovir ophthalmic solution"
    assert query.requested_route == "OPHTHALMIC"
    assert query.requested_dose_form == "SOLUTION"


def test_build_query_key_is_stable_for_same_normalized_query():
    first = normalize_query("Metformin tablet")
    second = normalize_query("Metformin   tablet")

    assert build_query_key(first) == build_query_key(second)
