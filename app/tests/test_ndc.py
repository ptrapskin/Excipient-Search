import pytest

from app.services.ndc import ndc11_list, ndc_search_keys, to_ndc11


@pytest.mark.parametrize(
    ("ndc", "expected"),
    [
        ("0378-1800-01", "00378180001"),   # 4-4-2
        ("51662-140-12", "51662014012"),   # 5-3-2
        ("51662-1407-1", "51662140701"),   # 5-4-1
        ("00378-1800-01", "00378180001"),  # 11-digit hyphenated
        ("00378180001", "00378180001"),
        ("0378-1800", "003781800"),        # product code
        ("not-an-ndc", None),
    ],
)
def test_to_ndc11(ndc, expected):
    assert to_ndc11(ndc) == expected


def test_ndc11_list_joins_package_codes():
    assert ndc11_list("0378-1800-01; 51662-1407-1") == "00378180001 51662140701"


def test_unhyphenated_ten_digit_query_tries_each_layout():
    assert ndc_search_keys("0378180001") == ["00378180001", "03781080001", "03781800001"]
    assert ndc_search_keys("00378-1800-01") == ["00378180001"]
