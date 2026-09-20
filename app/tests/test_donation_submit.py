import io
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import create_app

MINIMAL_PDF = b"%PDF-1.1\n1 0 obj<</Type/Catalog>>endobj\n%%EOF"

VALID_FIELDS = {
    "donor_name": "Jane Doe",
    "donor_street_address": "123 Main St",
    "donor_city": "Madison",
    "donor_state": "WI",
    "donor_zip_code": "53703",
    "recipient_facility": "St. Vincent de Paul Charitable Pharmacy",
    "date_donated": "2026-09-20",
    "date_signed": "2026-09-20",
    "items": [
        {
            "drug_name": "Lantus",
            "strength": "100 units/mL",
            "ndc": "00000-1234-56",
            "lot_number": "AB1234C",
            "expiration_date": "2027-12-31",
            "quantity": 5,
            "unit": "pens",
        }
    ],
}


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


@pytest.fixture
def client(tmp_path: Path):
    from app.api import routes_donation_submit

    routes_donation_submit._request_log.clear()
    db_path = tmp_path / "donation-submit-tests.db"
    os.environ["EXCIPIENT_SEARCH_DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["EXCIPIENT_SEARCH_COGNITO_API_KEY"] = "test-key"
    get_settings.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    os.environ.pop("EXCIPIENT_SEARCH_DATABASE_URL", None)
    os.environ.pop("EXCIPIENT_SEARCH_COGNITO_API_KEY", None)
    get_settings.cache_clear()
    routes_donation_submit._request_log.clear()


def post_submission(client, *, pdf_bytes=MINIMAL_PDF, content_type="application/pdf", fields=None):
    fields_json = json.dumps(fields if fields is not None else VALID_FIELDS)
    return client.post(
        "/api/donation-submit",
        files={"record": ("record.pdf", io.BytesIO(pdf_bytes), content_type)},
        data={"fields": fields_json},
    )


def mock_cognito_success(client):
    fake_post = AsyncMock(
        side_effect=[
            FakeResponse(200, {"Id": "F-fake-file-id"}),
            FakeResponse(200, {"Id": "38-1"}),
        ]
    )
    client.app.state.container.http_client.post = fake_post
    return fake_post


def test_rejects_non_pdf_content_type(client):
    mock_cognito_success(client)
    response = post_submission(client, content_type="text/plain")

    assert response.status_code == 400
    assert "PDF" in response.json()["detail"]


def test_rejects_oversized_file(client):
    mock_cognito_success(client)
    oversized = b"%PDF-1.1\n" + b"0" * (5 * 1024 * 1024 + 1)
    response = post_submission(client, pdf_bytes=oversized)

    assert response.status_code == 400


def test_rejects_content_that_is_not_actually_a_pdf(client):
    mock_cognito_success(client)
    response = post_submission(client, pdf_bytes=b"not a pdf at all")

    assert response.status_code == 400


def test_rejects_malformed_fields_json(client):
    mock_cognito_success(client)
    response = client.post(
        "/api/donation-submit",
        files={"record": ("record.pdf", io.BytesIO(MINIMAL_PDF), "application/pdf")},
        data={"fields": "{not valid json"},
    )

    assert response.status_code == 400


def test_rejects_fields_missing_required_data(client):
    mock_cognito_success(client)
    incomplete = {**VALID_FIELDS}
    del incomplete["donor_name"]
    response = post_submission(client, fields=incomplete)

    assert response.status_code == 400


def test_forwards_correct_payload_and_auth_to_cognito(client):
    fake_post = mock_cognito_success(client)
    response = post_submission(client)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert fake_post.call_count == 2

    upload_call = fake_post.call_args_list[0]
    assert upload_call.args[0] == "https://www.cognitoforms.com/api/files"
    assert upload_call.kwargs["params"] == {"access_token": "test-key"}
    assert "file" in upload_call.kwargs["files"]

    entry_call = fake_post.call_args_list[1]
    assert entry_call.args[0] == "https://www.cognitoforms.com/api/forms/38/entries"
    assert entry_call.kwargs["params"] == {"access_token": "test-key"}
    entry_json = entry_call.kwargs["json"]
    assert entry_json["DonorName"] == "Jane Doe"
    assert entry_json["DonationRecordPDF"] == [{"Id": "F-fake-file-id"}]
    assert entry_json["Entry"] == {"Action": "MarkSubmitted", "Role": "External Submission Agent"}
    assert entry_json["Items"][0]["DrugName"] == "Lantus"


def test_returns_generic_502_when_cognito_upload_fails(client):
    fake_post = AsyncMock(side_effect=[FakeResponse(500, {"Message": "internal error"})])
    client.app.state.container.http_client.post = fake_post

    response = post_submission(client)

    assert response.status_code == 502
    detail = response.json()["detail"]
    assert "Jane Doe" not in detail
    assert "123 Main St" not in detail


def test_returns_generic_502_when_cognito_entry_creation_fails(client):
    fake_post = AsyncMock(
        side_effect=[
            FakeResponse(200, {"Id": "F-fake-file-id"}),
            FakeResponse(400, {"Message": "File Id not specified.", "Data": {"Property": "DonationRecordPDF"}}),
        ]
    )
    client.app.state.container.http_client.post = fake_post

    response = post_submission(client)

    assert response.status_code == 502
    detail = response.json()["detail"]
    assert "Jane Doe" not in detail
    assert "File Id not specified" not in detail


def test_logs_never_contain_donor_phi(client, caplog):
    mock_cognito_success(client)
    with caplog.at_level("DEBUG"):
        response = post_submission(client)

    assert response.status_code == 200
    log_text = "\n".join(record.getMessage() for record in caplog.records)
    assert "Jane Doe" not in log_text
    assert "123 Main St" not in log_text
    assert "53703" not in log_text


def test_returns_500_when_api_key_not_configured(client):
    os.environ.pop("EXCIPIENT_SEARCH_COGNITO_API_KEY", None)
    get_settings.cache_clear()
    mock_cognito_success(client)

    response = post_submission(client)

    assert response.status_code == 500
    os.environ["EXCIPIENT_SEARCH_COGNITO_API_KEY"] = "test-key"
    get_settings.cache_clear()


def test_rate_limit_blocks_after_max_requests(client):
    from app.api import routes_donation_submit

    mock_cognito_success(client)

    for _ in range(routes_donation_submit.RATE_LIMIT_MAX_REQUESTS):
        client.app.state.container.http_client.post = AsyncMock(
            side_effect=[
                FakeResponse(200, {"Id": "F-fake-file-id"}),
                FakeResponse(200, {"Id": "38-1"}),
            ]
        )
        response = post_submission(client)
        assert response.status_code == 200

    client.app.state.container.http_client.post = mock_cognito_success(client)
    response = post_submission(client)
    assert response.status_code == 429
