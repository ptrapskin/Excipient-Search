"""Forwards a completed drug donation record to the organization's Cognito
Forms account (form "Drug Donation Record Submission").

Cognito's Entries API requires a two-step flow for a File Upload field —
confirmed against a live test form and test entries (since removed), not
guessed from documentation, which does not spell this out:
  1. POST /api/files - upload the PDF bytes, get back a File Id.
  2. POST /api/forms/{formId}/entries - create the entry, referencing that
     File Id (not the raw file content) in the DonationRecordPDF field.
Both calls authenticate via an `access_token` query parameter carrying the
organization's Cognito API key. Submitting an entry also requires an
Entry.Action/Entry.Role pair matching the form's single workflow action.
"""

from __future__ import annotations

import httpx

COGNITO_FILES_URL = "https://www.cognitoforms.com/api/files"
COGNITO_ENTRIES_URL_TEMPLATE = "https://www.cognitoforms.com/api/forms/{form_id}/entries"


class CognitoSubmissionError(Exception):
    """Raised when Cognito Forms rejects the file upload or entry creation.

    The message is safe to surface to the caller (it never includes request
    bodies, form field values, or the API key) but is intentionally generic —
    callers should not relay Cognito's raw response text, which could echo
    back submitted PHI.
    """


async def submit_donation_record(
    http_client: httpx.AsyncClient,
    *,
    api_key: str,
    form_id: str,
    pdf_bytes: bytes,
    fields: dict,
) -> str:
    """Upload the PDF and create the Cognito Forms entry.

    `fields` must already be in Cognito's field-name shape (DonorName,
    DonorStreetAddress, ... , Items). Returns the created entry's Id.
    """

    try:
        upload_response = await http_client.post(
            COGNITO_FILES_URL,
            params={"access_token": api_key},
            files={"file": ("donation-record.pdf", pdf_bytes, "application/pdf")},
        )
    except httpx.HTTPError as exc:
        raise CognitoSubmissionError("file upload request failed") from exc

    if upload_response.status_code != 200:
        raise CognitoSubmissionError(f"file upload rejected (status {upload_response.status_code})")

    file_id = upload_response.json().get("Id")
    if not file_id:
        raise CognitoSubmissionError("file upload response missing Id")

    entry_data = {
        **fields,
        "Entry": {"Action": "MarkSubmitted", "Role": "External Submission Agent"},
        "DonationRecordPDF": [{"Id": file_id}],
    }

    try:
        entry_response = await http_client.post(
            COGNITO_ENTRIES_URL_TEMPLATE.format(form_id=form_id),
            params={"access_token": api_key},
            json=entry_data,
        )
    except httpx.HTTPError as exc:
        raise CognitoSubmissionError("entry creation request failed") from exc

    if entry_response.status_code != 200:
        raise CognitoSubmissionError(f"entry creation rejected (status {entry_response.status_code})")

    entry_id = entry_response.json().get("Id")
    if not entry_id:
        raise CognitoSubmissionError("entry creation response missing Id")
    return entry_id
