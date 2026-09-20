"""Stateless endpoint that forwards a generated donation record to Cognito
Forms. No database, no disk writes — the PDF is streamed straight through to
Cognito's API and never persisted here. See app.services.cognito_donation_service
for the two-step upload/entry-create flow this calls.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict, deque

from fastapi import APIRouter, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field, ValidationError

from app.api.dependencies import get_container
from app.config import get_settings
from app.services.cognito_donation_service import CognitoSubmissionError, submit_donation_record

router = APIRouter(prefix="/api", tags=["donation-submit"])

logger = logging.getLogger("donation_submit")

MAX_PDF_BYTES = 5 * 1024 * 1024  # 5 MB

# Simple in-memory per-IP rate limit — abuse mitigation only, not a security
# boundary. Resets on process restart; fine for a low-traffic donation tool
# with no auth in front of it. Intentionally holds no PHI, just IP + timestamps.
RATE_LIMIT_MAX_REQUESTS = 5
RATE_LIMIT_WINDOW_SECONDS = 60.0
_request_log: dict[str, deque[float]] = defaultdict(deque)


def _check_rate_limit(client_ip: str) -> None:
    now = time.monotonic()
    timestamps = _request_log[client_ip]
    while timestamps and now - timestamps[0] > RATE_LIMIT_WINDOW_SECONDS:
        timestamps.popleft()
    if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS:
        raise HTTPException(status_code=429, detail="Too many submissions — please wait a minute and try again.")
    timestamps.append(now)


class DonationItemIn(BaseModel):
    drug_name: str = Field(min_length=1)
    strength: str = ""
    ndc: str = ""
    lot_number: str = ""
    expiration_date: str = ""  # ISO yyyy-mm-dd, or "" if not provided
    quantity: float = Field(gt=0)
    unit: str = ""


class DonationFieldsIn(BaseModel):
    donor_name: str = Field(min_length=1)
    donor_street_address: str = Field(min_length=1)
    donor_city: str = Field(min_length=1)
    donor_state: str = Field(min_length=1)
    donor_zip_code: str = Field(min_length=1)
    recipient_facility: str = Field(min_length=1)
    date_donated: str = Field(min_length=1)  # ISO yyyy-mm-dd
    date_signed: str = ""  # ISO yyyy-mm-dd, or "" if unsigned
    items: list[DonationItemIn] = Field(min_length=1)


def _to_cognito_fields(fields: DonationFieldsIn) -> dict:
    """Map the app's field names to Cognito's exact form field names."""

    return {
        "DonorName": fields.donor_name,
        "DonorStreetAddress": fields.donor_street_address,
        "DonorCity": fields.donor_city,
        "DonorState": fields.donor_state,
        "DonorZipCode": fields.donor_zip_code,
        "RecipientFacility": fields.recipient_facility,
        "DateDonated": fields.date_donated,
        "DateSigned": fields.date_signed or None,
        "Items": [
            {
                "DrugName": item.drug_name,
                "Strength": item.strength or None,
                "NDC": item.ndc or None,
                "LotNumber": item.lot_number or None,
                "ExpirationDate": item.expiration_date or None,
                "Quantity": item.quantity,
                "Unit": item.unit or None,
            }
            for item in fields.items
        ],
    }


@router.post("/donation-submit")
async def donation_submit(request: Request, record: UploadFile, fields: str = Form(...)):
    start = time.monotonic()
    status_code = 500
    try:
        client_ip = request.client.host if request.client else "unknown"
        _check_rate_limit(client_ip)

        if record.content_type != "application/pdf":
            status_code = 400
            raise HTTPException(status_code=400, detail="The record must be a PDF file.")

        pdf_bytes = await record.read()
        if len(pdf_bytes) > MAX_PDF_BYTES:
            status_code = 400
            raise HTTPException(status_code=400, detail="The record file is too large.")
        if not pdf_bytes.startswith(b"%PDF"):
            status_code = 400
            raise HTTPException(status_code=400, detail="The record must be a valid PDF file.")

        try:
            fields_dict = json.loads(fields)
        except json.JSONDecodeError as exc:
            status_code = 400
            raise HTTPException(status_code=400, detail="Malformed submission data.") from exc

        try:
            parsed_fields = DonationFieldsIn.model_validate(fields_dict)
        except ValidationError as exc:
            status_code = 400
            raise HTTPException(status_code=400, detail="Malformed submission data.") from exc

        settings = get_settings()
        if not settings.cognito_api_key:
            logger.error("donation-submit misconfigured: no Cognito API key set")
            status_code = 500
            raise HTTPException(status_code=500, detail="Submission is not configured. Use Print instead.")

        container = get_container(request)
        try:
            await submit_donation_record(
                container.http_client,
                api_key=settings.cognito_api_key,
                form_id=settings.cognito_donation_form_id,
                pdf_bytes=pdf_bytes,
                fields=_to_cognito_fields(parsed_fields),
            )
        except CognitoSubmissionError as exc:
            logger.warning("donation-submit failed: %s", exc)
            status_code = 502
            raise HTTPException(status_code=502, detail="Could not send the record. Use Print instead.") from exc

        status_code = 200
        return {"status": "ok"}
    except HTTPException as exc:
        status_code = exc.status_code
        raise
    finally:
        duration_ms = round((time.monotonic() - start) * 1000, 1)
        logger.info("donation-submit status=%s duration_ms=%s", status_code, duration_ms)
