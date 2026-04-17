from __future__ import annotations

from datetime import datetime
from typing import Any

from app.exceptions import IngestionValidationError
from app.ingestion.normalizer import NormalizedRecord


REQUIRED_FIELDS = ("external_id",)


def is_valid_date_str(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if len(s) < 10:
        return False
    try:
        datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        return False
    else:
        return True


def validate_raw_dates(record: dict[str, Any]) -> None:
    for field in ("published_at", "updated_at"):
        value = record.get(field)
        if value is None or value == "":
            continue
        if not is_valid_date_str(value):
            raise IngestionValidationError(f"Invalid date: {field}={value!r}")


def validate_raw_record(record: dict[str, Any]) -> None:
    if not record:
        raise IngestionValidationError("empty record")
    for field in REQUIRED_FIELDS:
        v = record.get(field)
        if v is None or (isinstance(v, str) and not v.strip()):
            raise IngestionValidationError(f"missing required field: {field}")
    validate_raw_dates(record)


def validate_normalized(rec: NormalizedRecord) -> None:
    if not rec.external_id:
        raise IngestionValidationError("missing external_id")
