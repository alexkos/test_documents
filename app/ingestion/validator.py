from __future__ import annotations

import re
from urllib.parse import urlparse

from app.exceptions import IngestionValidationError
from app.ingestion.normalizer import NormalizedRecord

_DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$")


def validate_normalized(rec: NormalizedRecord) -> None:
    if not rec.external_id:
        raise IngestionValidationError("missing external_id")

    if rec.doi and not _DOI_PATTERN.match(rec.doi):
        raise IngestionValidationError(f"invalid DOI: {rec.doi}")

    if rec.url:
        parsed = urlparse(rec.url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise IngestionValidationError(f"invalid URL: {rec.url}")
