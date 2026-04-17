from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.exceptions import IngestionValidationError, SemanticDuplicateError
from app.ingestion.normalizer import normalize_raw_record
from app.ingestion.parser import parse_line
from app.ingestion.validator import validate_normalized
from app.models import IngestionRun
from app.repositories.document_repo import upsert_document
from app.repositories.ingestion_repo import log_event, utcnow


def ingest_file(session: Session, path: Path, run: IngestionRun) -> None:
    run.started_at = run.started_at or utcnow()
    run.status = "running"

    success = error = skipped = 0
    total = 0

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            raw: dict[str, Any] | None = None
            ext_for_log: str | None = None
            try:
                raw = parse_line(line)
            except json.JSONDecodeError as e:
                error += 1
                log_event(
                    session,
                    run,
                    external_id=None,
                    stage="parsing",
                    status="error",
                    message=f"invalid JSON: {e}",
                )
                continue

            if len(raw) == 0:
                error += 1
                log_event(
                    session,
                    run,
                    external_id=None,
                    stage="parsing",
                    status="error",
                    message="empty record",
                )
                continue

            ext_for_log = str(raw.get("external_id", "")).strip() or None

            try:
                normalized = normalize_raw_record(raw)
                validate_normalized(normalized)
                upsert_document(session, normalized)
                success += 1
                log_event(
                    session,
                    run,
                    external_id=normalized.external_id,
                    stage="completed",
                    status="success",
                    raw_payload=None,
                )
            except IngestionValidationError as e:
                error += 1
                log_event(
                    session,
                    run,
                    external_id=ext_for_log,
                    stage="validation",
                    status="error",
                    message=e.message,
                    raw_payload=raw,
                )
            except SemanticDuplicateError as e:
                skipped += 1
                log_event(
                    session,
                    run,
                    external_id=ext_for_log,
                    stage="deduplication",
                    status="skipped",
                    message=e.message,
                    raw_payload=raw,
                )

    run.total_records = total
    run.success_count = success
    run.error_count = error
    run.skipped_count = skipped
    run.finished_at = utcnow()
    run.status = "completed"
