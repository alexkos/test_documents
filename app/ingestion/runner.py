from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.exceptions import IngestionValidationError, SemanticDuplicateError
from app.ingestion.normalizer import normalize_raw_record
from app.ingestion.parser import parse_line
from app.ingestion.validator import validate_normalized, validate_raw_record
from app.models import IngestionRun
from app.repositories.document_repo import upsert_document
from app.repositories.ingestion_repo import log_event, utcnow
from app.utils.logger import logger


def ingest_file(session: Session, path: Path, run: IngestionRun) -> None:
    run.started_at = run.started_at or utcnow()
    run.status = "running"

    success = error = skipped = 0
    total = 0

    logger.info(f"Starting ingestion run_id={run.id} path={path}")

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            raw: dict[str, Any] | None = None
            ext_for_log: str | None = None
            raw = parse_line(line)
            if raw is None:
                error += 1
                logger.debug(f"Parse failure or empty JSON on line (run_id={run.id})")
                log_event(
                    session,
                    run,
                    external_id=None,
                    stage="parsing",
                    status="error",
                    message="invalid JSON or empty payload",
                )
                continue

            if len(raw) == 0:
                error += 1
                logger.debug(f"Empty JSON object record (run_id={run.id})")
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
                validate_raw_record(raw)
                normalized = normalize_raw_record(raw)
                validate_normalized(normalized)
                upsert_document(session, normalized)
                success += 1
                logger.debug(f"Ingested external_id={normalized.external_id!r} run_id={run.id}")
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
                logger.debug(
                    f"Validation error run_id={run.id} external_id={ext_for_log!r}: {e.message}"
                )
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
                logger.debug(
                    f"Skipped row after duplicate handling run_id={run.id} external_id={ext_for_log!r}: {e.message}"
                )
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
    logger.info(
        f"Finished ingestion run_id={run.id} total={total} success={success} errors={error} skipped={skipped}"
    )
