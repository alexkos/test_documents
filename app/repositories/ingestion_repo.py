from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models import IngestionEvent, IngestionRun


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_ingestion_run(session: Session) -> IngestionRun:
    run = IngestionRun(
        started_at=utcnow(),
        status="running",
        total_records=0,
        success_count=0,
        error_count=0,
        skipped_count=0,
    )
    session.add(run)
    session.flush()
    return run


def log_event(
    session: Session,
    run: IngestionRun,
    *,
    external_id: str | None,
    stage: str,
    status: str,
    message: str | None = None,
    raw_payload: dict[str, Any] | None = None,
) -> None:
    ev = IngestionEvent(
        ingestion_id=run.id,
        external_id=external_id,
        stage=stage,
        status=status,
        message=message,
        raw_payload=raw_payload,
        created_at=utcnow(),
    )
    session.add(ev)
