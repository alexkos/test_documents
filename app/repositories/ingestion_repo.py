from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models import IngestionEvent, IngestionRun
from app.utils.logger import logger


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_ingestion_run(session: Session, *, queued: bool = False) -> IngestionRun:
    """Create a run. Use ``queued=True`` for API/Celery (``started_at`` set when worker begins)."""
    if queued:
        run = IngestionRun(
            started_at=None,
            status="queued",
            total_records=0,
            success_count=0,
            error_count=0,
            skipped_count=0,
        )
    else:
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
    logger.debug(f"create_ingestion_run id={run.id} queued={queued} status={run.status}")
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
