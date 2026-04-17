"""Background ingestion jobs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.celery_app import celery_app
from app.db import get_session_factory
from app.ingestion.runner import ingest_file
from app.models import IngestionRun
from app.utils.logger import logger


@celery_app.task(name="ingestion.run_ingestion_task")
def run_ingestion_task(run_id: int, file_path: str) -> dict[str, Any]:
    """Execute JSONL ingestion for a persisted run. Uses a fresh DB session (not FastAPI's)."""
    path = Path(file_path)
    logger.info("Celery ingestion task started run_id=%s path=%s", run_id, path)
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run_row = db.get(IngestionRun, run_id)
        if run_row is None:
            logger.error("Celery ingestion task: run_id=%s not found in database", run_id)
            return {"run_id": run_id, "missing": 1}

        ingest_file(db, path, run_row)
        db.commit()
        logger.info(
            "Celery ingestion task completed run_id=%s total=%s success=%s errors=%s skipped=%s",
            run_id,
            run_row.total_records,
            run_row.success_count,
            run_row.error_count,
            run_row.skipped_count,
        )
        return {
            "run_id": run_id,
            "total_records": run_row.total_records,
            "success_count": run_row.success_count,
            "error_count": run_row.error_count,
            "skipped_count": run_row.skipped_count,
        }
    except Exception:
        logger.exception("Celery ingestion task failed run_id=%s path=%s", run_id, path)
        db.rollback()
        run_row = db.get(IngestionRun, run_id)
        if run_row is not None:
            run_row.status = "failed"
            run_row.finished_at = datetime.now(timezone.utc)
            db.commit()
        raise
    finally:
        db.close()
