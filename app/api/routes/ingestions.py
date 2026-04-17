from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import IngestionRun
from app.services.ingestion_service import queue_ingestion_path
from app.tasks.ingestion_tasks import run_ingestion_task

router = APIRouter()


@router.post("")
def trigger_ingestion(
    file_path: str | None = None,
) -> dict:
    try:
        run_id, resolved = queue_ingestion_path(file_path)
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=f"file not found: {e.args[0]}") from e
    run_ingestion_task.delay(run_id, str(resolved.resolve()))
    return {"run_id": run_id, "status": "queued"}


@router.get("/{run_id}")
def ingestion_detail(run_id: int, db: Session = Depends(get_db)) -> dict:
    run = db.get(IngestionRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="ingestion run not found")
    return {
        "ingestion_id": run.id,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "total_records": run.total_records,
        "success_count": run.success_count,
        "error_count": run.error_count,
        "skipped_count": run.skipped_count,
        "status": run.status,
        "events": [
            {
                "external_id": e.external_id,
                "status": e.status,
                "message": e.message,
                "stage": e.stage,
            }
            for e in run.events
        ],
    }
