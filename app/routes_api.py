from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import resolved_jsonl_path
from app.db import get_session_factory, get_db
from app.ingestion import create_ingestion_run
from app.models import Document, IngestionRun, Tag
from app.services import document_service
from app.tasks.ingestion_tasks import run_ingestion_task
from app.utils.logger import logger

router = APIRouter()


def _document_to_out(doc: Document) -> dict:
    return {
        **{c.name: getattr(doc, c.name) for c in Document.__table__.columns},
        "tags": [t.name for t in doc.tags],
    }


@router.post("/ingestions")
def run_ingestion(file_path: str | None = None) -> dict:
    path = Path(file_path) if file_path else resolved_jsonl_path()
    if not path.is_file():
        logger.warning(f"Ingestion requested but file not found: {path}")
        raise HTTPException(status_code=400, detail=f"file not found: {path}")

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run = create_ingestion_run(db, queued=True)
        run_id = run.id
        db.commit()
    finally:
        db.close()

    run_ingestion_task.delay(run_id, str(path.resolve()))
    logger.info(f"Queued ingestion run_id={run_id} path={path.resolve()} (status=queued)")
    return {"run_id": run_id, "status": "queued"}


@router.get("/ingestions/{run_id}")
def get_ingestion(run_id: int, db: Session = Depends(get_db)) -> dict:
    run = db.get(IngestionRun, run_id)
    if run is None:
        logger.warning(f"GET /ingestions/{run_id}: run not found")
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


@router.get("/documents")
def list_documents(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    date_from: str | None = None,
    date_to: str | None = None,
    tag: str | None = None,
    organization: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> dict:
    return document_service.list_documents(
        db,
        skip=skip,
        limit=limit,
        date_from=date_from,
        date_to=date_to,
        tag=tag,
        organization=organization,
        status=status,
        search=search,
    )


@router.get("/documents/{doc_id}")
def get_document(doc_id: int, db: Session = Depends(get_db)) -> dict:
    doc = db.get(Document, doc_id)
    if doc is None:
        logger.warning(f"GET /documents/{doc_id}: document not found")
        raise HTTPException(status_code=404, detail="document not found")
    return _document_to_out(doc)


@router.get("/stats")
def stats(db: Session = Depends(get_db)) -> dict:
    total = db.scalar(select(func.count()).select_from(Document)) or 0

    by_status: dict[str, int] = {}
    for st, cnt in db.execute(
        select(Document.status, func.count()).group_by(Document.status)
    ).all():
        key = st or "null"
        by_status[key] = int(cnt)

    by_type: dict[str, int] = {}
    for dt_row, cnt in db.execute(
        select(Document.document_type, func.count()).group_by(Document.document_type)
    ).all():
        key = dt_row or "null"
        by_type[key] = int(cnt)

    top_tags_rows = db.execute(
        select(Tag.name, func.count())
        .join(Tag.documents)
        .group_by(Tag.name)
        .order_by(func.count().desc())
        .limit(10)
    ).all()
    top_tags = [{"name": n, "count": int(c)} for n, c in top_tags_rows]

    avg = db.scalar(select(func.avg(Document.score)).where(Document.score.isnot(None)))

    return {
        "total_documents": int(total),
        "by_status": by_status,
        "by_type": by_type,
        "top_tags": top_tags,
        "avg_score": float(avg) if avg is not None else None,
    }
