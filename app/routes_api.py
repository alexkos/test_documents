from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import resolved_jsonl_path
from app.db import get_session_factory, get_db
from app.ingestion import create_ingestion_run, ingest_file
from app.models import Document, IngestionRun, Organization, Tag

router = APIRouter()


def _document_to_out(doc: Document) -> dict:
    return {
        **{c.name: getattr(doc, c.name) for c in Document.__table__.columns},
        "tags": [t.name for t in doc.tags],
    }


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    return date.fromisoformat(s[:10])


@router.post("/ingestions")
def run_ingestion(
    background_tasks: BackgroundTasks,
    file_path: str | None = None,
) -> dict:
    path = Path(file_path) if file_path else resolved_jsonl_path()
    if not path.is_file():
        raise HTTPException(status_code=400, detail=f"file not found: {path}")

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run = create_ingestion_run(db)
        run_id = run.id
        db.commit()
    finally:
        db.close()

    def _job() -> None:
        db2 = SessionLocal()
        try:
            run_row = db2.get(IngestionRun, run_id)
            if run_row is None:
                return
            ingest_file(db2, path, run_row)
            db2.commit()
        except Exception:
            db2.rollback()
            run_row = db2.get(IngestionRun, run_id)
            if run_row is not None:
                run_row.status = "failed"
                run_row.finished_at = datetime.now(timezone.utc)
                db2.commit()
        finally:
            db2.close()

    background_tasks.add_task(_job)
    return {"run_id": run_id}


@router.get("/ingestions/{run_id}")
def get_ingestion(run_id: int, db: Session = Depends(get_db)) -> dict:
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
    df = _parse_date(date_from)
    dt = _parse_date(date_to)

    q = select(Document)
    count_q = select(func.count(func.distinct(Document.id))).select_from(Document)

    if tag:
        q = q.join(Document.tags).where(Tag.name == tag)
        count_q = count_q.join(Document.tags).where(Tag.name == tag)
    if organization:
        q = q.join(Document.organization).where(Organization.name == organization)
        count_q = count_q.join(Document.organization).where(Organization.name == organization)
    if status:
        q = q.where(Document.status == status)
        count_q = count_q.where(Document.status == status)
    if df:
        q = q.where(Document.published_at >= df)
        count_q = count_q.where(Document.published_at >= df)
    if dt:
        q = q.where(Document.published_at <= dt)
        count_q = count_q.where(Document.published_at <= dt)
    if search:
        term = f"%{search}%"
        q = q.where((Document.title.ilike(term)) | (Document.body.ilike(term)))
        count_q = count_q.where((Document.title.ilike(term)) | (Document.body.ilike(term)))

    total = db.scalar(count_q) or 0
    q = q.distinct().offset(skip).limit(limit)
    rows = db.execute(q).scalars().unique().all()
    return {
        "items": [_document_to_out(d) for d in rows],
        "total": total,
        "skip": skip,
        "limit": limit,
    }


@router.get("/documents/{doc_id}")
def get_document(doc_id: int, db: Session = Depends(get_db)) -> dict:
    doc = db.get(Document, doc_id)
    if doc is None:
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
