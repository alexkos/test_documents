from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Document, Tag


def get_stats(db: Session) -> dict:
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
