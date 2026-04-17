from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.models import Document, Organization, Tag

_SKIP_COLUMNS = frozenset({"author_id", "organization_id"})


def _document_to_out(doc: Document) -> dict:
    base = {
        c.name: getattr(doc, c.name)
        for c in Document.__table__.columns
        if c.name not in _SKIP_COLUMNS
    }
    author: dict[str, int | str] | None = None
    if doc.author_id is not None and doc.author is not None:
        author = {"id": doc.author.id, "name": doc.author.name}
    organization: dict[str, int | str] | None = None
    if doc.organization_id is not None and doc.organization is not None:
        organization = {"id": doc.organization.id, "name": doc.organization.name}
    return {
        **base,
        "author": author,
        "organization": organization,
        "tags": [t.name for t in doc.tags],
    }


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    return date.fromisoformat(s[:10])


def list_documents(
    db: Session,
    *,
    skip: int = 0,
    limit: int = 20,
    date_from: str | None = None,
    date_to: str | None = None,
    tag: str | None = None,
    organization: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> dict:
    df = _parse_date(date_from)
    dt = _parse_date(date_to)

    # DISTINCT on full Document rows fails on PostgreSQL when `keywords` is JSON (no = operator).
    # De-duplicate by document id first, then load rows (portable across SQLite/Postgres).
    id_q = select(Document.id).select_from(Document)
    count_q = select(func.count(func.distinct(Document.id))).select_from(Document)

    if tag:
        id_q = id_q.join(Document.tags).where(Tag.name == tag)
        count_q = count_q.join(Document.tags).where(Tag.name == tag)
    if organization:
        id_q = id_q.join(Document.organization).where(Organization.name == organization)
        count_q = count_q.join(Document.organization).where(Organization.name == organization)
    if status:
        id_q = id_q.where(Document.status == status)
        count_q = count_q.where(Document.status == status)
    if df:
        id_q = id_q.where(Document.published_at >= df)
        count_q = count_q.where(Document.published_at >= df)
    if dt:
        id_q = id_q.where(Document.published_at <= dt)
        count_q = count_q.where(Document.published_at <= dt)
    if search:
        term = f"%{search}%"
        id_q = id_q.where((Document.title.ilike(term)) | (Document.body.ilike(term)))
        count_q = count_q.where((Document.title.ilike(term)) | (Document.body.ilike(term)))

    total = db.scalar(count_q) or 0
    id_subq = id_q.distinct().order_by(Document.id).offset(skip).limit(limit).subquery()
    q = (
        select(Document)
        .options(selectinload(Document.author), selectinload(Document.organization))
        .join(id_subq, Document.id == id_subq.c.id)
        .order_by(Document.id)
    )
    rows = db.execute(q).scalars().all()
    return {
        "items": [_document_to_out(d) for d in rows],
        "total": total,
        "skip": skip,
        "limit": limit,
    }


def get_document(db: Session, doc_id: int) -> dict | None:
    doc = db.scalar(
        select(Document)
        .where(Document.id == doc_id)
        .options(selectinload(Document.author), selectinload(Document.organization))
    )
    if doc is None:
        return None
    return _document_to_out(doc)
