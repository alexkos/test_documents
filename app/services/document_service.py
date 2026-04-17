from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.config import elasticsearch_enabled
from app.models import Document, Organization, Tag
from app.search.queries import search_document_ids
from app.utils.logger import logger

_SKIP_COLUMNS = frozenset({"author_id", "organization_id"})

_LOG_SEARCH_MAX = 160


def format_search_query_for_log(search_term: str) -> str:
    if len(search_term) <= _LOG_SEARCH_MAX:
        return search_term
    return f"{search_term[: _LOG_SEARCH_MAX - 3]}..."


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
    search_term = (search or "").strip()
    sql_search_fallback = False
    sql_search_reason = ""

    if search_term and elasticsearch_enabled():
        try:
            ids, total = search_document_ids(
                search_term,
                skip=skip,
                limit=limit,
                date_from=df,
                date_to=dt,
                tag=tag,
                organization=organization,
                status=status,
            )
            qlog = format_search_query_for_log(search_term)
            if not ids:
                logger.info(
                    f"Document search: Elasticsearch returned no documents "
                    f"(total={total}) query={qlog!r}"
                )
                logger.info(
                    f"Document list: data_from=elasticsearch (no hits); "
                    f"postgresql not queried for items total={total}"
                )
                return {
                    "items": [],
                    "total": total,
                    "skip": skip,
                    "limit": limit,
                }
            logger.info(
                f"Document search: Elasticsearch matched total={total} "
                f"page_count={len(ids)} query={qlog!r}"
            )
            id_order = {doc_id: idx for idx, doc_id in enumerate(ids)}
            q = (
                select(Document)
                .where(Document.id.in_(ids))
                .options(
                    selectinload(Document.author),
                    selectinload(Document.organization),
                    selectinload(Document.tags),
                )
            )
            rows = list(db.execute(q).scalars().all())
            rows.sort(key=lambda d: id_order.get(d.id, 10**9))
            logger.info(
                f"Document list: data_from=elasticsearch (match/rank) + postgresql "
                f"(load rows by id) total={total} page_count={len(rows)}"
            )
            return {
                "items": [_document_to_out(d) for d in rows],
                "total": total,
                "skip": skip,
                "limit": limit,
            }
        except Exception as e:
            sql_search_fallback = True
            sql_search_reason = "elasticsearch search failed"
            logger.warning(f"Elasticsearch search failed; falling back to SQL ilike: {e}")

    if search_term and not elasticsearch_enabled():
        sql_search_fallback = True
        sql_search_reason = "elasticsearch disabled or ELASTICSEARCH_URL unset"

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
    if search_term:
        term = f"%{search_term}%"
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
    if search_term and sql_search_fallback:
        qlog = format_search_query_for_log(search_term)
        logger.info(
            f"Document search: using SQL ilike ({sql_search_reason}) "
            f"total={total} page_count={len(rows)} query={qlog!r}"
        )
    logger.info(
        f"Document list: data_from=postgresql (sql) total={total} page_count={len(rows)}"
    )
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
