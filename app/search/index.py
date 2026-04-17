from __future__ import annotations

from datetime import date

from elasticsearch import ApiError, Elasticsearch
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.config import elasticsearch_enabled
from app.models import Document
from app.search.client import INDEX_NAME, get_es_client
from app.utils.logger import logger

# document_id: Postgres primary key; used for sort tie-break (ES 8+ disallows sorting on _id).
_INDEX_PROPERTIES: dict = {
    "title": {"type": "text"},
    "abstract": {"type": "text"},
    "body": {"type": "text"},
    "tags": {"type": "keyword"},
    "organization": {"type": "keyword"},
    "status": {"type": "keyword"},
    "published_at": {"type": "date"},
    "score": {"type": "float"},
    "document_id": {"type": "integer"},
}


def _ensure_document_id_mapping(es: Elasticsearch) -> None:
    """Add document_id to indexes created before this field existed."""
    try:
        es.indices.put_mapping(
            index=INDEX_NAME,
            properties={"document_id": {"type": "integer"}},
        )
    except Exception as e:
        logger.debug(f"Elasticsearch put_mapping document_id skipped or failed: {e}")


def ensure_index(es: Elasticsearch) -> None:
    if es.indices.exists(index=INDEX_NAME):
        _ensure_document_id_mapping(es)
        return
    es.indices.create(
        index=INDEX_NAME,
        mappings={"properties": _INDEX_PROPERTIES},
    )


def ensure_elasticsearch_index() -> Elasticsearch | None:
    """Create the ``documents`` index if missing. Returns the client when ES is usable."""
    if not elasticsearch_enabled():
        return None
    es = get_es_client()
    if es is None:
        return None
    try:
        ensure_index(es)
        return es
    except Exception as e:
        logger.warning(f"Elasticsearch ensure_index failed: {e}")
        return None


def _published_at_for_es(d: date | None) -> str | None:
    if d is None:
        return None
    return d.isoformat()


def index_document(es: Elasticsearch, doc: Document) -> None:
    org_name = doc.organization.name if doc.organization is not None else None
    tag_names = [t.name for t in doc.tags]
    es.index(
        index=INDEX_NAME,
        id=str(doc.id),
        document={
            "document_id": doc.id,
            "title": doc.title,
            "abstract": doc.abstract,
            "body": doc.body,
            "tags": tag_names,
            "organization": org_name,
            "status": doc.status,
            "published_at": _published_at_for_es(doc.published_at),
            "score": doc.score,
        },
    )


def maybe_index_document(doc: Document) -> None:
    es = ensure_elasticsearch_index()
    if es is None:
        return
    try:
        index_document(es, doc)
    except Exception as e:
        logger.warning(f"Elasticsearch index failed document_id={doc.id}: {e}")


def reindex_all_documents(session: Session) -> int:
    """Write every document row from the database into Elasticsearch. Returns rows indexed."""
    es = ensure_elasticsearch_index()
    if es is None:
        logger.warning(
            "Elasticsearch reindex skipped: index unavailable or search disabled "
            "(set ELASTICSEARCH_URL and ensure ELASTICSEARCH_ENABLED is not off)"
        )
        return 0
    q = (
        select(Document)
        .options(selectinload(Document.organization), selectinload(Document.tags))
        .order_by(Document.id)
    )
    docs = list(session.scalars(q).all())
    n = 0
    for doc in docs:
        try:
            index_document(es, doc)
            n += 1
        except Exception as e:
            logger.warning(f"Elasticsearch index failed document_id={doc.id}: {e}")
    return n


def delete_document_from_index(doc_id: int) -> None:
    es = ensure_elasticsearch_index()
    if es is None:
        return
    try:
        es.delete(index=INDEX_NAME, id=str(doc_id))
    except ApiError as e:
        if e.status_code != 404:
            logger.warning(f"Elasticsearch delete failed document_id={doc_id}: {e}")
    except Exception as e:
        logger.warning(f"Elasticsearch delete failed document_id={doc_id}: {e}")
