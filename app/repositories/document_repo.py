from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.exceptions import SemanticDuplicateError
from app.ingestion.normalizer import NormalizedRecord
from app.models import Author, Document, Organization, Tag
from app.processing import apply_processing
from app.utils.hash import content_fingerprint
from app.utils.logger import logger


def _get_or_create_author(session: Session, name: str) -> Author:
    existing = session.scalar(select(Author).where(Author.name == name))
    if existing:
        return existing
    a = Author(name=name)
    session.add(a)
    session.flush()
    return a


def _get_or_create_organization(session: Session, name: str) -> Organization:
    existing = session.scalar(select(Organization).where(Organization.name == name))
    if existing:
        return existing
    o = Organization(name=name)
    session.add(o)
    session.flush()
    return o


def _get_or_create_tag(session: Session, name: str) -> Tag:
    existing = session.scalar(select(Tag).where(Tag.name == name))
    if existing:
        return existing
    t = Tag(name=name)
    session.add(t)
    session.flush()
    return t


def _sync_tags(session: Session, doc: Document, tag_names: list[str]) -> None:
    doc.tags.clear()
    session.flush()
    for nm in tag_names:
        tag = _get_or_create_tag(session, nm)
        doc.tags.append(tag)


def _apply_record_to_document(
    session: Session, doc: Document, rec: NormalizedRecord, fp: str
) -> None:
    doc.title = rec.title
    doc.abstract = rec.abstract
    doc.body = rec.body
    doc.published_at = rec.published_at
    doc.updated_at = rec.updated_at
    doc.language = rec.language
    doc.status = rec.status
    doc.document_type = rec.document_type
    doc.region = rec.region
    doc.url = rec.url
    doc.doi = rec.doi
    doc.citation_count = rec.citation_count
    doc.relevance_score = rec.relevance_score
    doc.word_count = rec.word_count
    doc.page_count = rec.page_count
    doc.version = rec.version
    doc.open_access = rec.open_access
    doc.peer_reviewed = rec.peer_reviewed
    doc.content_fingerprint = fp

    if rec.author_name:
        auth = _get_or_create_author(session, rec.author_name)
        doc.author_id = auth.id
    else:
        doc.author_id = None

    if rec.organization_name:
        org = _get_or_create_organization(session, rec.organization_name)
        doc.organization_id = org.id
    else:
        doc.organization_id = None

    _sync_tags(session, doc, rec.tags)
    apply_processing(doc, rec)


def upsert_document(session: Session, rec: NormalizedRecord) -> tuple[Document, str]:
    fp = content_fingerprint(rec.title, rec.body)
    existing = session.scalar(select(Document).where(Document.external_id == rec.external_id))

    if existing:
        conflict = session.scalar(
            select(Document).where(
                Document.content_fingerprint == fp,
                Document.id != existing.id,
            )
        )
        if conflict:
            logger.warning(
                f"Semantic duplicate fingerprint for external_id={rec.external_id!r} conflicts with "
                f"document id={conflict.id} (external_id={conflict.external_id!r}); skipping"
            )
            raise SemanticDuplicateError(
                "semantic duplicate fingerprint belongs to another document",
                existing_external_id=conflict.external_id,
            )
        _apply_record_to_document(session, existing, rec, fp)
        logger.info(
            f"Document {existing.id} (external_id={rec.external_id!r}) was updated successfully"
        )
        return existing, "updated"

    other = session.scalar(select(Document).where(Document.content_fingerprint == fp))
    if other:
        logger.warning(
            f"Semantic duplicate content fingerprint for new external_id={rec.external_id!r} "
            f"(existing document id={other.id} external_id={other.external_id!r}); skipping"
        )
        raise SemanticDuplicateError(
            f"semantic duplicate of document {other.external_id}",
            existing_external_id=other.external_id,
        )

    doc = Document(external_id=rec.external_id)
    session.add(doc)
    session.flush()
    _apply_record_to_document(session, doc, rec, fp)
    logger.info(f"Document {doc.id} was created successfully")
    return doc, "created"
