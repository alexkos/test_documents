from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.exceptions import IngestionValidationError, SemanticDuplicateError
from app.models import Author, Document, IngestionEvent, IngestionRun, Organization, Tag
from app.normalize import NormalizedRecord, normalize_raw_record
from app.processing import apply_processing, content_fingerprint
from app.validate import validate_normalized


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _log_event(
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
        created_at=_utcnow(),
    )
    session.add(ev)


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
            raise SemanticDuplicateError(
                "semantic duplicate fingerprint belongs to another document",
                existing_external_id=conflict.external_id,
            )
        _apply_record_to_document(session, existing, rec, fp)
        return existing, "updated"

    other = session.scalar(select(Document).where(Document.content_fingerprint == fp))
    if other:
        raise SemanticDuplicateError(
            f"semantic duplicate of document {other.external_id}",
            existing_external_id=other.external_id,
        )

    doc = Document(external_id=rec.external_id)
    session.add(doc)
    session.flush()
    _apply_record_to_document(session, doc, rec, fp)
    return doc, "created"


def ingest_file(session: Session, path: Path, run: IngestionRun) -> None:
    run.started_at = run.started_at or _utcnow()
    run.status = "running"

    success = error = skipped = 0
    total = 0

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            raw: dict[str, Any] | None = None
            ext_for_log: str | None = None
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                error += 1
                _log_event(
                    session,
                    run,
                    external_id=None,
                    stage="parsing",
                    status="error",
                    message=f"invalid JSON: {e}",
                )
                continue

            if len(raw) == 0:
                error += 1
                _log_event(
                    session,
                    run,
                    external_id=None,
                    stage="parsing",
                    status="error",
                    message="empty record",
                )
                continue

            ext_for_log = str(raw.get("external_id", "")).strip() or None

            try:
                normalized = normalize_raw_record(raw)
                validate_normalized(normalized)
                _doc, _action = upsert_document(session, normalized)
                success += 1
                _log_event(
                    session,
                    run,
                    external_id=normalized.external_id,
                    stage="completed",
                    status="success",
                    raw_payload=None,
                )
            except IngestionValidationError as e:
                error += 1
                _log_event(
                    session,
                    run,
                    external_id=ext_for_log,
                    stage="validation",
                    status="error",
                    message=e.message,
                    raw_payload=raw,
                )
            except SemanticDuplicateError as e:
                skipped += 1
                _log_event(
                    session,
                    run,
                    external_id=ext_for_log,
                    stage="deduplication",
                    status="skipped",
                    message=e.message,
                    raw_payload=raw,
                )

    run.total_records = total
    run.success_count = success
    run.error_count = error
    run.skipped_count = skipped
    run.finished_at = _utcnow()
    run.status = "completed"


def create_ingestion_run(session: Session) -> IngestionRun:
    run = IngestionRun(
        started_at=_utcnow(),
        status="running",
        total_records=0,
        success_count=0,
        error_count=0,
        skipped_count=0,
    )
    session.add(run)
    session.flush()
    return run
