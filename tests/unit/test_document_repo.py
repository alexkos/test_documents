"""Unit tests for ``app.repositories.document_repo`` (upsert = create/update; read via session)."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import select

from app.exceptions import SemanticDuplicateError
from app.ingestion.normalizer import NormalizedRecord
from app.models import Document
from app.repositories.document_repo import upsert_document


def _rec(external_id: str, **overrides: Any) -> NormalizedRecord:
    kw: dict[str, Any] = {
        "external_id": external_id,
        "title": "Title",
        "abstract": None,
        "body": "Body content for tests.",
        "published_at": None,
        "updated_at": None,
        "language": None,
        "status": "published",
        "document_type": None,
        "region": None,
        "url": "https://example.org/doc",
        "doi": "10.1000/182",
        "citation_count": None,
        "relevance_score": None,
        "word_count": None,
        "page_count": None,
        "version": None,
        "open_access": None,
        "peer_reviewed": None,
        "tags": [],
        "author_name": None,
        "organization_name": None,
    }
    kw.update(overrides)
    return NormalizedRecord(**kw)


def test_create_document_via_upsert(db_session) -> None:
    doc, action = upsert_document(db_session, _rec("new-ext"))
    db_session.commit()

    assert action == "created"
    assert doc.id is not None
    assert doc.external_id == "new-ext"
    assert doc.title == "Title"

    loaded = db_session.get(Document, doc.id)
    assert loaded is not None
    assert loaded.external_id == "new-ext"


def test_read_document_by_external_id(db_session) -> None:
    upsert_document(db_session, _rec("read-me", title="Hello", body="World"))
    db_session.commit()

    found = db_session.scalar(select(Document).where(Document.external_id == "read-me"))
    assert found is not None
    assert found.title == "Hello"
    assert found.body == "World"


def test_update_document_via_upsert(db_session) -> None:
    upsert_document(db_session, _rec("upd", title="First", body="Alpha"))
    db_session.commit()

    doc, action = upsert_document(db_session, _rec("upd", title="Second", body="Beta"))
    db_session.commit()

    assert action == "updated"
    assert doc.title == "Second"
    assert doc.body == "Beta"

    again = db_session.scalar(select(Document).where(Document.external_id == "upd"))
    assert again is not None
    assert again.title == "Second"


def test_upsert_links_author_organization_and_tags(db_session) -> None:
    upsert_document(
        db_session,
        _rec(
            "rich",
            author_name="Ada Lovelace",
            organization_name="Analytical Engines Ltd",
            tags=["math", "cs"],
        ),
    )
    db_session.commit()

    doc = db_session.scalar(select(Document).where(Document.external_id == "rich"))
    assert doc is not None
    assert doc.author is not None
    assert doc.author.name == "Ada Lovelace"
    assert doc.organization is not None
    assert doc.organization.name == "Analytical Engines Ltd"
    tag_names = sorted(t.name for t in doc.tags)
    assert tag_names == ["cs", "math"]


def test_semantic_duplicate_raises_for_new_external_id(db_session) -> None:
    upsert_document(db_session, _rec("first", title="T", body="Shared body."))
    db_session.commit()

    with pytest.raises(SemanticDuplicateError) as excinfo:
        upsert_document(db_session, _rec("second", title="T", body="Shared body."))

    assert excinfo.value.existing_external_id == "first"


def test_semantic_duplicate_raises_when_update_conflicts_with_other_document(
    db_session,
) -> None:
    upsert_document(db_session, _rec("doc-a", title="A", body="Body A unique."))
    upsert_document(db_session, _rec("doc-b", title="B", body="Body B unique."))
    db_session.commit()

    with pytest.raises(SemanticDuplicateError) as excinfo:
        upsert_document(db_session, _rec("doc-a", title="B", body="Body B unique."))

    assert excinfo.value.existing_external_id == "doc-b"
