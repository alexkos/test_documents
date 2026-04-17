"""Integration tests for GET /documents query filters."""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import Document, Organization, Tag


@pytest.fixture
def client_with_docs(db_session):
    o_a = Organization(name="Acme Corp")
    o_b = Organization(name="Beta Inc")
    db_session.add_all([o_a, o_b])
    db_session.flush()

    t_alpha = Tag(name="alpha")
    t_beta = Tag(name="beta")
    db_session.add_all([t_alpha, t_beta])
    db_session.flush()

    d1 = Document(
        external_id="ext-filter-1",
        title="UniqueTitleOne",
        body="body-foo",
        published_at=date(2020, 1, 10),
        status="published",
        organization_id=o_a.id,
    )
    d1.tags.append(t_alpha)

    d2 = Document(
        external_id="ext-filter-2",
        title="Second Doc",
        body="unique-bar-text",
        published_at=date(2021, 6, 1),
        status="draft",
        organization_id=o_b.id,
    )
    d2.tags.append(t_beta)

    d3 = Document(
        external_id="ext-filter-3",
        title="Third",
        body="nothing",
        published_at=date(2020, 1, 20),
        status="published",
        organization_id=o_a.id,
    )
    d3.tags.extend([t_alpha, t_beta])

    db_session.add_all([d1, d2, d3])
    db_session.commit()

    from app.db import get_db

    def _override():
        yield db_session

    app.dependency_overrides[get_db] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _external_ids(data: dict) -> set[str]:
    return {item["external_id"] for item in data["items"]}


def test_list_documents_unfiltered(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3


def test_filter_by_tag(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"tag": "alpha"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert _external_ids(data) == {"ext-filter-1", "ext-filter-3"}


def test_filter_by_organization(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"organization": "Acme Corp"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert _external_ids(data) == {"ext-filter-1", "ext-filter-3"}


def test_filter_by_status(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"status": "draft"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert _external_ids(data) == {"ext-filter-2"}


def test_filter_by_date_from(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"date_from": "2020-01-15"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert _external_ids(data) == {"ext-filter-2", "ext-filter-3"}


def test_filter_by_date_to(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"date_to": "2020-12-31"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert _external_ids(data) == {"ext-filter-1", "ext-filter-3"}


def test_filter_by_search_title(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"search": "UniqueTitle"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert _external_ids(data) == {"ext-filter-1"}


def test_filter_by_search_body_case_insensitive(client_with_docs: TestClient) -> None:
    r = client_with_docs.get("/documents", params={"search": "UNIQUE-BAR"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert _external_ids(data) == {"ext-filter-2"}


def test_combined_tag_and_organization(client_with_docs: TestClient) -> None:
    r = client_with_docs.get(
        "/documents",
        params={"tag": "alpha", "organization": "Acme Corp"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert _external_ids(data) == {"ext-filter-1", "ext-filter-3"}
