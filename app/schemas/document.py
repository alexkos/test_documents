from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict


class DocumentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str
    title: str | None
    abstract: str | None
    body: str | None
    published_at: date | None
    updated_at: date | None
    language: str | None
    status: str | None
    document_type: str | None
    region: str | None
    url: str | None
    doi: str | None
    citation_count: int | None
    relevance_score: float | None
    word_count: int | None
    page_count: int | None
    version: str | None
    open_access: bool | None
    peer_reviewed: bool | None
    summary: str | None
    score: float | None
    classification: str | None
    keywords: list[Any] | None
    content_fingerprint: str | None
    author_id: int | None
    organization_id: int | None
    tags: list[str] = []


class DocumentListResponse(BaseModel):
    items: list[DocumentOut]
    total: int
    skip: int
    limit: int
