from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class DocumentTag(Base):
    __tablename__ = "document_tags"

    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class Document(Base):
    __tablename__ = "documents"
    __table_args__ = (
        UniqueConstraint("content_fingerprint", name="uq_documents_content_fingerprint"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    abstract: Mapped[str | None] = mapped_column(Text, nullable=True)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)

    published_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    updated_at: Mapped[date | None] = mapped_column(Date, nullable=True)

    language: Mapped[str | None] = mapped_column(String(16), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    document_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    region: Mapped[str | None] = mapped_column(String(128), nullable=True)

    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    doi: Mapped[str | None] = mapped_column(String(512), nullable=True)

    citation_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    relevance_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    word_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    version: Mapped[str | None] = mapped_column(String(64), nullable=True)

    open_access: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    peer_reviewed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    classification: Mapped[str | None] = mapped_column(String(128), nullable=True)
    keywords: Mapped[list[Any] | None] = mapped_column(JSON, nullable=True)
    content_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)

    author_id: Mapped[int | None] = mapped_column(ForeignKey("authors.id"), nullable=True)
    organization_id: Mapped[int | None] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )

    author: Mapped["Author | None"] = relationship("Author", back_populates="documents")
    organization: Mapped["Organization | None"] = relationship(
        "Organization",
        back_populates="documents",
    )
    tags: Mapped[list["Tag"]] = relationship(
        "Tag",
        secondary="document_tags",
        back_populates="documents",
        lazy="selectin",
    )
