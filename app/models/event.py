from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class IngestionEvent(Base):
    __tablename__ = "ingestion_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingestion_id: Mapped[int] = mapped_column(ForeignKey("ingestion_runs.id"), index=True)

    external_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    stage: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    ingestion_run: Mapped["IngestionRun"] = relationship("IngestionRun", back_populates="events")
