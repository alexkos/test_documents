from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class StatsOut(BaseModel):
    total_documents: int
    by_status: dict[str, int]
    by_type: dict[str, int]
    top_tags: list[dict[str, Any]]
    avg_score: float | None
    total_ingestion_events: int
    events_by_stage: dict[str, int]
    events_by_status: dict[str, int]
