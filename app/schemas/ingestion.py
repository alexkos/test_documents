from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class IngestionEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    external_id: str | None
    status: str
    message: str | None = None
    stage: str


class IngestionRunDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    started_at: datetime
    finished_at: datetime | None
    total_records: int
    success_count: int
    error_count: int
    skipped_count: int
    status: str
    events: list[IngestionEventOut]


class IngestionQueuedResponse(BaseModel):
    run_id: int
