from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.stats import StatsOut
from app.services.stats_service import get_stats

router = APIRouter()


@router.get("")
def stats(db: Session = Depends(get_db)) -> StatsOut:
    return get_stats(db)
