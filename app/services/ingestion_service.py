from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.config import resolved_jsonl_path


def discover_ingestion_files() -> list[Path]:
    """Paths to ingest when no file_path is given: *.jsonl and *.json in the default feed directory."""
    directory = resolved_jsonl_path().parent
    if not directory.is_dir():
        raise FileNotFoundError(directory)
    exts = {".json", ".jsonl"}
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() in exts
    )
from app.db import get_session_factory
from app.ingestion.runner import ingest_file
from app.models import IngestionRun
from app.repositories.ingestion_repo import create_ingestion_run
from app.utils.logger import logger


def queue_ingestion_path(file_path: str | None) -> tuple[int, Path]:
    path = Path(file_path) if file_path else resolved_jsonl_path()
    if not path.is_file():
        raise FileNotFoundError(path)
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run = create_ingestion_run(db, queued=True)
        run_id = run.id
        db.commit()
        logger.info(f"Created ingestion run_id={run_id} (queued) for path={path}")
        return run_id, path
    except Exception:
        db.rollback()
        logger.exception(f"Failed to queue ingestion for path={path}")
        raise
    finally:
        db.close()


def run_ingestion_job(run_id: int, path: Path) -> None:
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run_row = db.get(IngestionRun, run_id)
        if run_row is None:
            return
        ingest_file(db, path, run_row)
        db.commit()
        logger.info(
            f"Synchronous ingestion job finished run_id={run_id} total={run_row.total_records}"
        )
    except Exception:
        logger.exception(f"Synchronous ingestion job failed run_id={run_id} path={path}")
        db.rollback()
        run_row = db.get(IngestionRun, run_id)
        if run_row is not None:
            run_row.status = "failed"
            run_row.finished_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()
