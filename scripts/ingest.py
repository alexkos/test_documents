#!/usr/bin/env python3
"""CLI: run ingestion pipeline for a JSONL file (synchronous)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from app.config import resolved_jsonl_path
from app.db import get_session_factory
from app.ingestion.runner import ingest_file
from app.repositories.ingestion_repo import create_ingestion_run


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest JSONL documents")
    p.add_argument("file", nargs="?", help="Path to .jsonl (default: DEFAULT_JSONL_PATH)")
    args = p.parse_args()
    path = Path(args.file) if args.file else resolved_jsonl_path()
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        run = create_ingestion_run(db)
        db.commit()
        ingest_file(db, path, run)
        db.commit()
        print(
            f"Run {run.id}: total={run.total_records} ok={run.success_count} "
            f"errors={run.error_count} skipped={run.skipped_count}"
        )
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
