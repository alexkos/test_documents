#!/usr/bin/env python3
"""Index all PostgreSQL documents into Elasticsearch (backfill)."""

from __future__ import annotations

from sqlalchemy import func, select

from app.config import elasticsearch_startup_summary, elasticsearch_enabled
from app.db import get_session_factory
from app.models import Document
from app.search.index import ensure_elasticsearch_index, reindex_all_documents
from app.utils.logger import logger


def main() -> int:
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        total_in_db = db.scalar(select(func.count()).select_from(Document)) or 0
        print(f"Documents in database: {total_in_db}")
        print(elasticsearch_startup_summary())

        if not elasticsearch_enabled():
            print(
                "Reindex did nothing: Elasticsearch is off for this process. "
                "Uncomment or set ELASTICSEARCH_URL in .env (e.g. http://localhost:9200)."
            )
            return 1

        if ensure_elasticsearch_index() is None:
            print(
                "Reindex did nothing: could not create or reach the Elasticsearch index. "
                "Check that ES is running and ELASTICSEARCH_URL is correct."
            )
            return 1

        if total_in_db == 0:
            print("Nothing to index: the database has no document rows (run ingestion first).")
            return 0

        n = reindex_all_documents(db)
        logger.info(f"Elasticsearch reindex complete: {n} documents")
        print(f"Indexed {n} document(s) into Elasticsearch")
        if n < total_in_db:
            print(
                f"Warning: {total_in_db - n} document(s) failed to index; see logs for per-row errors."
            )
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
