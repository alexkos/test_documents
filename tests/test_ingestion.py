from __future__ import annotations

import json
from pathlib import Path

from app.ingestion import create_ingestion_run, ingest_file


def test_ingest_skips_semantic_duplicate(tmp_path: Path, db_session) -> None:
    body = "Same body content for dedup test."
    line1 = json.dumps(
        {
            "external_id": "a1",
            "title": "Shared Title",
            "body": body,
            "doi": "10.1000/182",
            "url": "https://example.org/x",
        }
    )
    line2 = json.dumps(
        {
            "external_id": "a2",
            "title": "Shared Title",
            "body": body,
            "doi": "10.1000/183",
            "url": "https://example.org/y",
        }
    )
    path = tmp_path / "sample.jsonl"
    path.write_text(line1 + "\n" + line2 + "\n", encoding="utf-8")

    run = create_ingestion_run(db_session)
    db_session.commit()
    ingest_file(db_session, path, run)
    db_session.commit()

    assert run.success_count == 1
    assert run.skipped_count == 1


def test_ingest_invalid_doi_normalized_to_null(tmp_path: Path, db_session) -> None:
    """Malformed DOI strings are dropped during normalization; ingestion still succeeds."""
    line = json.dumps(
        {
            "external_id": "bad-doi",
            "title": "T",
            "doi": "not-a-doi",
            "url": "https://example.org/z",
        }
    )
    path = tmp_path / "bad.jsonl"
    path.write_text(line + "\n", encoding="utf-8")
    run = create_ingestion_run(db_session)
    db_session.commit()
    ingest_file(db_session, path, run)
    db_session.commit()
    assert run.error_count == 0
    assert run.success_count == 1


def test_ingest_validation_error_invalid_raw_date(tmp_path: Path, db_session) -> None:
    line = json.dumps(
        {
            "external_id": "bad-date",
            "title": "T",
            "published_at": "not-a-date",
        }
    )
    path = tmp_path / "dates.jsonl"
    path.write_text(line + "\n", encoding="utf-8")
    run = create_ingestion_run(db_session)
    db_session.commit()
    ingest_file(db_session, path, run)
    db_session.commit()
    assert run.error_count == 1
    assert run.success_count == 0
