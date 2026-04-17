"""Unit tests for ``app.repositories.ingestion_repo``."""

from __future__ import annotations

from sqlalchemy import select

from app.models import IngestionEvent, IngestionRun
from app.repositories.ingestion_repo import create_ingestion_run, log_event


def test_create_ingestion_run_starts_immediately(db_session) -> None:
    run = create_ingestion_run(db_session)
    db_session.commit()

    assert run.id is not None
    assert run.status == "running"
    assert run.started_at is not None
    assert run.total_records == 0
    assert run.success_count == 0
    assert run.error_count == 0
    assert run.skipped_count == 0

    loaded = db_session.get(IngestionRun, run.id)
    assert loaded is not None
    assert loaded.status == "running"


def test_create_ingestion_run_queued(db_session) -> None:
    run = create_ingestion_run(db_session, queued=True)
    db_session.commit()

    assert run.status == "queued"
    assert run.started_at is None


def test_read_ingestion_run_by_id(db_session) -> None:
    run = create_ingestion_run(db_session)
    db_session.commit()

    found = db_session.scalar(select(IngestionRun).where(IngestionRun.id == run.id))
    assert found is not None
    assert found.id == run.id


def test_log_event_create_and_read(db_session) -> None:
    run = create_ingestion_run(db_session)
    log_event(
        db_session,
        run,
        external_id="ext-42",
        stage="parse",
        status="ok",
        message="done",
        raw_payload={"line": 1},
    )
    db_session.flush()

    events = db_session.scalars(
        select(IngestionEvent).where(IngestionEvent.ingestion_id == run.id)
    ).all()
    assert len(events) == 1
    ev = events[0]
    assert ev.external_id == "ext-42"
    assert ev.stage == "parse"
    assert ev.status == "ok"
    assert ev.message == "done"
    assert ev.raw_payload == {"line": 1}
    assert ev.created_at is not None

    db_session.commit()
    again = db_session.get(IngestionEvent, ev.id)
    assert again is not None
    assert again.stage == "parse"


def test_log_event_multiple_records_per_run(db_session) -> None:
    run = create_ingestion_run(db_session)
    log_event(db_session, run, external_id="a", stage="s1", status="ok")
    log_event(db_session, run, external_id="b", stage="s2", status="error", message="fail")
    db_session.flush()

    events = db_session.scalars(
        select(IngestionEvent).where(IngestionEvent.ingestion_id == run.id)
    ).all()
    assert len(events) == 2
    stages = {e.stage for e in events}
    assert stages == {"s1", "s2"}
