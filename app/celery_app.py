"""Celery application (broker: Redis)."""

from __future__ import annotations

import os

from celery import Celery
from celery.signals import worker_ready

from app.config import get_celery_broker_url, get_celery_result_backend
from app.utils.logger import logger

celery_app = Celery(
    "document_intake",
    broker=get_celery_broker_url(),
    backend=get_celery_result_backend(),
    include=["app.tasks.ingestion_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
)

if os.getenv("CELERY_TASK_ALWAYS_EAGER", "").lower() in ("1", "true", "yes"):
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True


@worker_ready.connect
def _ensure_elasticsearch_index_on_worker_start(sender=None, **kwargs) -> None:
    from app.search.index import ensure_elasticsearch_index

    if ensure_elasticsearch_index() is not None:
        logger.info("Celery worker: Elasticsearch index ready")
