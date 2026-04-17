from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()


@lru_cache
def get_database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://docintake:docintake@localhost:5432/docintake",
    )


@lru_cache
def get_default_jsonl_path() -> str:
    return os.getenv("DEFAULT_JSONL_PATH", "input_docs/documents_1.jsonl")


def resolved_jsonl_path(project_root: Path | None = None) -> Path:
    root = project_root or Path(__file__).resolve().parent.parent
    p = Path(get_default_jsonl_path())
    return p if p.is_absolute() else (root / p)


@lru_cache
def get_celery_broker_url() -> str:
    return os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")


@lru_cache
def get_celery_result_backend() -> str:
    return os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")


@lru_cache
def get_elasticsearch_url() -> str:
    return os.getenv("ELASTICSEARCH_URL", "").strip()


def elasticsearch_enabled() -> bool:
    """Use Elasticsearch for full-text search when URL is set and not explicitly disabled."""
    flag = os.getenv("ELASTICSEARCH_ENABLED", "1").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return False
    return bool(get_elasticsearch_url())


def elasticsearch_startup_summary() -> str:
    """One-line description for logging whether full-text search uses Elasticsearch."""
    flag = os.getenv("ELASTICSEARCH_ENABLED", "1").strip()
    if flag.lower() in ("0", "false", "no", "off"):
        return f"Elasticsearch: off (ELASTICSEARCH_ENABLED={flag!r})"
    url = get_elasticsearch_url()
    if not url:
        return "Elasticsearch: off (ELASTICSEARCH_URL not set or empty)"
    return f"Elasticsearch: on (url={url})"
