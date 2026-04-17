from __future__ import annotations

from functools import lru_cache

from elasticsearch import Elasticsearch

from app.config import elasticsearch_enabled, get_elasticsearch_url

INDEX_NAME = "documents"


@lru_cache
def get_es_client() -> Elasticsearch | None:
    if not elasticsearch_enabled():
        return None
    url = get_elasticsearch_url()
    if not url:
        return None
    return Elasticsearch(url)
