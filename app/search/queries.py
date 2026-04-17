from __future__ import annotations

from datetime import date

from app.search.client import INDEX_NAME
from app.search.index import ensure_elasticsearch_index


def search_document_ids(
    query: str,
    *,
    skip: int,
    limit: int,
    date_from: date | None,
    date_to: date | None,
    tag: str | None,
    organization: str | None,
    status: str | None,
) -> tuple[list[int], int]:
    """Return (ordered document ids, total hit count) from Elasticsearch."""
    es = ensure_elasticsearch_index()
    if es is None:
        # Caller (document list) should fall back to SQL ilike when index is unavailable.
        raise RuntimeError("Elasticsearch index unavailable")

    must: list[dict] = []
    if query:
        must.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "abstract", "body"],
                }
            }
        )

    filter_clauses: list[dict] = []
    if tag:
        filter_clauses.append({"term": {"tags": tag}})
    if organization:
        filter_clauses.append({"term": {"organization": organization}})
    if status:
        filter_clauses.append({"term": {"status": status}})
    if date_from or date_to:
        range_body: dict[str, str] = {}
        if date_from:
            range_body["gte"] = date_from.isoformat()
        if date_to:
            range_body["lte"] = date_to.isoformat()
        filter_clauses.append({"range": {"published_at": range_body}})

    bool_query: dict = {"filter": filter_clauses}
    if must:
        bool_query["must"] = must
    else:
        bool_query["must"] = [{"match_all": {}}]

    response = es.search(
        index=INDEX_NAME,
        query={"bool": bool_query},
        from_=skip,
        size=limit,
        track_total_hits=True,
        sort=[
            {"_score": {"order": "desc"}},
            {"document_id": {"order": "asc"}},
        ],
    )

    hits = response.get("hits", {})
    raw_total = hits.get("total", 0)
    if isinstance(raw_total, dict):
        total = int(raw_total.get("value", 0))
    else:
        total = int(raw_total or 0)

    ids: list[int] = []
    for hit in hits.get("hits", []):
        try:
            ids.append(int(hit["_id"]))
        except (KeyError, TypeError, ValueError):
            continue

    return ids, total
