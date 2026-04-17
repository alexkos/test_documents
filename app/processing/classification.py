from __future__ import annotations

DOCUMENT_TYPE_MAPPING = {
    "report": "report",
    "working_paper": "research",
    "journal_article": "research",
    "policy_brief": "policy",
    "news_article": "news",
    "press_release": "news",
    "dataset": "data",
}


def normalize(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip().lower()
    return value or None


def _document_type_key(doc_type: str) -> str:
    """Map normalized labels like 'journal article' to mapping keys like 'journal_article'."""
    return doc_type.replace(" ", "_").replace("-", "_")


def classify_document(
    document_type: str | None,
    title: str | None,
    body: str | None,
) -> str:
    doc_type = normalize(document_type)
    if doc_type:
        key = _document_type_key(doc_type)
        if key in DOCUMENT_TYPE_MAPPING:
            return DOCUMENT_TYPE_MAPPING[key]

    blob = f"{title or ''} {body or ''}".lower()

    if "policy" in blob:
        return "policy"
    if "report" in blob:
        return "report"
    if "data" in blob:
        return "data"

    return "other"
