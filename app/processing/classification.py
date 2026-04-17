from __future__ import annotations


def classify_document(title: str | None, body: str | None) -> str:
    blob = f"{title or ''} {body or ''}".lower()
    if "policy" in blob:
        return "policy"
    if "report" in blob:
        return "report"
    return "general"
