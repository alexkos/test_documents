from __future__ import annotations

from app.models import Document


def compute_score(doc: Document) -> float:
    c = doc.citation_count or 0
    r = doc.relevance_score or 0.0
    w = doc.word_count or 0
    return c * 0.3 + r * 0.5 + w * 0.2
