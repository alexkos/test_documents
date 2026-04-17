from __future__ import annotations

import hashlib
import re
from collections import Counter

from app.models import Document
from app.normalize import NormalizedRecord

_STOPWORDS = frozenset(
    """
    the a an and or of to in for on with as at by from into through during before after
    above below between under again further then once here there when where why how all
    each both few more most other some such no nor not only own same so than too very
    can will just should now longer body text short abstract
    """.split()
)


def content_fingerprint(title: str | None, body: str | None) -> str:
    t = (title or "").strip().lower()
    b = (body or "").strip()
    payload = f"{t}\0{b}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def extract_keywords(text: str | None, top_n: int = 12) -> list[str]:
    if not text:
        return []
    words = re.findall(r"[a-zA-Z][a-zA-Z\-]{2,}", text.lower())
    filtered = [w for w in words if w not in _STOPWORDS]
    counts = Counter(filtered)
    return [w for w, _ in counts.most_common(top_n)]


def classify_document(title: str | None, body: str | None) -> str:
    blob = f"{title or ''} {body or ''}".lower()
    if "policy" in blob:
        return "policy"
    if "report" in blob:
        return "report"
    return "general"


def compute_score(doc: Document) -> float:
    c = doc.citation_count or 0
    r = doc.relevance_score or 0.0
    w = doc.word_count or 0
    return c * 0.3 + r * 0.5 + w * 0.2


def first_sentences(text: str | None, max_sentences: int = 2) -> str | None:
    if not text:
        return None
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    chunk = parts[:max_sentences]
    out = " ".join(chunk).strip()
    return out or None


def apply_processing(doc: Document, rec: NormalizedRecord) -> None:
    blob = " ".join(filter(None, [rec.title, rec.abstract, rec.body]))
    doc.keywords = extract_keywords(blob)
    doc.classification = classify_document(rec.title, rec.body)
    doc.score = compute_score(doc)
    doc.summary = first_sentences(rec.body or rec.abstract)
