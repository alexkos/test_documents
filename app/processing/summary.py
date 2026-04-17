from __future__ import annotations

from app.utils.text import first_sentences


def summarize_body_or_abstract(body: str | None, abstract: str | None) -> str | None:
    return first_sentences(body or abstract)
