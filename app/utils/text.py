"""Text helpers."""

from __future__ import annotations

import re


def first_sentences(text: str | None, max_sentences: int = 2) -> str | None:
    if not text:
        return None
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    chunk = parts[:max_sentences]
    out = " ".join(chunk).strip()
    return out or None
