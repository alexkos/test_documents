"""Content hashing for semantic deduplication."""

from __future__ import annotations

import hashlib


def content_fingerprint(title: str | None, body: str | None) -> str:
    t = (title or "").strip().lower()
    b = (body or "").strip()
    payload = f"{t}\0{b}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
