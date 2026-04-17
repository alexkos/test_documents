from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

# Minimal ISO 639-1 style normalization
_LANG_ALIASES = {
    "english": "en",
    "deutsch": "de",
    "": None,
}


def _to_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, bool):
        return None
    return str(v).strip() or None


def _parse_date(v: Any) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s or s == "invalid-date":
        return None
    try:
        y, m, d = s[:10].split("-")
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def _coerce_bool(v: Any) -> bool | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        if v == 0:
            return False
        if v == 1:
            return True
        return None
    s = str(v).strip().lower()
    if s in ("true", "yes", "1"):
        return True
    if s in ("false", "no", "0"):
        return False
    return None


def _normalize_tags(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[str] = []
        for x in raw:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    if isinstance(raw, str):
        parts = re.split(r"[,;|]", raw)
        return [p.strip() for p in parts if p.strip()]
    return []


def _normalize_language(raw: Any) -> str | None:
    s = _to_str(raw)
    if not s:
        return None
    low = s.lower()
    if low in _LANG_ALIASES:
        return _LANG_ALIASES[low]
    if low == "xx" or len(low) > 8:
        return None
    if re.fullmatch(r"[a-z]{2}", low):
        return low
    return low[:8] if low else None


def _normalize_status(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return "unknown" if not raw else "published"
    if isinstance(raw, (int, float)):
        return "unknown"
    s = str(raw).strip().lower()
    if not s:
        return None
    allowed = frozenset(
        {
            "published",
            "draft",
            "archived",
            "unknown",
        }
    )
    if s in allowed:
        return s
    if "publish" in s:
        return "published"
    if "draft" in s:
        return "draft"
    if "archive" in s:
        return "archived"
    return "unknown"


def _normalize_document_type(raw: Any) -> str | None:
    s = _to_str(raw)
    if not s:
        return None
    return s.lower().replace(" ", "_")


def _coerce_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == int(v) else None
    try:
        return int(str(v).strip())
    except ValueError:
        return None


def _coerce_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().lower()
    if s in ("high", "low", "medium"):
        return {"high": 0.9, "medium": 0.5, "low": 0.1}.get(s)
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_version(v: Any) -> str | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return str(int(v)) if isinstance(v, float) and v == int(v) else str(v)
    return _to_str(v)


@dataclass
class NormalizedRecord:
    external_id: str
    title: str | None
    abstract: str | None
    body: str | None
    published_at: date | None
    updated_at: date | None
    language: str | None
    status: str | None
    document_type: str | None
    region: str | None
    url: str | None
    doi: str | None
    citation_count: int | None
    relevance_score: float | None
    word_count: int | None
    page_count: int | None
    version: str | None
    open_access: bool | None
    peer_reviewed: bool | None
    tags: list[str] = field(default_factory=list)
    author_name: str | None = None
    organization_name: str | None = None


def normalize_raw_record(raw: dict[str, Any]) -> NormalizedRecord:
    ext = raw.get("external_id")
    external_id = str(ext).strip() if ext is not None else ""

    author = _to_str(raw.get("author_name"))
    if author and author.upper() in ("N/A", "UNKNOWN AUTHOR"):
        author = None

    org = raw.get("organization_name")
    org_s = _to_str(org) if not isinstance(org, (int, float)) else str(org)
    if org_s and not org_s.strip():
        org_s = None

    return NormalizedRecord(
        external_id=external_id,
        title=_to_str(raw.get("title")),
        abstract=_to_str(raw.get("abstract")),
        body=_to_str(raw.get("body")),
        published_at=_parse_date(raw.get("published_at")),
        updated_at=_parse_date(raw.get("updated_at")),
        language=_normalize_language(raw.get("language")),
        status=_normalize_status(raw.get("status")),
        document_type=_normalize_document_type(raw.get("document_type")),
        region=_to_str(raw.get("region")),
        url=_to_str(raw.get("url")),
        doi=_to_str(raw.get("doi")),
        citation_count=_coerce_int(raw.get("citation_count")),
        relevance_score=_coerce_float(raw.get("relevance_score")),
        word_count=_coerce_int(raw.get("word_count")),
        page_count=_coerce_int(raw.get("page_count")),
        version=_normalize_version(raw.get("version")),
        open_access=_coerce_bool(raw.get("open_access")),
        peer_reviewed=_coerce_bool(raw.get("peer_reviewed")),
        tags=_normalize_tags(raw.get("tags")),
        author_name=author,
        organization_name=org_s,
    )
