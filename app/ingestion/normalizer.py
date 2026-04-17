from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

# ISO 639-1 style codes and common aliases
_LANG_MAP = {
    "english": "en",
    "en": "en",
    "de": "de",
    "pt": "pt",
    "fr": "fr",
}

_DOI_REGEX = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$", re.I)


def _normalize_str(value: Any, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ValueError("Required string missing")
        return None
    if isinstance(value, bool):
        return None
    s = str(value).strip()
    return s if s else None


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s or s == "invalid-date":
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _normalize_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 0:
            return False
        if value == 1:
            return True
        return None
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _normalize_tags(raw: Any) -> list[str]:
    if raw is None:
        return []
    parts: list[str]
    if isinstance(raw, str):
        parts = re.split(r"[,;|]", raw)
    elif isinstance(raw, list):
        parts = [str(x) for x in raw if x is not None]
    else:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        t = str(p).strip().lower()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _normalize_language(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    s = str(raw).lower().strip()
    if s in _LANG_MAP:
        return _LANG_MAP[s]
    if re.fullmatch(r"[a-z]{2}", s):
        return s
    return s[:2] if len(s) >= 2 else s


def _normalize_status(raw: Any) -> str:
    if raw is None or raw == "":
        return "unknown"
    if isinstance(raw, bool):
        return "unknown" if not raw else "published"
    if isinstance(raw, (int, float)):
        return "unknown"
    s = str(raw).strip().lower()
    if not s or s == "none":
        return "unknown"
    mapping = {
        "published": "published",
        "archived": "archived",
        "draft": "draft",
        "unknown": "unknown",
    }
    if s in mapping:
        return mapping[s]
    if "publish" in s:
        return "published"
    if "draft" in s:
        return "draft"
    if "archive" in s:
        return "archived"
    return s


def _normalize_document_type(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    return str(raw).lower().strip()


def _normalize_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value == int(value) else None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    if s in ("high", "low", "medium"):
        return {"high": 0.9, "medium": 0.5, "low": 0.1}.get(s)
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_version(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_doi(value: Any) -> str | None:
    s = _normalize_str(value)
    if not s:
        return None
    return s if _DOI_REGEX.match(s) else None


def _normalize_url(value: Any) -> str | None:
    s = _normalize_str(value)
    if not s:
        return None
    return s if s.startswith("http") else None


def _normalize_author(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    s = str(value).strip()
    if not s or s.upper() in ("N/A", "UNKNOWN AUTHOR"):
        return None
    return s


def _normalize_org(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return None
    if isinstance(value, bool):
        return None
    return str(value).strip() or None


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

    return NormalizedRecord(
        external_id=external_id,
        title=_normalize_str(raw.get("title")),
        abstract=_normalize_str(raw.get("abstract")),
        body=_normalize_str(raw.get("body")),
        published_at=_parse_date(raw.get("published_at")),
        updated_at=_parse_date(raw.get("updated_at")),
        language=_normalize_language(raw.get("language")),
        status=_normalize_status(raw.get("status")),
        document_type=_normalize_document_type(raw.get("document_type")),
        region=_normalize_str(raw.get("region")),
        url=_normalize_url(raw.get("url")),
        doi=_normalize_doi(raw.get("doi")),
        citation_count=_normalize_int(raw.get("citation_count")),
        relevance_score=_normalize_float(raw.get("relevance_score")),
        word_count=_normalize_int(raw.get("word_count")),
        page_count=_normalize_int(raw.get("page_count")),
        version=_normalize_version(raw.get("version")),
        open_access=_normalize_bool(raw.get("open_access")),
        peer_reviewed=_normalize_bool(raw.get("peer_reviewed")),
        tags=_normalize_tags(raw.get("tags")),
        author_name=_normalize_author(raw.get("author_name")),
        organization_name=_normalize_org(raw.get("organization_name")),
    )
