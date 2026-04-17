"""Backward-compatible re-exports; canonical implementation lives in ``app.ingestion.normalizer``."""

from __future__ import annotations

from app.ingestion.normalizer import NormalizedRecord, normalize_raw_record

__all__ = ("NormalizedRecord", "normalize_raw_record")
