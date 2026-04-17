"""Backward-compatible re-exports; canonical implementation lives in ``app.ingestion.validator``."""

from __future__ import annotations

from app.ingestion.validator import validate_normalized, validate_raw_record

__all__ = ("validate_normalized", "validate_raw_record")
