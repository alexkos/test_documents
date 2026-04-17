from __future__ import annotations

import json
from typing import Any


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse a single JSONL line; return None for blank lines or invalid JSON."""
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None
