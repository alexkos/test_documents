from __future__ import annotations

import json
from typing import Any


def parse_line(line: str) -> dict[str, Any]:
    """Parse a single JSONL line. Raises json.JSONDecodeError on invalid JSON."""
    return json.loads(line)
