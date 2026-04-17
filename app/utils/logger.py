"""Application logger: use ``from app.utils.logger import logger``."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("document_intake")

_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logger.setLevel(_level)
