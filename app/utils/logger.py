"""Application logger: use ``from app.utils.logger import logger``."""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger("document_intake")

_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logger.setLevel(_level)

# Emit INFO+ to stderr: the root logger defaults to WARNING, so without a handler
# nothing from this logger would appear when running under uvicorn.
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setLevel(_level)
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)

logger.propagate = False
