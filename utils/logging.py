"""Central logging utilities used across the service.

The helper below configures a consistent logging format while still allowing
consumers to fetch component specific loggers by name.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional

DEFAULT_LOG_LEVEL = logging.INFO


@lru_cache(maxsize=None)
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a module level logger configured with sensible defaults."""

    logger = logging.getLogger(name or "Ashare")
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(DEFAULT_LOG_LEVEL)
    logger.propagate = False
    return logger
