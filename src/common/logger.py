"""Logging utilities.

On Databricks the cluster captures everything written to stdout/stderr into the
driver logs, so we do not configure file handlers the way the original on-prem
project did (``logging.config.fileConfig``). Instead we attach a single stream
handler with a consistent format and let the platform handle persistence,
rotation and shipping to Log Analytics.
"""
from __future__ import annotations

import logging
import sys

_LOG_FORMAT = "Healthcare Project | %(asctime)s | %(name)s | %(levelname)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False


def _configure_root(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a namespaced logger wired into the driver log stream.

    :param name: typically ``__name__`` of the calling module.
    :param level: logging level for the root logger.
    """
    _configure_root(level)
    return logging.getLogger(name)
