"""Process-wide logging configuration for PyQueue."""

import logging
import os

DEFAULT_LEVEL = "INFO"
_FORMAT = "%(asctime)s [%(levelname)s] [%(processName)s: %(process)d] %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def configure(level: str | int | None = None) -> None:
    """Install one stderr handler on the root logger. Safe to call repeatedly."""
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(handler)
    root.setLevel(level or os.environ.get("PYQUEUE_LOG_LEVEL", DEFAULT_LEVEL))
