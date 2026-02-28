"""Structured JSON logging configuration for dev-health-ops.

Configures python-json-logger for all application log output and
provides a matching uvicorn JSON log config dict.

Usage:
    from dev_health_ops.logging_config import configure_logging, uvicorn_log_config
    configure_logging()
    uvicorn.run(app, log_config=uvicorn_log_config())

Environment variables:
    LOG_LEVEL  — root log level (default: INFO)
    LOG_JSON   — set to "false" to use plain text logging (useful in dev)
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any


def configure_logging(level: str | None = None) -> None:
    """Set up JSON structured logging for the entire application.

    Safe to call multiple times (idempotent).
    """
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    use_json = os.getenv("LOG_JSON", "true").lower() not in ("false", "0", "no")

    if use_json:
        try:
            from pythonjsonlogger.json import JsonFormatter

            handler = logging.StreamHandler(sys.stdout)
            formatter = JsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
                rename_fields={"asctime": "timestamp", "levelname": "level"},
            )
            handler.setFormatter(formatter)
        except ImportError:
            # Fallback to standard logging if python-json-logger isn't installed
            handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.StreamHandler(sys.stdout)

    root = logging.getLogger()
    # Avoid double-adding handlers if configure_logging is called multiple times
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(log_level)

    # Silence noisy third-party loggers
    for noisy in ("uvicorn.access", "watchfiles"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def uvicorn_log_config(level: str | None = None) -> dict[str, Any]:
    """Return a uvicorn log_config dict that emits JSON access logs.

    Pass to ``uvicorn.Config(log_config=uvicorn_log_config())``.
    """
    log_level = (level or os.getenv("LOG_LEVEL", "info")).lower()
    use_json = os.getenv("LOG_JSON", "true").lower() not in ("false", "0", "no")

    if use_json:
        formatter_class = "pythonjsonlogger.json.JsonFormatter"
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    else:
        formatter_class = "logging.Formatter"
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": formatter_class,
                "fmt": fmt,
                "datefmt": "%Y-%m-%dT%H:%M:%S",
                "rename_fields": {"asctime": "timestamp", "levelname": "level"},
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "json",
            },
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": log_level.upper(),
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": "WARNING",
                "propagate": False,
            },
        },
    }
