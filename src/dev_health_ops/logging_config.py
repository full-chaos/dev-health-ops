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

# TRACE level (numeric 5, below DEBUG=10) — matches uvicorn's convention.
# Registered here so LOG_LEVEL=trace works before uvicorn initialises.
TRACE_LOG_LEVEL = 5
if logging.getLevelName(TRACE_LOG_LEVEL) == f"Level {TRACE_LOG_LEVEL}":
    logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

# Loggers owned by HTTP/provider client SDKs that log full request/response
# bodies (prompts, tool schemas, tool-result payloads, endpoint URLs) at
# DEBUG. Ask Dev never logs conversation or evidence content itself (see
# openai_compatible.py / orchestrator.py), so the only leak vector is these
# third-party loggers inheriting an operator-raised root LOG_LEVEL (e.g.
# DEBUG, set via compose.yml for local diagnostics). They are pinned to
# WARNING regardless of root LOG_LEVEL so enabling debug logging elsewhere in
# the application can never copy tenant conversation or evidence content into
# ordinary application logs (CHAOS-3258).
#
# - "openai": the OpenAI Python SDK (openai._base_client logs the full
#   "Request options" -- messages, tools, schemas -- at DEBUG, on SDK
#   versions old enough to still include it; see the CHAOS-4346 note below).
# - "httpx": logs request/response lines; DEBUG adds headers.
# - "httpcore": httpx's transport; DEBUG logs raw connection/wire detail.
# - "httpx2" / "httpcore2": the openai SDK's own pinned transport dependency
#   (pyproject.toml: "httpx2<3,>=2.7.0") is a distinctly-named package, not
#   "httpx" -- its loggers are "httpx2"/"httpcore2.*", so clamping only
#   "httpx"/"httpcore" never reached the loggers the SDK's requests actually
#   go through (CHAOS-4346: found while investigating an openai 3.4.0 CI
#   break -- that release also stopped including request/response bodies in
#   its own "openai" logger's DEBUG line, per its own _base_client.py:
#   "Request bodies, files, URLs, and custom options can contain private
#   data" -- an upstream privacy improvement, not something this pin should
#   rely on staying true).
_CONTENT_CARRYING_CLIENT_LOGGERS = (
    "openai",
    "httpx",
    "httpcore",
    "httpx2",
    "httpcore2",
)


def pin_content_carrying_client_loggers() -> None:
    """(Re)pin the content-carrying client loggers to WARNING.

    ``configure_logging()`` calls this at process startup, but it must also
    be called again immediately after any lazy ``import openai`` (see
    ``llm/agent/openai_compatible.py``, ``llm/providers/openai.py``,
    ``llm/providers/local.py``). The OpenAI Python SDK runs its own logging
    setup exactly once, at import time (``openai._utils._logs.setup_logging``),
    and -- when the operator sets the standard ``OPENAI_LOG`` env var to
    ``"debug"``/``"info"`` -- that setup unconditionally resets the
    ``openai`` and ``httpx`` loggers, regardless of what
    ``configure_logging()`` already pinned. Because Ask Dev imports the SDK
    lazily, per first use, that reset happens well after process-startup
    ``configure_logging()`` has already run, silently reopening the exact
    content-logging leak CHAOS-3258 closes. Reapplying this pin immediately
    after the (possibly first-ever) import closes that import-order gap;
    since Python caches the module, this only needs to happen once per
    process, at each known import call site.
    """
    for name in _CONTENT_CARRYING_CLIENT_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def _resolve_log_level(raw: str) -> int:
    """Convert a level name to its numeric value, falling back to INFO."""
    numeric = logging.getLevelName(raw)
    if isinstance(numeric, int):
        return numeric
    logging.getLogger(__name__).warning(
        "Unknown LOG_LEVEL %r, falling back to INFO",
        raw,
    )
    return logging.INFO


def configure_logging(level: str | None = None) -> None:
    """Set up JSON structured logging for the entire application.

    Safe to call multiple times (idempotent).
    """
    raw_level = level or os.getenv("LOG_LEVEL") or "INFO"
    raw_log_json = os.getenv("LOG_JSON") or "true"
    log_level = _resolve_log_level(raw_level.upper())
    use_json = raw_log_json.lower() not in ("false", "0", "no")

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

    # Silence noisy third-party loggers.
    for noisy in ("uvicorn.access", "watchfiles"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Pinned unconditionally -- never derived from log_level -- so a DEBUG
    # root level can never resurrect provider request/response body logging
    # (CHAOS-3258). Must also be reapplied after any lazy `import openai`
    # (see pin_content_carrying_client_loggers's docstring for why).
    pin_content_carrying_client_loggers()


def uvicorn_log_config(level: str | None = None) -> dict[str, Any]:
    """Return a uvicorn log_config dict that emits JSON access logs.

    Pass to ``uvicorn.Config(log_config=uvicorn_log_config())``.
    """
    raw_level = level or os.getenv("LOG_LEVEL") or "info"
    raw_log_json = os.getenv("LOG_JSON") or "true"
    log_level = raw_level.lower()
    use_json = raw_log_json.lower() not in ("false", "0", "no")

    if use_json:
        formatter_class = "pythonjsonlogger.json.JsonFormatter"
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        formatter_config: dict[str, Any] = {
            "()": formatter_class,
            "fmt": fmt,
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "rename_fields": {"asctime": "timestamp", "levelname": "level"},
        }
    else:
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        formatter_config = {
            "()": "logging.Formatter",
            "fmt": fmt,
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": formatter_config,
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
