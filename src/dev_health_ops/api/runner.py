import argparse
import logging
import os

import uvicorn

from dev_health_ops.logging_config import configure_logging, uvicorn_log_config


def run_api_server(ns: argparse.Namespace) -> int:
    """Start the FastAPI server."""
    if ns.db:
        os.environ["DATABASE_URI"] = ns.db

    if getattr(ns, "analytics_db", None):
        os.environ["CLICKHOUSE_URI"] = ns.analytics_db

    log_level = str(getattr(ns, "log_level", "") or "INFO").upper()
    configure_logging(level=log_level)

    logger = logging.getLogger(__name__)

    reload = bool(getattr(ns, "reload", False))
    workers = ns.workers or 1

    reload_dirs: list[str] | None = None
    if reload and os.path.isdir("/app/src"):
        # In the docker image the src/ tree is mounted at /app/src; scope the
        # watcher so we don't churn on venv or pyc changes. Locally (no /app),
        # fall through to uvicorn's default of watching the cwd.
        reload_dirs = ["/app/src"]

    logger.info(
        "Starting API server",
        extra={"host": ns.host, "port": ns.port, "reload": reload, "workers": workers},
    )
    try:
        # uvicorn.Server(config).run() does NOT honor reload/workers — those
        # require the ChangeReload / Multiprocess supervisors, which only get
        # wired up when going through uvicorn.run(). Passing reload=True
        # directly to Server silently degrades to no-reload.
        uvicorn.run(
            "dev_health_ops.api.main:app",
            host=ns.host,
            port=ns.port,
            log_level=log_level.lower(),
            log_config=uvicorn_log_config(level=log_level),
            workers=workers,
            reload=reload,
            reload_dirs=reload_dirs,
        )
        return 0
    except Exception as e:
        logger.error("API server failed", extra={"error": str(e)})
        return 1


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    api = subparsers.add_parser("api", help="Run the Dev Health Ops API server.")
    api.add_argument("--host", default="127.0.0.1", help="Bind host.")
    api.add_argument("--port", type=int, default=8000, help="Bind port.")
    api.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes."
    )
    api.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for local development.",
    )
    from dev_health_ops.llm.cli import add_llm_arguments

    add_llm_arguments(api)
    api.set_defaults(func=run_api_server)
