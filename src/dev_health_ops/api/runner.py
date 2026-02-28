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

    config = uvicorn.Config(
        "dev_health_ops.api.main:app",
        host=ns.host,
        port=ns.port,
        log_level=log_level.lower(),
        log_config=uvicorn_log_config(level=log_level),
        workers=ns.workers or 1,
        reload=ns.reload if hasattr(ns, "reload") else False,
    )
    server = uvicorn.Server(config)

    logger.info("Starting API server", extra={"host": ns.host, "port": ns.port})
    try:
        server.run()
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
