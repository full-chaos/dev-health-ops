from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import timedelta

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

from .operational_clickhouse import run_canonical_operational_backfill
from .runner import run_backfill_for_config

logger = logging.getLogger(__name__)


def register_backfill_commands(subparsers: argparse._SubParsersAction) -> None:
    backfill_parser = subparsers.add_parser(
        "backfill", help="Historical backfill operations."
    )
    backfill_subparsers = backfill_parser.add_subparsers(
        dest="backfill_command", required=True
    )

    run_parser = backfill_subparsers.add_parser("run", help="Run historical backfill.")
    run_parser.add_argument(
        "--config-id",
        required=True,
        help="Sync configuration UUID (its organization is used; --org is optional)",
    )
    add_date_range_args(run_parser)
    add_sink_arg(run_parser)
    run_parser.set_defaults(func=_cmd_backfill_run)

    operational_parser = backfill_subparsers.add_parser(
        "operational",
        help="Migrate legacy incident producers into canonical operational tables.",
    )
    operational_parser.add_argument("--org", required=True, help="Organization id")
    operational_parser.add_argument(
        "--github-provider-instance-id",
        help="Explicit GitHub instance override for legacy rows without a persisted host",
    )
    operational_parser.add_argument(
        "--gitlab-provider-instance-id",
        help="Explicit GitLab instance override for legacy rows without a persisted host",
    )
    operational_parser.add_argument(
        "--atlassian-provider-instance-id",
        default="atlassian-ops",
        help="Atlassian Ops instance",
    )
    add_sink_arg(operational_parser)
    operational_parser.set_defaults(func=_cmd_backfill_operational)


def _cmd_backfill_run(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        since = end_day - timedelta(days=backfill_days - 1)
        before = end_day
        # org is derived from the sync configuration (--config-id); --org is an
        # optional assertion validated inside run_backfill_for_config.
        org_id = str(getattr(ns, "org", "") or "") or None

        def _progress(index: int, total: int, window_since, window_before) -> None:
            print(
                f"Syncing window {index}/{total}: {window_since.isoformat()} to {window_before.isoformat()}..."
            )

        run_backfill_for_config(
            db_url=resolve_sink_uri(ns),
            sync_config_id=ns.config_id,
            org_id=org_id,
            since=since,
            before=before,
            sink=ns.sink,
            chunk_days=7,
            progress_cb=_progress,
        )
        return 0
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        return 1


def _cmd_backfill_operational(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        result = asyncio.run(
            run_canonical_operational_backfill(
                clickhouse_uri=resolve_sink_uri(ns),
                org_id=ns.org,
                github_provider_instance_id=ns.github_provider_instance_id,
                gitlab_provider_instance_id=ns.gitlab_provider_instance_id,
                atlassian_provider_instance_id=ns.atlassian_provider_instance_id,
            )
        )
        print(
            "Migrated canonical operational rows: "
            f"services={result.services}, incidents={result.incidents}, "
            f"alerts={result.alerts}, schedules={result.schedules}, "
            f"service_repository_mappings={result.service_repository_mappings}; "
            f"parity_verified={str(result.parity_verified).lower()}, "
            f"incidents={result.verified_incidents}/{result.expected_incidents}, "
            "service_repository_mappings="
            f"{result.verified_service_repository_mappings}/"
            f"{result.expected_service_repository_mappings}"
        )
        return 0
    except Exception as exc:
        logger.error("Canonical operational backfill failed: %s", exc)
        return 1
