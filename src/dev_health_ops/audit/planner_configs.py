from __future__ import annotations

import argparse
import json

from dev_health_ops.db import get_postgres_session_sync_for_uri, resolve_db_uri
from dev_health_ops.sync.planner_config_audit import (
    audit_active_planner_managed_configs,
)


def register_commands(audit_subparsers: argparse._SubParsersAction) -> None:
    audit_parser = audit_subparsers.add_parser(
        "planner-configs",
        help="Audit active planner-managed sync configs for source tag drift.",
    )
    audit_parser.add_argument(
        "--format", choices=["json"], default="json", help="Output format."
    )
    audit_parser.set_defaults(func=_cmd_audit_planner_configs)


def _cmd_audit_planner_configs(ns: argparse.Namespace) -> int:
    with get_postgres_session_sync_for_uri(resolve_db_uri(ns)) as session:
        findings = audit_active_planner_managed_configs(
            session,
            org_id=getattr(ns, "org", None),
        )
    print(json.dumps([finding.to_dict() for finding in findings], indent=2))
    return 1 if findings else 0
