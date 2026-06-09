"""Admin CLI for the org-level AI tool allowlist (CHAOS-2209).

The allowlist source of truth is admin-seeded: entries are written through
the ClickHouse sink (sinks-only persistence) and read back with
latest-version semantics (``ai_tool_allowlist`` is a ReplacingMergeTree
versioned by ``computed_at``).

Usage::

    dev-hops ai allowlist set --tool claude-code --status allowed \
        [--model claude] [--reason "Org policy AI-001"]
    dev-hops ai allowlist list
"""

from __future__ import annotations

import argparse
import logging
import os

from dev_health_ops.audit.ai_governance.models import (
    AIToolAllowlistEntry,
    ToolAllowlistStatus,
)

logger = logging.getLogger(__name__)

_SETTABLE_STATUSES = (
    ToolAllowlistStatus.ALLOWED.value,
    ToolAllowlistStatus.DISALLOWED.value,
    ToolAllowlistStatus.DEPRECATED.value,
)


def _require_org(ns: argparse.Namespace) -> str:
    org_id = str(getattr(ns, "org", None) or "")
    if not org_id:
        raise SystemExit("--org (or ORG_ID) is required for allowlist commands.")
    return org_id


def _require_analytics_db(ns: argparse.Namespace) -> str:
    db_url = str(getattr(ns, "analytics_db", None) or os.getenv("CLICKHOUSE_URI") or "")
    if not db_url:
        raise SystemExit(
            "--analytics-db (or CLICKHOUSE_URI) is required for allowlist commands."
        )
    return db_url


def _sink(ns: argparse.Namespace):
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    sink = ClickHouseMetricsSink(_require_analytics_db(ns))
    sink.ensure_tables()
    return sink


def _cmd_allowlist_set(ns: argparse.Namespace) -> int:
    org_id = _require_org(ns)
    entry = AIToolAllowlistEntry(
        org_id=org_id,
        tool_name=ns.tool,
        model_name=ns.model,
        status=ToolAllowlistStatus(ns.status),
        reason=ns.reason,
    )
    sink = _sink(ns)
    sink.write_ai_tool_allowlist([entry])
    scope = (
        f"{entry.tool_name}/{entry.model_name}" if entry.model_name else entry.tool_name
    )
    logger.info("Allowlist updated: org=%s %s -> %s", org_id, scope, entry.status.value)
    return 0


def _cmd_allowlist_list(ns: argparse.Namespace) -> int:
    org_id = _require_org(ns)
    sink = _sink(ns)
    rows = sink.query_dicts(
        "SELECT tool_name,"
        " model_name,"
        " argMax(status, computed_at) AS status,"
        " argMax(reason, computed_at) AS reason,"
        " max(updated_at) AS updated_at"
        " FROM ai_tool_allowlist"
        " WHERE org_id = {org_id:String}"
        " GROUP BY tool_name, model_name"
        " ORDER BY tool_name, model_name",
        {"org_id": org_id},
    )
    if not rows:
        print(f"No allowlist entries for org {org_id}.")
        return 0
    print(f"AI tool allowlist for org {org_id}:")
    for row in rows:
        model = row.get("model_name") or "*"
        reason = row.get("reason") or ""
        print(
            f"  {row.get('tool_name')} / {model}: {row.get('status')}"
            + (f" — {reason}" if reason else "")
        )
    return 0


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``ai`` command group (``ai allowlist set|list``)."""
    ai_parser = subparsers.add_parser(
        "ai", help="AI governance administration commands."
    )
    ai_sub = ai_parser.add_subparsers(dest="ai_command", required=True)

    allowlist_parser = ai_sub.add_parser(
        "allowlist", help="Manage the org-level AI tool allowlist."
    )
    allowlist_sub = allowlist_parser.add_subparsers(
        dest="allowlist_command", required=True
    )

    set_parser = allowlist_sub.add_parser(
        "set", help="Create or update an allowlist entry."
    )
    set_parser.add_argument(
        "--tool", required=True, help="Tool name (e.g. claude-code)."
    )
    set_parser.add_argument(
        "--model",
        default=None,
        help="Optional model name. Omit to apply to every model of the tool.",
    )
    set_parser.add_argument(
        "--status",
        required=True,
        choices=_SETTABLE_STATUSES,
        help="Policy status for the tool/model.",
    )
    set_parser.add_argument(
        "--reason", default=None, help="Optional human-readable policy rationale."
    )
    set_parser.set_defaults(func=_cmd_allowlist_set)

    list_parser = allowlist_sub.add_parser(
        "list", help="Show the latest allowlist entries for the org."
    )
    list_parser.set_defaults(func=_cmd_allowlist_list)
