"""Resolver for the rule-based recommendations engine queries."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.recommendations import (
    EvidenceRef,
    Recommendation,
    Severity,
    WindowInput,
    WindowUnit,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL — argMax reads the latest computed row per (org_id, team_id, rule_id, window_end).
# These are the ORDER BY columns in migration 039_recommendations.sql.
# Table: recommendations_daily (ClickHouse, append-only / ReplacingMergeTree).
#
# Resolution is two-stage so a *recovered* signal stops showing (CHAOS-2373):
#   1. inner: argMax(..., computed_at) collapses re-runs of the SAME window_end.
#   2. outer: argMax(..., window_end) keeps ONLY the latest window_end (as-of
#      day) per (org, team, rule). Each scheduled run writes the full rule
#      state — fired rows AND explicit fired=false tombstones — at
#      window_end=today, so the most recent as-of dominates and a stale fired
#      row from an earlier day is superseded instead of lingering in-range.
# Final HAVING keeps only currently-fired rules.
# ---------------------------------------------------------------------------
_RECOMMENDATIONS_SQL = """\
SELECT
    team_id,
    org_id,
    rule_id,
    argMax(latest_fired,             window_end) AS latest_fired,
    argMax(latest_severity,          window_end) AS latest_severity,
    argMax(latest_title,             window_end) AS latest_title,
    argMax(latest_rationale,         window_end) AS latest_rationale,
    argMax(latest_success_criterion, window_end) AS latest_success_criterion,
    argMax(latest_evidence_json,     window_end) AS latest_evidence_json,
    argMax(latest_window_start,      window_end) AS latest_window_start,
    max(window_end)                              AS latest_window_end,
    argMax(latest_computed_at,       window_end) AS latest_computed_at
FROM (
    SELECT
        team_id,
        org_id,
        rule_id,
        window_end,
        argMax(fired,               computed_at) AS latest_fired,
        argMax(severity,            computed_at) AS latest_severity,
        argMax(title,               computed_at) AS latest_title,
        argMax(rationale,           computed_at) AS latest_rationale,
        argMax(success_criterion,   computed_at) AS latest_success_criterion,
        argMax(evidence_json,       computed_at) AS latest_evidence_json,
        argMax(window_start,        computed_at) AS latest_window_start,
        max(computed_at)                         AS latest_computed_at
    FROM recommendations_daily
    WHERE team_id  = {team_id:String}
      AND org_id   = {org_id:String}
      AND window_end >= {window_start:Date}
      AND window_end <= {window_end:Date}
    GROUP BY org_id, team_id, rule_id, window_end
)
GROUP BY org_id, team_id, rule_id
HAVING latest_fired = true
ORDER BY latest_window_end DESC, rule_id
"""


def _window_to_dates(window: WindowInput) -> tuple[date, date]:
    """Convert a WindowInput to (window_start, window_end) date bounds.

    window_end is always today (UTC); window_start is computed from the
    unit/value pair.  A *cycle* is treated as 14 days (two-week sprint).
    """
    today = datetime.now(tz=timezone.utc).date()
    days = window.value * 7
    match window.unit:
        case WindowUnit.DAY:
            days = window.value
        case WindowUnit.WEEK:
            days = window.value * 7
        case WindowUnit.CYCLE:
            days = window.value * 14
    return today - timedelta(days=days), today


def _parse_evidence(raw: str | list[Any] | None) -> list[EvidenceRef]:
    """Deserialise the ``evidence_json`` column into EvidenceRef objects.

    The engine serialises evidence as a JSON array whose object keys match
    the canonical ``EvidenceRef`` field names exactly:
    ``team_id``, ``metric_table``, ``window_start``, ``window_end``,
    ``field``, ``value``.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            items: list[Any] = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            logger.warning("Failed to parse evidence_json: %r", raw[:200])
            return []
    else:
        items = list(raw)

    refs: list[EvidenceRef] = []
    for ev in items:
        if not isinstance(ev, dict):
            continue
        try:
            ws_raw = ev.get("window_start", "")
            we_raw = ev.get("window_end", "")
            refs.append(
                EvidenceRef(
                    team_id=str(ev.get("team_id", "")),
                    metric_table=str(ev.get("metric_table", "")),
                    window_start=date.fromisoformat(str(ws_raw))
                    if ws_raw
                    else date.min,
                    window_end=date.fromisoformat(str(we_raw)) if we_raw else date.min,
                    field=str(ev.get("field", "")),
                    value=float(ev.get("value", 0.0)),
                )
            )
        except (KeyError, ValueError, TypeError):
            logger.warning("Skipping malformed evidence entry: %r", ev)
    return refs


def _row_to_recommendation(row: dict[str, Any]) -> Recommendation | None:
    """Map a ClickHouse result row to a Recommendation GraphQL type.

    Returns None for rows that cannot be safely coerced (logged as warnings).
    """
    try:
        raw_sev = str(
            row.get("latest_severity") or row.get("severity", "warning")
        ).lower()
        try:
            severity = Severity(raw_sev)
        except ValueError:
            severity = Severity.WARNING

        raw_ws = row.get("latest_window_start") or row.get("window_start")
        raw_we = row.get("latest_window_end") or row.get("window_end")
        window_start = (
            raw_ws if isinstance(raw_ws, date) else date.fromisoformat(str(raw_ws))
        )
        window_end = (
            raw_we if isinstance(raw_we, date) else date.fromisoformat(str(raw_we))
        )

        raw_cat = row.get("latest_computed_at") or row.get("computed_at")
        if isinstance(raw_cat, datetime):
            computed_at = raw_cat
        else:
            computed_at = datetime.fromisoformat(str(raw_cat))
        if computed_at.tzinfo is None:
            computed_at = computed_at.replace(tzinfo=timezone.utc)

        return Recommendation(
            rule_id=str(row["rule_id"]),
            team_id=str(row["team_id"]),
            org_id=str(row["org_id"]),
            computed_at=computed_at,
            window_start=window_start,
            window_end=window_end,
            severity=severity,
            title=str(row.get("latest_title") or row.get("title", "")),
            rationale=str(row.get("latest_rationale") or row.get("rationale", "")),
            success_criterion=str(
                row.get("latest_success_criterion") or row.get("success_criterion", "")
            ),
            evidence=_parse_evidence(
                row.get("latest_evidence_json") or row.get("evidence_json")
            ),
        )
    except (KeyError, ValueError, TypeError):
        logger.warning("Skipping malformed recommendation row: %r", row)
        return None


async def resolve_recommendations(
    context: GraphQLContext,
    team: str,
    window: WindowInput,
) -> list[Recommendation]:
    """Return the latest persisted recommendations for a team within a window.

    Reads from ``recommendations_daily`` (ClickHouse, append-only) using
    ``argMax(..., computed_at)`` to retrieve the most recent computation for
    each ``(team_id, rule_id, day)`` tuple.  Only rows where ``fired = 1``
    are returned.

    Args:
        context: GraphQL request context (carries org_id, ClickHouse client).
        team:    Team ID to query.
        window:  Lookback window (value + unit) translated to a date range.

    Returns:
        Ordered list of Recommendation objects, most-recent day first.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")

    window_start, window_end = _window_to_dates(window)
    params: dict[str, Any] = {
        "team_id": team,
        "org_id": org_id,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }

    try:
        rows = await query_dicts(client, _RECOMMENDATIONS_SQL, params)
    except Exception:
        logger.exception(
            "Failed to fetch recommendations for team=%s window=%s–%s",
            team,
            window_start,
            window_end,
        )
        return []

    results: list[Recommendation] = []
    for row in rows:
        rec = _row_to_recommendation(row)
        if rec is not None:
            results.append(rec)
    return results
