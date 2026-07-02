from __future__ import annotations

import logging
from typing import Any, Literal, NamedTuple, cast

from dev_health_ops.api.graphql.resolvers._membership_run_scope import (
    LEGACY_NODE_MAX_JOIN,
    RUN_SCOPE_PREDICATE,
)
from dev_health_ops.metrics.prometheus import record_investment_membership_scope_stale
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts

logger = logging.getLogger(__name__)

ScopeMode = Literal["scoped", "unscoped_no_marker", "unscoped_fallback"]


class InvestmentMembershipScopeState(NamedTuple):
    scope_mode: ScopeMode
    lag_seconds: int


INVESTMENT_MEMBERSHIP_SCOPE_STATE_CTES = """
        latest_complete_membership_run AS (
            SELECT
                argMax(run_id, completed_at) AS latest_run_id,
                max(completed_at) AS latest_run_completed_at,
                count() AS marker_count
            FROM work_unit_membership_runs
            WHERE org_id = %(org_id)s
        ),
        latest_investment_clock AS (
            SELECT max(computed_at) AS latest_investment_computed_at
            FROM work_unit_investments
            WHERE org_id = %(org_id)s
        ),
        investment_membership_scope_state AS (
            SELECT
                if(
                    marker_count > 0
                    AND latest_run_id != ''
                    AND (
                        latest_investment_computed_at IS NULL
                        OR latest_investment_computed_at <= latest_run_completed_at
                    ),
                    1,
                    0
                ) AS scope_enabled,
                multiIf(
                    marker_count = 0 OR latest_run_id = '', 'unscoped_no_marker',
                    latest_investment_computed_at IS NOT NULL
                    AND latest_investment_computed_at > latest_run_completed_at,
                    'unscoped_fallback',
                    'scoped'
                ) AS scope_mode,
                greatest(
                    0,
                    if(
                        latest_investment_computed_at IS NULL,
                        0,
                        dateDiff('second', latest_run_completed_at, latest_investment_computed_at)
                    )
                ) AS lag_seconds
            FROM latest_complete_membership_run
            CROSS JOIN latest_investment_clock
        )
""".rstrip()

INVESTMENT_MEMBERSHIP_SCOPED_WORK_UNITS_CTE = f"""
        membership_scoped_work_unit_ids AS (
            SELECT DISTINCT m.work_unit_id AS work_unit_id
            FROM work_unit_membership AS m
            INNER JOIN latest_complete_membership_run AS latest_run ON 1 = 1
            {LEGACY_NODE_MAX_JOIN}
            WHERE m.org_id = %(org_id)s
              AND latest_run.latest_run_id != ''
              AND ({RUN_SCOPE_PREDICATE})
        )
""".rstrip()

INVESTMENT_MEMBERSHIP_SCOPE_CTES = f"""
{INVESTMENT_MEMBERSHIP_SCOPE_STATE_CTES},
{INVESTMENT_MEMBERSHIP_SCOPED_WORK_UNITS_CTE}
""".rstrip()

INVESTMENT_MEMBERSHIP_SCOPE_FILTER = """
              AND (
                  (SELECT scope_enabled FROM investment_membership_scope_state) = 0
                  OR work_unit_id IN (
                      SELECT work_unit_id FROM membership_scoped_work_unit_ids
                  )
              )
""".rstrip()


async def fetch_investment_membership_scope_state(
    sink: BaseMetricsSink,
    *,
    org_id: str,
) -> InvestmentMembershipScopeState:
    query = f"""
        WITH {INVESTMENT_MEMBERSHIP_SCOPE_STATE_CTES}
        SELECT scope_mode, lag_seconds
        FROM investment_membership_scope_state
    """
    rows = await query_dicts(sink, query, {"org_id": org_id})
    if not rows:
        return InvestmentMembershipScopeState("unscoped_no_marker", 0)
    row = rows[0]
    mode = str(row.get("scope_mode") or "unscoped_no_marker")
    if mode not in {"scoped", "unscoped_no_marker", "unscoped_fallback"}:
        mode = "unscoped_no_marker"
    return InvestmentMembershipScopeState(
        cast(ScopeMode, mode), int(float(row.get("lag_seconds") or 0))
    )


async def record_stale_investment_membership_scope(
    sink: BaseMetricsSink,
    *,
    org_id: str,
) -> None:
    try:
        state = await fetch_investment_membership_scope_state(sink, org_id=org_id)
    except Exception as exc:
        logger.debug("investment membership scope metric skipped: %s", exc)
        return
    if state.scope_mode != "unscoped_fallback":
        return
    record_investment_membership_scope_stale(
        lag_seconds=state.lag_seconds,
        scope_mode=state.scope_mode,
    )
    logger.warning(
        "investment membership scope stale for org %s; falling back unscoped "
        "(lag_seconds=%s)",
        org_id,
        state.lag_seconds,
    )


def extract_scope_state_from_rows(
    rows: list[dict[str, Any]],
) -> InvestmentMembershipScopeState:
    if not rows:
        return InvestmentMembershipScopeState("unscoped_no_marker", 0)
    row = rows[0]
    mode = str(row.get("scope_mode") or "unscoped_no_marker")
    if mode not in {"scoped", "unscoped_no_marker", "unscoped_fallback"}:
        mode = "unscoped_no_marker"
    return InvestmentMembershipScopeState(
        cast(ScopeMode, mode), int(float(row.get("lag_seconds") or 0))
    )
