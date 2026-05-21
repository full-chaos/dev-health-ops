"""Resolver for weekly Engineering Operating Review queries."""

from __future__ import annotations

import logging
from typing import Any

from dev_health_ops.metrics.operating_review import (
    OperatingReviewRows,
    build_operating_review_queries,
    compute_operating_review,
    prior_week_start,
    week_bounds,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import OperatingReviewInput
from ..models.outputs import (
    OperatingReview,
    OperatingReviewDelta,
    OperatingReviewMetric,
    OperatingReviewSection,
)

logger = logging.getLogger(__name__)


async def resolve_operating_review(
    context: GraphQLContext,
    input: OperatingReviewInput,
) -> OperatingReview:
    """Return the weekly operating review payload for a team/week tuple."""

    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")

    current = await _fetch_period_rows(
        client,
        query_dicts,
        org_id=org_id,
        team_id=input.team_id,
        start=input.week_start,
    )
    prior = await _fetch_period_rows(
        client,
        query_dicts,
        org_id=org_id,
        team_id=input.team_id,
        start=prior_week_start(input.week_start),
    )
    review = compute_operating_review(
        org_id=org_id,
        team_id=input.team_id,
        week_start=input.week_start,
        current=current,
        prior=prior,
    )
    return _to_graphql_review(review)


async def _fetch_period_rows(
    client: Any,
    query_dicts: Any,
    *,
    org_id: str,
    team_id: str | None,
    start: Any,
) -> OperatingReviewRows:
    end = week_bounds(start)[1]
    params: dict[str, Any] = {"org_id": org_id, "start": start, "end": end}
    if team_id is not None:
        params["team_id"] = team_id
    rows: dict[str, list[dict[str, Any]]] = {}
    for query in build_operating_review_queries(team_id=team_id):
        try:
            rows[query.key] = await query_dicts(client, query.sql, params)
        except Exception:
            logger.exception("Failed to fetch operating review rows for %s", query.key)
            rows[query.key] = []
    return OperatingReviewRows(
        work_items=rows.get("work_items", []),
        state_durations=rows.get("state_durations", []),
        repo_metrics=rows.get("repo_metrics", []),
        hotspots=rows.get("hotspots", []),
        complexity=rows.get("complexity", []),
        deployments=rows.get("deployments", []),
        incidents=rows.get("incidents", []),
        investment=rows.get("investment", []),
    )


def _to_graphql_review(
    review: Any,
) -> OperatingReview:
    return OperatingReview(
        org_id=review.org_id,
        team_id=review.team_id,
        week_start=review.week_start,
        prior_week_start=review.prior_week_start,
        sections=[_to_graphql_section(section) for section in review.sections],
        recommendations=review.recommendations,
        recommendations_empty_state=review.recommendations_empty_state,
    )


def _to_graphql_section(section: Any) -> OperatingReviewSection:
    return OperatingReviewSection(
        key=section.key,
        title=section.title,
        metrics=[_to_graphql_metric(metric) for metric in section.metrics],
        changed=section.changed,
        improved=section.improved,
        worsened=section.worsened,
    )


def _to_graphql_metric(metric: Any) -> OperatingReviewMetric:
    return OperatingReviewMetric(
        key=metric.key,
        label=metric.label,
        value=metric.value,
        unit=metric.unit,
        delta=OperatingReviewDelta(
            value=metric.delta.value,
            prior_value=metric.delta.prior_value,
            absolute=metric.delta.absolute,
            percent=metric.delta.percent,
            status=metric.delta.status,
        ),
    )
