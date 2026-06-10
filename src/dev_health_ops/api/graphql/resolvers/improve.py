"""Resolver for the Improve area — Experiments sub-area.

v1 strategy: DERIVED resolver.  Experiments are assembled at query-time from
``OpportunityCard.suggested_experiments``; no persistence table is required.

Each opportunity card holds ``suggested_experiments: list[str]`` (free-text
hypothesis strings keyed by metric).  The resolver calls
``build_opportunities_response``, iterates the resulting cards, and promotes
every suggestion string into a typed ``Experiment`` object whose ``metric``
traces back to the triggering opportunity metric.  ``status`` is always
``SUGGESTED`` for these derived experiments.

Promotion path (v2): once a user assigns an owner + stop_condition the record
can be written to an ``experiments`` table (Postgres semantic / ClickHouse
analytics) and the resolver switches to reading persisted rows, falling back to
derived if the table is empty.  The GraphQL contract is unchanged.

ID stability note: experiment IDs are derived from (metric, suggestion_text)
via SHA-256 so the same logical experiment always receives the same ID
regardless of the opportunity card's daily rank position.  See
``_stable_experiment_id`` for the derivation.
"""

from __future__ import annotations

import hashlib
import logging

from ..context import GraphQLContext
from ..models.improve import Experiment, ExperimentsResult, ExperimentStatus

logger = logging.getLogger(__name__)


def _stable_experiment_id(metric: str, suggestion: str) -> str:
    """Return a 16-hex-char content-stable ID for a derived experiment.

    The ID is derived from (metric, suggestion_text) so it is invariant to
    daily rank-order shifts of the parent opportunity card.  The same
    suggestion text for the same metric always produces the same ID, which
    makes client-side caching and deduplication safe.

    Uses the same SHA-256-prefix approach as
    ``dev_health_ops.metrics.opportunities.scoring.stable_opportunity_id``.
    """
    raw = f"{metric}:{suggestion}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


async def resolve_experiments(
    context: GraphQLContext,
    filters: object | None = None,
) -> ExperimentsResult:
    """Return derived experiments assembled from opportunity suggested_experiments.

    Calls ``build_opportunities_response`` via the same filter path used by the
    REST ``/api/v1/opportunities`` endpoint so results are consistent with the
    Opportunities page.

    Args:
        context: GraphQL request context (carries org_id, db_url, ClickHouse client).
        filters:  Optional ``FilterInput``; forwarded to the opportunities service.

    Returns:
        ``ExperimentsResult`` with ``derived_from_opportunities=True`` when the
        opportunities service returned data, ``False`` on failure (empty items).
    """
    from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
    from dev_health_ops.api.services.cache import TTLCache
    from dev_health_ops.api.services.opportunities import build_opportunities_response

    # Build a MetricFilter from the GraphQL FilterInput (mirrors the REST path).
    try:
        if (
            filters is not None
            and hasattr(filters, "scope")
            and filters.scope is not None
        ):
            scope_in = filters.scope
            # ScopeFilterInput.level may be a Strawberry enum (ScopeLevelInput)
            # whose .value is already the lowercase string, or a plain str.
            raw_level = getattr(scope_in, "level", "org")
            level_str = (
                raw_level.value if hasattr(raw_level, "value") else str(raw_level)
            ).lower() or "org"
            scope = ScopeFilter(
                level=level_str,
                ids=list(getattr(scope_in, "ids", None) or []),
            )
        else:
            scope = ScopeFilter(level="org", ids=[])

        if (
            filters is not None
            and hasattr(filters, "time")
            and filters.time is not None
        ):
            time_in = filters.time
            time_filter = TimeFilter(
                range_days=int(getattr(time_in, "range_days", 30)),
                compare_days=int(getattr(time_in, "compare_days", 30)),
            )
        else:
            time_filter = TimeFilter(range_days=30, compare_days=30)

        metric_filter = MetricFilter(scope=scope, time=time_filter)
    except Exception:
        logger.exception(
            "Failed to build MetricFilter from GraphQL FilterInput; using defaults"
        )
        metric_filter = MetricFilter(
            scope=ScopeFilter(level="org", ids=[]),
            time=TimeFilter(range_days=30, compare_days=30),
        )

    try:
        # Re-use the module-level cache injected by the GraphQL app (warm across
        # requests).  Fall back to a fresh in-memory instance only when running
        # outside the normal app lifecycle (e.g. tests that don't wire a cache).
        cache = context.cache or TTLCache(ttl_seconds=60)
        response = await build_opportunities_response(
            db_url=context.db_url,
            filters=metric_filter,
            cache=cache,
            org_id=context.org_id,
        )
    except Exception:
        logger.exception("Experiments resolver: opportunities service failed")
        return ExperimentsResult(items=[], derived_from_opportunities=False)

    experiments: list[Experiment] = []
    for card in response.items:
        for suggestion in card.suggested_experiments:
            metric = _metric_from_card(card)
            experiments.append(
                Experiment(
                    id=_stable_experiment_id(metric, suggestion),
                    opportunity_id=card.id,
                    hypothesis=suggestion,
                    metric=metric,
                    owner="",
                    stop_condition="",
                    status=ExperimentStatus.SUGGESTED,
                    start_date=None,
                    stop_date=None,
                    outcome=None,
                )
            )

    return ExperimentsResult(items=experiments, derived_from_opportunities=True)


def _metric_from_card(card: object) -> str:
    """Infer the metric key from the opportunity card title.

    The opportunities service currently sets ``title`` as ``"Reduce <metric_label>"``.
    For the baseline "Maintain steady flow" card (no specific metric) we return
    an empty string.

    NOTE: this title-reversal is intentionally lightweight for v1.  The correct
    fix for v2 is to thread a ``metric_key`` field onto ``OpportunityCard``
    directly so callers never need to reverse-engineer it from display text.
    """
    title: str = str(getattr(card, "title", ""))
    if title.startswith("Reduce "):
        # Strip the leading verb and normalise to a metric-key-style slug.
        label = title.split(" ", 1)[1].lower().replace(" ", "_")
        return label
    return ""
