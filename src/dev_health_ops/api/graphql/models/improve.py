"""Strawberry GraphQL types for the Improve area — Experiments + Automations.

This module holds ALL Improve-area types:

- **Experiments** (CHAOS-2219): DERIVED types assembled at query-time from
  ``OpportunityCard.suggested_experiments``.  No persistence table is needed
  for v1 — the type contract is stable so persistence can be added in v2
  without a breaking change.

- **Automations / Flow opportunities** (CHAOS-2220): Non-AI flow opportunity
  types powering the Improve → Automations surface.  They mirror the domain
  models in ``dev_health_ops.metrics.opportunities.models`` but carry
  ``@strawberry`` decorators for direct schema serving.  Defined here (not in
  ``models/ai.py``) because the Improve opportunity kinds are **non-AI** — the
  detector fires on flow / cycle / rework signals from ``repo_metrics_daily``
  and ``work_item_metrics_daily``, not on AI attribution data.

The ``AIScopeInput`` re-use for Automations is intentional: the scope shape
(optional repo_id, team_id) is the same; introducing a duplicate input type
would be noise.
"""

from __future__ import annotations

from datetime import date
from enum import Enum

import strawberry


# ── Experiments (CHAOS-2219) ──────────────────────────────────────────────────


@strawberry.enum
class ExperimentStatus(Enum):
    """Lifecycle state of an experiment.

    ``SUGGESTED`` is the v1 state for experiments that are derived from
    opportunity cards but have not yet been promoted into a tracked run.
    """

    SUGGESTED = "suggested"
    """Derived from an opportunity card — not yet owned or started."""

    ACTIVE = "active"
    """An owner has claimed the experiment and it is in flight."""

    COMPLETED = "completed"
    """The experiment has a measured outcome."""

    ABANDONED = "abandoned"
    """The experiment was dropped before producing an outcome."""


@strawberry.type
class Experiment:
    """A single process experiment derived from or promoted from an opportunity.

    In v1 every ``Experiment`` is derived from
    ``OpportunityCard.suggested_experiments`` at query-time.  Fields that
    belong to a promoted, tracked experiment (``owner``, ``metric``,
    ``stop_condition``, ``start_date``, ``stop_date``, ``outcome``) are
    included in the schema now so the GraphQL contract is stable when
    persistence is added in v2.

    ``opportunity_id`` traces the experiment back to the source opportunity
    so consumers can navigate opportunity → experiments and vice-versa.
    """

    id: str
    """Stable synthetic id of the form ``<opportunity_id>-exp-<index>``."""

    opportunity_id: str
    """The parent opportunity this experiment was derived from."""

    hypothesis: str
    """The experiment hypothesis, sourced from the opportunity's suggested text."""

    metric: str
    """Metric key the experiment is expected to improve (e.g. ``cycle_time``)."""

    owner: str
    """Team or person running the experiment.  Empty string for SUGGESTED."""

    stop_condition: str
    """Measurable criterion for concluding the experiment.  Empty for SUGGESTED."""

    status: ExperimentStatus
    """Current lifecycle state.  Always ``SUGGESTED`` for derived experiments."""

    start_date: date | None
    """Date the experiment started.  None for SUGGESTED."""

    stop_date: date | None
    """Date the experiment concluded.  None for SUGGESTED."""

    outcome: str | None
    """Observed outcome.  None for SUGGESTED."""


@strawberry.type
class ExperimentsResult:
    """Container for the experiments query response.

    ``items`` is empty when no opportunities are available — the caller should
    render the appropriate DataState variant (``detector-enabled-no-findings``)
    rather than inferring data availability from ``len(items) == 0`` alone.
    The ``derived_from_opportunities`` flag signals whether the list was built
    from live opportunity data (``True``) or is empty because the opportunities
    service was unavailable (``False``).
    """

    items: list[Experiment]
    derived_from_opportunities: bool


# ── Automations / Flow opportunities (CHAOS-2220) ─────────────────────────────


@strawberry.enum
class ImproveOpportunityKind(Enum):
    """Non-AI flow opportunity kinds detected by FlowOpportunityDetector.

    Each value maps to one of the seven threshold rules implemented in
    ``metrics.opportunities.flow_detector``.
    """

    HIGH_REVIEW_LATENCY = "high_review_latency"
    SLOW_CYCLE_TIME = "slow_cycle_time"
    HIGH_REWORK = "high_rework"
    HIGH_WIP = "high_wip"
    LOW_THROUGHPUT = "low_throughput"
    HIGH_CHURN = "high_churn"
    HIGH_CHANGE_FAILURE = "high_change_failure"


@strawberry.type
class ImproveOpportunity:
    """A single scored flow / improve opportunity (non-AI kind)."""

    opportunity_id: str
    kind: ImproveOpportunityKind
    entity_type: str
    entity_id: str
    title: str
    rationale: str
    score: float
    severity: str
    evidence_refs: list[str]
    recommended_action: str


@strawberry.type
class ImproveOpportunitiesResult:
    """Improve automations surface: non-AI flow opportunity candidates.

    ``detector_ready`` is ``True`` whenever the FlowOpportunityDetector ran
    without a total failure (it can return an empty list when no thresholds
    fire — that is a valid "all green" state, not an error).
    ``detector_ready = False`` means the detector could not connect to
    ClickHouse or the org scope could not be resolved.
    """

    org_id: str
    opportunities: list[ImproveOpportunity]
    detector_ready: bool
    total_count: int
