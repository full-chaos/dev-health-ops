"""Strawberry GraphQL types for the Improve area — Experiments sub-area.

v1 design decision: experiments are DERIVED (computed) from the per-opportunity
``suggested_experiments`` strings already produced by the opportunities service.
No persistence table is needed for v1 — each ``Experiment`` is assembled at
query-time from ``OpportunityCard.suggested_experiments``.  The ``status``
field and the ``opportunity_id`` back-reference keep the schema honest about
the derivation path so the promotion flow (user assigns owner/metric and saves)
can be added in v2 without a breaking change.
"""

from __future__ import annotations

from datetime import date
from enum import Enum

import strawberry


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
