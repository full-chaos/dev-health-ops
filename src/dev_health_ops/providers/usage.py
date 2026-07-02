"""Shared provider request-usage recorder (CHAOS-2754).

Three provider clients (github, gitlab, jira) historically carried a
near-identical in-memory usage recorder that keyed observations by the raw,
interpolated ``(transport, operation)`` string. That keying had two problems:

* Per-issue-number operation labels (e.g. ``"GET issue events for #123"``)
  produced an unbounded number of distinct keys, overflowing the 50-key cap so
  most actuals were dropped as ``summary/overflow``.
* The keys did not share the ``route_family`` / ``dimension`` vocabulary the
  budget *estimators* emit (see ``providers/*/budget.py``), so recorded actuals
  could never be joined against the budget estimate for calibration.

This module extracts the recorder once and re-keys observations by
``(transport, route_family, dimension)`` using a per-provider
:class:`OperationResolver` built from the declarative registries exported by
each ``providers/<provider>/budget.py`` (mirroring the shape of
``LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES``). Per-issue labels now collapse onto the
route-family key while the most recent interpolated label is retained as
``example_operation`` for debugging.

The recorder is deliberately provider-neutral: the per-provider REST/GraphQL
header extraction (which differs across providers) stays in each client and
feeds the already-normalized ``headers`` / ``rate_limit`` / ``status`` into
:meth:`UsageRecorder.record`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dev_health_ops.sync.budget_types import BudgetDimension

# Bounded key cap retained as a defensive backstop. After re-keying by
# route_family the key cardinality is inherently small (one per instrumented
# route_family x dimension x transport), so overflow should never trip in
# practice; it only fires if a resolver were mis-wired to pass raw labels.
MAX_USAGE_OBSERVATION_KEYS = 50

# Sentinel route_family used when an operation cannot be resolved to any budget
# route_family. Aggregating unresolved operations under one key keeps the
# recorder bounded (no per-operation overflow) instead of leaking cardinality.
UNCLASSIFIED_ROUTE_FAMILY = "unclassified"


@dataclass(frozen=True)
class UsageRouteFamily:
    """Declarative operation -> ``(route_family, dimension)`` mapping entry.

    Mirrors ``LaunchDarklyBudgetRouteFamily``: pairs a budget ``route_family`` +
    ``dimension`` (the identifiers the estimator emits) with the client
    operation labels that consume that budget, so recorded actuals key by the
    same vocabulary an estimate is keyed by.

    ``operation_markers`` are lowercase substrings tested against the recorded
    operation label; the first family whose transport matches and whose marker
    is present wins. An **empty** ``operation_markers`` marks a family the
    estimator budgets for but whose fetch path is not instrumented here — such
    entries document the full budget vocabulary (and satisfy the
    estimator-coverage contract test) without ever matching a live operation.
    """

    route_family: str
    dimension: BudgetDimension
    transport: str | None = None
    operation_markers: tuple[str, ...] = ()


@dataclass(frozen=True)
class OperationResolver:
    """Resolves a recorded ``(transport, operation)`` to ``(route_family,
    dimension)`` using an ordered registry plus per-transport fallbacks.

    ``families`` is scanned in order; marker matching is case-insensitive.
    When no family matches, the transport's entry in ``defaults`` is used; if
    the transport itself is unknown the observation collapses onto
    :data:`UNCLASSIFIED_ROUTE_FAMILY` (still bounded, never per-operation).

    **Explicit-prefix short-circuit (CHAOS-2773 CS1).** Before the substring
    marker scan runs, an operation label of the form ``"<route_family>:..."``
    resolves DIRECTLY to that family (still transport-filtered), bypassing
    the marker scan entirely. This exists because broad legacy substring
    markers are ambiguous by construction -- e.g. GitLab's ``project`` family
    registers ``"/projects/:id"`` / ``"/projects/"`` as markers, so a
    self-authored label like ``"pipelines:GET /projects/:id/pipelines"``
    would otherwise be swallowed by ``project`` (listed first) before
    ``pipelines`` is ever consulted. Canonical code clients author both the
    label and the family prefix themselves, so resolution for THOSE labels is
    deterministic and unambiguous by construction rather than by marker
    tuning. Unprefixed labels (every existing work-client operation label
    today) never hit this branch and take the marker-scan path UNCHANGED --
    pinned by ``tests/providers/test_usage_resolver_prefix.py``, which also
    asserts no existing work-client label accidentally starts with a
    registered ``"<family>:"`` prefix (the false-trigger regression guard).
    """

    families: tuple[UsageRouteFamily, ...] = ()
    # transport -> (route_family, dimension) fallback for instrumented
    # operations that carry no distinguishing marker (e.g. a client that labels
    # every paginated read identically).
    defaults: tuple[tuple[str, str, BudgetDimension], ...] = ()

    def resolve(self, *, transport: str, operation: str) -> tuple[str, BudgetDimension]:
        lowered = operation.lower()
        prefix_match = self._resolve_explicit_prefix(
            transport=transport, lowered=lowered
        )
        if prefix_match is not None:
            return prefix_match
        for family in self.families:
            if family.transport is not None and family.transport != transport:
                continue
            if family.operation_markers and any(
                marker in lowered for marker in family.operation_markers
            ):
                return (family.route_family, family.dimension)
        for default_transport, route_family, dimension in self.defaults:
            if default_transport == transport:
                return (route_family, dimension)
        return (UNCLASSIFIED_ROUTE_FAMILY, _default_dimension_for_transport(transport))

    def _resolve_explicit_prefix(
        self, *, transport: str, lowered: str
    ) -> tuple[str, BudgetDimension] | None:
        """Return the first registered family whose ``"<family>:"`` prefix
        matches ``lowered``, respecting the same per-family transport filter
        the marker scan uses, or ``None`` when no registered family prefixes
        the label (the common case for every unprefixed/legacy label)."""
        for family in self.families:
            if family.transport is not None and family.transport != transport:
                continue
            if lowered.startswith(f"{family.route_family}:"):
                return (family.route_family, family.dimension)
        return None


def _default_dimension_for_transport(transport: str) -> BudgetDimension:
    if transport == "graphql":
        return BudgetDimension.GRAPHQL_COST
    return BudgetDimension.REST_CORE


@dataclass
class UsageRecorder:
    """In-memory per-client request-usage recorder keyed by
    ``(transport, route_family, dimension)``.

    A single instance is owned by one work-item client, which is built per sync
    unit, so no cross-unit / cross-org state is shared.
    """

    resolver: OperationResolver
    _observations: dict[tuple[str, str, str], dict[str, Any]] = field(
        default_factory=dict
    )
    _overflow: int = 0

    def record(
        self,
        *,
        transport: str,
        operation: str,
        headers: dict[str, str],
        rate_limit: dict[str, Any],
        status: int | None = None,
    ) -> None:
        """Aggregate one request into its route-family bucket.

        No-ops when there is nothing worth recording (no diagnostic headers, no
        rate-limit signal, and no status) so callers can invoke unconditionally.
        """

        if not headers and not rate_limit and status is None:
            return
        route_family, dimension = self.resolver.resolve(
            transport=transport, operation=operation
        )
        dimension_value = dimension.value
        key = (transport, route_family, dimension_value)
        observation = self._observations.get(key)
        if observation is None:
            if len(self._observations) >= MAX_USAGE_OBSERVATION_KEYS:
                self._overflow += 1
                return
            observation = {
                "transport": transport,
                "route_family": route_family,
                "dimension": dimension_value,
                "request_count": 0,
                # Sampled interpolated label kept purely for debugging; the
                # aggregation key intentionally ignores it.
                "example_operation": operation,
            }
            self._observations[key] = observation
        observation["request_count"] = int(observation["request_count"]) + 1
        observation["example_operation"] = operation
        if status is not None:
            observation["latest_status"] = status
        if headers:
            observation["latest_headers"] = dict(headers)
        if rate_limit:
            observation["rate_limit"] = dict(rate_limit)

    def drain(self) -> list[dict[str, Any]]:
        """Return and clear the accumulated observations."""

        observations = [dict(value) for value in self._observations.values()]
        if self._overflow:
            observations.append(
                {
                    "transport": "summary",
                    "route_family": "overflow",
                    "dimension": "summary",
                    "dropped_operation_count": self._overflow,
                }
            )
        self._observations.clear()
        self._overflow = 0
        return observations


# Observation key emitted alongside (never replacing) provider-specific legacy
# keys such as 'github_usage'. Provider-neutral so gitlab/jira/linear drains and
# the calibration join can consume a single key regardless of provider.
PROVIDER_USAGE_OBSERVATION_KEY = "provider_usage"


def drain_provider_usage(client: object) -> list[dict[str, Any]]:
    """Drain a client's usage recorder if it exposes one, else return ``[]``.

    Tolerant of clients built without a recorder (e.g. the atlassian-client Jira
    path, or a test double) so drains can be wired unconditionally at the
    provider boundary.
    """

    drain = getattr(client, "drain_usage_observations", None)
    if not callable(drain):
        return []
    observations = drain()
    return observations if isinstance(observations, list) else []


def provider_usage_observations(client: object) -> dict[str, Any]:
    """Return a ``ProviderBatch.observations`` fragment carrying a client's
    drained usage under :data:`PROVIDER_USAGE_OBSERVATION_KEY` (empty when
    nothing was recorded)."""

    observations = drain_provider_usage(client)
    return {PROVIDER_USAGE_OBSERVATION_KEY: observations} if observations else {}
