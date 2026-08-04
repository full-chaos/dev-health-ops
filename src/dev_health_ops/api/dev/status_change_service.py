"""Deterministic status_snapshot.v1 and change_summary.v1 services.

The source boundary returns authorized, structured facts.  This module owns the
versioned completion rules and comparison semantics; neither GraphQL nor an LLM
may replace the rule result or widen the requested scope.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from .contracts import (
    ClaimKind,
    DevScope,
    DevTimeRange,
    DirectScope,
    FreshnessState,
    MetricID,
)
from .metrics.service import (
    MetricDataState,
    MetricQueryRequest,
    MetricQueryResult,
)

STATUS_CONTRACT_VERSION = "status_snapshot.v1"
CHANGE_CONTRACT_VERSION = "change_summary.v1"
STATUS_RULE_ID = "actual-completion"
STATUS_RULE_VERSION = "actual-completion.v4"
MAX_STATUS_ITEMS = 100
MAX_CHANGE_ITEMS = 100
MAX_STATUS_ASSESSMENT_ITEMS = 1_000

#: CHAOS-3377: the closed vocabulary of ``ActualCompletion.reason_codes`` this
#: module's ``_assess`` can emit, listed once here rather than only as the
#: scattered ``reasons.add(...)`` literals below, so a consumer that must
#: never let a raw reason code reach user-visible prose (the internal-token
#: denylist in ``no_match_terminal.py``, and the closed-vocabulary
#: translation table in ``status_answer_render.py``) has one place to derive
#: its "every code this rule can produce" set from instead of hand-copying
#: the list a second time. ``test_status_change_service.py`` pins this
#: against the literal ``reasons.add(...)`` calls in ``_assess`` so the two
#: cannot silently drift apart.
STATUS_REASON_CODES: frozenset[str] = frozenset(
    {
        "child_requirement_unknown",
        "declared_status_missing",
        "required_source_not_fresh",
        "assessment_source_limit_reached",
        "required_release_evidence_missing",
        "required_child_incomplete",
        "open_blocker",
        "required_pull_request_unmerged",
        "required_review_unresolved",
        "review_changes_requested",
        "ci_requirement_unknown",
        "required_ci_skip_state_unknown",
        "required_ci_work_skipped",
        "required_ci_not_passing",
        "required_deployment_not_succeeded",
        "active_blocking_incident",
    }
)


class StatusResultState(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    DEGRADED = "degraded"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class CompletionState(StrEnum):
    READY = "ready"
    NOT_READY = "not_ready"
    INDETERMINATE = "indeterminate"


class ConflictSeverity(StrEnum):
    WARNING = "warning"
    BLOCKING = "blocking"


class ChangeCategory(StrEnum):
    ENTITY = "entity"
    STATUS = "status"
    RELATIONSHIP = "relationship"
    BLOCKER = "blocker"
    DEPENDENCY = "dependency"
    PULL_REQUEST = "pull_request"
    REVIEW = "review"
    CI = "ci"
    DEPLOYMENT = "deployment"
    INCIDENT = "incident"
    METRIC = "metric"


@dataclass(frozen=True, slots=True)
class SourceReference:
    ref_id: str
    source_system: str
    source_version: str
    freshness: FreshnessState
    watermark: datetime | None
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StatusFact:
    entity_type: str
    entity_id: str
    display_label: str
    status: str
    observed_at: datetime
    source_ref_id: str
    evidence_ref_ids: tuple[str, ...]
    required: bool | None = False


@dataclass(frozen=True, slots=True)
class PullRequestFact:
    entity_id: str
    display_label: str
    state: str
    review_state: str | None
    changes_requested: int
    merged: bool
    observed_at: datetime
    source_ref_id: str
    evidence_ref_ids: tuple[str, ...]
    required: bool = False


@dataclass(frozen=True, slots=True)
class CIFact:
    entity_id: str
    display_label: str
    conclusion: str
    required: bool | None
    skipped_required_work: bool | None
    observed_at: datetime
    source_ref_id: str
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DeploymentFact:
    entity_id: str
    display_label: str
    status: str
    environment: str | None
    required: bool
    observed_at: datetime
    source_ref_id: str
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IncidentFact:
    entity_id: str
    display_label: str
    status: str
    active: bool
    blocking: bool
    observed_at: datetime
    source_ref_id: str
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StatusConflict:
    code: str
    message: str
    severity: ConflictSeverity
    source_ref_ids: tuple[str, ...]
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ActualCompletion:
    state: CompletionState
    rule_id: str
    rule_version: str
    reason_codes: tuple[str, ...]
    required_children: tuple[StatusFact, ...]
    # Captured in ``_assess`` from the UNBOUNDED assessment set, before
    # ``status_snapshot`` replaces ``required_children`` with the
    # bounded display list -- these are the complete denominator/numerator,
    # never the truncated-for-display counterparts. See
    # ``docs`` note on CHAOS-3297 stack #2: the display bound must never
    # silently become the completion denominator.
    #
    # Both are ``None`` (never a fabricated count) whenever the required-
    # child *source* itself was truncated (round 2, codex HIGH): an
    # undercounted total is worse than an admittedly unknown one, because
    # it can look deceptively complete (e.g. 999/999) while genuinely
    # omitting children the source never returned.
    required_child_total: int | None
    required_child_complete: int | None
    display_truncated: bool
    conflicts: tuple[StatusConflict, ...]
    source_ref_ids: tuple[str, ...]
    evidence_ref_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RawStatusSnapshot:
    declared: StatusFact | None
    children: tuple[StatusFact, ...] = ()
    blockers: tuple[StatusFact, ...] = ()
    pull_requests: tuple[PullRequestFact, ...] = ()
    ci: tuple[CIFact, ...] = ()
    deployments: tuple[DeploymentFact, ...] = ()
    incidents: tuple[IncidentFact, ...] = ()
    source_refs: tuple[SourceReference, ...] = ()
    warnings: tuple[str, ...] = ()
    # True when the required-child *source query itself* returned more
    # rows than the assessment bound before any parent/child split -- a
    # signal distinct from ``len(children) >= MAX_STATUS_ASSESSMENT_ITEMS``
    # because a source that fetches a declared parent and its children in
    # one limited query (see native_status_change._WORK_ITEMS_SQL) can
    # undercount children by exactly the parent's slot, letting the plain
    # length check silently pass while real children were dropped.
    children_source_truncated: bool = False
    # Same shape of defect, one hop further upstream (round 3, codex
    # HIGH): for WORK_UNIT scope, native_status_change._WORK_UNIT_MEMBERS_SQL
    # mixes issue and PR members in one limited query, then splits them by
    # node_type -- membership truncation can silently drop members of
    # EITHER type without either post-split list ever reaching its own
    # length bound. True only when the source itself reports truncation
    # (see the sentinel fetch there); never inferred from a length check.
    membership_source_truncated: bool = False
    # Round 5 (codex MEDIUM): the same explicit-provenance-only rule,
    # finished for every remaining assessment category. Each is True only
    # when that category's source reports truncation via its own
    # limit+1 sentinel (native_status_change._bounded_read) -- never
    # inferred from ``len(category) >= MAX_STATUS_ASSESSMENT_ITEMS``,
    # which both false-negatives (a bound shared with a collapsed/reduced
    # set, e.g. CI runs collapsed to latest-per-PR after a global-order
    # LIMIT) and false-positives (exactly MAX_STATUS_ASSESSMENT_ITEMS
    # legitimate, untruncated results).
    blockers_source_truncated: bool = False
    pull_requests_source_truncated: bool = False
    ci_source_truncated: bool = False
    deployments_source_truncated: bool = False
    incidents_source_truncated: bool = False


@dataclass(frozen=True, slots=True)
class StatusSnapshotRequest:
    scope: DevScope
    as_of: datetime | None = None
    max_items: int = MAX_STATUS_ITEMS

    def __post_init__(self) -> None:
        if self.as_of is not None and self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        if self.max_items < 1 or self.max_items > MAX_STATUS_ITEMS:
            raise ValueError(f"max_items must be between 1 and {MAX_STATUS_ITEMS}")


@dataclass(frozen=True, slots=True)
class StatusSnapshotResult:
    contract_version: str
    state: StatusResultState
    scope: DevScope
    as_of: datetime
    declared: StatusFact | None
    actual: ActualCompletion
    children: tuple[StatusFact, ...]
    blockers: tuple[StatusFact, ...]
    pull_requests: tuple[PullRequestFact, ...]
    ci: tuple[CIFact, ...]
    deployments: tuple[DeploymentFact, ...]
    incidents: tuple[IncidentFact, ...]
    source_refs: tuple[SourceReference, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ChangeWindow:
    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class ObservedChange:
    change_id: str
    category: ChangeCategory
    entity_type: str
    entity_id: str
    display_label: str
    before: str | None
    after: str | None
    observed_at: datetime
    claim_kind: ClaimKind
    relationship_chain: tuple[str, ...]
    metric_id: MetricID | None
    metric_value: float | None
    metric_comparison_value: float | None
    source_ref_ids: tuple[str, ...]
    evidence_ref_ids: tuple[str, ...]
    confidence: float | None = None


@dataclass(frozen=True, slots=True)
class RawChangeSummary:
    changes: tuple[ObservedChange, ...]
    source_refs: tuple[SourceReference, ...]
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ChangeSummaryRequest:
    scope: DevScope
    current_start: datetime
    current_end: datetime
    comparison_start: datetime
    comparison_end: datetime
    max_items: int = MAX_CHANGE_ITEMS

    def __post_init__(self) -> None:
        boundaries = (
            self.current_start,
            self.current_end,
            self.comparison_start,
            self.comparison_end,
        )
        if any(value.tzinfo is None for value in boundaries):
            raise ValueError("change windows must be timezone-aware")
        if self.current_end <= self.current_start:
            raise ValueError("current_end must be after current_start")
        if self.comparison_end <= self.comparison_start:
            raise ValueError("comparison_end must be after comparison_start")
        if (
            self.current_end - self.current_start
            != self.comparison_end - self.comparison_start
        ):
            raise ValueError("current and comparison windows must have equal duration")
        if self.max_items < 1 or self.max_items > MAX_CHANGE_ITEMS:
            raise ValueError(f"max_items must be between 1 and {MAX_CHANGE_ITEMS}")


@dataclass(frozen=True, slots=True)
class ChangeSummaryResult:
    contract_version: str
    state: StatusResultState
    current_window: ChangeWindow
    comparison_window: ChangeWindow
    changes: tuple[ObservedChange, ...]
    source_refs: tuple[SourceReference, ...]
    warnings: tuple[str, ...]


class StatusChangeSource(Protocol):
    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot: ...

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary: ...


class CanonicalMetricService(Protocol):
    def list_metrics(self, scope: DevScope | None = None) -> tuple[object, ...]: ...

    async def query(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: MetricQueryRequest,
        *,
        now: datetime | None = None,
    ) -> MetricQueryResult: ...


class StatusChangeService:
    """Shared service for both agent tools and typed GraphQL fields."""

    def __init__(
        self,
        source: StatusChangeSource,
        *,
        metric_service: CanonicalMetricService | None = None,
    ) -> None:
        self._source = source
        self._metric_service = metric_service

    async def status_snapshot(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: StatusSnapshotRequest,
    ) -> StatusSnapshotResult:
        self._validate_identity(org_id, permission_fingerprint, request.scope)
        as_of = request.as_of or request.scope.time_range.end
        raw = await self._source.status_snapshot(
            org_id=org_id,
            scope=request.scope,
            as_of=as_of,
            limit=MAX_STATUS_ASSESSMENT_ITEMS,
        )
        # CHAOS-3297 s2 round 2 (codex HIGH): a plain ``len(raw.children) >=
        # MAX_STATUS_ASSESSMENT_ITEMS`` check can never fire for a source
        # that fetches the declared parent and its children in one
        # limited query (the parent consumes one row of that budget), so
        # the source's own truncation signal -- computed before that split
        # -- is required. Round 3 (codex MEDIUM): that length check is not
        # just insufficient, it is actively wrong to keep as a fallback --
        # exactly ``MAX_STATUS_ASSESSMENT_ITEMS`` legitimate children (no
        # truncation at all) would trip it and falsely withhold a real,
        # complete denominator. Truncation for children comes ONLY from
        # explicit source-provided provenance now: the sentinel-detected
        # ``children_source_truncated`` (native_status_change._WORK_ITEMS_SQL)
        # and, one hop further upstream for WORK_UNIT scope,
        # ``membership_source_truncated`` (_WORK_UNIT_MEMBERS_SQL) -- a
        # dropped issue OR PR member there can undercount required work
        # without either post-split list ever reaching its own bound.
        children_source_truncated = (
            raw.children_source_truncated or raw.membership_source_truncated
        )
        # Round 5 (codex MEDIUM): finish what round 3 started -- every
        # remaining category's truncation signal is now explicit
        # source-provided provenance too (see the RawStatusSnapshot field
        # comments above); the ``len(category) >= MAX_STATUS_ASSESSMENT_ITEMS``
        # equality inference is gone entirely, not just for children.
        assessment_source_limit_reached = (
            children_source_truncated
            or raw.blockers_source_truncated
            or raw.pull_requests_source_truncated
            or raw.ci_source_truncated
            or raw.deployments_source_truncated
            or raw.incidents_source_truncated
        )
        bounded = replace(
            raw,
            children=self._bounded(raw.children, request.max_items),
            blockers=self._bounded(raw.blockers, request.max_items),
            pull_requests=self._bounded(raw.pull_requests, request.max_items),
            ci=self._bounded(raw.ci, request.max_items),
            deployments=self._bounded(raw.deployments, request.max_items),
            incidents=self._bounded(raw.incidents, request.max_items),
        )
        actual = self._assess(
            raw,
            assessment_source_limit_reached=assessment_source_limit_reached,
            children_source_truncated=children_source_truncated,
            # CHAOS-3303: a team subject has no single declared/children
            # completion tree (_WORK_ITEMS_SQL is never queried for team
            # scope -- see native_status_change.TEAM_NOT_APPLICABLE_SOURCES),
            # exactly like PROJECT/WORK_UNIT already. Without TEAM here,
            # every team snapshot would report declared_status_missing ->
            # CompletionState.INDETERMINATE -> StatusResultState.
            # INSUFFICIENT_EVIDENCE regardless of how much real pull-request/
            # CI/deployment/incident evidence the team's SQL arms return.
            declared_optional=request.scope.direct_scope
            in {DirectScope.PROJECT, DirectScope.WORK_UNIT, DirectScope.TEAM},
            release_evidence_required=request.scope.direct_scope
            in {
                DirectScope.ISSUE,
                DirectScope.PROJECT,
                DirectScope.WORK_UNIT,
                DirectScope.PULL_REQUEST,
            },
        )
        actual = replace(
            actual,
            # Read from the still-unbounded ``actual.required_children``
            # (set by ``_assess``) before it is overwritten below with the
            # bounded display list -- both keyword arguments are evaluated
            # against the pre-replace object.
            display_truncated=len(actual.required_children) > request.max_items,
            required_children=self._bounded(
                actual.required_children, request.max_items
            ),
        )
        assessment_source_ids = set(actual.source_ref_ids)
        freshness = {
            ref.freshness
            for ref in bounded.source_refs
            if ref.ref_id in assessment_source_ids
        }
        if FreshnessState.UNAVAILABLE in freshness:
            result_state = StatusResultState.DEGRADED
        elif not bounded.source_refs:
            result_state = StatusResultState.INSUFFICIENT_EVIDENCE
        elif FreshnessState.STALE in freshness or FreshnessState.UNKNOWN in freshness:
            result_state = StatusResultState.PARTIAL
        elif actual.state is CompletionState.INDETERMINATE:
            result_state = StatusResultState.INSUFFICIENT_EVIDENCE
        else:
            result_state = StatusResultState.COMPLETE
        return StatusSnapshotResult(
            contract_version=STATUS_CONTRACT_VERSION,
            state=result_state,
            scope=request.scope,
            as_of=as_of,
            declared=bounded.declared,
            actual=actual,
            children=self._ordered_status(bounded.children),
            blockers=self._ordered_status(bounded.blockers),
            pull_requests=tuple(sorted(bounded.pull_requests, key=self._pr_key)),
            ci=tuple(sorted(bounded.ci, key=self._ci_key)),
            deployments=tuple(sorted(bounded.deployments, key=self._deployment_key)),
            incidents=tuple(sorted(bounded.incidents, key=self._incident_key)),
            source_refs=tuple(sorted(bounded.source_refs, key=lambda ref: ref.ref_id)),
            warnings=tuple(
                sorted(
                    {
                        *bounded.warnings,
                        *(
                            ("status assessment source bound reached",)
                            if assessment_source_limit_reached
                            else ()
                        ),
                    }
                )
            ),
        )

    async def change_summary(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ChangeSummaryRequest,
    ) -> ChangeSummaryResult:
        self._validate_identity(org_id, permission_fingerprint, request.scope)
        current = ChangeWindow(request.current_start, request.current_end)
        comparison = ChangeWindow(request.comparison_start, request.comparison_end)
        scoped = request.scope.model_copy(
            update={
                "time_range": DevTimeRange(
                    start=current.start,
                    end=current.end,
                    timezone=request.scope.time_range.timezone,
                ),
                "comparison_range": DevTimeRange(
                    start=comparison.start,
                    end=comparison.end,
                    timezone=request.scope.time_range.timezone,
                ),
            }
        )
        raw = await self._source.change_summary(
            org_id=org_id,
            scope=scoped,
            current=current,
            comparison=comparison,
            limit=request.max_items,
        )
        changes = list(raw.changes)
        metric_changes: list[ObservedChange] = []
        warnings = list(raw.warnings)
        source_refs = list(raw.source_refs)
        if self._metric_service is not None:
            for definition in self._metric_service.list_metrics(scoped):
                metric_id = getattr(definition, "metric_id")
                result = await self._metric_service.query(
                    org_id,
                    permission_fingerprint,
                    MetricQueryRequest(metric_id=metric_id, scope=scoped),
                    now=current.end,
                )
                if result.state not in {MetricDataState.VALUE, MetricDataState.ZERO}:
                    warnings.extend(result.warnings)
                for metric_source in result.source_refs:
                    source_refs.append(
                        SourceReference(
                            ref_id=metric_source.ref_id,
                            source_system=metric_source.source_table,
                            source_version=metric_source.source_version,
                            freshness=result.freshness,
                            watermark=metric_source.watermark,
                            evidence_ref_ids=(),
                        )
                    )
                if result.state not in {MetricDataState.VALUE, MetricDataState.ZERO}:
                    continue
                for value in result.values:
                    metric_changes.append(
                        ObservedChange(
                            change_id=(
                                f"metric:{metric_id.value}:"
                                f"{','.join(f'{key}={item}' for key, item in value.dimensions)}"
                            ),
                            category=ChangeCategory.METRIC,
                            entity_type="metric",
                            entity_id=metric_id.value,
                            display_label=result.definition.label,
                            before=str(value.comparison_value)
                            if value.comparison_value is not None
                            else None,
                            after=str(value.value),
                            observed_at=current.end,
                            claim_kind=ClaimKind.OBSERVED,
                            relationship_chain=(),
                            metric_id=metric_id,
                            metric_value=value.value,
                            metric_comparison_value=value.comparison_value,
                            source_ref_ids=tuple(
                                ref.ref_id for ref in result.source_refs
                            ),
                            evidence_ref_ids=tuple(
                                ref.ref_id for ref in result.source_refs
                            ),
                        )
                    )
        ordered_metrics = sorted(
            metric_changes,
            key=lambda change: (change.entity_id, change.change_id),
        )
        remaining = max(0, request.max_items - len(ordered_metrics))
        ordered = tuple(
            ordered_metrics[: request.max_items]
            + sorted(changes, key=self._change_key)[:remaining]
        )
        source_refs = list({ref.ref_id: ref for ref in source_refs}.values())
        freshness = {ref.freshness for ref in source_refs}
        state = (
            StatusResultState.DEGRADED
            if FreshnessState.UNAVAILABLE in freshness
            else StatusResultState.PARTIAL
            if FreshnessState.STALE in freshness or FreshnessState.UNKNOWN in freshness
            else StatusResultState.COMPLETE
            if source_refs
            else StatusResultState.INSUFFICIENT_EVIDENCE
        )
        return ChangeSummaryResult(
            contract_version=CHANGE_CONTRACT_VERSION,
            state=state,
            current_window=current,
            comparison_window=comparison,
            changes=ordered,
            source_refs=tuple(sorted(source_refs, key=lambda ref: ref.ref_id)),
            warnings=tuple(sorted(set(warnings))),
        )

    @staticmethod
    def _validate_identity(
        org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> None:
        if not org_id or not permission_fingerprint:
            raise ValueError(
                "server-owned organization and permission scope are required"
            )
        if scope.organization_id != org_id:
            raise ValueError(
                "scope organization does not match authenticated organization"
            )

    @staticmethod
    def _bounded(values: tuple[object, ...], limit: int) -> tuple:
        return values[:limit]

    @staticmethod
    def _ordered_status(values: tuple[StatusFact, ...]) -> tuple[StatusFact, ...]:
        return tuple(
            sorted(
                values,
                key=lambda fact: (
                    fact.entity_type,
                    fact.display_label.casefold(),
                    fact.entity_id,
                ),
            )
        )

    @staticmethod
    def _pr_key(fact: PullRequestFact) -> tuple:
        return (fact.display_label.casefold(), fact.entity_id)

    @staticmethod
    def _ci_key(fact: CIFact) -> tuple:
        return (fact.display_label.casefold(), fact.entity_id)

    @staticmethod
    def _deployment_key(fact: DeploymentFact) -> tuple:
        return (fact.display_label.casefold(), fact.entity_id)

    @staticmethod
    def _incident_key(fact: IncidentFact) -> tuple:
        return (fact.display_label.casefold(), fact.entity_id)

    @staticmethod
    def _change_key(change: ObservedChange) -> tuple:
        return (
            change.observed_at,
            change.category.value,
            change.display_label.casefold(),
            change.change_id,
        )

    def _assess(
        self,
        raw: RawStatusSnapshot,
        *,
        assessment_source_limit_reached: bool = False,
        children_source_truncated: bool = False,
        declared_optional: bool = False,
        release_evidence_required: bool = False,
    ) -> ActualCompletion:
        reasons: set[str] = set()
        conflicts: list[StatusConflict] = []
        required_children = self._ordered_status(
            tuple(child for child in raw.children if child.required is True)
        )
        if any(child.required is None for child in raw.children):
            reasons.add("child_requirement_unknown")
        source_by_id = {ref.ref_id: ref for ref in raw.source_refs}
        required_source_ids = set(self._fact_sources(raw)) | {
            ref.ref_id
            for ref in raw.source_refs
            if ref.source_system in {"work_items", "work_graph", "work_units"}
            or ref.freshness is FreshnessState.UNAVAILABLE
        }
        unavailable = [
            ref.ref_id
            for ref in raw.source_refs
            if ref.ref_id in required_source_ids
            if ref.freshness
            in {
                FreshnessState.STALE,
                FreshnessState.UNAVAILABLE,
                FreshnessState.UNKNOWN,
            }
        ]
        if raw.declared is None and not declared_optional:
            reasons.add("declared_status_missing")
        if unavailable:
            reasons.add("required_source_not_fresh")
        if assessment_source_limit_reached:
            reasons.add("assessment_source_limit_reached")
        if release_evidence_required and not raw.deployments:
            reasons.add("required_release_evidence_missing")
        incomplete_children = [
            child
            for child in required_children
            if child.status.casefold()
            not in {"complete", "completed", "done", "closed", "canceled", "cancelled"}
        ]
        if incomplete_children:
            reasons.add("required_child_incomplete")
        open_blockers = [
            blocker
            for blocker in raw.blockers
            if blocker.status.casefold()
            not in {
                "complete",
                "completed",
                "done",
                "closed",
                "resolved",
                "canceled",
                "cancelled",
            }
        ]
        if open_blockers:
            reasons.add("open_blocker")
        if any(pr.required and not pr.merged for pr in raw.pull_requests):
            reasons.add("required_pull_request_unmerged")
        if any(pr.required and not pr.review_state for pr in raw.pull_requests):
            reasons.add("required_review_unresolved")
        if any(pr.required and pr.changes_requested > 0 for pr in raw.pull_requests):
            reasons.add("review_changes_requested")
        if any(ci.required is None for ci in raw.ci):
            reasons.add("ci_requirement_unknown")
        if any(ci.required and ci.skipped_required_work is None for ci in raw.ci):
            reasons.add("required_ci_skip_state_unknown")
        if any(ci.required and ci.skipped_required_work is True for ci in raw.ci):
            reasons.add("required_ci_work_skipped")
        if any(
            ci.required
            and ci.conclusion.casefold() not in {"success", "passed", "green"}
            for ci in raw.ci
        ):
            reasons.add("required_ci_not_passing")
        if any(
            deployment.required
            and deployment.status.casefold()
            not in {"success", "succeeded", "deployed", "complete", "completed"}
            for deployment in raw.deployments
        ):
            reasons.add("required_deployment_not_succeeded")
        if any(incident.blocking and incident.active for incident in raw.incidents):
            reasons.add("active_blocking_incident")

        declared_complete = declared_optional or (
            raw.declared is not None
            and raw.declared.status.casefold()
            in {"complete", "completed", "done", "closed", "merged"}
        )
        contradiction_codes = {
            "required_child_incomplete",
            "open_blocker",
            "required_pull_request_unmerged",
            "required_review_unresolved",
            "review_changes_requested",
            "required_ci_work_skipped",
            "required_ci_not_passing",
            "required_deployment_not_succeeded",
            "active_blocking_incident",
        }
        if declared_complete and reasons & contradiction_codes:
            evidence = self._fact_evidence(raw)
            source_ids = self._fact_sources(raw)
            conflicts.append(
                StatusConflict(
                    code="declared_complete_conflicts_with_observed_work",
                    message="Declared completion conflicts with required work or delivery evidence.",
                    severity=ConflictSeverity.BLOCKING,
                    source_ref_ids=source_ids,
                    evidence_ref_ids=evidence,
                )
            )
        blocking_reasons = reasons & contradiction_codes
        unknown_reasons = reasons - contradiction_codes
        state = (
            CompletionState.NOT_READY
            if blocking_reasons
            else CompletionState.INDETERMINATE
            if unknown_reasons or not declared_complete
            else CompletionState.READY
        )
        used_source_ids = tuple(sorted(required_source_ids & source_by_id.keys()))
        # The complete denominator/numerator: computed here, against the
        # UNBOUNDED ``raw.children`` (``required_children`` above), before
        # any display truncation is applied by the caller. Withheld
        # (``None``, never a fabricated count) when the required-child
        # source itself was truncated -- an honest "unknown" beats a
        # count that looks complete only because the omitted children
        # were never fetched.
        required_child_total: int | None
        required_child_complete: int | None
        if children_source_truncated:
            required_child_total = None
            required_child_complete = None
        else:
            required_child_total = len(required_children)
            required_child_complete = required_child_total - len(incomplete_children)
        return ActualCompletion(
            state=state,
            rule_id=STATUS_RULE_ID,
            rule_version=STATUS_RULE_VERSION,
            reason_codes=tuple(sorted(reasons)),
            required_children=required_children,
            required_child_total=required_child_total,
            required_child_complete=required_child_complete,
            display_truncated=False,
            conflicts=tuple(conflicts),
            source_ref_ids=used_source_ids,
            evidence_ref_ids=self._fact_evidence(raw),
        )

    @staticmethod
    def _fact_sources(raw: RawStatusSnapshot) -> tuple[str, ...]:
        refs: set[str] = set()
        if raw.declared is not None:
            refs.add(raw.declared.source_ref_id)
        for facts in (
            raw.children,
            raw.blockers,
            raw.pull_requests,
            raw.ci,
            raw.deployments,
            raw.incidents,
        ):
            refs.update(fact.source_ref_id for fact in facts)
        return tuple(sorted(refs))

    @staticmethod
    def _fact_evidence(raw: RawStatusSnapshot) -> tuple[str, ...]:
        refs: set[str] = set()
        if raw.declared is not None:
            refs.update(raw.declared.evidence_ref_ids)
        for facts in (
            raw.children,
            raw.blockers,
            raw.pull_requests,
            raw.ci,
            raw.deployments,
            raw.incidents,
        ):
            for fact in facts:
                refs.update(fact.evidence_ref_ids)
        return tuple(sorted(refs))
