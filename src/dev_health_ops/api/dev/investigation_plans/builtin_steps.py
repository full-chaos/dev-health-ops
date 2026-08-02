"""Step implementations for the six core plans (CHAOS-3295).

Every step here calls exactly one existing canonical service
(``StatusChangeService``, ``MetricQueryService``, ``WorkGraphNeighborsService``,
``DataHealthService``, ``EvidenceService``) through the narrow
:class:`PlanExecutorRuntime` port -- never a parallel query, never the
provider tool registry. Production wiring (``production_runtime.py``)
implements the port over the exact service instances
``_assemble_production_runtime`` already constructs.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Protocol

from ..contracts import DevMetricPoint, DevScope, FreshnessState, MetricID
from ..contracts_v2.base import EvidenceHandle, SourceClass, SourceRequirementState
from ..contracts_v2.embedded import (
    DevCIFactV2,
    DevDeploymentFactV2,
    DevGraphEdgeV2,
    DevIncidentFactV2,
    DevMetricRefV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevScopeV2,
    DevStatusFactV2,
)
from ..contracts_v2.result import DevObservedChangeV2, DevSourceContent
from ..data_health_service import DataHealthResult
from ..metrics.definitions import MetricDefinition
from ..metrics.service import MetricQueryResult
from ..status_change_service import (
    ChangeSummaryResult,
    CIFact,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    StatusFact,
    StatusSnapshotResult,
)
from ..work_graph_neighbors_service import (
    WorkGraphNeighborEdge,
    WorkGraphNeighborsResult,
)
from .state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
    data_health_state_to_requirement_state,
    metric_data_state_to_requirement_state,
    queried_semantics,
    status_result_state_to_requirement_state,
    work_graph_result_state_to_requirement_state,
)
from .steps import PlanStepDefinition, StepContext, StepOutcome, StepRegistry

__all__ = ["PlanExecutorRuntime", "register_builtin_steps"]

#: The exact warning token ``status_change_service.status_snapshot`` appends
#: (status_change_service.py) when ``assessment_source_limit_reached`` fires
#: -- MAX_STATUS_ASSESSMENT_ITEMS was hit on at least one raw fact category.
#: No dedicated field carries this signal, so it is recognized here by its
#: pinned warning string.
_ASSESSMENT_SOURCE_BOUND_WARNING = "status assessment source bound reached"

#: A signer-issued evidence handle minted for one fact.
_Mint = Callable[..., EvidenceHandle]

#: CHAOS-3296 evidence-minting identity conventions. These mirror
#: ``production_runtime.py``'s proven v1 tool-call wiring
#: (``_STATUS_ENTITY_SOURCE_SYSTEM`` et al.) *by value*, not by import:
#: ``production_runtime.py`` already imports this package
#: (``build_default_registry``), so importing back would be circular. A
#: drift between the two surfaces as an evidence-expansion ``NO_MATCHES``,
#: never a type error -- keep in sync deliberately.
_STATUS_ENTITY_SOURCE_SYSTEM: Mapping[str, str] = {
    "issue": "work_items",
    "pull_request": "pull_requests",
    "work_unit": "work_units",
    "project": "work_items",
}
_CHANGE_CATEGORY_SOURCE_SYSTEM: Mapping[str, str] = {
    "entity": "work_items",
    "status": "work_items",
    "relationship": "work_graph",
    "blocker": "work_items",
    "dependency": "work_items",
    "pull_request": "pull_requests",
    "review": "reviews",
    "ci": "ci_runs",
    "deployment": "deployments",
    "incident": "incidents",
    "metric": "metrics",
}
_CHANGE_COLLISION_PRONE_CATEGORIES = frozenset({"status", "metric"})
_CI_ACCEPTANCE_CHECK_MARKER = "#check"
_STATUS_EVIDENCE_SOURCE_VERSION = "status-snapshot-evidence.v1"
_CHANGE_EVIDENCE_SOURCE_VERSION = "change-summary-evidence.v1"
_GRAPH_EVIDENCE_SOURCE_VERSION = "work-graph-evidence.v1"
_METRIC_EVIDENCE_SOURCE_VERSION = "metric-query-evidence.v1"
#: Every ``DevSourceContent`` field caps at 25 except ``required_children``
#: (100, matching ``StatusSnapshotRequest``'s own assessment bound). A
#: category over this limit is capped here, deterministically (encounter
#: order) -- priority-ordered truncation is explicitly a later "Evidence
#: prioritization and bounds" concern (issue body), not this deliverable's.
_CONTENT_CATEGORY_LIMIT = 25
_REQUIRED_CHILDREN_LIMIT = 100


def _ci_evidence_identity(entity_id: str) -> str:
    # ci_acceptance_checks rows carry a "{repo}#ci{run}#check{key}" entity_id;
    # the "ci_runs" adapter only indexes "{repo}#ci{run}". Coarsen to the run
    # so expansion resolves the underlying CI run instead of NO_MATCHES.
    return entity_id.split(_CI_ACCEPTANCE_CHECK_MARKER, 1)[0]


def _content_digest(**claim_fields: object) -> str:
    """Canonical digest of a fact's own ASSERTED CONTENT -- the claim
    fields beyond bare identity, keyed by the exact canonical model field
    names, never a display/truncated form. Embedded into a minted
    ``source_version`` (see :func:`_bind_content`) so the real signer's
    HMAC binds not just WHICH entity a piece of evidence is for, but WHAT
    it is being cited to prove.

    CHAOS-3296 round-5 structural inversion (Codex finding, HIGH, round 4,
    2026-08-02, generalized beyond its original ``ci_checks``-only scope
    after review: the gap is not CI-specific -- ANY category whose minted
    identity tuple does not already uniquely determine its content lets a
    genuinely-minted handle for one claim "verify" a fabricated, different
    claim about the same identity). Binding a digest of the claim itself at
    mint means two facts differing in any bound field mint different,
    non-interchangeable handles: a real handle for one claim can never
    verify a different claim about the same entity. ``default=str`` so a
    non-JSON-native claim field (e.g. an enum) never raises here -- the
    digest only needs to be a deterministic function of the value, not a
    faithful re-encoding of it.
    """

    canonical = json.dumps(
        claim_fields, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _bind_content(source_version: str, **claim_fields: object) -> str:
    """Fold :func:`_content_digest` of ``claim_fields`` into ``source_version``.

    Every ``wire_*`` mint call site below uses this (never a bespoke
    string-format) and every corresponding
    ``relationship_matrix.EVIDENCE_IDENTITY_TABLE`` cell recomputes the
    SAME claim fields from the wire fact and calls this SAME function --
    the only way mint and verify can be PROVEN to agree, rather than
    merely described as agreeing, is for both to call the exact same code.
    """

    return f"{source_version}#content:{_content_digest(**claim_fields)}"


def _ci_check_source_version(
    fact_entity_id: str,
    *,
    conclusion: str,
    required: bool | None,
    skipped_required_work: bool | None,
) -> str:
    """The exact ``source_version`` :func:`_wire_status_snapshot_content`'s
    ``wire_ci`` mints against for one CI check fact -- round 4's run-vs-check
    discriminator (embed the full, uncoarsened ``entity_id`` when coarsening
    actually changed it) plus round 5's content digest (``conclusion``/
    ``required``/``skipped_required_work``, ``DevCIFactV2``'s own claim
    fields), always. A SHARED function, imported directly by
    ``relationship_matrix.py``'s ``_identity_ci_check`` cell (never
    duplicated)."""

    lookup_entity_id = _ci_evidence_identity(fact_entity_id)
    discriminator = f":{fact_entity_id}" if lookup_entity_id != fact_entity_id else ""
    return _bind_content(
        f"{_STATUS_EVIDENCE_SOURCE_VERSION}{discriminator}",
        conclusion=conclusion,
        required=required,
        skipped_required_work=skipped_required_work,
    )


def _scope_evidence_binding(scope: DevScope) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(valid_entity_ids, repository_ids) bound to the caller's own
    already-authorized scope -- never to a supporting fact's own id."""

    return (
        tuple(item.entity_id for item in scope.entity_refs),
        tuple(scope.repositories),
    )


def _capped(items: object, limit: int = _CONTENT_CATEGORY_LIMIT) -> tuple:
    return tuple(items)[:limit]  # type: ignore[arg-type]


def _wire_status_snapshot_content(
    result: StatusSnapshotResult,
    *,
    mint: _Mint,
    org_id: str,
    scope: DevScope,
) -> DevSourceContent:
    freshness_by_ref: dict[str, FreshnessState] = {
        ref.ref_id: ref.freshness for ref in result.source_refs
    }
    valid_entity_ids, repository_ids = _scope_evidence_binding(scope)

    def status_source_system(entity_type: str) -> str:
        return _STATUS_ENTITY_SOURCE_SYSTEM.get(entity_type, "work_items")

    def mint_status(fact: StatusFact) -> str:
        # CHAOS-3296 round 5: the SAME composed "{label}: {status}" string
        # ``wire_status_fact`` writes verbatim onto ``DevStatusFactV2.text``
        # -- that field IS this fact's asserted content on the wire, so
        # binding it here (rather than the raw ``fact.status`` alone) means
        # ``_identity_status_fact`` can recompute the identical claim
        # straight from ``fact.text`` with no parsing. ``wire_required_child``
        # stores ``display_label``/``status`` as two SEPARATE wire fields
        # instead, but ``_identity_required_child`` reconstructs this exact
        # same composed string from them (see relationship_matrix.py) -- one
        # shared mint-time claim, two ways to recover it depending on which
        # wire shape kept which piece.
        return mint(
            org_id=org_id,
            source_system=status_source_system(fact.entity_type),
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION,
                claim=f"{fact.display_label}: {fact.status}",
            ),
            entity_type=fact.entity_type,
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            freshness=freshness_by_ref.get(fact.source_ref_id, FreshnessState.UNKNOWN),
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )

    def wire_status_fact(fact: StatusFact) -> DevStatusFactV2:
        evidence_id = mint_status(fact)
        return DevStatusFactV2(
            fact_id=f"{fact.entity_type}:{fact.entity_id}",
            text=f"{fact.display_label}: {fact.status}",
            evidence_ref_ids=(evidence_id,),
        )

    def wire_required_child(fact: StatusFact) -> DevRequiredChildFactV2:
        evidence_id = mint_status(fact)
        return DevRequiredChildFactV2(
            fact_id=f"{fact.entity_type}:{fact.entity_id}",
            text=fact.display_label,
            status=fact.status,
            evidence_ref_ids=(evidence_id,),
        )

    def mint_delivery(
        *,
        source_system: str,
        entity_type: str,
        entity_id: str,
        display_label: str,
        observed_at: datetime,
        source_ref_id: str,
        source_version: str = _STATUS_EVIDENCE_SOURCE_VERSION,
    ) -> str:
        return mint(
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness_by_ref.get(source_ref_id, FreshnessState.UNKNOWN),
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )

    def wire_pull_request(fact: PullRequestFact) -> DevPullRequestFactV2:
        evidence_id = mint_delivery(
            source_system="pull_requests",
            entity_type="pull_request",
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            source_ref_id=fact.source_ref_id,
            # CHAOS-3296 round 5: bind DevPullRequestFactV2's own claim
            # fields -- a genuine handle for one PR state must never
            # authenticate a fabricated claim of a different state.
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION,
                state=fact.state,
                review_state=fact.review_state,
                changes_requested=fact.changes_requested,
                merged=fact.merged,
                required=fact.required,
            ),
        )
        return DevPullRequestFactV2(
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            state=fact.state,
            review_state=fact.review_state,
            changes_requested=fact.changes_requested,
            merged=fact.merged,
            required=fact.required,
            observed_at=fact.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    def wire_ci(fact: CIFact) -> DevCIFactV2:
        lookup_entity_id = _ci_evidence_identity(fact.entity_id)
        source_version = _ci_check_source_version(
            fact.entity_id,
            conclusion=fact.conclusion,
            required=fact.required,
            skipped_required_work=fact.skipped_required_work,
        )
        evidence_id = mint_delivery(
            source_system="ci_runs",
            entity_type="ci_run",
            entity_id=lookup_entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            source_ref_id=fact.source_ref_id,
            source_version=source_version,
        )
        return DevCIFactV2(
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            conclusion=fact.conclusion,
            required=fact.required,
            skipped_required_work=fact.skipped_required_work,
            observed_at=fact.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    def wire_deployment(fact: DeploymentFact) -> DevDeploymentFactV2:
        evidence_id = mint_delivery(
            source_system="deployments",
            entity_type="deployment",
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            source_ref_id=fact.source_ref_id,
            # CHAOS-3296 round 5: bind DevDeploymentFactV2's own claim
            # fields.
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION,
                status=fact.status,
                environment=fact.environment,
                required=fact.required,
            ),
        )
        return DevDeploymentFactV2(
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            status=fact.status,
            environment=fact.environment,
            required=fact.required,
            observed_at=fact.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    def wire_incident(fact: IncidentFact) -> DevIncidentFactV2:
        evidence_id = mint_delivery(
            source_system="incidents",
            entity_type="incident",
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            source_ref_id=fact.source_ref_id,
            # CHAOS-3296 round 5: bind DevIncidentFactV2's own claim fields.
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION,
                status=fact.status,
                active=fact.active,
                blocking=fact.blocking,
            ),
        )
        return DevIncidentFactV2(
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            status=fact.status,
            active=fact.active,
            blocking=fact.blocking,
            observed_at=fact.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    status_facts = _capped(
        wire_status_fact(fact)
        for fact in (
            ([result.declared] if result.declared else [])
            + list(result.children)
            + list(result.blockers)
        )
    )
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        status_facts=status_facts,
        required_children=_capped(
            (wire_required_child(fact) for fact in result.actual.required_children),
            limit=_REQUIRED_CHILDREN_LIMIT,
        ),
        pull_requests=_capped(wire_pull_request(fact) for fact in result.pull_requests),
        ci_checks=_capped(wire_ci(fact) for fact in result.ci),
        deployments=_capped(wire_deployment(fact) for fact in result.deployments),
        incidents=_capped(wire_incident(fact) for fact in result.incidents),
    )


def _wire_change_summary_content(
    result: ChangeSummaryResult,
    *,
    mint: _Mint,
    org_id: str,
    scope: DevScope,
) -> DevSourceContent:
    freshness_by_ref: dict[str, FreshnessState] = {
        ref.ref_id: ref.freshness for ref in result.source_refs
    }
    valid_entity_ids, repository_ids = _scope_evidence_binding(scope)

    def change_freshness(item: ObservedChange) -> FreshnessState:
        for ref_id in item.source_ref_ids:
            freshness = freshness_by_ref.get(ref_id)
            if freshness is not None:
                return freshness
        return FreshnessState.UNKNOWN

    def change_evidence_identity(item: ObservedChange) -> tuple[str, str, str]:
        category = item.category.value
        source_system = _CHANGE_CATEGORY_SOURCE_SYSTEM.get(category, "work_items")
        if category == "relationship":
            base_source_version = _CHANGE_EVIDENCE_SOURCE_VERSION
            lookup_entity_id = item.change_id
        elif category in _CHANGE_COLLISION_PRONE_CATEGORIES:
            base_source_version = f"{_CHANGE_EVIDENCE_SOURCE_VERSION}:{item.change_id}"
            lookup_entity_id = item.entity_id
        else:
            base_source_version = _CHANGE_EVIDENCE_SOURCE_VERSION
            lookup_entity_id = item.entity_id
        # CHAOS-3296 round 5: bind DevObservedChangeV2's own claim fields --
        # ``before``/``after`` are the only ``ObservedChange`` fields that
        # actually survive onto the wire type (``claim_kind``/
        # ``relationship_chain``/``metric_value``/``metric_comparison_value``
        # do not), so they are the only ones a verifier could ever
        # recompute; binding them closes the reachable half of this
        # category's forgery surface.
        source_version = _bind_content(
            base_source_version, before=item.before, after=item.after
        )
        return source_system, lookup_entity_id, source_version

    def wire_change(item: ObservedChange) -> DevObservedChangeV2:
        source_system, lookup_entity_id, source_version = change_evidence_identity(item)
        evidence_id = mint(
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=item.entity_type,
            entity_id=lookup_entity_id,
            display_label=item.display_label,
            observed_at=item.observed_at,
            freshness=change_freshness(item),
            confidence=item.confidence if item.confidence is not None else 1.0,
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )
        return DevObservedChangeV2(
            change_id=item.change_id,
            category=item.category.value,
            entity_type=item.entity_type,
            entity_id=item.entity_id,
            display_label=item.display_label,
            before=item.before,
            after=item.after,
            observed_at=item.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    return DevSourceContent(
        schema_version="dev_source_content.v1",
        observed_changes=_capped(wire_change(item) for item in result.changes),
    )


def _wire_work_graph_content(
    result: WorkGraphNeighborsResult,
    *,
    mint: _Mint,
    org_id: str,
    scope: DevScope,
) -> DevSourceContent:
    valid_entity_ids, repository_ids = _scope_evidence_binding(scope)

    def wire_edge(item: WorkGraphNeighborEdge) -> DevGraphEdgeV2:
        evidence_id = mint(
            org_id=org_id,
            source_system="work_graph",
            # CHAOS-3296 round 5: bind DevGraphEdgeV2's own claim fields --
            # a genuine handle for edge_id X must never authenticate a
            # fabricated relationship/orientation for that same edge_id.
            source_version=_bind_content(
                _GRAPH_EVIDENCE_SOURCE_VERSION,
                relationship=item.relationship_type,
                source_entity_id=item.source_id,
                target_entity_id=item.target_id,
            ),
            entity_type="work_graph_edge",
            entity_id=item.edge_id,
            display_label=(
                f"{item.source_type}:{item.source_id} {item.relationship_type} "
                f"{item.target_type}:{item.target_id}"
            ),
            observed_at=item.observed_at,
            freshness=FreshnessState.UNKNOWN,
            confidence=item.confidence,
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )
        return DevGraphEdgeV2(
            edge_id=item.edge_id,
            source_entity_id=item.source_id,
            relationship=item.relationship_type,
            target_entity_id=item.target_id,
            provenance=(item.provenance.strip() or "persisted")[:2_048],
            confidence=max(0.0, min(1.0, item.confidence)),
            observed_at=item.observed_at,
            evidence_ref_ids=(evidence_id,),
        )

    return DevSourceContent(
        schema_version="dev_source_content.v1",
        graph_edges=_capped(wire_edge(item) for item in result.edges),
    )


def _wire_metric_content(
    results: Sequence[MetricQueryResult],
    *,
    mint: _Mint,
    org_id: str,
    scope: DevScope,
) -> DevSourceContent:
    valid_entity_ids, repository_ids = _scope_evidence_binding(scope)
    scope_v2 = DevScopeV2.model_validate(scope.model_dump(mode="json"))
    refs: list[DevMetricRefV2] = []
    for result in results:
        for value in result.values:
            dimensions = tuple(f"{key}={item}" for key, item in value.dimensions)
            digest_seed = (
                f"{result.definition.metric_id.value}:{value.dimensions}:"
                f"{scope.time_range.start.isoformat()}:{scope.time_range.end.isoformat()}"
            )
            metric_ref_id = (
                f"metric:{hashlib.sha256(digest_seed.encode()).hexdigest()[:32]}"
            )
            evidence_id = mint(
                org_id=org_id,
                source_system="metrics",
                # CHAOS-3296 round 5: bind DevMetricRefV2's own claim
                # fields -- a genuine handle for one metric value must
                # never authenticate a fabricated different value for the
                # same ``metric_ref_id``.
                source_version=_bind_content(
                    _METRIC_EVIDENCE_SOURCE_VERSION,
                    value=value.value,
                    comparison_value=value.comparison_value,
                ),
                entity_type="metric",
                entity_id=metric_ref_id,
                display_label=result.definition.label,
                observed_at=result.watermark or scope.time_range.end,
                freshness=result.freshness,
                valid_entity_ids=valid_entity_ids,
                repository_ids=repository_ids,
            )
            refs.append(
                DevMetricRefV2(
                    schema_version="dev_metric_ref.v1",
                    metric_ref_id=metric_ref_id,
                    metric_id=result.definition.metric_id,
                    label=result.definition.label,
                    definition_version=result.definition.definition_version,
                    unit=result.definition.unit,
                    aggregation=result.definition.aggregation,
                    display_precision=result.definition.display_precision,
                    resolved_scope=scope_v2,
                    dimensions=dimensions,
                    current_window=scope.time_range,
                    comparison_window=(
                        scope.comparison_range
                        if value.comparison_value is not None
                        else None
                    ),
                    value=value.value,
                    comparison_value=value.comparison_value,
                    series=tuple(
                        DevMetricPoint(timestamp=point.timestamp, value=point.value)
                        for point in value.series
                    ),
                    query_version=result.definition.query_version,
                    source_version=result.definition.source_version,
                    freshness=result.freshness,
                    coverage=result.coverage,
                    evidence_ref_ids=(evidence_id,),
                )
            )
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        metric_refs=_capped(refs),
    )


class PlanExecutorRuntime(Protocol):
    """The exact canonical-service surface builtin steps are allowed to call."""

    async def status_snapshot(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> StatusSnapshotResult: ...

    async def change_summary(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> ChangeSummaryResult: ...

    def list_metrics(self, scope: DevScope) -> Sequence[MetricDefinition]: ...

    async def query_metric(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        metric_id: str,
        scope: DevScope,
    ) -> MetricQueryResult: ...

    async def work_graph_neighbors(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> WorkGraphNeighborsResult: ...

    async def data_health(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> DataHealthResult:
        """Enumerate required source health for ``scope`` (trust.data.v1 core call)."""
        ...

    def mint_evidence(
        self,
        *,
        org_id: str,
        source_system: str,
        source_version: str,
        entity_type: str,
        entity_id: str,
        display_label: str,
        observed_at: datetime,
        freshness: FreshnessState,
        confidence: float = 1.0,
        valid_entity_ids: Sequence[str] = (),
        repository_ids: Sequence[str] = (),
    ) -> EvidenceHandle:
        """CHAOS-3296: issue one signer-backed, ``get_evidence.v1``-expandable
        evidence handle for a fact a step already fetched.

        ``valid_entity_ids``/``repository_ids`` must be bound to the
        caller's own already-authorized scope, never to the supporting
        fact's own entity id -- see ``production_runtime._mint_evidence``,
        the concrete production implementation of this method.
        """
        ...


def _work_graph_applicable(ctx: StepContext) -> bool:
    """Only entities the work graph actually indexes have neighbors at all.

    Mirrors ``production_runtime._work_graph_roots``'s own kind filter
    (issue/pull_request, aliased to "pr" -- a project/work_unit/team never
    resolves to a work-graph root) so the applicability predicate agrees
    with what the runtime adapter can actually find roots for.
    """

    return any(
        ref.entity_type.value in {"issue", "pull_request"}
        for ref in ctx.scope.entity_refs
    )


def _requested_metrics_applicable(ctx: StepContext) -> bool:
    return bool(ctx.requested_metric_ids)


def _usability_requested_applicable(ctx: StepContext) -> bool:
    # v1 scope: readiness/data-health enrichment always runs alongside the
    # metric catalog listing rather than depending on unmodeled per-request
    # "usability requested" signal; a future caller-supplied flag can narrow
    # this predicate without touching the registry or executor.
    del ctx
    return True


def _data_health_outcome(result: DataHealthResult) -> StepOutcome:
    if not result.sources:
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="no_data",
            usable_fact_count=0,
        )
    # Worst-case (least available) source wins the aggregate observation --
    # a plan-level "is data trustworthy" answer cannot be better than its
    # weakest required source.
    _STATE_SEVERITY = {
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE: 0,
        SourceRequirementState.UNAVAILABLE: 1,
        SourceRequirementState.UNCONFIGURED: 2,
        SourceRequirementState.AVAILABLE_STALE: 3,
        SourceRequirementState.AVAILABLE_UNKNOWN: 4,
        SourceRequirementState.AVAILABLE_CURRENT: 5,
        SourceRequirementState.NOT_APPLICABLE: 6,
        SourceRequirementState.TRUNCATED: 3,
    }
    mapped = [data_health_state_to_requirement_state(s.state) for s in result.sources]
    worst = min(mapped, key=lambda state: _STATE_SEVERITY[state])
    usable = sum(1 for s in result.sources if s.state.value == "complete")
    watermark = max(
        (s.watermark for s in result.sources if s.watermark is not None), default=None
    )
    if worst in {
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNCONFIGURED,
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
    }:
        return StepOutcome(
            observed_state=worst,
            data_semantics="not_measured",
            usable_fact_count=0,
            watermark=watermark,
            limitation=f"required_source_{worst.value}",
        )
    return StepOutcome(
        observed_state=worst,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        watermark=watermark,
        subject_coverage=(
            sum(s.coverage for s in result.sources) / len(result.sources)
        ),
    )


def _status_mapped_outcome(
    state: SourceRequirementState,
    *,
    usable_fact_count: int,
    limitation: str,
    watermark=None,
    query_version: str = "unversioned",
    content: DevSourceContent | None = None,
) -> StepOutcome:
    """Build a ``StepOutcome`` from a state already mapped through this
    module's ``state_mapping`` functions.

    Codex finding (HIGH, 2026-08-01): ``StatusResultState.
    INSUFFICIENT_EVIDENCE`` maps to ``SourceRequirementState.UNAVAILABLE``, an
    unmeasured state -- reporting queried semantics for it (a positive
    ``usable_fact_count``, no ``limitation``) fails ``DevSourceObservation``'s
    own "a source that was not fully measured requires a bounded limitation"
    validator, which the orchestrator's outer exception handler then turns
    into a user-visible ``internal_error`` instead of a typed unavailable
    observation. Every caller that maps a canonical result state through
    ``state_mapping`` and then builds a ``StepOutcome`` must route through
    here rather than assuming the mapped state is always queryable.
    """

    if state in UNMEASURED_REQUIREMENT_STATES:
        # DevSourceObservation.validate_content_semantics rejects content on
        # an unmeasured state -- never forward it here even if a caller
        # passed one (defense in depth; every caller below already only
        # builds content on the queried branch).
        return StepOutcome(
            observed_state=state,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation=limitation,
            watermark=watermark,
            query_version=query_version,
        )
    return StepOutcome(
        observed_state=state,
        data_semantics=queried_semantics(usable_fact_count),
        usable_fact_count=usable_fact_count,
        watermark=watermark,
        query_version=query_version,
        content=content,
    )


def register_builtin_steps(
    registry: StepRegistry, runtime: PlanExecutorRuntime
) -> None:
    """Populate ``registry`` with every step the six core plans declare."""

    async def status_snapshot_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.status_snapshot(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        # Recon finding (team-lead, 3297 grounding): MAX_STATUS_ASSESSMENT_ITEMS
        # firing (status_change_service.py's `assessment_source_limit_reached`)
        # has no dedicated SourceRequirementState field of its own -- if this
        # executor doesn't map it, a completion block downstream would treat a
        # truncated assessment as a complete one. TRUNCATED is an unmeasured
        # state (usable_fact_count must be 0), so this deliberately discards
        # the partial fact count in favor of disclosing the truncation.
        if _ASSESSMENT_SOURCE_BOUND_WARNING in result.warnings:
            return StepOutcome(
                observed_state=SourceRequirementState.TRUNCATED,
                data_semantics="not_measured",
                usable_fact_count=0,
                limitation="assessment_source_limit_reached",
            )
        usable = (
            (1 if result.declared else 0) + len(result.children) + len(result.blockers)
        )
        state = status_result_state_to_requirement_state(result.state)
        content = (
            None
            if state in UNMEASURED_REQUIREMENT_STATES
            else _wire_status_snapshot_content(
                result,
                mint=runtime.mint_evidence,
                org_id=ctx.org_id,
                scope=ctx.scope,
            )
        )
        return _status_mapped_outcome(
            state,
            usable_fact_count=usable,
            limitation="status_snapshot_insufficient_evidence",
            watermark=max(
                (ref.watermark for ref in result.source_refs if ref.watermark),
                default=None,
            ),
            query_version="status-snapshot.v1",
            content=content,
        )

    async def change_summary_run(ctx: StepContext) -> StepOutcome:
        if ctx.scope.comparison_range is None:
            return StepOutcome(
                observed_state=SourceRequirementState.UNAVAILABLE,
                data_semantics="not_measured",
                usable_fact_count=0,
                limitation="comparison_window_unavailable",
            )
        result = await runtime.change_summary(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        usable = len(result.changes)
        state = status_result_state_to_requirement_state(result.state)
        content = (
            None
            if state in UNMEASURED_REQUIREMENT_STATES
            else _wire_change_summary_content(
                result,
                mint=runtime.mint_evidence,
                org_id=ctx.org_id,
                scope=ctx.scope,
            )
        )
        return _status_mapped_outcome(
            state,
            usable_fact_count=usable,
            limitation="change_summary_insufficient_evidence",
            query_version="change-summary.v1",
            content=content,
        )

    async def required_source_health_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.data_health(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        return _data_health_outcome(result)

    async def work_graph_expansion_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.work_graph_neighbors(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        # Recon finding: `WorkGraphNeighborsResult.truncated` is the precise
        # bounds-exceeded signal (MAX_NEIGHBORS hit) -- distinct from the
        # coarser `state` enum, which conflates PARTIAL with "some edges
        # returned but not truncated" in principle. Truncation always wins.
        if result.truncated:
            return StepOutcome(
                observed_state=SourceRequirementState.TRUNCATED,
                data_semantics="not_measured",
                usable_fact_count=0,
                watermark=result.watermark,
                limitation="work_graph_result_truncated",
                query_version=result.query_version,
            )
        usable = len(result.edges)
        return StepOutcome(
            observed_state=work_graph_result_state_to_requirement_state(result.state),
            data_semantics=queried_semantics(usable),
            usable_fact_count=usable,
            watermark=result.watermark,
            query_version=result.query_version,
            content=_wire_work_graph_content(
                result,
                mint=runtime.mint_evidence,
                org_id=ctx.org_id,
                scope=ctx.scope,
            ),
        )

    async def list_metrics_run(ctx: StepContext) -> StepOutcome:
        definitions = list(runtime.list_metrics(ctx.scope))
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics=queried_semantics(len(definitions)),
            usable_fact_count=len(definitions),
            query_version="list-metrics.v1",
        )

    async def readiness_data_health_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.data_health(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        return _data_health_outcome(result)

    def registered_metrics_present_applicable(ctx: StepContext) -> bool:
        """Codex finding (MEDIUM, 2026-08-01): the plan declares this step
        conditional on ``registered_metrics_present.v1``, but registration
        previously hardcoded ``applicable=True`` -- an empty metric catalog
        then ran the step anyway and recorded UNAVAILABLE/
        ``all_requested_metrics_failed``, misreporting an absent optional
        source as an answer-completeness gap instead of skipping it as
        NOT_APPLICABLE. Applicability must be derived from the same catalog
        the step itself would query.
        """

        return bool(runtime.list_metrics(ctx.scope))

    async def registered_metric_deltas_run(ctx: StepContext) -> StepOutcome:
        definitions = list(runtime.list_metrics(ctx.scope))
        results = await asyncio.gather(
            *(
                runtime.query_metric(
                    org_id=ctx.org_id,
                    permission_fingerprint=ctx.permission_fingerprint,
                    metric_id=definition.metric_id.value,
                    scope=ctx.scope,
                )
                for definition in definitions
            ),
            return_exceptions=True,
        )
        return _combined_metric_outcome(
            results,
            query_version="query-metric.v1",
            mint=runtime.mint_evidence,
            org_id=ctx.org_id,
            scope=ctx.scope,
        )

    async def registered_metric_query_run(ctx: StepContext) -> StepOutcome:
        metric_ids = ctx.requested_metric_ids or tuple(
            MetricID(definition.metric_id).value
            for definition in runtime.list_metrics(ctx.scope)
        )
        results = await asyncio.gather(
            *(
                runtime.query_metric(
                    org_id=ctx.org_id,
                    permission_fingerprint=ctx.permission_fingerprint,
                    metric_id=metric_id,
                    scope=ctx.scope,
                )
                for metric_id in metric_ids
            ),
            return_exceptions=True,
        )
        return _combined_metric_outcome(
            results,
            query_version="query-metric.v1",
            mint=runtime.mint_evidence,
            org_id=ctx.org_id,
            scope=ctx.scope,
        )

    registry.register(
        PlanStepDefinition(
            step_id="status_snapshot",
            plan_id="status.entity.v2",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            run=status_snapshot_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="status.entity.v2",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="work_graph_expansion",
            plan_id="status.entity.v2",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            run=work_graph_expansion_run,
            applicable=_work_graph_applicable,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="evidence_expansion",
            plan_id="status.entity.v2",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="evidence_service.search.v1",
            requirement_level="conditional",
            run=_evidence_expansion_run,
            applicable=lambda _ctx: False,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="status_snapshot",
            plan_id="work.remaining.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            run=status_snapshot_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="work_graph_expansion",
            plan_id="work.remaining.v1",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            run=work_graph_expansion_run,
            applicable=_work_graph_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="change_summary",
            plan_id="change.observed.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.change_summary.v1",
            requirement_level="mandatory",
            run=change_summary_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="change.observed.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="registered_metric_deltas",
            plan_id="change.observed.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="conditional",
            run=registered_metric_deltas_run,
            applicable=registered_metrics_present_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="list_metrics",
            plan_id="statistics.registered.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.list_metrics.v1",
            requirement_level="mandatory",
            run=list_metrics_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="readiness_data_health",
            plan_id="statistics.registered.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="conditional",
            run=readiness_data_health_run,
            applicable=_usability_requested_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="registered_metric_query",
            plan_id="metric.comparison.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="mandatory",
            run=registered_metric_query_run,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="trust.data.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )


def _combined_metric_outcome(
    results: Sequence[MetricQueryResult | BaseException],
    *,
    query_version: str,
    mint: _Mint,
    org_id: str,
    scope: DevScope,
) -> StepOutcome:
    """One combined observation across every requested metric (batched_fan_out)."""

    successful = [item for item in results if not isinstance(item, BaseException)]
    if not successful:
        return StepOutcome(
            observed_state=SourceRequirementState.UNAVAILABLE,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation="all_requested_metrics_failed",
            query_version=query_version,
        )
    mapped = [metric_data_state_to_requirement_state(item.state) for item in successful]
    _SEVERITY = {
        SourceRequirementState.UNAVAILABLE: 0,
        SourceRequirementState.UNCONFIGURED: 1,
        SourceRequirementState.AVAILABLE_STALE: 2,
        SourceRequirementState.AVAILABLE_UNKNOWN: 3,
        SourceRequirementState.AVAILABLE_CURRENT: 4,
    }
    worst = min(mapped, key=lambda state: _SEVERITY.get(state, 0))
    usable = sum(len(item.values) for item in successful)
    if worst in {
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNCONFIGURED,
    }:
        return StepOutcome(
            observed_state=worst,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation=f"required_metric_{worst.value}",
            query_version=query_version,
        )
    return StepOutcome(
        observed_state=worst,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        query_version=query_version,
        content=_wire_metric_content(successful, mint=mint, org_id=org_id, scope=scope),
    )


async def _evidence_expansion_run(ctx: StepContext) -> StepOutcome:
    """Placeholder for a future direct evidence-service wiring.

    Registered separately from :func:`register_builtin_steps`'s runtime
    closures because evidence expansion needs a query string this
    deterministic executor deliberately never has (``StepContext`` carries
    no raw question text, by design). v1 scope: report the step
    not-applicable-by-construction rather than call
    ``EvidenceService.search`` with a fabricated query -- prioritized
    evidence enrichment remains available to the model's bounded optional
    enrichment round when ``plan.enrichment_allowed`` is true.
    """

    del ctx
    return StepOutcome(
        observed_state=SourceRequirementState.NOT_APPLICABLE,
        data_semantics="not_measured",
        usable_fact_count=0,
        limitation="deterministic_query_text_unavailable",
    )
