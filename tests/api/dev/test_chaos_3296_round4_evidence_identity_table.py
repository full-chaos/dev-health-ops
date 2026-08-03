"""Round-4 closure (CHAOS-3296 Codex round 3, 2026-08-02): the evidence
identity table, require-known-good verification, and the three permanent
RED tests for round 3's confirmed findings.

Round 3 found the mint-receipt verification built in rounds 1-2 was
reject-known-bad (flag a handle only if PRESENT and wrong) rather than
require-known-good (every evidence-capable fact MUST cite >=1 receipted
handle whose FULL identity matches):

1. [HIGH] A fact citing zero evidence handles skipped every check entirely
   -- no mint call needed at all to fabricate a fully-believed fact.
2. [HIGH] graph_edges was excluded from identity comparison because
   ``DevGraphEdgeV2`` never preserved the ``edge_id`` minting bound
   identity to -- CHAOS-3296 round 4 adds that field.
3. [MEDIUM] CI-check identity collapsed to the run level, discarding the
   check-specific discriminator the real signer's HMAC actually binds into
   ``source_version`` -- one check's handle could "verify" a different
   check's fabricated fact on the same run.

``relationship_matrix.EVIDENCE_IDENTITY_TABLE`` closes all three: one
import-time-total cell per ``CONTENT_SLOT_FIELDS`` entry, each deriving the
exact ``(source_system, source_version, entity_type, entity_id)`` its own
``builtin_steps.py`` minting call site uses. This file's "source-anchored"
tests prove each cell against the REAL production step (via
``register_builtin_steps`` + a spy ``mint_evidence``), not a hand-authored
fixture -- if a minting call site's identity derivation ever changes shape,
these tests (not just the table's own import-time totality check) catch it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DirectScope,
    EntityType,
    FreshnessState,
    MetricID,
)
from dev_health_ops.api.dev.investigation_plans import (
    StepRegistry,
    register_builtin_steps,
)
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    CONTENT_SLOT_FIELDS,
    EVIDENCE_IDENTITY_TABLE,
)
from dev_health_ops.api.dev.metrics.definitions import MetricDefinition
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryResult,
    MetricQueryValue,
    MetricSourceRef,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    ChangeCategory,
    ChangeSummaryResult,
    ChangeWindow,
    CIFact,
    CompletionState,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    StatusFact,
    StatusResultState,
    StatusSnapshotResult,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    QUERY_VERSION as WORK_GRAPH_QUERY_VERSION,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    SCHEMA_VERSION as WORK_GRAPH_SCHEMA_VERSION,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    GraphDirection,
    WorkGraphNeighborEdge,
    WorkGraphNeighborsResult,
    WorkGraphResultState,
)
from tests._chaos_3295_plan_executor import (
    TEST_EVIDENCE_SIGNER,
    project_scope,
    sign_evidence,
    sign_evidence_for_scope,
    step_context_for,
)

OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
ORG_ID = "org_fullchaos"


# -- shared spy runtime: source-anchors every test in this file against the
# real production wiring, never a hand-authored shortcut. ------------------


class _SpyRuntime:
    def __init__(self, **kwargs: object) -> None:
        self.mint_calls: dict[str, dict[str, str]] = {}
        self._counter = 0
        self.status_result: StatusSnapshotResult | None = None
        self.change_result: ChangeSummaryResult | None = None
        self.work_graph_result: WorkGraphNeighborsResult | None = None
        self.metric_results: list[MetricQueryResult] = []
        for key, value in kwargs.items():
            setattr(self, key, value)

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        assert self.status_result is not None
        return self.status_result

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        assert self.change_result is not None
        return self.change_result

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        assert self.work_graph_result is not None
        return self.work_graph_result

    def list_metrics(self, scope):
        return [result.definition for result in self.metric_results]

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        for result in self.metric_results:
            if result.definition.metric_id.value == metric_id:
                return result
        raise AssertionError(f"no fixture metric result for {metric_id!r}")

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        self._counter += 1
        # CHAOS-3296 round 5: a REAL, ``TEST_EVIDENCE_SIGNER``-verifiable
        # handle -- not an opaque counter string -- so this spy's minted
        # handles genuinely pass ``PlanExecutor``'s signature check wherever
        # a test in this file exercises it (see ``_run_single_step``).
        handle = sign_evidence(
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            repository_ids=repository_ids,
        )
        self.mint_calls[handle] = {
            "source_system": source_system,
            "source_version": source_version,
            "entity_type": entity_type,
            "entity_id": entity_id,
        }
        return handle


def _registry(spy: _SpyRuntime) -> StepRegistry:
    registry = StepRegistry()
    register_builtin_steps(registry, spy)
    return registry


def _minted_identity(spy: _SpyRuntime, fact) -> tuple[str, str, str, str]:
    """The real minted identity for ``fact``'s (assumed single) evidence
    handle, straight from the spy's recorded mint-call kwargs.

    CHAOS-3296 round 6: the recorded ``source_version`` carries a
    ``#scope:...`` suffix -- ``_scope_bound_mint`` (applied inside the real
    ``wire_*_content`` functions, upstream of this spy) appends it to EVERY
    mint call, but ``EVIDENCE_IDENTITY_TABLE`` cells' ``derive`` never
    include it (the executor appends it centrally, once per verification
    pass, not per-cell -- see ``executor._evidence_signature_failures``).
    Stripped here so this test compares content-identity apples to apples;
    the scope suffix itself is proven correct separately (round-6 tests in
    ``test_chaos_3296_round5_signature_verification.py``)."""

    assert len(fact.evidence_ref_ids) == 1
    call = spy.mint_calls[fact.evidence_ref_ids[0]]
    return (
        call["source_system"],
        call["source_version"].split("#scope:", 1)[0],
        call["entity_type"],
        call["entity_id"],
    )


def _assert_cell_matches_minting(field: str, fact, spy: _SpyRuntime) -> None:
    cell = EVIDENCE_IDENTITY_TABLE[field]
    assert cell.mode == "required"
    assert cell.derive is not None
    assert cell.derive(fact) == _minted_identity(spy, fact)


# -- source-anchored tests: one per CONTENT_SLOT_FIELDS cell -----------------


def _status_snapshot_result(
    *,
    declared: StatusFact | None = None,
    children: tuple[StatusFact, ...] = (),
    required_children: tuple[StatusFact, ...] = (),
    pull_requests: tuple[PullRequestFact, ...] = (),
    ci: tuple[CIFact, ...] = (),
    deployments: tuple[DeploymentFact, ...] = (),
    incidents: tuple[IncidentFact, ...] = (),
) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=StatusResultState.COMPLETE,
        scope=project_scope(),
        as_of=OBSERVED_AT,
        declared=declared,
        actual=ActualCompletion(
            state=CompletionState.NOT_READY,
            rule_id="actual-completion",
            rule_version="v1",
            reason_codes=(),
            required_children=required_children,
            required_child_total=len(required_children),
            required_child_complete=len(required_children),
            display_truncated=False,
            conflicts=(),
            source_ref_ids=(),
            evidence_ref_ids=(),
        ),
        children=children,
        blockers=(),
        pull_requests=pull_requests,
        ci=ci,
        deployments=deployments,
        incidents=incidents,
        source_refs=(),
        warnings=(),
    )


async def _run_status_snapshot(spy: _SpyRuntime):
    registry = _registry(spy)
    ctx = step_context_for()
    return await registry.get("status.entity.v2", "status_snapshot").run(ctx)


@pytest.mark.asyncio
async def test_status_facts_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        declared=StatusFact(
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            status="in_progress",
            observed_at=OBSERVED_AT,
            source_ref_id="ref-declared",
            evidence_ref_ids=(),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.status_facts) == 1
    _assert_cell_matches_minting("status_facts", outcome.content.status_facts[0], spy)


@pytest.mark.asyncio
async def test_required_children_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        required_children=(
            StatusFact(
                entity_type="issue",
                entity_id="issue-child-1",
                display_label="Child",
                status="open",
                observed_at=OBSERVED_AT,
                source_ref_id="ref-child",
                evidence_ref_ids=(),
            ),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.required_children) == 1
    _assert_cell_matches_minting(
        "required_children", outcome.content.required_children[0], spy
    )


@pytest.mark.asyncio
async def test_pull_requests_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        pull_requests=(
            PullRequestFact(
                entity_id="pr-1",
                display_label="Fix bug",
                state="open",
                review_state="approved",
                changes_requested=0,
                merged=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-pr",
                evidence_ref_ids=(),
                required=True,
            ),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.pull_requests) == 1
    _assert_cell_matches_minting("pull_requests", outcome.content.pull_requests[0], spy)


@pytest.mark.asyncio
async def test_ci_checks_identity_cell_matches_real_minting_including_check_discriminator():
    """The cell most directly implicated in round-3 finding 3: a check-level
    entity_id (with the "#check..." acceptance-check suffix) must derive the
    SAME coarsened entity_id AND the SAME check-specific source_version the
    real mint call actually used."""

    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        ci=(
            CIFact(
                entity_id="repo#ci7#checkA",
                display_label="build",
                conclusion="success",
                required=True,
                skipped_required_work=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-ci",
                evidence_ref_ids=(),
            ),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.ci_checks) == 1
    fact = outcome.content.ci_checks[0]
    _assert_cell_matches_minting("ci_checks", fact, spy)
    # The discriminators themselves, spelled out: source_version must embed
    # the FULL check-specific id (round 4), not just the coarsened run id,
    # AND a digest of the check's own asserted content (round 5).
    ci_derive = EVIDENCE_IDENTITY_TABLE["ci_checks"].derive
    assert ci_derive is not None
    _source_system, source_version, _entity_type, entity_id = ci_derive(fact)
    assert entity_id == "repo#ci7"
    assert ":repo#ci7#checkA#content:" in source_version


@pytest.mark.asyncio
async def test_deployments_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        deployments=(
            DeploymentFact(
                entity_id="deploy-1",
                display_label="prod",
                status="succeeded",
                environment="production",
                required=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-deploy",
                evidence_ref_ids=(),
            ),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.deployments) == 1
    _assert_cell_matches_minting("deployments", outcome.content.deployments[0], spy)


@pytest.mark.asyncio
async def test_incidents_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.status_result = _status_snapshot_result(
        incidents=(
            IncidentFact(
                entity_id="incident-1",
                display_label="Outage",
                status="resolved",
                active=False,
                blocking=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-incident",
                evidence_ref_ids=(),
            ),
        )
    )
    outcome = await _run_status_snapshot(spy)
    assert outcome.content is not None
    assert len(outcome.content.incidents) == 1
    _assert_cell_matches_minting("incidents", outcome.content.incidents[0], spy)


@pytest.mark.asyncio
async def test_observed_changes_identity_cell_matches_real_minting_all_three_branches():
    """``_identity_observed_change`` branches three ways -- exercise all
    three from one real ``change_summary`` run."""

    spy = _SpyRuntime()
    scope = project_scope()
    scope = scope.model_copy(update={"comparison_range": scope.time_range})
    window = ChangeWindow(OBSERVED_AT, OBSERVED_AT)
    spy.change_result = ChangeSummaryResult(
        contract_version="change_summary.v1",
        state=StatusResultState.COMPLETE,
        current_window=window,
        comparison_window=window,
        changes=(
            ObservedChange(
                change_id="change-relationship",
                category=ChangeCategory.RELATIONSHIP,
                entity_type="issue",
                entity_id="issue-1",
                display_label="Linked",
                before=None,
                after="pr-1",
                observed_at=OBSERVED_AT,
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(),
                evidence_ref_ids=(),
            ),
            ObservedChange(
                change_id="change-status",
                category=ChangeCategory.STATUS,
                entity_type="project",
                entity_id="project-ask-dev",
                display_label="Ask Dev",
                before="planned",
                after="in_progress",
                observed_at=OBSERVED_AT,
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(),
                evidence_ref_ids=(),
            ),
            ObservedChange(
                change_id="change-entity",
                category=ChangeCategory.ENTITY,
                entity_type="issue",
                entity_id="issue-2",
                display_label="Issue Two",
                before=None,
                after=None,
                observed_at=OBSERVED_AT,
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(),
                evidence_ref_ids=(),
            ),
        ),
        source_refs=(),
        warnings=(),
    )
    ctx = step_context_for(scope=scope)
    registry = _registry(spy)
    outcome = await registry.get("change.observed.v1", "change_summary").run(ctx)

    assert outcome.content is not None
    assert len(outcome.content.observed_changes) == 3
    for change in outcome.content.observed_changes:
        _assert_cell_matches_minting("observed_changes", change, spy)


@pytest.mark.asyncio
async def test_graph_edges_identity_cell_matches_real_minting():
    spy = _SpyRuntime()
    spy.work_graph_result = WorkGraphNeighborsResult(
        schema_version=WORK_GRAPH_SCHEMA_VERSION,
        state=WorkGraphResultState.COMPLETE,
        nodes=(),
        edges=(
            WorkGraphNeighborEdge(
                edge_id="edge-1",
                source_type="issue",
                source_id="issue-1",
                target_type="pr",
                target_id="pr-1",
                relationship_type="references",
                direction=GraphDirection.BOTH,
                provenance="persisted",
                confidence=0.9,
                source_ref_id="ref-edge",
                observed_at=OBSERVED_AT,
            ),
        ),
        source_refs=(),
        warnings=(),
        total_count=1,
        returned_count=1,
        truncated=False,
        depth=1,
        query_version=WORK_GRAPH_QUERY_VERSION,
        watermark=None,
    )
    base = project_scope()
    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=base.organization_id,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue-1",
                display_label="Issue One",
            )
        ],
        time_range=base.time_range,
    )
    ctx = step_context_for(scope=scope)
    registry = _registry(spy)
    outcome = await registry.get("status.entity.v2", "work_graph_expansion").run(ctx)

    assert outcome.content is not None
    assert len(outcome.content.graph_edges) == 1
    _assert_cell_matches_minting("graph_edges", outcome.content.graph_edges[0], spy)


@pytest.mark.asyncio
async def test_metric_refs_identity_cell_matches_real_minting():
    definition = MetricDefinition(
        metric_id=MetricID.CYCLE_TIME_P50_HOURS,
        label="Cycle Time",
        owner="ask-dev",
        description="desc",
        definition_version="v1",
        source_table="work_items",
        source_version="v1",
        query_version="v1",
        unit="days",
        aggregation="avg",
        display_precision=1,
        null_semantics="no_data",
        zero_semantics="measured_zero",
        supported_scopes=(),
        supports_team_filter=False,
        supported_dimensions=(),
        min_range_days=1,
        max_range_days=90,
        supported_presets=(),
        supported_time_grains=("day",),
        comparison_rule="prior_period",
        freshness_policy="p.v1",
        expected_materialization="daily",
        upstream_sources=("work_items",),
        sensitivity="internal",
        entitlement="community",
    )
    metric_result = MetricQueryResult(
        definition=definition,
        state=MetricDataState.VALUE,
        freshness=FreshnessState.FRESH,
        values=(
            MetricQueryValue(
                dimensions=(), value=3.5, comparison_value=None, series=()
            ),
        ),
        coverage=1.0,
        current_window_start=OBSERVED_AT,
        current_window_end=OBSERVED_AT,
        comparison_window_start=None,
        comparison_window_end=None,
        watermark=OBSERVED_AT,
        source_refs=(
            MetricSourceRef(
                ref_id="ref-metric",
                source_table="work_items",
                source_version="v1",
                watermark=OBSERVED_AT,
                query_version="v1",
            ),
        ),
    )
    spy = _SpyRuntime(metric_results=[metric_result])
    ctx = step_context_for(requested_metric_ids=(MetricID.CYCLE_TIME_P50_HOURS.value,))
    registry = _registry(spy)
    outcome = await registry.get("metric.comparison.v1", "registered_metric_query").run(
        ctx
    )

    assert outcome.content is not None
    assert len(outcome.content.metric_refs) == 1
    _assert_cell_matches_minting("metric_refs", outcome.content.metric_refs[0], spy)


def test_evidence_identity_table_is_total_over_content_slot_fields():
    assert set(EVIDENCE_IDENTITY_TABLE) == set(CONTENT_SLOT_FIELDS)
    for field, cell in EVIDENCE_IDENTITY_TABLE.items():
        assert cell.mode == "required", (
            f"{field}: every wire_* helper in builtin_steps.py mints "
            "unconditionally today -- an accepted_risk cell here would be a "
            "fabricated exemption, not an honest one; if this ever "
            "legitimately changes, this assertion is the reminder to add a "
            "real rationale rather than silently loosen the table."
        )
        assert cell.derive is not None


# -- permanent RED tests: the three round-3 Codex repros, verbatim ----------
#
# Below this line uses a lower-level, hand-built-content harness (like the
# round-1/round-2 suites) rather than the spy runtime above -- these tests
# are about the EXECUTOR's verification decision given a step's content, not
# about proving a table cell matches a real mint call (already covered
# above).

from dev_health_ops.api.dev.contracts import DevTimeRange  # noqa: E402
from dev_health_ops.api.dev.contracts_v2.base import (  # noqa: E402
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import (  # noqa: E402
    DevCIFactV2,
    DevDeploymentFactV2,
    DevIncidentFactV2,
    DevMetricRefV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevScopeV2,
    DevStatusFactV2,
)
from dev_health_ops.api.dev.contracts_v2.plan import (  # noqa: E402
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import DevSourceContent  # noqa: E402
from dev_health_ops.api.dev.investigation_plans import (  # noqa: E402
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
)
from dev_health_ops.api.dev.investigation_plans.builtin_steps import (  # noqa: E402
    _ci_check_source_version,
    _claim_projection,
)

ROOT_ENTITY_ID = "project-1"


def _now() -> datetime:
    return OBSERVED_AT


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": ROOT_ENTITY_ID,
                "display_label": "Project One",
                "repository_id": None,
            }
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def _context() -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(),
        run_id="run-1",
        now=_now(),
    )


def _plan(source_class: SourceClass) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.1",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
        mandatory_steps=("one",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=source_class,
                adapter_id="test.one.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )


async def _run_single_step(
    *, source_class: SourceClass, run, verify_mint_receipts: bool
):
    plan = _plan(source_class)
    registry = StepRegistry()
    registry.register(
        PlanStepDefinition(
            step_id="one",
            plan_id=plan.plan_id,
            source_class=source_class,
            adapter_id="test.one.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    executor = PlanExecutor(
        registry=registry,
        now=_now,
        evidence_signer=TEST_EVIDENCE_SIGNER if verify_mint_receipts else None,
    )
    result = await executor.run(
        plan=plan, context=_context(), run_id="run-1", subject_entity_id=ROOT_ENTITY_ID
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _queried_outcome(content: DevSourceContent) -> StepOutcome:
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        usable_fact_count=1,
        content=content,
    )


@pytest.mark.asyncio
async def test_red_evidence_free_ci_fact_is_rejected():
    """RED (Codex round 3, [HIGH]): the exact repro -- a fully fabricated
    ``DevCIFactV2`` with ``evidence_ref_ids=()`` and zero ``mint_evidence``
    calls anywhere in the step. Pre-round-4 this was accepted as
    ``available_current`` with a minted "verified" relationship path and
    ``relationship_closure_verified=True``. Must now be rejected, never
    raise."""

    fabricated_ci = DevCIFactV2(
        entity_id="fake-ci-run",
        display_label="totally fabricated, no mint call ever happened",
        conclusion="success",
        required=True,
        skipped_required_work=False,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=(),
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", ci_checks=(fabricated_ci,)
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_required:")
    assert "ci_checks" in observation.limitation
    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_red_evidence_free_fact_still_accepted_when_verification_is_off():
    """Backward-compat control: ``verify_mint_receipts=False`` (every
    pre-3296 test/harness, and the default) must remain completely
    unaffected by the require-known-good check -- it only ever runs inside
    the ``verify_mint_receipts`` gate."""

    fabricated_ci = DevCIFactV2(
        entity_id="fake-ci-run",
        display_label="no evidence, verification off",
        conclusion="success",
        required=True,
        skipped_required_work=False,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=(),
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", ci_checks=(fabricated_ci,)
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=False
    )

    assert observation.content is not None
    assert len(observation.content.ci_checks) == 1
    del result


class _MintOnlyRuntime:
    def __init__(self) -> None:
        self.mint_calls = 0

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    def list_metrics(self, scope):
        raise AssertionError("not exercised by this suite")

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError("not exercised by this suite")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        self.mint_calls += 1
        # CHAOS-3296 round 6: route through the scope-bound helper (using
        # THIS file's fixed ``_scope()``) so a handle minted here carries
        # the same authorization-scope digest suffix a genuine
        # ``_scope_bound_mint``-wrapped mint call would, and verifies
        # under PlanExecutor's round-6 scope-fingerprint check.
        return sign_evidence_for_scope(
            scope=_scope(),
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            repository_ids=repository_ids,
        )


def _ci_claim(
    *, entity_id: str, display_label: str, conclusion: str
) -> dict[str, object]:
    """The EXACT projection a genuine ``wire_ci`` mint computes for a CI
    fact with these fields (required=True, skipped_required_work=False,
    observed_at=OBSERVED_AT fixed, matching every CI fact this file's
    permanent REDs construct) -- built from the SAME provisional-then-
    project pattern production uses, over the SAME entity_id/display_label
    the corresponding genuine ``DevCIFactV2`` will carry (the projection
    binds those too -- they are real wire fields, not excluded)."""

    provisional = DevCIFactV2(
        entity_id=entity_id,
        display_label=display_label,
        conclusion=conclusion,
        required=True,
        skipped_required_work=False,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=("ev1_" + "0" * 40,),
    )
    return _claim_projection(provisional)


@pytest.mark.asyncio
async def test_red_ci_check_a_handle_reused_on_check_b_is_rejected():
    """RED (Codex round 3, [MEDIUM]): a handle genuinely minted for
    repo#ci7/checkA -- coarsened to entity_id "repo#ci7", source_version
    embedding the full check-specific id (round 4) plus a digest of checkA's
    own asserted content (round 5) -- reused verbatim on a fabricated fact
    for repo#ci7/checkB. Round 2's entity_type/entity_id-only comparison
    could not distinguish them (both coarsen to the same entity_id);
    recomputing the real signature from each fact's own derived identity
    must now catch it."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        handle = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=_ci_check_source_version(
                "repo#ci7#checkA",
                claim=_ci_claim(
                    entity_id="repo#ci7#checkA",
                    display_label="checkA really succeeded",
                    conclusion="success",
                ),
            ),
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_check_a = DevCIFactV2(
            entity_id="repo#ci7#checkA",
            display_label="checkA really succeeded",
            conclusion="success",
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        forged_check_b = DevCIFactV2(
            entity_id="repo#ci7#checkB",
            display_label="checkB fabricated success (really failing)",
            conclusion="success",
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                ci_checks=(genuine_check_a, forged_check_b),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 1
    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert "repo#ci7" in observation.limitation
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_ci_check_a_and_check_b_each_with_their_own_handle_are_both_accepted():
    """Positive control for the coarsening fix: two DIFFERENT checks on the
    same run, each with its own genuinely-minted, check-specific handle,
    must both be accepted."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        handle_a = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=_ci_check_source_version(
                "repo#ci7#checkA",
                claim=_ci_claim(
                    entity_id="repo#ci7#checkA",
                    display_label="checkA",
                    conclusion="success",
                ),
            ),
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        handle_b = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=_ci_check_source_version(
                "repo#ci7#checkB",
                claim=_ci_claim(
                    entity_id="repo#ci7#checkB",
                    display_label="checkB",
                    conclusion="failure",
                ),
            ),
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkB",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        check_a = DevCIFactV2(
            entity_id="repo#ci7#checkA",
            display_label="checkA",
            conclusion="success",
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle_a,),
        )
        check_b = DevCIFactV2(
            entity_id="repo#ci7#checkB",
            display_label="checkB",
            conclusion="failure",
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle_b,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", ci_checks=(check_a, check_b)
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 2
    assert observation.content is not None
    assert len(observation.content.ci_checks) == 2
    assert result.relationship_closure_verified is True


# -- evidence_required coverage across every evidence-capable slot ----------


def _fact_with_no_evidence(field: str):
    # "status_facts" is deliberately absent: DevStatusFactV2.evidence_ref_ids
    # already carries min_length=1 at the contract layer (inherited from v1's
    # DevStatusFact) -- pydantic itself rejects an empty tuple before this
    # fixture, let alone the executor, ever sees it. The require-known-good
    # check is real defense-in-depth for that one category (unreachable
    # today, not untested), not a gap; the other eight categories have no
    # such contract-layer backstop, which is exactly what this parametrized
    # test proves the executor itself now closes.
    if field == "required_children":
        return DevRequiredChildFactV2(
            fact_id="issue:x", text="t", status="open", evidence_ref_ids=()
        )
    if field == "pull_requests":
        return DevPullRequestFactV2(
            entity_id="pr-x",
            display_label="d",
            state="open",
            review_state=None,
            changes_requested=0,
            merged=False,
            required=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "ci_checks":
        return DevCIFactV2(
            entity_id="ci-x",
            display_label="d",
            conclusion="success",
            required=False,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "deployments":
        return DevDeploymentFactV2(
            entity_id="deploy-x",
            display_label="d",
            status="succeeded",
            environment=None,
            required=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "incidents":
        return DevIncidentFactV2(
            entity_id="incident-x",
            display_label="d",
            status="resolved",
            active=False,
            blocking=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "graph_edges":
        from dev_health_ops.api.dev.contracts_v2.embedded import DevGraphEdgeV2

        return DevGraphEdgeV2(
            edge_id="edge-x",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-x",
            provenance="work_graph",
            confidence=1.0,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "observed_changes":
        from dev_health_ops.api.dev.contracts_v2.result import DevObservedChangeV2

        return DevObservedChangeV2(
            change_id="change-x",
            category="entity",
            entity_type="issue",
            entity_id="issue-x",
            display_label="d",
            before=None,
            after=None,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
    if field == "metric_refs":
        scope_v2 = DevScopeV2.model_validate(_scope().model_dump(mode="json"))
        return DevMetricRefV2(
            schema_version="dev_metric_ref.v1",
            metric_ref_id="metric:x",
            metric_id=MetricID.CYCLE_TIME_P50_HOURS,
            label="l",
            definition_version="v1",
            unit="hours",
            aggregation="avg",
            display_precision=1,
            resolved_scope=scope_v2,
            dimensions=(),
            current_window=_scope().time_range,
            comparison_window=None,
            value=1.0,
            comparison_value=None,
            series=(),
            query_version="v1",
            source_version="v1",
            freshness=FreshnessState.FRESH,
            coverage=1.0,
            evidence_ref_ids=(),
        )
    raise AssertionError(f"no no-evidence builder for {field!r}")


@pytest.mark.parametrize(
    ("source_class", "field"),
    [
        (SourceClass.STATUS_CHANGE, "required_children"),
        (SourceClass.STATUS_CHANGE, "pull_requests"),
        (SourceClass.STATUS_CHANGE, "ci_checks"),
        (SourceClass.STATUS_CHANGE, "deployments"),
        (SourceClass.STATUS_CHANGE, "incidents"),
        (SourceClass.WORK_GRAPH, "graph_edges"),
        (SourceClass.STATUS_CHANGE, "observed_changes"),
        (SourceClass.WORK_ITEM, "metric_refs"),
    ],
)
@pytest.mark.asyncio
async def test_evidence_required_rejects_zero_evidence_across_every_slot(
    source_class, field
):
    fact = _fact_with_no_evidence(field)

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", **{field: (fact,)})
        )

    result, observation = await _run_single_step(
        source_class=source_class, run=run, verify_mint_receipts=True
    )

    assert observation.content is None
    assert observation.limitation is not None
    assert observation.limitation == f"evidence_required:{field}"
    assert result.relationship_closure_verified is False


def test_status_facts_ninth_slot_is_covered_by_the_contract_layer_itself():
    """The ninth slot ``test_evidence_required_rejects_zero_evidence_
    across_every_slot`` cannot parametrize: ``DevStatusFactV2.evidence_
    ref_ids`` already carries ``min_length=1`` (inherited from v1's
    ``DevStatusFact``), so pydantic itself rejects an empty tuple before
    construction ever completes -- proven here directly, so this coverage
    claim is a demonstrated fact, not an assumption. Every one of the other
    eight ``CONTENT_SLOT_FIELDS`` categories has no such backstop, which is
    exactly why the executor's own require-known-good check (the
    parametrized test above) is load-bearing for them."""

    import pydantic

    with pytest.raises(pydantic.ValidationError, match="too_short"):
        DevStatusFactV2(fact_id="issue:x", text="t", evidence_ref_ids=())
