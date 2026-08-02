"""Shared deterministic harness for the CHAOS-3295 plan-executor suites.

Lives in the ``tests`` package (which has an ``__init__.py``, unlike
``tests/api/dev``) so both mypy and pytest resolve it the same way, mirroring
``tests/_chaos_3292_preflight`` and ``tests/_chaos_3301_subjects``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.contracts_v2 import DevInvestigationResult
from dev_health_ops.api.dev.data_health_service import (
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceRecord,
    EvidenceReferenceSigner,
)
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    StepContext,
    StepRegistry,
    register_builtin_steps,
)
from dev_health_ops.api.dev.metrics.definitions import MetricDefinition
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    ChangeSummaryResult,
    ChangeWindow,
    CompletionState,
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
    WorkGraphNeighborsResult,
    WorkGraphResultState,
)
from tests._chaos_3292_preflight import Recorder

__all__ = [
    "FakePlanExecutorRuntime",
    "InvestigationRecorder",
    "TEST_EVIDENCE_SIGNER",
    "executor_for",
    "fixed_now",
    "project_scope",
    "sign_evidence",
    "step_context_for",
]

ORG_ID = "org_fullchaos"

#: A fixed, valid (>=32 byte) test secret. CHAOS-3296 round 5's
#: signature-verification tests need a REAL ``EvidenceReferenceSigner`` --
#: the exact class ``production_runtime.py`` wires into
#: ``PlanExecutor(evidence_signer=...)`` -- rather than a fabricated handle
#: string, so a forged/reused/cross-tenant/wrong-content handle genuinely
#: fails ``signer.verify`` the same way it would in production, and a
#: genuine handle genuinely passes.
TEST_EVIDENCE_SIGNER = EvidenceReferenceSigner(
    b"chaos-3296-round5-signature-test-secret"
)


def sign_evidence(
    *,
    org_id: str,
    source_system: str,
    source_version: str,
    entity_type: str,
    entity_id: str,
    display_label: str,
    observed_at: datetime,
    freshness,
    confidence: float = 1.0,
    repository_ids=(),
) -> str:
    """Mint a real, :data:`TEST_EVIDENCE_SIGNER`-verifiable handle -- the
    exact ``EvidenceRecord``/``issue`` call
    ``production_runtime._mint_evidence`` makes for every real evidence
    handle in production -- so a test double's ``mint_evidence`` hands back
    a handle that genuinely passes ``PlanExecutor``'s round-5 signature
    check, rather than an opaque counter string no verifier would ever
    accept."""

    record = EvidenceRecord(
        source_system=source_system,
        source_version=source_version,
        entity_type=entity_type,
        entity_id=entity_id,
        display_label=display_label,
        observed_at=observed_at,
        freshness=freshness,
        provenance=source_system,
        confidence=confidence,
        repository_ids=tuple(repository_ids),
    )
    return TEST_EVIDENCE_SIGNER.issue(org_id, record)


def project_scope(*, entity_id: str = "project-ask-dev") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": entity_id,
                "display_label": "Ask Dev",
                "repository_id": None,
            }
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def step_context_for(
    *,
    scope: DevScope | None = None,
    run_id: str = "run-direct",
    requested_metric_ids: tuple[str, ...] = (),
) -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=scope or project_scope(),
        run_id=run_id,
        now=fixed_now(),
        requested_metric_ids=requested_metric_ids,
    )


def fixed_now() -> datetime:
    return datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)


def _status_result(
    state: StatusResultState = StatusResultState.COMPLETE,
    *,
    warnings: tuple[str, ...] = (),
) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=state,
        scope=None,  # type: ignore[arg-type]  # never read by any registered step
        as_of=fixed_now(),
        declared=None,
        # CHAOS-3296: content-minting reads ``actual.required_children`` on
        # every queried status_snapshot outcome, so this can no longer be
        # ``None`` (that was always a fixture shortcut -- production's
        # ``StatusChangeService._assess`` never returns one) -- an
        # intentionally empty, real ``ActualCompletion``, still "nothing to
        # wire" for this shared empty-facts fixture.
        actual=ActualCompletion(
            state=CompletionState.NOT_READY,
            rule_id="actual-completion",
            rule_version="unversioned",
            reason_codes=(),
            required_children=(),
            conflicts=(),
            source_ref_ids=(),
            evidence_ref_ids=(),
        ),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=(),
        source_refs=(),
        warnings=warnings,
    )


def _change_summary_result(
    state: StatusResultState = StatusResultState.COMPLETE,
) -> ChangeSummaryResult:
    window = ChangeWindow(fixed_now(), fixed_now())
    return ChangeSummaryResult(
        contract_version="change_summary.v1",
        state=state,
        current_window=window,
        comparison_window=window,
        changes=(),
        source_refs=(),
        warnings=(),
    )


def _data_health(state: DataHealthState) -> DataHealthResult:
    return DataHealthResult(
        sources=(
            DataHealthSource(
                source_system="work_items",
                state=state,
                required=True,
                last_successful_at=fixed_now(),
                watermark=fixed_now(),
                missing_repository_ids=(),
                missing_entity_ids=(),
                coverage=1.0,
                confidence_impact=None,
                freshness_policy_version="v1",
            ),
        ),
        complete_eligible=state is DataHealthState.COMPLETE,
    )


class FakePlanExecutorRuntime:
    """A controllable ``PlanExecutorRuntime`` double.

    Every method is a spy (call count tracked) so negative controls can
    assert a canonical service was never reached, not merely that the run
    ended in the right terminal state.
    """

    def __init__(
        self,
        *,
        status_state: StatusResultState = StatusResultState.COMPLETE,
        data_health_state: DataHealthState = DataHealthState.COMPLETE,
        status_snapshot_fails: bool = False,
        status_snapshot_warnings: tuple[str, ...] = (),
        change_summary_state: StatusResultState = StatusResultState.COMPLETE,
        metric_definitions: tuple[MetricDefinition, ...] = (),
    ) -> None:
        self.status_snapshot_calls = 0
        self.data_health_calls = 0
        self.work_graph_calls = 0
        self.change_summary_calls = 0
        self.query_metric_calls = 0
        self._status_state = status_state
        self._data_health_state = data_health_state
        self._status_snapshot_fails = status_snapshot_fails
        self._status_snapshot_warnings = status_snapshot_warnings
        self._change_summary_state = change_summary_state
        self._metric_definitions = metric_definitions

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        self.status_snapshot_calls += 1
        if self._status_snapshot_fails:
            raise RuntimeError("status source unavailable")
        return _status_result(
            self._status_state, warnings=self._status_snapshot_warnings
        )

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        self.change_summary_calls += 1
        return _change_summary_result(self._change_summary_state)

    def list_metrics(self, scope):
        return self._metric_definitions

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        self.query_metric_calls += 1
        raise AssertionError("not exercised by this suite")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        self.work_graph_calls += 1
        return WorkGraphNeighborsResult(
            schema_version=WORK_GRAPH_SCHEMA_VERSION,
            state=WorkGraphResultState.EMPTY,
            nodes=(),
            edges=(),
            source_refs=(),
            warnings=(),
            total_count=0,
            returned_count=0,
            truncated=False,
            depth=1,
            query_version=WORK_GRAPH_QUERY_VERSION,
            watermark=None,
        )

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        self.data_health_calls += 1
        return _data_health(self._data_health_state)

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
        # Every fixture this shared double serves returns all-empty
        # canonical results (no declared fact, no children, no edges), so no
        # registered step's content-wiring loop ever actually reaches this --
        # it exists only so ``runtime.mint_evidence`` is a valid attribute
        # access (Protocol conformance). Suites that need real minted
        # evidence build their own richer double (see
        # ``test_chaos_3296_evidence_minting.py``).
        raise AssertionError(
            "mint_evidence should not be reachable from an all-empty fixture result"
        )


class InvestigationRecorder(Recorder):
    """``Recorder`` plus a capture point for ``record_investigation_result``."""

    def __init__(self) -> None:
        super().__init__()
        self.results: list[DevInvestigationResult] = []

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        self.results.append(result)


def executor_for(runtime: FakePlanExecutorRuntime) -> PlanExecutor:
    registry = StepRegistry()
    register_builtin_steps(registry, runtime)
    return PlanExecutor(registry=registry, now=fixed_now)
