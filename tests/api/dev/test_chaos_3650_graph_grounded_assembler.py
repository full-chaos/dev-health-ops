"""CHAOS-3502/CHAOS-3650: the graph-routing assembler itself.

Drives the real orchestrator seam (``run_preflight_orchestrator`` -- a
genuine ``DISCOVERED_COHORT`` ``preflight_result`` only comes from the real
interpreter/preflight pipeline) with a real ``EvidenceService`` and a real
``CanonicalEnrichmentAccessor``, both backed by minimal fakes at their own
sub-collaborator boundaries. This is the "increment-2 assembler"
``graph_evidence_admission.extract_evidence_candidates``'s own docstring
says was "not yet built" -- the tests here are what proves it now is.

Covers, per the CHAOS-3502/3650 sign-off:
* drop-and-disclose: a canonically refused candidate is excluded and
  disclosed, never admitted, never aborts the run;
* the two degrade causes (evidence refused vs. enrichment gap) stay
  independently distinguishable in ``warnings``, never conflated;
* "COMPLETED-with-no-material still falls through" -- byte-identical to
  the pre-existing fallthrough behavior;
* the broad exception boundary is loud (ERROR + distinct telemetry) and
  does not swallow a genuine ``asyncio.CancelledError``;
* the metric ``evidence_ref_ids`` scrub (the recon's own landmine finding)
  is pinned so a future change that starts emitting real linkage here
  fails loudly instead of silently.
"""

from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dev_health_ops.api.dev.canonical_enrichment import CanonicalEnrichmentAccessor
from dev_health_ops.api.dev.contract_fixtures import (
    positive_fixtures as v1_positive_fixtures,
)
from dev_health_ops.api.dev.contracts import AnswerStatus, DevMetricRef, FreshnessState
from dev_health_ops.api.dev.evidence_service import (
    EvidenceRecord,
    EvidenceReferenceSigner,
    EvidenceService,
)
from dev_health_ops.api.dev.investigation_contract import AskDevInvestigationPacket
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    positive_fixtures as packet_positive_fixtures,
)
from dev_health_ops.api.dev.orchestrator import (
    GRAPH_ASSISTED_GROUNDING_STATUS,
    GRAPH_GROUNDED_WARNING_ENRICHMENT_GAP,
    GRAPH_GROUNDED_WARNING_EVIDENCE_REFUSED,
    GRAPH_ROUTING_RUNTIME_FLAG,
)
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeResolution,
    ScopeResolutionOutcome,
)
from dev_health_ops.metrics.prometheus import ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL
from tests._chaos_3292_preflight import ORG_ID, Recorder, run_preflight_orchestrator
from tests._chaos_3502_graph_investigation_fake import FakeGraphInvestigationQuery

#: A real, zero-mention, cohort-discovery-shaped question -- same fixture
#: question ``test_chaos_3502_graph_routing_branch.py`` uses, so this suite
#: exercises the identical real DISCOVERED_COHORT/TEAM_PRESSURE
#: classification path.
_DISCOVERED_COHORT_QUESTION = "Which teams are struggling right now?"

_EVIDENCE_SIGNING_SECRET = "chaos-3650-assembler-test-secret-at-least-32-bytes"
_ADMIT_LOCATOR = "graph-loc-admit"
_REFUSE_LOCATOR = "graph-loc-refuse"

_AUTHORIZED_TEAM_ID = "team_platform"


def _graph_arm_packet(locators: list[str]) -> AskDevInvestigationPacket:
    """The published packet fixture, with its first N evidence-index
    entries repointed at ``context_fabric_graph_arm`` locators.

    ``source_system``/``record_locator`` are never referenced by ANY
    packet validator (confirmed by inspection of ``investigation_contract/
    packet.py`` -- zero hits for either field name in any ``@model_
    validator``), so this is a safe, surgical edit that leaves every
    entity/driver/path cross-reference the fixture already satisfies
    untouched. The fixture's remaining entries stay ``work_graph``/
    ``review``/``work_item`` sourced -- no resolver in this suite is
    registered for those, so they are silently dropped as UNCONFIGURED,
    exactly like any other source this org has not wired a resolver for.
    """

    payload = deepcopy(packet_positive_fixtures()["ask_dev_investigation_packet.v1"])
    entries = payload["evidence_coverage"]["evidence_index"]
    assert len(locators) <= len(entries)
    for entry, locator in zip(entries, locators):
        entry["evidence"]["source_system"] = "context_fabric_graph_arm"
        entry["evidence"]["record_locator"] = locator
    return AskDevInvestigationPacket.model_validate(payload)


def _time_range() -> ResolvedTimeRange:
    now = datetime(2026, 8, 10, 12, tzinfo=UTC)
    return ResolvedTimeRange(
        timezone="UTC",
        utc_start=now - timedelta(days=30),
        utc_end=now,
        local_start=(now - timedelta(days=30)).isoformat(),
        local_end=now.isoformat(),
        comparison_utc_start=now - timedelta(days=60),
        comparison_utc_end=now - timedelta(days=30),
        comparison_local_start=(now - timedelta(days=60)).isoformat(),
        comparison_local_end=(now - timedelta(days=30)).isoformat(),
    )


class _OrgAuthorizer:
    """Grants the organization ref plus one fixed authorized team entity --
    just enough for ``EvidenceService.admit()``'s own re-authorization
    check to pass for a record genuinely about that team."""

    async def resolve(self, org_id, permission_fingerprint, request):
        del permission_fingerprint, request
        return ScopeResolution(
            outcome=ScopeResolutionOutcome.EXACT,
            entities=(
                AuthorizedEntity(EntityKind.ORGANIZATION, org_id, "Org"),
                AuthorizedEntity(EntityKind.TEAM, _AUTHORIZED_TEAM_ID, "Platform"),
            ),
            team_filters=(),
            candidates=(),
            time_range=_time_range(),
        )


class _Entitlement:
    async def require(self, org_id: str) -> None:
        del org_id


class _RaisingEntitlement:
    """Simulates an unexpected failure BEFORE ``admit()`` reaches its own
    per-candidate exception handling (``entitlement.require`` runs first,
    unguarded) -- the one path that reaches ``_attempt_graph_grounded_
    answer``'s broad ``except Exception`` for real, rather than being
    absorbed as an ordinary UNAVAILABLE/UNCONFIGURED admission the way a
    resolver's own exception already is.
    """

    async def require(self, org_id: str) -> None:
        del org_id
        raise RuntimeError("entitlement backend unavailable")


class _CancellingEntitlement:
    """A genuine ``asyncio.CancelledError`` from inside admission -- must
    propagate THROUGH the broad ``except Exception`` boundary, never be
    absorbed as an ordinary fallthrough (CHAOS-3502 Condition 3)."""

    async def require(self, org_id: str) -> None:
        del org_id
        raise asyncio.CancelledError()


class _GraphArmResolver:
    """One locator admits, one is canonically refused (NO_MATCHES) --
    mirrors ``test_evidence_service.py``'s own ``_MixedOutcomeResolver`` at
    the admission layer, narrowed to the two outcomes this suite needs."""

    source_system = "context_fabric_graph_arm"

    async def resolve(self, *, org_id, scope, candidate):
        del org_id, scope
        if candidate.locator == _REFUSE_LOCATOR:
            return None
        return EvidenceRecord(
            source_system=self.source_system,
            source_version="test.v1",
            entity_type="team",
            entity_id=_AUTHORIZED_TEAM_ID,
            display_label="Platform team pressure signal",
            observed_at=datetime(2026, 8, 9, tzinfo=UTC),
            freshness=FreshnessState.FRESH,
            provenance="native",
            confidence=1.0,
            repository_ids=(),
        )


def _evidence_service(*, entitlement=None) -> EvidenceService:
    return EvidenceService(
        entitlement=entitlement or _Entitlement(),
        authorizer=_OrgAuthorizer(),
        signer=EvidenceReferenceSigner(_EVIDENCE_SIGNING_SECRET),
        native_adapters=(),
        candidate_resolvers=(_GraphArmResolver(),),
    )


class _StatusSource:
    def __init__(self, *, raise_error: bool = False) -> None:
        self.raise_error = raise_error

    async def status_snapshot(self, org_id, permission_fingerprint, request):
        del org_id, permission_fingerprint, request
        if self.raise_error:
            raise RuntimeError("status source unavailable")
        return "status-ok"


class _TeamRuleSource:
    async def evaluate_team(self, **kwargs):
        del kwargs
        return "ok"


class _WorkloadSource:
    async def evaluate_workload(self, **kwargs):
        del kwargs
        return "ok"


class _ReadinessSource:
    async def evaluate_project(self, **kwargs):
        del kwargs
        return "ok"

    async def evaluate_team(self, **kwargs):
        del kwargs
        return "ok"


class _MetricSource:
    """Empty by default -- ``_enrich_metrics`` then contributes nothing,
    which is what every test not specifically pinning the evidence-ref-ids
    scrub wants. ``metric_refs`` lets the scrub test inject a real
    ``dev_metric_ref.v1`` fixture carrying non-empty ``evidence_ref_ids``
    from a vocabulary ``EvidenceService.admit()`` never minted.
    """

    def __init__(self, metric_refs: tuple = ()) -> None:
        self.metric_refs = metric_refs

    def list_metrics(self, scope):
        del scope
        return (
            (SimpleNamespace(metric_id="items_completed"),) if self.metric_refs else ()
        )

    async def query(self, org_id, permission_fingerprint, request, *, now=None):
        del org_id, permission_fingerprint, request, now
        return SimpleNamespace(contract_refs=lambda scope: self.metric_refs)


def _canonical_enrichment(
    *, status_raises: bool = False, metric_refs: tuple = ()
) -> CanonicalEnrichmentAccessor:
    return CanonicalEnrichmentAccessor(
        status=_StatusSource(raise_error=status_raises),
        health=_TeamRuleSource(),
        workload=_WorkloadSource(),
        readiness=_ReadinessSource(),
        metrics=_MetricSource(metric_refs=metric_refs),
    )


class _GroundingStatusRecorder(Recorder):
    """Captures ``terminal()``'s own ``grounding_validation_status`` kwarg,
    which the base ``Recorder`` fake (shared by every other suite) discards
    -- mirrors the module's own documented convention of subclassing for
    one extra capture point (see its docstring re: ``record_subject_set``).
    """

    def __init__(self) -> None:
        super().__init__()
        self.grounding_validation_statuses: list[str | None] = []

    async def terminal(self, **values) -> None:
        self.grounding_validation_statuses.append(
            values.get("grounding_validation_status")
        )
        await super().terminal(**values)


# ---------------------------------------------------------------------------
# Drop-and-disclose: clean admission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_completed_with_admissible_evidence_produces_a_graph_grounded_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)
    recorder = _GroundingStatusRecorder()

    output = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3650-clean",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(),
        recorder_factory=lambda: recorder,
    )

    assert output.result.state is RunState.COMPLETED
    answer = output.result.answer
    assert answer is not None
    assert answer.status is AnswerStatus.PARTIAL
    assert len(answer.evidence) == 1
    assert answer.warnings == []
    assert recorder.grounding_validation_statuses[-1] == GRAPH_ASSISTED_GROUNDING_STATUS
    # The legacy model-tool-choice loop must never even start: the graph
    # path returned directly through finish().
    assert output.calls == []


# ---------------------------------------------------------------------------
# CHAOS-3650: drop-and-disclose on a canonical refusal, never an abort
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_refused_candidate_degrades_but_never_aborts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR, _REFUSE_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)

    output = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3650-degrade",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(),
    )

    assert output.result.state is RunState.COMPLETED
    assert output.result.state is not RunState.FAILED
    assert output.result.state is not RunState.INSUFFICIENT_EVIDENCE
    answer = output.result.answer
    assert answer is not None
    assert answer.status is AnswerStatus.DEGRADED
    # Exactly the admitted record -- the refused one never appears.
    assert len(answer.evidence) == 1
    assert GRAPH_GROUNDED_WARNING_EVIDENCE_REFUSED in answer.warnings
    # CHAOS-3502 Condition 5: the two degrade causes are independent -- no
    # enrichment gap was injected here, so its warning must be absent.
    assert GRAPH_GROUNDED_WARNING_ENRICHMENT_GAP not in answer.warnings


# ---------------------------------------------------------------------------
# CHAOS-3502 Condition 5: the two degrade causes stay independently visible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_enrichment_gap_degrades_independently_of_evidence_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)

    output = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3650-enrichment-gap",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(status_raises=True),
    )

    assert output.result.state is RunState.COMPLETED
    answer = output.result.answer
    assert answer is not None
    assert answer.status is AnswerStatus.DEGRADED
    assert GRAPH_GROUNDED_WARNING_ENRICHMENT_GAP in answer.warnings
    # No evidence was refused this time -- the OTHER cause's warning must
    # not appear just because the answer is degraded for a different reason.
    assert GRAPH_GROUNDED_WARNING_EVIDENCE_REFUSED not in answer.warnings


# ---------------------------------------------------------------------------
# "COMPLETED-with-no-material still falls through" (binding condition 2 of
# the team-lead scope message)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_completed_with_no_material_still_falls_through(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    # Every candidate refused, no metrics -- admission + enrichment together
    # produce nothing to ground an answer on.
    packet = _graph_arm_packet([_REFUSE_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)

    with caplog.at_level(logging.INFO, logger="dev_health_ops.api.dev.orchestrator"):
        output = await run_preflight_orchestrator(
            question=_DISCOVERED_COHORT_QUESTION,
            entities=[],
            org_id=ORG_ID,
            script_id="chaos3650-no-material",
            graph_investigation_query=fake,
            evidence_service=_evidence_service(),
            graph_routing_entitlement=_Entitlement(),
            canonical_enrichment=_canonical_enrichment(),
        )

    # Falls through to the legacy loop and still answers -- never FAILED,
    # never a graph-assisted grounding status.
    assert output.result.state is not RunState.FAILED
    assert output.result.answer is not None
    assert output.calls, "the legacy loop must actually have run"
    assert any(
        record.message == "ask_dev.orchestrator.graph_routing_completed_no_material"
        for record in caplog.records
    )
    assert not any(
        record.message == "ask_dev.orchestrator.graph_routing_completed_assembled"
        for record in caplog.records
    )


# ---------------------------------------------------------------------------
# CHAOS-3502 Condition 3: the broad exception boundary is loud and distinct,
# and never swallows a genuine cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unexpected_exception_during_assembly_falls_through_and_is_counted(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)
    before = ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL.labels(
        outcome="assembly_raised"
    )._value.get()

    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.dev.orchestrator"):
        output = await run_preflight_orchestrator(
            question=_DISCOVERED_COHORT_QUESTION,
            entities=[],
            org_id=ORG_ID,
            script_id="chaos3650-raised",
            graph_investigation_query=fake,
            evidence_service=_evidence_service(entitlement=_RaisingEntitlement()),
            graph_routing_entitlement=_Entitlement(),
            canonical_enrichment=_canonical_enrichment(),
        )

    # Degrades to the legacy loop -- never surfaced as a hard failure.
    assert output.result.state is not RunState.FAILED
    assert output.result.state is not RunState.CANCELLED
    assert output.result.answer is not None

    after = ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL.labels(
        outcome="assembly_raised"
    )._value.get()
    assert after == before + 1

    error_records = [
        record
        for record in caplog.records
        if record.message
        == "ask_dev.orchestrator.graph_routing_completed_assembly_raised"
    ]
    assert len(error_records) == 1
    assert error_records[0].levelno == logging.ERROR
    # exc_info=True was passed -- the record must actually carry exception
    # information, not just a bare message at ERROR level.
    assert error_records[0].exc_info is not None


@pytest.mark.asyncio
async def test_a_genuine_cancellation_during_assembly_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verified directly against this interpreter (CHAOS-3502 Condition 3),
    not assumed from memory: ``asyncio.CancelledError`` subclasses
    ``BaseException``, not ``Exception``, so it must propagate straight
    through ``_attempt_graph_grounded_answer``'s broad ``except Exception``
    rather than being absorbed as a routine fallthrough.
    """

    assert not issubclass(asyncio.CancelledError, Exception)
    assert issubclass(asyncio.CancelledError, BaseException)

    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)

    with pytest.raises(asyncio.CancelledError):
        await run_preflight_orchestrator(
            question=_DISCOVERED_COHORT_QUESTION,
            entities=[],
            org_id=ORG_ID,
            script_id="chaos3650-cancelled",
            graph_investigation_query=fake,
            evidence_service=_evidence_service(entitlement=_CancellingEntitlement()),
            graph_routing_entitlement=_Entitlement(),
            canonical_enrichment=_canonical_enrichment(),
        )


# ---------------------------------------------------------------------------
# CHAOS-3502 Condition 4: the metric evidence_ref_ids scrub is pinned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrichment_metric_evidence_ref_ids_are_scrubbed_before_assembly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``CanonicalEnrichmentAccessor``'s metrics cite ``MetricSourceRef.
    ref_id``s -- a vocabulary distinct from the signed evidence handles
    ``EvidenceService.admit()`` mints. Without the scrub, ``DevAnswer.
    validate_answer_invariants`` would reject construction (a real
    ``ev_01`` id that is not among this answer's own admitted evidence),
    and the whole graph answer would silently fall through as
    ``assembly_raised`` instead of shipping the metric. This test proves
    the metric ships AND that its evidence linkage is empty, not
    mismatched -- pinning the documented limitation rather than leaving it
    to accidentally keep working.
    """

    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    packet = _graph_arm_packet([_ADMIT_LOCATOR])
    fake = FakeGraphInvestigationQuery(packet=packet)
    metric_payload = deepcopy(v1_positive_fixtures()["dev_metric_ref.v1"])
    # The published fixture already carries a real, non-empty
    # evidence_ref_ids ("ev_01") from a vocabulary this suite's
    # EvidenceService never minted -- exactly the mismatch under test.
    assert metric_payload["evidence_ref_ids"]
    # Re-keyed away from the stock fixture's own ("metric_01", value=12):
    # the legacy model-tool-choice loop's own canned answer
    # (``_chaos_3292_preflight.answer_payload``) embeds that SAME stock
    # ``dev_metric_ref.v1`` fixture, so asserting against the unmodified
    # values would pass even if this leg's assembly never ran at all and
    # the run silently fell through to the legacy loop instead -- a false
    # green that would have hidden the exact ``assembly_raised`` regression
    # this test exists to catch (confirmed live: this test passed on a
    # revert that disabled assembly entirely, before this re-keying was
    # added).
    metric_payload["metric_ref_id"] = "metric_graph_scrub_test_01"
    metric_payload["value"] = 987654.0
    metric_ref = DevMetricRef.model_validate(metric_payload)

    output = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3650-metric-scrub",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(metric_refs=(metric_ref,)),
    )

    assert output.result.state is RunState.COMPLETED
    answer = output.result.answer
    assert answer is not None
    assert len(answer.metrics) == 1
    assert answer.metrics[0].evidence_ref_ids == []
    # And the metric's other fields survived untouched -- only the
    # mismatched evidence link was dropped, nothing else about the metric.
    assert answer.metrics[0].metric_ref_id == metric_payload["metric_ref_id"]
    assert answer.metrics[0].value == metric_payload["value"]
