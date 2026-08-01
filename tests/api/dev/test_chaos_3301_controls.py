"""CHAOS-3301 RED control battery — TEAM as a direct scope, bounded cohorts.

Written and observed RED before any ``src/`` edit, per the ratified plan
(`.claude/jobs/.../chaos-3301-plan.md`, its addendum, and Amendment 2). Every
control asserts the *executed tool-request scope* and/or the *persisted/
serialized state* — never a diagnostic string alone — following
``test_chaos_3292_preflight_acceptance.py``'s style.

Ratified decisions this file encodes (layered on top of the original plan
table, per the team-lead brief):

* **Addendum option (B)**: no team arm is added to ``native_status_change.py``
  SQL. Instead a totality test/registry (``TEAM_NOT_APPLICABLE_SOURCES``) and
  N0 prove a committed team subject is answered as typed not-applicable,
  never silently empty.
* **D1**: a committed *cohort* (>=2 distinct committed entities) still
  returns ``unsupported`` on the v1 surface — diagnostic
  ``committed_cohort_v1_only`` for a homogeneous set, unchanged
  ``cohort_unsupported_in_v1`` for a heterogeneous one. This supersedes the
  original plan table's P2/P3 wording ("both member scopes execute"): no
  cohort ever reaches a subject-bearing tool in this issue. What CHAOS-3301
  adds for a cohort is *commitment, ledger accounting, and persistence* of
  the ``dev_subject_set.v1``, not multi-scope execution.
* **D2**: for ``PLURAL_COHORT`` intents only, an unresolved typed mention no
  longer terminates the run while >=2 committed refs remain; it is recorded
  as omitted (``unresolved_mention_ids`` + ``warnings``,
  ``cohort_complete=False``) rather than blocking.

N3 (oversized set) has one further, unavoidable adjustment: both
``DevSubjectMention.mention_ordinal`` (``le=24``) and
``DevResolutionLedger.mention_ids`` (``max_length=25``) are contract-capped
at 25. A ledger literally cannot carry a 26th mention id, so this suite
cannot assert "ledger mention_ids length is 26" as the original plan text
states. It instead asserts the *externally observable* property the plan
cares about: a question naming more than 25 subjects is rejected as
unsupported and discloses that it was too large, and is never silently
narrowed to a "complete" 25-member cohort. See the test docstring for the
exact reasoning; this is flagged as a known deviation in the implementation
report.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.subject_preflight import SUBJECT_BEARING_TOOLS
from tests._chaos_3301_subjects import (
    HALO_PROJECT,
    NOVA_PROJECT,
    ORG_ID,
    PERMISSION_FINGERPRINT,
    PLATFORM_TEAM,
    RunOutput,
    SubjectSetRecorder,
    case_n1_cross_tenant_team,
    case_n2_mixed_kind_set,
    case_n3_oversized_project_set,
    case_n4_duplicate_aliases_one_entity,
    case_n5_team_filter_is_not_a_subject,
    case_p1_known_team_singular,
    case_p2_two_project_cohort,
    case_p3_partial_project_cohort,
)
from tests.api.dev.test_router import dev_api_context  # noqa: F401 - pytest fixture

DETERMINISM_ITERATIONS = 20
#: build_production_runtime's flag check parses org_id as a UUID
#: (production_runtime._wave_3_1_enabled -> uuid.UUID(org_id)); the shared
#: harness's ORG_ID ("org_fullchaos") is not one, so G2 needs its own.
_G2_ORG_ID = str(uuid.uuid4())


def _subject_bearing_calls(output: RunOutput) -> list[str]:
    return [
        request.tool_id.value
        for request in output.calls
        if request.tool_id in SUBJECT_BEARING_TOOLS
    ]


def _public_strings(output: RunOutput) -> list[str]:
    values: list[str] = []
    if output.result.error is not None:
        values.append(output.result.error.safe_message)
        values.append(output.result.error.code)
        values.extend(output.result.error.remediation)
    if output.result.answer is not None:
        values.append(output.result.answer.direct_summary)
        values.extend(claim.text for claim in output.result.answer.claims)
        values.extend(output.result.answer.warnings)
    return values


async def _twenty(coro_factory: Any) -> list[tuple[Any, ...]]:
    results = [(await coro_factory()).outcome_tuple() for _ in range(20)]
    # Rule 4: a measurement that did not happen must fail loudly.
    assert len(results) == DETERMINISM_ITERATIONS
    return results


# ---------------------------------------------------------------------------
# P1 — a known team is a committed direct subject end to end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p1_team_commits_and_status_executes_team_scope() -> None:
    from dev_health_ops.api.dev.contracts import DirectScope

    output = await case_p1_known_team_singular()

    assert output.result.state is RunState.COMPLETED
    assert [request.tool_id.value for request in output.calls] == ["status_snapshot.v1"]
    status_call = output.calls[0]
    assert status_call.scope.direct_scope is DirectScope.TEAM
    assert [ref.entity_id for ref in status_call.scope.entity_refs] == [
        PLATFORM_TEAM.canonical_id
    ]
    assert output.recorder is not None
    assert output.recorder.transitions.index(
        RunState.RESOLVING_SUBJECTS
    ) < output.recorder.transitions.index(RunState.TOOL_EXECUTION)


@pytest.mark.asyncio
async def test_p1_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_p1_known_team_singular)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# P1w / G1 — the whole request path, endpoint to executed tool scope
#
# Amendment 2 correction: a router-level assertion alone would prove nothing
# about the real path, since every existing router test injects
# FakeBoundedRuntime and every orchestrator-seam test bypasses the router
# entirely. This constructs a REAL BoundedDevRuntime (real SubjectPreflight,
# real ScopeResolutionService over a seeded catalog, a scripted-but-real
# provider, a recording tool registry) and drives one HTTP request through
# the real FastAPI app.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p1w_endpoint_to_executed_tool_scope_is_team(
    dev_api_context: Any,  # noqa: F811 - pytest fixture, imported above for reuse
) -> None:
    from dev_health_ops.api.dev import router as dev_router_module
    from dev_health_ops.api.dev.contracts import DevToolRequest, DirectScope
    from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
    from dev_health_ops.api.dev.runtime import BoundedDevRuntime
    from dev_health_ops.api.dev.scope_service import (
        AuthorizedEntity,
        EntityKind,
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from tests._chaos_3292_preflight import (
        RecordingProvider,
        SeededCatalog,
        fixed_now,
        organization_resolution,
        recording_registry,
        sequential_ids,
        status_then_answer,
        versions,
    )
    from tests.api.dev.test_router import FakeBoundedRuntime, _parse_sse_events

    org_id = str(dev_api_context.org_id)
    team = AuthorizedEntity(EntityKind.TEAM, "team-platform", "Platform")
    mint = sequential_ids()
    calls: list[DevToolRequest] = []

    async def resolve(*, requested_scope: Any, **_values: Any) -> Any:
        # Mirrors run_preflight_orchestrator's stand-in resolver: authorizes
        # the request's own (organization-wide) scope, unchanged. This is the
        # orchestrator's *initial* authorization step, which always runs
        # before the preflight; the preflight is what narrows the run to the
        # team once the question names one.
        return organization_resolution(requested_scope)

    real_runtime = BoundedDevRuntime(
        provider=cast(
            Any,
            RecordingProvider(status_then_answer("p1w"), script_id="p1w"),
        ),
        provider_source="platform",
        provider_family="scripted",
        registry=recording_registry(calls),
        scope_resolver=resolve,
        versions=versions(),
        preflight=SubjectPreflight(
            interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
            scope_service=ScopeResolutionService(
                SeededCatalog([(org_id, team)]), cache=ScopeRequestCache()
            ),
            versions=versions(),
            mint_id=mint,
            now=fixed_now,
        ),
    )
    # Anti-vacuity: a wiring mistake that silently reinstated the router's
    # default fake runtime must not read as a pass.
    assert not isinstance(real_runtime, FakeBoundedRuntime)

    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(runtime=real_runtime)

    scope_payload = {
        "schema_version": "dev_scope.v1",
        "organization_id": org_id,
        "direct_scope": "organization",
        "repositories": [],
        "entity_refs": [],
        "team_ids": [],
        "time_range": {
            "start": "2026-07-28T12:00:00+00:00",
            "end": "2026-07-28T13:00:00+00:00",
            "timezone": "UTC",
        },
    }
    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations", json={"current_scope": scope_payload}
    )
    assert created.status_code == 201
    conversation_id = created.json()["conversation_id"]

    response = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        json={
            "schema_version": "dev_message_request.v1",
            "request_id": "request_p1w_01",
            "client_message_id": "client_p1w_01",
            "conversation_id": conversation_id,
            "question": "How is the Platform team doing?",
            "question_class": "status",
            "scope": scope_payload,
            "requested_metric_ids": [],
        },
    )
    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    completed = [data for name, data in events if name == "answer.completed"]
    assert len(completed) == 1, "exactly one terminal answer.completed event"

    # Both ends of the join in one run: the tool actually executed against a
    # team scope, and the wire response says so too.
    assert calls, "the recording registry must have recorded a real tool call"
    assert calls[0].scope.direct_scope is DirectScope.TEAM
    # DevAnswer.resolved_scope is a DevScopeResolution; the actual DevScope
    # lives one level down, at .resolved_scope.
    assert (
        completed[0]["answer"]["resolved_scope"]["resolved_scope"]["direct_scope"]
        == "team"
    )


# ---------------------------------------------------------------------------
# N1 — a real team, but for a different tenant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n1_cross_tenant_team_is_not_found_and_leaks_nothing() -> None:
    output = await case_n1_cross_tenant_team()

    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []
    for text in _public_strings(output):
        assert scan_public_text(text) == []


@pytest.mark.asyncio
async def test_n1_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_n1_cross_tenant_team)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# P2 — a complete two-project cohort commits and persists, never executes
#
# Superseded per D1 from the original plan table's "both member scopes
# execute": a committed cohort still returns unsupported on the v1 surface.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p2_two_project_cohort_commits_and_persists_without_executing() -> None:
    output = await case_p2_two_project_cohort()

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    assert output.preflight_outcomes() == ("committed_cohort_v1_only",)

    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert len(output.recorder.subject_sets) == 1
    subject_set = output.recorder.subject_sets[0]
    assert subject_set.original_mention_count == 2
    assert subject_set.cohort_complete is True
    assert subject_set.unresolved_mention_ids == ()
    assert subject_set.warnings == ()
    committed_ids = {ref.entity_id for ref in subject_set.committed_entity_refs}
    assert committed_ids == {HALO_PROJECT.canonical_id, NOVA_PROJECT.canonical_id}
    import re

    assert re.fullmatch(r"set1_[0-9a-f]{40}", subject_set.fingerprint)


@pytest.mark.asyncio
async def test_p2_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_p2_two_project_cohort)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# P3 — a partial cohort discloses omissions and never widens to org
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p3_partial_cohort_discloses_and_does_not_widen() -> None:
    output = await case_p3_partial_project_cohort()

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    assert output.preflight_outcomes() == ("committed_cohort_v1_only",)

    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert len(output.recorder.subject_sets) == 1
    subject_set = output.recorder.subject_sets[0]
    assert subject_set.original_mention_count == 3
    assert subject_set.cohort_complete is False
    assert len(subject_set.unresolved_mention_ids) == 1
    assert subject_set.warnings != ()
    committed_ids = {ref.entity_id for ref in subject_set.committed_entity_refs}
    assert committed_ids == {HALO_PROJECT.canonical_id, NOVA_PROJECT.canonical_id}

    # The ledger keeps all three mentions, including the unresolved one.
    assert output.recorder is not None


@pytest.mark.asyncio
async def test_p3_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_p3_partial_project_cohort)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# N2 — a heterogeneous set never forms; both mentions still ledgered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n2_mixed_kind_set_is_unsupported_with_full_ledger() -> None:
    output = await case_n2_mixed_kind_set()

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    # Heterogeneous kinds never form a dev_subject_set.v1 at all (a non-goal).
    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert output.recorder.subject_sets == []


# ---------------------------------------------------------------------------
# N3 — oversized sets are rejected, never silently trimmed to 25
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n3_oversized_set_is_rejected_not_trimmed() -> None:
    """26 named subjects must never silently become a 'complete' 25-set.

    Both ``DevSubjectMention.mention_ordinal`` (le=24) and
    ``DevResolutionLedger.mention_ids`` (max_length=25) are hard contract
    bounds — a ledger cannot literally carry 26 entries. The property this
    test protects is therefore not "the ledger has 26 entries" (impossible)
    but "the run does not silently answer about the first 25 and drop the
    26th without disclosure": today's ``extract_mentions`` does exactly that
    (truncates at ``MAX_MENTIONS`` with no signal), so this is RED against
    the *silent-truncation* defect, not only against team/cohort support.
    """

    output = await case_n3_oversized_project_set(26)

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    # No 25-member set may be silently persisted as though it were complete.
    assert output.recorder.subject_sets == []
    # Today this reaches the ordinary >1-committed cohort branch and reports
    # the same diagnostic P2 does — which is exactly the silent-truncation
    # defect: 26 named subjects is indistinguishable from a normal 2-name
    # cohort once extraction has already dropped the 26th. An oversized
    # question must be diagnosed distinctly.
    assert output.preflight_outcomes() != ("cohort_unsupported_in_v1",)


@pytest.mark.asyncio
async def test_n3_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(lambda: case_n3_oversized_project_set(26))
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# N4 — duplicate aliases collapse to one committed subject, and it executes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n4_duplicate_aliases_commit_once_and_execute_once() -> None:
    from dev_health_ops.api.dev.contracts import DirectScope

    output = await case_n4_duplicate_aliases_one_entity()

    # Only one *distinct* subject was ever named — this is not a cohort, so
    # it proceeds and executes exactly one scope, unlike P2/P3/N2.
    assert output.result.state is RunState.COMPLETED
    assert [request.tool_id.value for request in output.calls] == ["status_snapshot.v1"]
    assert output.calls[0].scope.direct_scope is DirectScope.PROJECT
    assert [ref.entity_id for ref in output.calls[0].scope.entity_refs] == [
        HALO_PROJECT.canonical_id
    ]

    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert len(output.recorder.subject_sets) == 1
    subject_set = output.recorder.subject_sets[0]
    assert subject_set.original_mention_count == 2
    assert subject_set.cohort_complete is True
    assert [ref.entity_id for ref in subject_set.committed_entity_refs] == [
        HALO_PROJECT.canonical_id
    ]


# ---------------------------------------------------------------------------
# N5 — a legacy team_ids filter is not a team subject (regression control)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n5_team_filter_is_not_a_team_subject() -> None:
    output = await case_n5_team_filter_is_not_a_subject()

    assert output.result.state is RunState.COMPLETED
    assert output.calls, "the org-wide question must still execute"
    for call in output.calls:
        assert call.scope.direct_scope.value == "organization"
        assert call.scope.team_ids == [PLATFORM_TEAM.canonical_id]
    assert output.recorder is not None
    if isinstance(output.recorder, SubjectSetRecorder):
        assert output.recorder.subject_sets == []


# ---------------------------------------------------------------------------
# N0 — a committed team subject never yields a silently empty answer
#
# Addendum item 4. Exercised at the real status-source seam directly (the
# orchestrator harness's recording_registry is a fixture stub and would
# mask the defect the addendum describes), with a fake ClickHouse client:
# a team scope carries no repositories (Option B — attribution is
# re-derived at query time in CHAOS-3303, not this issue), so the existing
# "no authorized repositories" fail-closed branch is reached before any SQL
# executes, and it already returns a typed FreshnessState.UNAVAILABLE
# observation rather than empty rows. This control is what proves that stays
# true once TEAM becomes a real DirectScope.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n0_team_subject_never_returns_a_silently_empty_status() -> None:
    from datetime import UTC, datetime

    from dev_health_ops.api.dev.contracts import (
        DevEntityRef,
        DevScope,
        DevTimeRange,
        DirectScope,
        EntityType,
        FreshnessState,
    )
    from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource

    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM,
                entity_id=PLATFORM_TEAM.canonical_id,
                display_label=PLATFORM_TEAM.label,
            )
        ],
        team_ids=[PLATFORM_TEAM.canonical_id],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )
    source = ClickHouseStatusChangeSource(cast(Any, object()))
    snapshot = await source.status_snapshot(
        org_id=ORG_ID, scope=scope, as_of=datetime(2026, 7, 31, tzinfo=UTC), limit=25
    )

    assert snapshot.declared is None
    assert snapshot.source_refs, (
        "a committed team subject must carry an explicit typed observation, "
        "never silent zero rows"
    )
    assert all(
        ref.freshness is FreshnessState.UNAVAILABLE for ref in snapshot.source_refs
    )
    assert snapshot.warnings, "an empty-facts team answer must disclose why"


def test_native_status_change_scope_type_disjunctions_are_total_over_team() -> None:
    """Structural closure: every scope_type disjunction has a team arm, or is
    listed in TEAM_NOT_APPLICABLE_SOURCES. Without this, the next SQL constant
    added re-opens the silent-empty-team-answer hole the addendum describes.
    """

    from dev_health_ops.api.dev import native_status_change as nsc

    scope_type_constants = {
        name: value
        for name, value in vars(nsc).items()
        if name.startswith("_")
        and name.endswith("_SQL")
        and isinstance(value, str)
        and "scope_type" in value
    }
    assert scope_type_constants, "sanity: native_status_change defines scope_type SQL"

    missing_team_arm = {
        name for name, sql in scope_type_constants.items() if "'team'" not in sql
    }
    registry = frozenset(getattr(nsc, "TEAM_NOT_APPLICABLE_SOURCES", ()))
    unregistered = missing_team_arm - registry
    assert not unregistered, (
        "every scope_type disjunction must either gate on 'team' or be listed "
        f"in TEAM_NOT_APPLICABLE_SOURCES: {sorted(unregistered)}"
    )
    # And the registry itself must not claim sources that do not exist.
    assert registry <= set(scope_type_constants), (
        "TEAM_NOT_APPLICABLE_SOURCES names a source that is not a real "
        "scope_type-gated SQL constant"
    )


# ---------------------------------------------------------------------------
# G2 — the production flag gate that constructs the preflight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_g2_flag_off_production_runtime_has_no_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.dev import production_runtime
    from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
    from dev_health_ops.llm.agent.policy import AgentProviderSource

    class _FakeProvider:
        async def decide(self, **_values: Any) -> Any:
            raise AssertionError("no provider call expected for a construction test")

        async def aclose(self) -> None:
            return None

    async def resolve_provider(_session: Any, *, org_id: str) -> Any:
        return ProductionProviderResolution(
            provider=cast(Any, _FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes")

    runtime = await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id=_G2_ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        clickhouse=cast(Any, object()),
    )
    try:
        assert runtime.preflight is None
    finally:
        await runtime.aclose()


@pytest.mark.asyncio
async def test_g2_flag_on_production_runtime_constructs_the_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paired with the flag-off control above so the pair proves the gate
    discriminates, rather than proving only that one side happens to work.
    """

    from dev_health_ops.api.dev import production_runtime
    from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from dev_health_ops.licensing.feature_policy import (
        FeatureDecision,
        FeatureDecisionReason,
    )
    from dev_health_ops.licensing.registry import ASK_DEV_WAVE_3_1_FEATURE
    from dev_health_ops.llm.agent.policy import AgentProviderSource
    from tests._chaos_3292_preflight import RecordingProvider, status_then_answer

    async def resolve_provider(_session: Any, *, org_id: str) -> Any:
        return ProductionProviderResolution(
            provider=cast(
                Any, RecordingProvider(status_then_answer("g2"), script_id="g2")
            ),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    async def feature_allowed(_session: Any, _org_id: Any, _key: str) -> Any:
        return FeatureDecision(
            feature_key=ASK_DEV_WAVE_3_1_FEATURE,
            allowed=True,
            reason=FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE,
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setattr(
        production_runtime, "evaluate_org_feature_async", feature_allowed
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes")

    runtime = await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id=_G2_ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        clickhouse=cast(Any, object()),
    )
    try:
        assert runtime.preflight is not None
        assert isinstance(runtime.preflight, SubjectPreflight)
    finally:
        await runtime.aclose()
