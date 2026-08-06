"""CHAOS-3297 s2 round 9 (codex CONFIRMED): the completion-disclosure
obligation's trigger predicate must fire for every truncation flag that can
make a completion assessment untrustworthy, not just a withheld denominator.

``status_change_service._assess`` only nulls
``required_child_total``/``required_child_complete`` when the required-child
SOURCE itself was truncated (``children_source_truncated`` -- see
``status_change_service.py``, folding in ``membership_source_truncated``).
Every other assessment category -- blockers, pull_requests, ci, deployments,
incidents -- sets the general ``assessment_source_limit_reached`` reason code
while leaving the denominator non-``None`` (it genuinely counted every
required child it saw; it is the REST of the evidence behind a "complete"
claim that was cut off). Before this round, ``answer_validator.py`` gated the
disclosure obligation on the null denominator alone, so those five categories
were completely unguarded: a markerless "All required work is complete."
passed even though the underlying assessment could not be trusted.

These are real end-to-end tests: a fake ``ClickHouseStatusChangeSource``
returns a ``RawStatusSnapshot`` with exactly one ``*_source_truncated`` flag
set, driven through the REAL production tool boundary (the real
``StatusChangeService`` deterministic rule engine, the real
``production_runtime.py`` wire projection -- nothing about
``answer_validator.py`` or the completion-disclosure guard is mocked), and
the resulting real ``DevToolResult.actual_completion`` is fed into
``validate_answer_candidate`` with a markerless completion claim, asserting
rejection. Children and membership are included as regression controls,
proving the pre-existing correct behavior for those two categories survives
broadening the trigger predicate (no double-trigger regression).
"""

from __future__ import annotations

import asyncio
import secrets
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.answer_validator import (
    AnswerValidationContext,
    AnswerValidationError,
    validate_answer_candidate,
)
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevContractVersions,
    DevEntityRef,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevTimeRange,
    DevToolRequest,
    DirectScope,
    EntityType,
    ToolID,
)
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.api.dev.status_change_service import (
    RawChangeSummary,
    RawStatusSnapshot,
    SourceReference,
    StatusFact,
)
from dev_health_ops.api.dev.tool_registry import ToolExecutionContext
from dev_health_ops.llm.agent.policy import AgentProviderSource

ORG_ID = "3d3a2b1e-3259-4c3e-9e6a-325934592591"
EVIDENCE_SIGNING_FIXTURE_KEY = secrets.token_hex(32)
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


class _FakeProvider:
    async def decide(self, **_values: Any) -> Any:
        raise AssertionError("provider calls are outside this projection test")

    async def aclose(self) -> None:
        return None


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue_parent",
                display_label="Parent issue",
                repository_id="repo_dev_health",
            )
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 6, 30, tzinfo=UTC), end=NOW, timezone="UTC"
        ),
    )


class _FakeTruncatedStatusSource:
    """Stands in for ``ClickHouseStatusChangeSource``: returns a
    ``RawStatusSnapshot`` with exactly one ``*_source_truncated`` flag set,
    otherwise minimal well-formed data. Everything downstream --
    ``StatusChangeService`` and every ``production_runtime.py`` adapter
    closure -- runs unmodified. Named parameters (not ``**kwargs``) so the
    construction stays fully typed against ``RawStatusSnapshot``'s own
    field types -- mypy's dataclasses plugin checks a splatted dict
    argument's field types too, not just the constructor's own signature.
    """

    def __init__(
        self,
        *,
        children_source_truncated: bool = False,
        membership_source_truncated: bool = False,
        blockers_source_truncated: bool = False,
        pull_requests_source_truncated: bool = False,
        ci_source_truncated: bool = False,
        deployments_source_truncated: bool = False,
        incidents_source_truncated: bool = False,
    ) -> None:
        self._children_source_truncated = children_source_truncated
        self._membership_source_truncated = membership_source_truncated
        self._blockers_source_truncated = blockers_source_truncated
        self._pull_requests_source_truncated = pull_requests_source_truncated
        self._ci_source_truncated = ci_source_truncated
        self._deployments_source_truncated = deployments_source_truncated
        self._incidents_source_truncated = incidents_source_truncated

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        del org_id, scope, as_of, limit
        declared = StatusFact(
            entity_type="issue",
            entity_id="issue_parent",
            display_label="Parent issue",
            status="done",
            observed_at=NOW,
            source_ref_id="ref:work_items",
            evidence_ref_ids=(),
        )
        return RawStatusSnapshot(
            declared=declared,
            source_refs=(
                SourceReference(
                    ref_id="ref:work_items",
                    source_system="work_items",
                    source_version="work-items.v1",
                    freshness=production_runtime.FreshnessState.FRESH,
                    watermark=NOW,
                    evidence_ref_ids=(),
                ),
            ),
            children_source_truncated=self._children_source_truncated,
            membership_source_truncated=self._membership_source_truncated,
            blockers_source_truncated=self._blockers_source_truncated,
            pull_requests_source_truncated=self._pull_requests_source_truncated,
            ci_source_truncated=self._ci_source_truncated,
            deployments_source_truncated=self._deployments_source_truncated,
            incidents_source_truncated=self._incidents_source_truncated,
        )

    async def change_summary(self, **_kwargs: Any) -> RawChangeSummary:
        return RawChangeSummary(changes=(), source_refs=())


class _FakeEntitlementAuthorizer:
    def __init__(self, _session: Any) -> None:
        pass

    async def require(self, org_id: str) -> None:
        assert org_id == ORG_ID


class _FakeAuthorizedEntityCatalog:
    def __init__(self) -> None:
        self._entity = AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id="issue_parent",
            label="Parent issue",
            repository_id="repo_dev_health",
        )

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        del org_id, kinds
        return "watermark_01"

    async def exact(
        self, org_id: str, ref: Any, *, limit: int
    ) -> list[AuthorizedEntity]:
        del org_id, limit
        if ref.kind is EntityKind.ISSUE and ref.value == "issue_parent":
            return [self._entity]
        return []

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
        preferred_kinds: frozenset[EntityKind] = frozenset(),
    ) -> list[AuthorizedEntity]:
        del org_id, query, kinds, limit, include_alias_matches, preferred_kinds
        return []


async def _build_runtime(monkeypatch: pytest.MonkeyPatch, *, status_source: Any) -> Any:
    async def resolve_provider(_session, *, org_id: str):
        assert org_id == ORG_ID
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
    monkeypatch.setattr(
        production_runtime,
        "ClickHouseStatusChangeSource",
        lambda _clickhouse: status_source,
    )
    monkeypatch.setattr(
        production_runtime,
        "ClickHouseAuthorizedEntityCatalog",
        lambda _clickhouse: _FakeAuthorizedEntityCatalog(),
    )
    monkeypatch.setattr(
        production_runtime,
        "CanonicalAskDevEntitlementAuthorizer",
        _FakeEntitlementAuthorizer,
    )
    monkeypatch.setenv("JWT_SECRET_KEY", EVIDENCE_SIGNING_FIXTURE_KEY)
    return await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )


def _execution_context(scope: DevScope) -> ToolExecutionContext:
    return ToolExecutionContext(
        org_id=scope.organization_id,
        user_id="user_01",
        permission_fingerprint="permissions_01",
        authorized_scope=scope,
        cancellation=asyncio.Event(),
        remaining_seconds=15,
    )


_SEVEN_TRUNCATION_FLAGS = [
    "children_source_truncated",
    "membership_source_truncated",
    "blockers_source_truncated",
    "pull_requests_source_truncated",
    "ci_source_truncated",
    "deployments_source_truncated",
    "incidents_source_truncated",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("flag_name", _SEVEN_TRUNCATION_FLAGS)
async def test_every_truncation_flag_flows_through_to_a_rejected_markerless_answer(
    monkeypatch: pytest.MonkeyPatch, flag_name: str
) -> None:
    """Service assessment -> wire projection -> markerless-answer rejection,
    for all seven truncation flags. children_source_truncated and
    membership_source_truncated (rounds 2-3) already nulled
    required_child_total and are included here as REGRESSION CONTROLS,
    proving the round-9 broadened predicate doesn't disturb the
    pre-existing correct behavior for those two. blockers_source_truncated
    through incidents_source_truncated (round 5) are the five NEW lanes --
    before this round's fix, each of these left required_child_total
    non-None and only set assessment_source_limit_reached, so the old
    (required_child_total is None)-only predicate never fired for them.
    """
    runtime = await _build_runtime(
        monkeypatch,
        status_source=_FakeTruncatedStatusSource(**{flag_name: True}),
    )
    scope = _scope()
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _execution_context(scope),
    )
    tool_result = execution.result

    # Sanity: the real service assessment actually produced the
    # untrustworthy signal this test exists to exercise -- if this
    # assertion ever fails, the test below would pass VACUOUSLY (the
    # markerless answer would be rejected for some OTHER reason, or the
    # scenario stopped reproducing the truncation state at all), which is
    # exactly the kind of measurement-that-didn't-happen this suite must
    # never allow to look like coverage.
    assert tool_result.actual_completion is not None
    assert (
        tool_result.actual_completion.required_child_total is None
        or "assessment_source_limit_reached"
        in tool_result.actual_completion.reason_codes
    ), f"{flag_name} did not produce an untrustworthy real assessment"

    fixtures = positive_fixtures()
    answer_payload = deepcopy(fixtures["dev_answer.v1"])
    answer_payload["status"] = "partial"
    answer_payload["direct_summary"] = "See the linked claim for details."
    # evidence/metrics must be cleared -- the fixture's baseline entries
    # (ev_01/metric_01) don't match the REAL evidence/metrics minted by
    # this test's production execution, and an empty answer.evidence/
    # metrics trivially satisfies the canonical-object check (nothing to
    # validate against). The claim is switched to "inferred" so it needs
    # no evidence/metric reference of its own (DevClaim.validate_grounding
    # only requires one for kind="observed").
    answer_payload["evidence"] = []
    answer_payload["metrics"] = []
    answer_payload["claims"] = [
        {
            **answer_payload["claims"][0],
            "kind": "inferred",
            "text": "All required work is complete.",
            "confidence": 0.5,
            "evidence_ref_ids": [],
            "metric_ref_ids": [],
        }
    ]
    answer = DevAnswer.model_validate(fixtures["dev_answer.v1"])
    context = AnswerValidationContext(
        conversation_id=answer.conversation_id,
        answer_id=answer.answer_id,
        scope_resolution=DevScopeResolution.model_validate(
            fixtures["dev_scope_resolution.v1"]
        ),
        versions=DevContractVersions.model_validate(answer.versions),
        model=DevModelMetadata.model_validate(answer.model),
        tool_results=(tool_result,),
    )

    with pytest.raises(
        AnswerValidationError, match="omits the required disclosure"
    ) as raised:
        validate_answer_candidate(answer_payload, context)
    assert raised.value.repairable is True
    assert raised.value.code == "completion_denominator_withheld"
