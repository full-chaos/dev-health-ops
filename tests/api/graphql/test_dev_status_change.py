from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    FreshnessState,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    ChangeCategory,
    ChangeSummaryResult,
    ChangeWindow,
    CompletionState,
    ObservedChange,
    SourceReference,
    StatusFact,
    StatusResultState,
    StatusSnapshotResult,
)
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser

ORG_A = "00000000-0000-0000-0000-000000000001"
ORG_B = "00000000-0000-0000-0000-000000000002"
START = datetime(2026, 7, 1, tzinfo=UTC)
END = datetime(2026, 7, 8, tzinfo=UTC)
PRIOR_START = datetime(2026, 6, 24, tzinfo=UTC)
NOW = datetime(2026, 7, 8, 12, tzinfo=UTC)

_STATUS = """
query Status($orgId: String!, $input: DevStatusSnapshotInput!) {
  devStatusSnapshot(orgId: $orgId, input: $input) {
    contractVersion state asOf warnings
    scope { organizationId directScope repositoryIds currentStart currentEnd }
    declared { entityId status required evidenceRefIds }
    actual { state ruleId ruleVersion reasonCodes evidenceRefIds }
    blockers { entityId status }
    sourceRefs { refId sourceSystem freshness evidenceRefIds }
  }
}
"""

_CHANGE = """
query Change($orgId: String!, $input: DevChangeSummaryInput!) {
  devChangeSummary(orgId: $orgId, input: $input) {
    contractVersion state warnings
    currentWindow { start end }
    comparisonWindow { start end }
    changes {
      changeId category entityId before after claimKind relationshipChain
      metricId metricValue metricComparisonValue evidenceRefIds
    }
  }
}
"""


def _context(org_id: str = ORG_A) -> GraphQLContext:
    return GraphQLContext(
        org_id=org_id,
        db_url="clickhouse://test",
        client=object(),
        user=AuthenticatedUser(
            user_id="user-a",
            email="member@example.com",
            org_id=org_id,
            role="member",
            token_version=1,
        ),
    )


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_A,
        direct_scope=DirectScope.ISSUE,
        repositories=["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue-a",
                display_label="Issue A",
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(start=START, end=END, timezone="UTC"),
    )


def _variables() -> dict[str, Any]:
    return {
        "orgId": ORG_A,
        "input": {
            "scope": {
                "directScope": "ISSUE",
                "refs": ["issue-a"],
                "startDate": "2026-07-01",
                "endDate": "2026-07-08",
                "timezone": "UTC",
            }
        },
    }


@pytest.fixture
def _allow_and_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    async def allow(_org_id: str) -> None:
        return None

    async def resolve(_context: object, _input: object) -> DevScope:
        return _scope()

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_status_change."
        "dev_entitlement.require_ask_dev_entitlement",
        allow,
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_status_change."
        "resolve_dev_metric_scope",
        resolve,
    )


class FakeStatusChangeService:
    async def status_snapshot(self, org_id, fingerprint, request):
        assert org_id == ORG_A
        assert fingerprint
        assert request.max_items == 100
        declared = StatusFact(
            "issue",
            "issue-a",
            "Issue A",
            "done",
            NOW,
            "source-work",
            ("evidence-a",),
        )
        source = SourceReference(
            "source-work",
            "work_items",
            "work-items.v1",
            FreshnessState.FRESH,
            NOW,
            ("evidence-a",),
        )
        return StatusSnapshotResult(
            contract_version="status_snapshot.v1",
            state=StatusResultState.COMPLETE,
            scope=request.scope,
            as_of=NOW,
            declared=declared,
            actual=ActualCompletion(
                CompletionState.READY,
                "actual-completion",
                "actual-completion.v2",
                (),
                (),
                (),
                ("source-work",),
                ("evidence-a",),
            ),
            children=(),
            blockers=(),
            pull_requests=(),
            ci=(),
            deployments=(),
            incidents=(),
            source_refs=(source,),
            warnings=(),
        )

    async def change_summary(self, org_id, fingerprint, request):
        assert org_id == ORG_A
        assert fingerprint
        return ChangeSummaryResult(
            contract_version="change_summary.v1",
            state=StatusResultState.COMPLETE,
            current_window=ChangeWindow(request.current_start, request.current_end),
            comparison_window=ChangeWindow(
                request.comparison_start, request.comparison_end
            ),
            changes=(
                ObservedChange(
                    "change-a",
                    ChangeCategory.STATUS,
                    "issue",
                    "issue-a",
                    "Issue A",
                    "started",
                    "done",
                    NOW,
                    ClaimKind.OBSERVED,
                    (),
                    None,
                    None,
                    None,
                    ("source-work",),
                    ("evidence-a",),
                ),
            ),
            source_refs=(),
            warnings=(),
        )


@pytest.mark.asyncio
async def test_status_snapshot_is_typed_and_uses_shared_service(
    _allow_and_resolve,
) -> None:
    context = _context()
    context.dev_status_change_service = FakeStatusChangeService()  # type: ignore[attr-defined]
    result = await schema.execute(
        _STATUS, variable_values=_variables(), context_value=context
    )
    assert result.errors is None
    assert result.data is not None
    assert result.data["devStatusSnapshot"]["actual"] == {
        "state": "READY",
        "ruleId": "actual-completion",
        "ruleVersion": "actual-completion.v2",
        "reasonCodes": [],
        "evidenceRefIds": ["evidence-a"],
    }
    assert result.data["devStatusSnapshot"]["declared"]["status"] == "done"


@pytest.mark.asyncio
async def test_change_summary_preserves_explicit_equal_windows_and_claim_kind(
    _allow_and_resolve,
) -> None:
    context = _context()
    context.dev_status_change_service = FakeStatusChangeService()  # type: ignore[attr-defined]
    variables = _variables()
    variables["input"].update(
        {
            "comparisonStart": PRIOR_START.isoformat(),
            "comparisonEnd": START.isoformat(),
        }
    )
    result = await schema.execute(
        _CHANGE, variable_values=variables, context_value=context
    )
    assert result.errors is None
    assert result.data is not None
    change = result.data["devChangeSummary"]["changes"][0]
    assert change["claimKind"] == "observed"
    assert change["relationshipChain"] == []
    assert result.data["devChangeSummary"]["comparisonWindow"] == {
        "start": PRIOR_START.isoformat(),
        "end": START.isoformat(),
    }


@pytest.mark.asyncio
async def test_status_fields_reject_cross_tenant_before_shared_service(
    _allow_and_resolve,
) -> None:
    variables = _variables()
    variables["orgId"] = ORG_B
    result = await schema.execute(
        _STATUS, variable_values=variables, context_value=_context()
    )
    assert result.errors is not None
    assert result.data is None
