from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEvidenceFlags,
    DevEvidenceRef,
    FreshnessState,
)
from dev_health_ops.api.dev.data_health_service import (
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from dev_health_ops.api.dev.entitlement import AskDevEntitlementDeniedError
from dev_health_ops.api.dev.evidence_service import (
    EvidenceAvailability,
    EvidenceSearchResult,
    SourceSearchResult,
)
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing import FeatureDecisionReason

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
ORG_A = "00000000-0000-0000-0000-000000000001"
ORG_B = "00000000-0000-0000-0000-000000000002"

_SEARCH = """
query Evidence($orgId: String!, $input: DevEvidenceSearchInput!) {
  devEvidenceSearch(orgId: $orgId, input: $input) {
    queryVersion rankingVersion
    sources { sourceSystem state warning }
    evidence {
      evidenceRefId sourceSystem entityType entityId displayLabel
      freshness citationText repositoryIds validEntityIds
      flags { stale untrustedContent }
    }
  }
}
"""

_HEALTH = """
query Health($orgId: String!, $input: DevDataHealthInput!) {
  devDataHealth(orgId: $orgId, input: $input) {
    queryVersion completeEligible
    sources {
      sourceSystem state required coverage confidenceImpact
      freshnessPolicyVersion missingRepositoryIds
    }
  }
}
"""


def _context(org_id: str = ORG_A) -> GraphQLContext:
    context = GraphQLContext(
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
    setattr(context, "session", object())
    return context


def _scope() -> dict[str, Any]:
    return {
        "directScope": "REPOSITORY",
        "refs": [{"kind": "REPOSITORY", "value": "repo-a"}],
        "presetDays": 30,
        "timezone": "UTC",
    }


class FakeEvidenceService:
    calls = 0

    async def search(self, **kwargs: object) -> EvidenceSearchResult:
        type(self).calls += 1
        assert kwargs["org_id"] == ORG_A
        assert kwargs["query"] == "release blocker"
        return EvidenceSearchResult(
            evidence=(
                DevEvidenceRef(
                    schema_version="dev_evidence_ref.v1",
                    evidence_ref_id="ev1_0123456789012345678901234567890123456789",
                    source_system="work_items",
                    source_version="native.v1",
                    entity_type="issue",
                    entity_id="issue-1",
                    display_label="Release blocker",
                    observed_at=NOW,
                    freshness=FreshnessState.FRESH,
                    provenance="native",
                    confidence=1,
                    citation_text="Status: open",
                    repository_ids=["repo-a"],
                    valid_entity_ids=[],
                    flags=DevEvidenceFlags(),
                ),
            ),
            source_states=(
                SourceSearchResult("work_items", EvidenceAvailability.AVAILABLE),
                SourceSearchResult("acr", EvidenceAvailability.UNCONFIGURED),
            ),
        )


@pytest.mark.asyncio
async def test_dev_evidence_search_is_typed_and_uses_shared_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeEvidenceService.calls = 0
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_evidence._evidence_service",
        lambda _context, _entitlement: FakeEvidenceService(),
    )
    result = await schema.execute(
        _SEARCH,
        variable_values={
            "orgId": ORG_A,
            "input": {"query": "release blocker", "scope": _scope()},
        },
        context_value=_context(),
    )
    assert result.errors is None
    assert FakeEvidenceService.calls == 1
    assert result.data == {
        "devEvidenceSearch": {
            "queryVersion": "search-evidence.v1",
            "rankingVersion": "evidence-ranking.v1",
            "sources": [
                {"sourceSystem": "work_items", "state": "available", "warning": None},
                {"sourceSystem": "acr", "state": "unconfigured", "warning": None},
            ],
            "evidence": [
                {
                    "evidenceRefId": "ev1_0123456789012345678901234567890123456789",
                    "sourceSystem": "work_items",
                    "entityType": "issue",
                    "entityId": "issue-1",
                    "displayLabel": "Release blocker",
                    "freshness": "fresh",
                    "citationText": "Status: open",
                    "repositoryIds": ["repo-a"],
                    "validEntityIds": [],
                    "flags": {"stale": False, "untrustedContent": True},
                }
            ],
        }
    }


class FakeDataHealthService:
    async def inspect(self, **_kwargs: object) -> DataHealthResult:
        return DataHealthResult(
            sources=(
                DataHealthSource(
                    source_system="work_items",
                    state=DataHealthState.STALE,
                    required=True,
                    last_successful_at=NOW,
                    watermark=NOW,
                    missing_repository_ids=("repo-b",),
                    missing_entity_ids=(),
                    coverage=0.5,
                    confidence_impact="degraded",
                    freshness_policy_version="work-items-sync.v1",
                ),
            ),
            complete_eligible=False,
        )


@pytest.mark.asyncio
async def test_dev_data_health_exposes_completion_impact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_evidence.DataHealthService",
        lambda **_kwargs: FakeDataHealthService(),
    )
    result = await schema.execute(
        _HEALTH,
        variable_values={
            "orgId": ORG_A,
            "input": {"scope": _scope(), "requiredSources": ["work_items"]},
        },
        context_value=_context(),
    )
    assert result.errors is None
    assert result.data == {
        "devDataHealth": {
            "queryVersion": "data-health.v1",
            "completeEligible": False,
            "sources": [
                {
                    "sourceSystem": "work_items",
                    "state": "stale",
                    "required": True,
                    "coverage": 0.5,
                    "confidenceImpact": "degraded",
                    "freshnessPolicyVersion": "work-items-sync.v1",
                    "missingRepositoryIds": ["repo-b"],
                }
            ],
        }
    }


@pytest.mark.asyncio
async def test_dev_evidence_cross_tenant_is_rejected_before_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeEvidenceService.calls = 0
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_evidence._evidence_service",
        lambda _context, _entitlement: FakeEvidenceService(),
    )
    result = await schema.execute(
        _SEARCH,
        variable_values={
            "orgId": ORG_B,
            "input": {"query": "release blocker", "scope": _scope()},
        },
        context_value=_context(),
    )
    assert result.errors is not None
    assert result.data is None
    assert FakeEvidenceService.calls == 0


class DeniedEntitlement:
    async def require(self, _org_id: str) -> None:
        raise AskDevEntitlementDeniedError(
            FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("query", [_SEARCH, _HEALTH])
async def test_user_facing_evidence_fields_fail_closed_without_explicit_entitlement(
    monkeypatch: pytest.MonkeyPatch,
    query: str,
) -> None:
    monkeypatch.setenv(
        "JWT_SECRET_KEY", "ask-dev-test-secret-that-is-at-least-thirty-two-bytes"
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_evidence."
        "CanonicalAskDevEntitlementAuthorizer",
        lambda _session: DeniedEntitlement(),
    )
    variable_values: dict[str, Any] = {
        "orgId": ORG_A,
        "input": {"scope": _scope()},
    }
    if query == _SEARCH:
        variable_values["input"]["query"] = "release blocker"
    else:
        variable_values["input"]["requiredSources"] = ["work_items"]

    result = await schema.execute(
        query,
        variable_values=variable_values,
        context_value=_context(),
    )

    assert result.errors is not None
    assert result.data is None
    assert "Ask Dev is not available" in result.errors[0].message
    assert "tier" not in result.errors[0].message.casefold()
