from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contract_fixtures import (
    negative_fixtures,
    positive_fixtures,
    stream_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    CONTRACT_MODELS,
    DevAnswer,
    DevCapabilities,
    DevMessageRequest,
    DevScope,
    DevScopeResolution,
    DevStreamEvent,
    validate_stream,
)
from dev_health_ops.api.dev.export_contracts import (
    check_artifacts,
    expected_artifacts,
)


@pytest.mark.parametrize("schema_version", CONTRACT_MODELS)
def test_positive_fixture_validates(schema_version: str) -> None:
    CONTRACT_MODELS[schema_version].model_validate(positive_fixtures()[schema_version])


@pytest.mark.parametrize(
    ("schema_version", "case", "payload"),
    [
        (schema_version, case, payload)
        for schema_version, cases in negative_fixtures().items()
        for case, payload in cases
    ],
)
def test_negative_fixture_is_rejected(
    schema_version: str,
    case: str,
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match=".+"):
        CONTRACT_MODELS[schema_version].model_validate(payload)


def test_answer_rejects_unknown_evidence_and_metric_ids() -> None:
    answer = positive_fixtures()["dev_answer.v1"]
    unknown_evidence = deepcopy(answer)
    unknown_evidence["claims"][0]["evidence_ref_ids"] = ["ev_unknown"]
    with pytest.raises(ValidationError, match="unknown evidence"):
        DevAnswer.model_validate(unknown_evidence)

    unknown_metric = deepcopy(answer)
    unknown_metric["claims"][0]["metric_ref_ids"] = ["metric_unknown"]
    with pytest.raises(ValidationError, match="unknown metric"):
        DevAnswer.model_validate(unknown_metric)


def test_stream_sequences_require_exactly_one_terminal_then_done() -> None:
    fixtures = stream_fixtures()
    validate_stream([DevStreamEvent.model_validate(item) for item in fixtures["valid"]])
    for name, payloads in fixtures.items():
        if name == "valid":
            continue
        with pytest.raises((ValidationError, ValueError)):
            validate_stream([DevStreamEvent.model_validate(item) for item in payloads])


def test_capability_gates_are_independent() -> None:
    values = positive_fixtures()["dev_capabilities.v1"]
    capabilities = DevCapabilities.model_validate(values)
    assert capabilities.ask_dev is False
    assert capabilities.byo_llm is True
    assert capabilities.agent_context_runtime is False


def test_neutral_message_scope_without_surface_context_remains_valid() -> None:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["scope"]["direct_scope"] = "organization"
    payload["scope"]["repositories"] = []
    payload["scope"]["surface_context"] = None

    request = DevMessageRequest.model_validate(payload)

    assert request.scope.surface_context is None


@pytest.mark.parametrize("route_id", ["deployment_detail", "incident_detail"])
def test_deferred_contextual_routes_are_rejected(route_id: str) -> None:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["scope"]["surface_context"]["route_id"] = route_id

    with pytest.raises(ValidationError, match="route_id"):
        DevMessageRequest.model_validate(payload)


def test_surface_context_must_match_direct_scope() -> None:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["scope"]["surface_context"] = {
        "route_id": "issue_detail",
        "entity_refs": [
            {
                "entity_type": "issue",
                "entity_id": "issue_42",
                "display_label": "Issue 42",
            }
        ],
    }

    with pytest.raises(ValidationError, match="must match direct scope"):
        DevMessageRequest.model_validate(payload)


def test_scope_contract_accepts_canonical_pull_request_ids_and_repository_candidates() -> (
    None
):
    pull_request_scope = deepcopy(positive_fixtures()["dev_scope.v1"])
    pull_request_scope["direct_scope"] = "pull_request"
    pull_request_scope["repositories"] = []
    pull_request_scope["entity_refs"] = [
        {
            "entity_type": "pull_request",
            "entity_id": "550e8400-e29b-41d4-a716-446655440000#pr42",
            "display_label": "PR 42",
            "repository_id": "repo_dev_health",
        }
    ]
    pull_request_scope["surface_context"] = None
    DevScope.model_validate(pull_request_scope)

    ambiguous = deepcopy(positive_fixtures()["dev_scope_resolution.v1"])
    ambiguous["outcome"] = "ambiguous"
    ambiguous["resolved_scope"] = None
    ambiguous["candidates"] = [
        {
            "entity_ref": {
                "entity_type": "repository",
                "entity_id": "full-chaos/dev-health",
                "display_label": "full-chaos/dev-health",
                "repository_id": "repo_dev_health",
            },
            "repository_id": "repo_dev_health",
            "reason": "Repository name matched more than one authorized catalog entry.",
        }
    ]
    DevScopeResolution.model_validate(ambiguous)


def test_team_direct_scope_requires_one_matching_team_entity_ref() -> None:
    """CHAOS-3301: TEAM behaves like every other direct-entity scope kind."""

    team_scope = deepcopy(positive_fixtures()["dev_scope.v1"])
    team_scope["direct_scope"] = "team"
    team_scope["repositories"] = []
    team_scope["entity_refs"] = [
        {
            "entity_type": "team",
            "entity_id": "team_platform",
            "display_label": "Platform",
        }
    ]
    team_scope["team_ids"] = ["team_platform"]
    team_scope["surface_context"] = None
    DevScope.model_validate(team_scope)

    missing_entity_ref = deepcopy(team_scope)
    missing_entity_ref["entity_refs"] = []
    with pytest.raises(ValidationError, match="direct entity scope requires"):
        DevScope.model_validate(missing_entity_ref)


def test_team_direct_scope_requires_team_ids_to_name_exactly_that_team() -> None:
    """CHAOS-3301: a team filter can never be read as a team subject.

    This is the kill site for mutation M5 (removing this invariant): with it
    deleted, a team-direct ``DevScope`` with empty or mismatched
    ``team_ids`` would validate cleanly, and the metrics path's
    ``scope.team_ids``-gated filter (``metrics/clickhouse.py``) would then
    silently apply no team filter at all for a "committed" team subject.
    """

    team_scope = deepcopy(positive_fixtures()["dev_scope.v1"])
    team_scope["direct_scope"] = "team"
    team_scope["repositories"] = []
    team_scope["entity_refs"] = [
        {
            "entity_type": "team",
            "entity_id": "team_platform",
            "display_label": "Platform",
        }
    ]
    team_scope["surface_context"] = None

    empty_team_ids = deepcopy(team_scope)
    empty_team_ids["team_ids"] = []
    with pytest.raises(ValidationError, match="team_ids to name exactly that team"):
        DevScope.model_validate(empty_team_ids)

    mismatched_team_ids = deepcopy(team_scope)
    mismatched_team_ids["team_ids"] = ["team_other"]
    with pytest.raises(ValidationError, match="team_ids to name exactly that team"):
        DevScope.model_validate(mismatched_team_ids)

    extra_team_ids = deepcopy(team_scope)
    extra_team_ids["team_ids"] = ["team_platform", "team_other"]
    with pytest.raises(ValidationError, match="team_ids to name exactly that team"):
        DevScope.model_validate(extra_team_ids)


def test_organization_scope_with_a_team_filter_stays_a_filter() -> None:
    """The converse of the above: team_ids alongside a non-TEAM direct scope
    is a legitimate filter, structurally distinct from a team subject."""

    org_scope = deepcopy(positive_fixtures()["dev_scope.v1"])
    org_scope["direct_scope"] = "organization"
    org_scope["repositories"] = []
    org_scope["entity_refs"] = []
    org_scope["team_ids"] = ["team_platform"]
    org_scope["surface_context"] = None
    scope = DevScope.model_validate(org_scope)

    assert scope.team_ids == ["team_platform"]


def test_checked_in_contract_artifacts_have_no_drift() -> None:
    check_artifacts(expected_artifacts())


def test_contract_schemas_are_provider_neutral_and_closed() -> None:
    artifacts = expected_artifacts()
    schemas = "\n".join(
        contents for path, contents in artifacts.items() if path.startswith("schemas/")
    )
    assert 'additionalProperties": false' in schemas
    for provider_specific in ("openai_api_key", "anthropic_api_key", "tool_choice"):
        assert provider_specific not in schemas


@pytest.mark.parametrize("schema_version", CONTRACT_MODELS)
def test_every_contract_requires_its_explicit_version(schema_version: str) -> None:
    payload = deepcopy(positive_fixtures()[schema_version])
    payload.pop("schema_version")
    with pytest.raises(ValidationError, match="schema_version"):
        CONTRACT_MODELS[schema_version].model_validate(payload)
