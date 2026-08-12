from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contract_fixtures import (
    TEAM_ID,
    TEAM_LABEL,
    negative_fixtures,
    positive_fixtures,
    positive_variant_fixtures,
    stream_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    CONTRACT_MODELS,
    DevAnswer,
    DevAnswerGraphAssistance,
    DevCapabilities,
    DevEvidenceRef,
    DevFeedback,
    DevMessageRequest,
    DevScope,
    DevScopeResolution,
    DevStreamEvent,
    ToolID,
    validate_stream,
)
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.contracts_v2.compat import NO_ANSWER_ERROR_CODES
from dev_health_ops.api.dev.contracts_v2.validators import (
    CANONICAL_NO_ANSWER_COPY,
    CANONICAL_NO_ANSWER_REMEDIATION,
    NO_ANSWER_OUTCOMES,
)
from dev_health_ops.api.dev.export_contracts import (
    SOURCE_HEALTH_LABELS,
    check_artifacts,
    expected_artifacts,
)
from dev_health_ops.api.dev.no_match_terminal import INTERNAL_TOKEN_DENYLIST
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeResolutionService,
)


@pytest.mark.parametrize("schema_version", CONTRACT_MODELS)
def test_positive_fixture_validates(schema_version: str) -> None:
    CONTRACT_MODELS[schema_version].model_validate(positive_fixtures()[schema_version])


def test_record_locator_is_absent_by_default_on_every_existing_evidence_ref() -> None:
    """CHAOS-3633 / CHAOS-3660 §8(i). The canonical fixture predates this
    field and never sets it -- ``test_positive_fixture_validates`` already
    proves ``dev_evidence_ref.v1`` still validates without it, so this
    fixture is itself the backward-compatibility proof. This test pins the
    actual value down explicitly rather than leaving that proof implicit.
    """
    ref = DevEvidenceRef.model_validate(positive_fixtures()["dev_evidence_ref.v1"])
    assert ref.record_locator is None


def test_record_locator_accepts_an_opaque_id_when_present() -> None:
    payload = deepcopy(positive_fixtures()["dev_evidence_ref.v1"])
    payload["record_locator"] = "loc_pr_review_01"
    ref = DevEvidenceRef.model_validate(payload)
    assert ref.record_locator == "loc_pr_review_01"


@pytest.mark.parametrize(
    ("schema_version", "case", "payload"),
    [
        (schema_version, case, payload)
        for schema_version, cases in positive_variant_fixtures().items()
        for case, payload in cases
    ],
)
def test_positive_variant_fixture_validates(
    schema_version: str, case: str, payload: dict[str, object]
) -> None:
    CONTRACT_MODELS[schema_version].model_validate(payload)


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


def test_graph_assisted_is_absent_by_default_on_every_existing_answer() -> None:
    """CHAOS-3660 §8(b). The canonical answer fixture predates this field
    and never sets it -- pinning the default explicitly rather than
    leaving the backward-compatibility proof implicit in the parametrized
    fixture pass.
    """
    answer = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"])
    assert answer.graph_assisted is None


def test_graph_assisted_driver_evidence_must_reference_the_answers_own_evidence() -> (
    None
):
    """CHAOS-3660 §8(d). ``ranked_drivers[].evidence_ref_ids`` point into
    THIS answer's own ``evidence[]`` array -- enforced as a real
    constraint by ``DevAnswer.validate_answer_invariants``, the same way
    claims/conflicts/metrics already are, not just a naming convention.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["graph_assisted"] = {
        "schema_version": "dev_answer_graph_assistance.v1",
        "state": "enabled",
        "as_of": payload["as_of"],
        "ranked_drivers": [
            {"rank": 1, "contribution": 0.5, "evidence_ref_ids": ["ev_unknown"]}
        ],
    }
    with pytest.raises(ValidationError, match="graph-assisted driver"):
        DevAnswer.model_validate(payload)

    payload["graph_assisted"]["ranked_drivers"][0]["evidence_ref_ids"] = [
        payload["evidence"][0]["evidence_ref_id"]
    ]
    answer = DevAnswer.model_validate(payload)
    assert answer.graph_assisted is not None
    assert answer.graph_assisted.ranked_drivers[0].rank == 1


def test_graph_state_stream_event_carries_the_graph_assistance_object() -> None:
    """CHAOS-3660 §8(c). ``graph.state`` requires its own payload (like
    every other interior event) and rejects payloads meant for a
    different event type, via the same ``allowed``/``required_payload``
    machinery every existing event type already goes through.
    """
    payload = deepcopy(positive_fixtures()["dev_stream_event.v1"])
    payload["event"] = "graph.state"
    with pytest.raises(ValidationError, match="graph.state requires graph_state"):
        DevStreamEvent.model_validate(payload)

    graph_assistance = {
        "schema_version": "dev_answer_graph_assistance.v1",
        "state": "stale",
        "as_of": payload["occurred_at"],
    }
    payload["graph_state"] = graph_assistance
    event = DevStreamEvent.model_validate(payload)
    assert event.graph_state == DevAnswerGraphAssistance.model_validate(
        graph_assistance
    )

    payload["progress"] = "resolving_scope"
    with pytest.raises(ValidationError, match="unexpected payloads"):
        DevStreamEvent.model_validate(payload)


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


def test_ask_dev_graph_routing_defaults_false_on_every_existing_capabilities_payload() -> (
    None
):
    """CHAOS-3660 §8(a). The canonical fixture predates this flag and never
    sets it -- pinning the default explicitly rather than leaving the
    backward-compatibility proof implicit in the parametrized fixture pass.
    """
    capabilities = DevCapabilities.model_validate(
        positive_fixtures()["dev_capabilities.v1"]
    )
    assert capabilities.ask_dev_graph_routing is False


def test_neutral_message_scope_without_surface_context_remains_valid() -> None:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["scope"]["direct_scope"] = "organization"
    payload["scope"]["repositories"] = []
    payload["scope"]["surface_context"] = None

    request = DevMessageRequest.model_validate(payload)

    assert request.scope.surface_context is None


def test_client_contract_version_is_absent_by_default_and_accepted_when_declared() -> (
    None
):
    """CHAOS-3660 §8(k). Request-direction additive: an old client's
    request (the canonical fixture, which never sets this) validates
    unchanged, and a client that DOES declare a version round-trips it.
    """
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    assert "client_contract_version" not in payload
    request = DevMessageRequest.model_validate(payload)
    assert request.client_contract_version is None

    payload["client_contract_version"] = "dev_stream_event.v2"
    declared = DevMessageRequest.model_validate(payload)
    assert declared.client_contract_version == "dev_stream_event.v2"


def test_feedback_reasons_accepts_the_six_new_additive_members() -> None:
    """CHAOS-3660 §8(f)/(j). None of the 6 new reasons were folded onto the
    pre-existing, narrower ``wrong_scope`` -- each validates as its own
    sibling value.
    """
    payload = deepcopy(positive_fixtures()["dev_feedback.v1"])
    for reason in (
        "wrong_subject",
        "wrong_cohort",
        "wrong_driver",
        "unsafe_certainty",
        "other",
    ):
        feedback = DevFeedback.model_validate({**payload, "reasons": [reason]})
        assert feedback.reasons == [reason]


def test_feedback_unspecified_reason_must_stand_alone() -> None:
    """CHAOS-3660 §8(f)/(j). 'unspecified' is exactly ["unspecified"] or
    contains no "unspecified" at all -- the constraint the server actually
    enforces at runtime (pydantic), asserted here directly.

    The exported JSON Schema's ``reasons`` property carries the SAME
    constraint as a real ``oneOf``/``not``/``contains`` combinator (see
    ``_DEV_FEEDBACK_REASONS_JSON_SCHEMA_EXTRA`` in contracts.py), so a
    client validating locally catches the same violation -- hand-verified
    during implementation against a real Draft 2020-12 validator
    (``jsonschema==4.26.0``, not a project dependency, so not re-asserted
    here as a permanent test) over exactly these cases:
    ``["unspecified"]`` valid, ``["unclear", "wrong_subject"]`` valid,
    ``["unclear", "unspecified"]`` and ``["unspecified", "unclear"]``
    invalid, ``[]`` invalid (``minItems``).
    """
    payload = deepcopy(positive_fixtures()["dev_feedback.v1"])

    alone = DevFeedback.model_validate({**payload, "reasons": ["unspecified"]})
    assert alone.reasons == ["unspecified"]

    with pytest.raises(ValidationError, match="only reason"):
        DevFeedback.model_validate({**payload, "reasons": ["unclear", "unspecified"]})
    with pytest.raises(ValidationError, match="only reason"):
        DevFeedback.model_validate({**payload, "reasons": ["unspecified", "unclear"]})


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


def _live_producer_team_scope() -> dict[str, object]:
    """Re-derive the committed TEAM scope straight from the producer.

    Deliberately does NOT go through ``contract_fixtures``: this is the
    other half of a differential oracle, so it has to call
    ``ScopeResolutionService.committed_resolution_for`` itself. If the
    fixture module ever stops producing its golden from the producer (a
    hand-edit, a copy-paste of the JSON), the comparison below breaks.
    """

    base_scope = DevScope.model_validate(
        {
            "schema_version": "dev_scope.v1",
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "team_ids": [],
            "time_range": {
                "start": "2026-06-28T00:00:00Z",
                "end": "2026-07-28T00:00:00Z",
                "timezone": "America/Los_Angeles",
            },
            "comparison_range": {
                "start": "2026-05-29T00:00:00Z",
                "end": "2026-06-28T00:00:00Z",
                "timezone": "America/Los_Angeles",
            },
            "surface_context": None,
        }
    )
    service = ScopeResolutionService.__new__(ScopeResolutionService)
    resolution = service.committed_resolution_for(
        AuthorizedEntity(EntityKind.TEAM, TEAM_ID, TEAM_LABEL),
        org_id="org_fullchaos",
        base_scope=base_scope,
        resolved_at=datetime.fromisoformat("2026-07-28T12:00:00+00:00"),
    )
    assert resolution.resolved_scope is not None
    return resolution.resolved_scope.model_dump(mode="json")


def test_team_scope_golden_is_exported_and_matches_the_live_producer() -> None:
    """CHAOS-3338: the shipped team golden is producer output, not JSON.

    Before this, ``contracts/ask-dev/v1`` shipped no positive example of a
    TEAM ``DevScope`` at all, even though CHAOS-3301 made TEAM a real
    committed direct scope -- so ``dev-health-web``'s ``validateScope``
    team arm had nothing to verify against but a live call into this same
    producer.
    """

    artifacts = expected_artifacts()
    path = "examples/positive/dev_scope.v1.team_direct_scope.json"
    assert path in artifacts, "team-scope golden is not in the exported artifact set"

    payload = json.loads(artifacts[path])
    assert payload["direct_scope"] == "team"
    DevScope.model_validate(payload)
    assert payload == _live_producer_team_scope()


def test_team_scope_golden_is_listed_in_the_manifest() -> None:
    """A file nobody can find from the manifest is not shipped, only present."""

    manifest = json.loads(expected_artifacts()["manifest.json"])
    entry = next(
        item
        for item in manifest["contracts"]
        if item["schema_version"] == "dev_scope.v1"
    )
    variants = {case["case"]: case["path"] for case in entry["positive_variants"]}
    assert variants == {
        "team_direct_scope": "examples/positive/dev_scope.v1.team_direct_scope.json"
    }


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("team_scope_with_repository_list", "cannot carry a repository list"),
        (
            "team_scope_without_matching_team_id",
            "team_ids to name exactly that team",
        ),
        (
            "team_scope_entity_ref_is_not_a_team",
            "direct entity scope requires one matching entity",
        ),
    ],
)
def test_team_scope_negative_examples_fail_for_their_named_reason(
    case: str, reason: str
) -> None:
    """The generic negative sweep only asserts *some* error; each team
    mutation has to be rejected by the clause its label names, or the
    example documents an invariant it does not actually exercise."""

    payload = dict(negative_fixtures()["dev_scope.v1"])[case]
    with pytest.raises(ValidationError, match=reason):
        DevScope.model_validate(payload)


def test_team_resolution_negative_example_fails_for_its_named_reason() -> None:
    payload = dict(negative_fixtures()["dev_scope_resolution.v1"])[
        "team_resolution_scope_with_repository_list"
    ]
    with pytest.raises(ValidationError, match="cannot carry a repository list"):
        DevScopeResolution.model_validate(payload)


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


def test_published_denylist_full_set_matches_the_live_ops_side_union() -> None:
    """CHAOS-3660 §8(h). Root-cause fix for a real finding (lane-W): the
    published artifact's ``reason_codes``/``completion_states``/
    ``extra_tokens`` keys were a hand-picked SUBSET of
    ``INTERNAL_TOKEN_DENYLIST`` that could silently drift from it (a new
    denylisted enum registered on the ops side had no path onto the wire).
    ``full_denylist`` is computed directly from the live union, so this
    test is really pinning that the wiring is correct -- and, concretely,
    that this PR's own two new enums (``CohortDiscoveryFamily``,
    ``PacketLimitationKind``) actually made it through both sites, not
    just one.
    """
    denylist = json.loads(
        expected_artifacts()["vocabulary/internal_prose_denylist.v1.json"]
    )
    assert set(denylist["full_denylist"]) == INTERNAL_TOKEN_DENYLIST
    for expected_token in ("team_pressure", "project_capacity", "missing_source"):
        assert expected_token in denylist["full_denylist"]


def test_source_health_labels_cover_every_known_required_source_producer() -> None:
    """CHAOS-3660 §8(g). ``SOURCE_HEALTH_LABELS`` must have an entry for
    every id ``DevCoverage.{unavailable,stale,degraded}_required_sources``
    can actually carry -- derived here from the two live enums that feed
    it (``ToolID``, ``SourceClass``) plus the one hardcoded literal
    (``"tool_results"``), not a hand list independently re-typed.

    Honesty about what this proves: it is a totality check against every
    producer identified by reading ``orchestrator.py``'s
    ``_coverage_from_tool_results``/``_coverage_with_plan_sources``/
    ``_budget_exhausted_answer`` -- the only three call sites in the
    codebase that assign into these three ``DevCoverage`` fields (verified
    by grep, not assumed). It cannot dynamically catch a hypothetical
    FUTURE fourth producer emitting a still-different literal; the
    exact-equality assertion below at least ensures a member removed from
    either enum (or the union going stale some other way) fails loudly
    here rather than silently leaving a dead label.
    """
    expected = (
        frozenset(member.value for member in ToolID)
        | frozenset(member.value for member in SourceClass)
        | {"tool_results"}
    )
    assert set(SOURCE_HEALTH_LABELS) == expected

    published = json.loads(
        expected_artifacts()["vocabulary/source_health_labels.v1.json"]
    )
    assert set(published["labels"]) == expected
    for source_id, label in SOURCE_HEALTH_LABELS.items():
        assert label, f"{source_id} has an empty label"


def test_published_no_answer_vocabulary_matches_the_live_policy_tables() -> None:
    """CHAOS-3471. The entire user-visible text of a v1 ``DevError`` for a
    ``dev_answer.v2`` no-answer outcome is the server-owned
    ``CANONICAL_NO_ANSWER_COPY`` / ``CANONICAL_NO_ANSWER_REMEDIATION``
    tables plus the ``(code, retryable)`` pair
    ``compat.NO_ANSWER_ERROR_CODES`` attaches (``compat._project_error``).
    Before this artifact, web hand-mirrored all three in
    ``tests/mocks/devScenario.ts``'s ``NO_ANSWER_OUTCOMES`` -- a copy that
    proves the UI renders ``safe_message`` verbatim but cannot detect an
    ops-side reword. This test pins that the published artifact is exactly
    the live tables, keyed by every member of ``NO_ANSWER_OUTCOMES`` (not a
    hand-picked subset), so a future outcome addition or a copy/remediation
    edit either lands in the artifact automatically or fails this test.
    """
    published = json.loads(
        expected_artifacts()["vocabulary/no_answer_vocabulary.v1.json"]
    )
    assert published["schema_version"] == "ask_dev_no_answer_vocabulary.v1"
    assert set(published["outcomes"]) == NO_ANSWER_OUTCOMES
    for outcome in NO_ANSWER_OUTCOMES:
        code, retryable = NO_ANSWER_ERROR_CODES[outcome]
        assert published["outcomes"][outcome] == {
            "safe_message": CANONICAL_NO_ANSWER_COPY[outcome],
            "remediation": list(CANONICAL_NO_ANSWER_REMEDIATION[outcome]),
            "code": code,
            "retryable": retryable,
        }


def test_no_answer_vocabulary_is_listed_in_the_manifest() -> None:
    artifacts = expected_artifacts()
    manifest = json.loads(artifacts["manifest.json"])
    entry = next(
        item
        for item in manifest["vocabulary"]
        if item["case"] == "no_answer_vocabulary"
    )
    assert entry["path"] == "vocabulary/no_answer_vocabulary.v1.json"
    assert (
        entry["sha256"]
        == hashlib.sha256(
            artifacts["vocabulary/no_answer_vocabulary.v1.json"].encode("utf-8")
        ).hexdigest()
    )


@pytest.mark.parametrize("schema_version", CONTRACT_MODELS)
def test_every_contract_requires_its_explicit_version(schema_version: str) -> None:
    payload = deepcopy(positive_fixtures()[schema_version])
    payload.pop("schema_version")
    with pytest.raises(ValidationError, match="schema_version"):
        CONTRACT_MODELS[schema_version].model_validate(payload)
