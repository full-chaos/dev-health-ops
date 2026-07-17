"""Unit tests for external_ingest.normalize (CHAOS-2697).

Pure — no services, no mocks of our own modules: ``normalize_batch`` runs the
real ``validate_records`` (CC17) and the real pydantic kind models, so these
tests pin the actual reject/accept decisions the worker will make.
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    RecordEnvelope,
)
from dev_health_ops.external_ingest.normalize import (
    ALLOWED_KINDS_BY_SYSTEM,
    normalize_batch,
)
from dev_health_ops.models.operational import (
    OperationalIncident,
    canonical_operational_id,
)

ORG = "org-1"
SOURCE_ID = uuid.uuid4()
INGESTION_ID = uuid.uuid4()
INSTANCE = "acme/api"

#: One minimal VALID payload per kind (github vocabulary, scoped to INSTANCE).
VALID_PAYLOADS: dict[str, dict] = {
    "repository.v1": {"externalId": INSTANCE, "sourceSystem": "github"},
    "identity.v1": {"canonicalId": "u1", "updatedAt": "2026-07-01T00:00:00Z"},
    "team.v1": {"id": "t1", "name": "Team One", "updatedAt": "2026-07-01T00:00:00Z"},
    "work_item.v1": {
        "externalKey": "42",
        "provider": "github",
        "title": "Fix it",
        "status": "todo",
        "createdAt": "2026-07-01T00:00:00Z",
    },
    "work_item_transition.v1": {
        "externalKey": "42",
        "provider": "github",
        "occurredAt": "2026-07-01T01:00:00Z",
        "fromStatus": "todo",
        "toStatus": "in_progress",
    },
    "work_item_dependency.v1": {
        "sourceExternalKey": "42",
        "targetExternalKey": "43",
        "relationshipType": "blocks",
    },
    "pull_request.v1": {
        "repositoryExternalId": INSTANCE,
        "number": 7,
        "state": "merged",
        "createdAt": "2026-07-01T00:00:00Z",
    },
    "review.v1": {
        "repositoryExternalId": INSTANCE,
        "pullRequestNumber": 7,
        "reviewId": "r1",
        "reviewer": "alice",
        "state": "APPROVED",
        "submittedAt": "2026-07-01T02:00:00Z",
    },
    "commit.v1": {
        "repositoryExternalId": INSTANCE,
        "hash": "abc1234",
        "authorWhen": "2026-07-01T00:00:00Z",
    },
}

#: kind -> (NormalizedBatch list attribute, record_index_by_kind key)
_EXPECTED_PLACEMENT = {
    "repository.v1": ("repositories", "repository"),
    "identity.v1": ("identities", "identity"),
    "team.v1": ("teams", "team"),
    "work_item.v1": ("work_items", "work_item"),
    "work_item_transition.v1": ("work_item_transitions", "work_item_transition"),
    "work_item_dependency.v1": ("work_item_dependencies", "work_item_dependency"),
    "pull_request.v1": ("pull_requests", "pull_request"),
    "review.v1": ("reviews", "review"),
    "commit.v1": ("commits", "commit"),
}


def _rec(
    kind: str, payload: dict | None = None, external_id: str = "x-1"
) -> RecordEnvelope:
    return RecordEnvelope(
        kind=kind,
        external_id=external_id,
        payload=VALID_PAYLOADS[kind] if payload is None else payload,
    )


def _normalize(records, *, source_system="github", source_instance=INSTANCE):
    return normalize_batch(
        org_id=ORG,
        source_id=SOURCE_ID,
        source_system=source_system,
        source_instance=source_instance,
        ingestion_id=INGESTION_ID,
        records=records,
    )


class TestHappyPath:
    def test_all_nine_kinds_accepted_and_placed(self) -> None:
        records = [_rec(kind) for kind in VALID_PAYLOADS]
        result = _normalize(records)

        assert result.items_received == 9
        assert result.items_rejected == 0
        assert result.items_accepted == 9
        assert result.record_counts == {kind: 1 for kind in VALID_PAYLOADS}
        for index, kind in enumerate(VALID_PAYLOADS):
            attr, index_key = _EXPECTED_PLACEMENT[kind]
            assert len(getattr(result.batch, attr)) == 1, kind
            assert result.batch.record_index_by_kind[index_key] == [index], kind

    def test_dict_kinds_dump_snake_case_keys(self) -> None:
        result = _normalize([_rec("pull_request.v1")])
        (row,) = result.batch.pull_requests
        assert isinstance(row, dict)
        # snake_case field names, matching what sinks.py's row builders read.
        assert row["repository_external_id"] == INSTANCE
        assert row["number"] == 7
        assert "repositoryExternalId" not in row

    def test_work_item_family_passes_model_instances(self) -> None:
        result = _normalize([_rec("work_item.v1")])
        (item,) = result.batch.work_items
        assert item.external_key == "42"  # attribute access (sinks duck-types)

    def test_batch_identity_fields_stamped(self) -> None:
        result = _normalize([_rec("repository.v1")])
        assert result.batch.org_id == ORG
        assert result.batch.source_id == SOURCE_ID
        assert result.batch.ingestion_id == INGESTION_ID
        assert result.batch.source_system == "github"
        assert result.batch.source_instance == INSTANCE

    def test_operational_incident_uses_canonical_identity_and_source_provenance(
        self,
    ) -> None:
        result = _normalize(
            [
                RecordEnvelope(
                    kind="operational_incident.v1",
                    external_id="inc-1",
                    payload={
                        "externalId": "inc-1",
                        "sourceVersionAt": "2026-07-01T00:00:00Z",
                        "title": "Payments latency",
                        "serviceExternalId": "payments",
                    },
                )
            ],
            source_system="pagerduty",
            source_instance="pd-example",
        )

        assert result.items_rejected == 0
        (incident,) = result.batch.operational_incidents
        assert isinstance(incident, OperationalIncident)
        assert incident.source_id == SOURCE_ID
        assert incident.id == canonical_operational_id(
            ORG, "pagerduty", "pd-example", "operational_incident", "inc-1"
        )

    def test_mapping_without_repository_identity_is_rejected(self) -> None:
        result = _normalize(
            [
                RecordEnvelope(
                    kind="service_repository_mapping.v1",
                    external_id="mapping-1",
                    payload={
                        "externalId": "mapping-1",
                        "sourceVersionAt": "2026-07-01T00:00:00Z",
                        "serviceExternalId": "payments",
                    },
                )
            ],
            source_system="pagerduty",
            source_instance="pd-example",
        )

        assert result.items_accepted == 0
        assert result.rejections[0].code == "repository_identity_required"


class TestShapeRejections:
    def test_invalid_payload_rejected_with_validate_py_code(self) -> None:
        records = [
            _rec("repository.v1"),
            _rec("work_item.v1", payload={"externalKey": "42"}),  # missing required
        ]
        result = _normalize(records)
        assert result.items_accepted == 1
        assert result.items_rejected == 1
        (rejection,) = result.rejections
        assert rejection.index == 1
        assert rejection.kind == "work_item.v1"
        assert rejection.code == "missing_required_field"
        assert rejection.external_id == "x-1"
        assert rejection.path is not None and "records[1].payload" in rejection.path

    def test_multiple_field_errors_collapse_to_one_rejection(self) -> None:
        # Three missing required fields -> validate_records emits three
        # items, but the (ingestion_id, record_index) unique constraint
        # allows exactly one persisted rejection per record.
        result = _normalize([_rec("work_item.v1", payload={})])
        assert result.items_rejected == 1
        assert result.rejections[0].code == "missing_required_field"

    def test_unknown_kind_rejected(self) -> None:
        record = RecordEnvelope(kind="nonsense.v9", external_id="x", payload={})
        result = _normalize([record])
        (rejection,) = result.rejections
        assert rejection.code == "unknown_kind"

    def test_rejections_preserve_original_indices(self) -> None:
        records = [
            _rec("work_item.v1", payload={}),  # 0: shape reject
            _rec("commit.v1"),  # 1: accepted
            _rec("work_item.v1", payload={}),  # 2: shape reject
        ]
        result = _normalize(records)
        assert [r.index for r in result.rejections] == [0, 2]
        assert result.batch.record_index_by_kind["commit"] == [1]


class TestKindSystemMatrix:
    @pytest.mark.parametrize(
        "kind", ["repository.v1", "pull_request.v1", "review.v1", "commit.v1"]
    )
    def test_git_family_rejected_for_jira_and_linear(self, kind: str) -> None:
        for system in ("jira", "linear"):
            result = _normalize([_rec(kind)], source_system=system)
            (rejection,) = result.rejections
            assert rejection.code == "unsupported_kind_for_system", (system, kind)

    @pytest.mark.parametrize(
        "kind",
        ["work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"],
    )
    def test_work_item_family_rejected_for_custom(self, kind: str) -> None:
        result = _normalize([_rec(kind)], source_system="custom")
        (rejection,) = result.rejections
        assert rejection.code == "unsupported_kind_for_system"
        assert rejection.path == "records[0].kind"

    def test_org_scoped_kinds_accepted_everywhere(self) -> None:
        for system in ALLOWED_KINDS_BY_SYSTEM:
            result = _normalize(
                [_rec("team.v1"), _rec("identity.v1")], source_system=system
            )
            assert result.items_rejected == 0, system

    def test_matrix_matches_master_spec_cc6(self) -> None:
        git = {"repository.v1", "pull_request.v1", "review.v1", "commit.v1"}
        wi = {"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
        org = {"team.v1", "identity.v1"}
        operational = {
            "operational_service.v1",
            "operational_incident.v1",
            "operational_alert.v1",
            "incident_timeline_event.v1",
            "incident_note.v1",
            "incident_responder.v1",
            "escalation_policy.v1",
            "on_call_schedule.v1",
            "on_call_assignment.v1",
            "operational_team.v1",
            "operational_user.v1",
            "service_repository_mapping.v1",
        }
        assert ALLOWED_KINDS_BY_SYSTEM["github"] == git | wi | org | operational
        assert ALLOWED_KINDS_BY_SYSTEM["gitlab"] == git | wi | org | operational
        assert ALLOWED_KINDS_BY_SYSTEM["jira"] == wi | org
        assert ALLOWED_KINDS_BY_SYSTEM["linear"] == wi | org
        assert ALLOWED_KINDS_BY_SYSTEM["custom"] == git | org | operational
        assert ALLOWED_KINDS_BY_SYSTEM["pagerduty"] == org | operational
        assert ALLOWED_KINDS_BY_SYSTEM["atlassian"] == org | operational
        # Every kind the wire schema knows is reachable through some system.
        reachable = frozenset().union(*ALLOWED_KINDS_BY_SYSTEM.values())
        assert reachable == set(RECORD_KIND_MODELS)


class TestInstanceScope:
    def test_out_of_instance_git_record_rejected(self) -> None:
        payload = dict(VALID_PAYLOADS["commit.v1"], repositoryExternalId="other/repo")
        result = _normalize([_rec("commit.v1", payload=payload)])
        (rejection,) = result.rejections
        assert rejection.code == "record_outside_source_instance"

    def test_out_of_instance_repository_rejected_via_own_external_id(self) -> None:
        payload = {"externalId": "other/repo", "sourceSystem": "github"}
        result = _normalize([_rec("repository.v1", payload=payload)])
        (rejection,) = result.rejections
        assert rejection.code == "record_outside_source_instance"

    def test_case_variant_instance_accepted(self) -> None:
        # GitHub identifiers are case-insensitive and derive_repo_uuid
        # lower-cases its seed: a case variant is the SAME repo, not an
        # out-of-scope one (module docstring rationale).
        payload = dict(VALID_PAYLOADS["commit.v1"], repositoryExternalId="Acme/API")
        result = _normalize([_rec("commit.v1", payload=payload)])
        assert result.items_rejected == 0

    def test_work_item_family_not_instance_scoped(self) -> None:
        # CC6 scopes only the git family; a work item referencing another
        # repo is allowed (repo linkage is optional metadata there).
        payload = dict(
            VALID_PAYLOADS["work_item.v1"], repositoryExternalId="other/repo"
        )
        result = _normalize([_rec("work_item.v1", payload=payload)])
        assert result.items_rejected == 0

    def test_jira_batches_never_instance_scoped(self) -> None:
        result = _normalize(
            [_rec("work_item.v1")], source_system="jira", source_instance="ABC"
        )
        assert result.items_rejected == 0
