from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.investigation_contract import QuestionFamilyID
from dev_health_ops.context_fabric.graph_arm.cohort_discovery import discover_cohort
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from scripts.acceptance.seed_ask_dev_graph_acceptance import (
    _primary_org_id,
    build_batch,
)


def test_primary_org_uses_the_canonical_world_id_derivation() -> None:
    assert (
        _primary_org_id(
            {
                "master_seed": 3219000,
                "orgs": [
                    {"alias": "primary", "id_seed": "ask-dev-world.v1:org:primary"}
                ],
            }
        )
        == "76dd314d-2b09-5f05-a75b-a1b4b35536d4"
    )


def _rows() -> list[dict[str, object]]:
    return [
        {
            "canonical_id": "11111111-1111-4111-8111-111111111111",
            "label": "Core",
            "work_in_progress": 15,
            "cycle_time_p90_days": 3.875,
            "work_in_progress_cohort_median": 7,
            "cycle_time_p90_days_cohort_median": 3.145833333333333,
        },
        {
            "canonical_id": "22222222-2222-4222-8222-222222222222",
            "label": "Growth",
            "work_in_progress": 18,
            "cycle_time_p90_days": 0.75,
            "work_in_progress_cohort_median": 7,
            "cycle_time_p90_days_cohort_median": 3.145833333333333,
        },
        {
            "canonical_id": "33333333-3333-4333-8333-333333333333",
            "label": "Platform",
            "work_in_progress": 22,
            "cycle_time_p90_days": 5.375,
            "work_in_progress_cohort_median": 7,
            "cycle_time_p90_days_cohort_median": 3.145833333333333,
        },
    ]


def test_batch_preserves_canonical_values_and_real_team_ids() -> None:
    batch = build_batch(
        org_id="76dd314d-2b09-5f05-a75b-a1b4b35536d4",
        rows=_rows(),
        observed_at=datetime(2026, 8, 10, tzinfo=UTC),
    )

    assert [entity.display_label for entity in batch.entities] == [
        "Core",
        "Growth",
        "Platform",
    ]
    platform = [
        observation
        for observation in batch.observations
        if observation.subjects[0].canonical_id
        == "33333333-3333-4333-8333-333333333333"
    ]
    assert {item.attributes["measurement_value"] for item in platform} == {22.0, 5.375}
    assert {item.attributes["measurement_cohort_median"] for item in platform} == {
        7.0,
        3.145833333333333,
    }
    assert {item.attributes["source_evidence_entity_id"] for item in platform} == {
        "33333333-3333-4333-8333-333333333333"
    }
    assert all("source_evidence_handle" not in item.attributes for item in platform)
    assert all("source_evidence_id" not in item.attributes for item in platform)


def test_batch_is_discoverable_without_fabricating_source_handles() -> None:
    observed_at = datetime(2026, 8, 10, tzinfo=UTC)
    batch = build_batch(
        org_id="76dd314d-2b09-5f05-a75b-a1b4b35536d4",
        rows=_rows(),
        observed_at=observed_at,
    )
    projection = build_projection(batch)

    discovery = discover_cohort(
        question_family=QuestionFamilyID.STRUGGLING_TEAMS,
        nodes=projection.nodes,
        edges=projection.edges,
        authorized_entity_ids=frozenset(str(row["canonical_id"]) for row in _rows()),
        as_of=observed_at,
    )

    assert discovery.is_comparable is True
    assert [member.canonical_id for member in discovery.proposal.members] == [
        "11111111-1111-4111-8111-111111111111",
        "33333333-3333-4333-8333-333333333333",
    ]


def test_batch_fails_when_the_canonical_data_cannot_corroborate_pressure() -> None:
    rows = _rows()
    rows[0]["cycle_time_p90_days"] = 3.0
    rows[2]["cycle_time_p90_days"] = 3.0
    with pytest.raises(RuntimeError, match="two canonical pressure signals"):
        build_batch(
            org_id="76dd314d-2b09-5f05-a75b-a1b4b35536d4",
            rows=rows,
            observed_at=datetime(2026, 8, 10, tzinfo=UTC),
        )
