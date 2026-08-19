"""CUT-08: the Go and Python provider matrices must be identical, and no
producer may route a scope whose Go descriptor is absent or disabled.

Both languages verify against the same checked-in artifact
(``contracts/provider-matrix/v1/matrix.json``). The Go side regenerates it from
the capability registry and the canonical ``CompleteRouteSwitches.Descriptor``
(``internal/providersync/capability_matrix_test.go``); this module regenerates
the Python side from ``dev_health_ops.sync.datasets`` and binds the producer
gate to it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.sync.datasets import (
    _PROVIDER_SUPPORTED_DATASETS,
    get_dataset_spec,
)
from dev_health_ops.workers.provider_unit_route import (
    ProviderUnitRouteSwitches,
)

CONTRACT = Path(__file__).parents[2] / "contracts/provider-matrix/v1/matrix.json"
TRANSITIONAL_INVENTORY = (
    Path(__file__).parents[2] / "contracts/jobs/v1/transitional-inventory.json"
)

# Independently asserted census from the CUT-08 audit. Hard-coding it means a
# silent registry deletion cannot be laundered through a contract regeneration.
EXPECTED_PAIR_COUNTS = {
    "github": 17,
    "gitlab": 19,
    "jira": 6,
    "launchdarkly": 1,
    "linear": 5,
    "pagerduty": 11,
}

# This is an acceptance census, not a projection of the generated registry.
# It catches deleting a GitHub alias from both registry and matrix generation.
EXPECTED_ROUTE_READY_COUNTS = {
    "github": 17,
    "gitlab": 19,
    "jira": 6,
    "launchdarkly": 1,
    "linear": 5,
    "pagerduty": 11,
}


@pytest.fixture(scope="module")
def matrix() -> dict[str, Any]:
    return json.loads(CONTRACT.read_text())


def _python_pairs() -> set[tuple[str, str]]:
    return {
        (provider, dataset)
        for provider, datasets in _PROVIDER_SUPPORTED_DATASETS.items()
        for dataset in datasets
    }


def _contract_pairs(matrix: dict[str, Any]) -> set[tuple[str, str]]:
    return {(pair["provider"], pair["dataset"]) for pair in matrix["pairs"]}


def _inventory_row_by_identity(
    inventory: dict[str, Any],
    *,
    surface_class: str,
    surface: str,
    target_kind_id: str,
) -> dict[str, Any]:
    """Find one inventory row without coupling this matrix test to source lines.

    The transitional-inventory checker owns source-line discovery. This test
    owns the factual capability wording on the two provider-unit surfaces,
    whose stable identities are their class, declared surface, and target kind.
    """

    matches = [
        row
        for row in inventory["rows"]
        if row["class"] == surface_class
        and row["surface"] == surface
        and row["target_kind_id"] == target_kind_id
    ]
    assert len(matches) == 1, (
        "expected exactly one transitional inventory row for "
        f"{surface_class}/{surface}/{target_kind_id}, got {len(matches)}"
    )
    return matches[0]


def test_contract_shape_is_frozen(matrix: dict[str, Any]) -> None:
    assert matrix["schema_version"] == 1
    assert sorted(matrix["providers"]) == sorted(EXPECTED_PAIR_COUNTS)
    assert len(matrix["pairs"]) == sum(EXPECTED_PAIR_COUNTS.values()) == 59


def test_go_and_python_matrix_sets_are_identical(matrix: dict[str, Any]) -> None:
    """CUT-08 acceptance: 'The Go and Python matrix sets are identical.'"""

    python_pairs = _python_pairs()
    contract_pairs = _contract_pairs(matrix)

    assert python_pairs == contract_pairs, {
        "missing_from_contract": sorted(python_pairs - contract_pairs),
        "missing_from_python": sorted(contract_pairs - python_pairs),
    }


def test_every_provider_pair_count_matches_the_audit(
    matrix: dict[str, Any],
) -> None:
    counts: dict[str, int] = {}
    for pair in matrix["pairs"]:
        counts[pair["provider"]] = counts.get(pair["provider"], 0) + 1
    assert counts == EXPECTED_PAIR_COUNTS


def test_route_ready_census_matches_aggregate_acceptance(
    matrix: dict[str, Any],
) -> None:
    counts: dict[str, int] = {provider: 0 for provider in EXPECTED_PAIR_COUNTS}
    for pair in matrix["pairs"]:
        if pair["route_ready"]:
            counts[pair["provider"]] += 1
    assert counts == EXPECTED_ROUTE_READY_COUNTS
    assert counts["github"] == EXPECTED_PAIR_COUNTS["github"] == 17
    assert counts == EXPECTED_PAIR_COUNTS
    assert sum(counts.values()) == 59


def test_transitional_inventory_route_readiness_notes_follow_matrix(
    matrix: dict[str, Any],
) -> None:
    """The inventory remains Python-compatible/deployment-inactive, but its
    factual note must track the generated capability matrix rather than retain
    the old "only LaunchDarkly" claim.

    This derives both numbers from the checked-in artifact. A later intentional
    matrix update therefore makes the inventory wording fail until it is
    refreshed, while the independent 59/59 acceptance census above prevents a
    coordinated deletion from laundering either number.
    """

    pairs = matrix["pairs"]
    ready = sum(pair["route_ready"] for pair in pairs)
    github_pairs = [pair for pair in pairs if pair["provider"] == "github"]
    github_ready = sum(pair["route_ready"] for pair in github_pairs)
    summary = f"{ready}/{len(pairs)}"
    github_summary = f"GitHub {github_ready}/{len(github_pairs)}"

    inventory = json.loads(TRANSITIONAL_INVENTORY.read_text())
    for row in (
        _inventory_row_by_identity(
            inventory,
            surface_class="celery_task",
            surface="run_sync_unit",
            target_kind_id="kind:sync.provider_unit",
        ),
        _inventory_row_by_identity(
            inventory,
            surface_class="registry_kind",
            surface="sync.provider_unit",
            target_kind_id="kind:sync.provider_unit",
        ),
    ):
        notes = row["notes"]
        assert summary in notes
        assert github_summary in notes
        assert "only launchdarkly" not in notes.lower()
        # Matrix readiness does not transfer runtime ownership. This is the
        # no-cutover invariant the corrected prose must retain.
        assert row["current_implementation_state"] == "python_compatibility"


def test_pagerduty_is_covered_by_the_same_contract(
    matrix: dict[str, Any],
) -> None:
    """PagerDuty was the one provider entirely absent from the Go registry."""

    expected = {
        "services",
        "business-services",
        "escalation-policies",
        "schedules",
        "on-calls",
        "users",
        "teams",
        "incidents",
        "incident-alerts",
        "incident-log-entries",
        "incident-notes",
    }
    covered = {
        pair["dataset"] for pair in matrix["pairs"] if pair["provider"] == "pagerduty"
    }
    assert covered == expected
    for pair in matrix["pairs"]:
        if pair["provider"] != "pagerduty":
            continue
        assert pair["legacy_targets"] == ["operational"]
        assert pair["credential_modes"]


def test_per_pair_metadata_matches_the_python_registry(
    matrix: dict[str, Any],
) -> None:
    """Membership equality is not enough: cost class, watermark behavior,
    legacy targets, and processor flags drive claim validation and post-sync
    fan-out, so drift there is as damaging as a missing pair."""

    for pair in matrix["pairs"]:
        spec = get_dataset_spec(pair["provider"], pair["dataset"])
        assert spec is not None, (pair["provider"], pair["dataset"])
        scope = f"{pair['provider']}/{pair['dataset']}"
        assert pair["cost_class"] == spec.default_cost_class.value, scope
        assert pair["watermark"] == spec.watermark_behavior.value, scope
        assert pair["legacy_targets"] == sorted(spec.legacy_targets), scope
        assert pair["processor_flags"] == {
            name: value for name, value in spec.processor_flags.items()
        }, scope
        assert pair["python_source"].endswith(f'["{pair["provider"]}"]'), scope


def test_producer_route_readiness_matches_the_checked_in_matrix_exactly(
    matrix: dict[str, Any],
) -> None:
    """CUT-08 acceptance, generalized for CHAOS-3131: 'No producer can route a
    scope whose Go descriptor is absent or disabled.'

    Pre-CHAOS-3131 this only had to hold for one hardcoded pair
    (``is_canary_scope``). Now that ``ProviderUnitRouteSwitches.is_route_ready``
    reads the checked-in matrix directly, this is the test that binds Python's
    readiness notion to Go's: Go regenerates the same contract from
    ``CompleteRouteSwitches.Descriptor`` and fails CI on any byte-level
    divergence (``internal/providersync/capability_matrix_test.go`` ->
    ``TestProviderMatrixMatchesCheckedInContract``), so asserting Python's
    reader agrees with every row of that file transitively binds Python to
    Go's descriptor logic without a live cross-language call.
    """

    route_ready = {
        (pair["provider"], pair["dataset"])
        for pair in matrix["pairs"]
        if pair["route_ready"]
    }
    python_ready = {
        (provider, dataset)
        for provider, dataset in _python_pairs()
        if ProviderUnitRouteSwitches.is_route_ready(provider, dataset)
    }

    assert python_ready, "the producer gate must recognise at least one scope"
    assert python_ready == route_ready, {
        "python_says_ready_matrix_disagrees": sorted(python_ready - route_ready),
        "matrix_says_ready_python_disagrees": sorted(route_ready - python_ready),
    }

    # Membership equality is necessary but not sufficient: a pair that is not
    # even a member of the frozen provider/dataset universe must also read as
    # not-ready, so a typo'd or renamed pair fails closed rather than
    # silently matching nothing.
    assert not ProviderUnitRouteSwitches.is_route_ready(
        "not-a-real-provider", "not-a-real-dataset"
    )


def test_producer_gate_cannot_route_any_scope_outside_the_ready_set(
    matrix: dict[str, Any],
) -> None:
    """The env-driven gate itself, not just the static scope predicate."""

    route_ready = {
        (pair["provider"], pair["dataset"])
        for pair in matrix["pairs"]
        if pair["route_ready"]
    }
    switches = ProviderUnitRouteSwitches(launchdarkly_feature_flags=True)
    routed = {
        (provider, dataset)
        for provider, dataset in _python_pairs()
        if switches.routes_to_river(provider, dataset)
    }
    assert routed <= route_ready
    assert routed == {("launchdarkly", "feature-flags")}

    github_work_items = ProviderUnitRouteSwitches(github_work_items=True)
    github_routed = {
        (provider, dataset)
        for provider, dataset in _python_pairs()
        if github_work_items.routes_to_river(provider, dataset)
    }
    # Matrix readiness is a five-alias capability/audit surface; direct
    # siblings cannot become Python producer routes just by sharing a switch.
    assert github_routed == {("github", "work-items")}

    # With every allowed switch off, nothing routes.
    closed = ProviderUnitRouteSwitches()
    assert not any(
        closed.routes_to_river(provider, dataset)
        for provider, dataset in _python_pairs()
    )


def test_go_executor_kinds_are_bounded_and_honest(
    matrix: dict[str, Any],
) -> None:
    for pair in matrix["pairs"]:
        assert pair["go_executor"] in {"native_go", "python_compatibility", "none"}
        # TRD §10.2's bounded compatibility endpoint does not exist yet, so no
        # pair may claim it. Claiming it would be a false pass under plan §5.
        assert pair["go_executor"] != "python_compatibility", pair
        if pair["route_ready"]:
            assert pair["go_executor"] == "native_go", pair
            assert pair["route_destinations"], pair
    assert all(pair["route_ready"] for pair in matrix["pairs"])
    assert all(pair["go_executor"] == "native_go" for pair in matrix["pairs"])
