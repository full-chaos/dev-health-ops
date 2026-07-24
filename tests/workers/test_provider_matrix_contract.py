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
    ProviderUnitRouteError,
    ProviderUnitRouteSwitches,
)

CONTRACT = Path(__file__).parents[2] / "contracts/provider-matrix/v1/matrix.json"

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


def test_producer_canary_scope_is_a_subset_of_route_ready_descriptors(
    matrix: dict[str, Any],
) -> None:
    """CUT-08 acceptance: 'No producer can route a scope whose Go descriptor is
    absent or disabled.'

    This is the one regression test binding both sides. It fails if a future
    change widens ``is_canary_scope`` before the corresponding Go descriptor
    becomes route-ready — the exact ordering that would make TRD §5.3's
    ``route_disabled`` hazard reachable.
    """

    route_ready = {
        (pair["provider"], pair["dataset"])
        for pair in matrix["pairs"]
        if pair["route_ready"]
    }
    canary = {
        (provider, dataset)
        for provider, dataset in _python_pairs()
        if ProviderUnitRouteSwitches.is_canary_scope(provider, dataset)
    }

    assert canary, "the producer gate must recognise at least one scope"
    assert canary <= route_ready, {
        "producer_scopes_without_a_ready_go_descriptor": sorted(canary - route_ready)
    }


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

    # With every allowed switch off, nothing routes.
    closed = ProviderUnitRouteSwitches()
    assert not any(
        closed.routes_to_river(provider, dataset)
        for provider, dataset in _python_pairs()
    )


def test_incomplete_route_switches_remain_forbidden(
    matrix: dict[str, Any],
) -> None:
    """The linear/jira scaffolding switches must stay unusable for as long as
    their Go descriptors are not route-ready."""

    scaffolding = {
        "linear_work_items": ("linear", "work-items"),
        "jira_work_items": ("jira", "work-items"),
        "jira_incidents": ("jira", "incidents"),
    }
    ready = {
        (pair["provider"], pair["dataset"])
        for pair in matrix["pairs"]
        if pair["route_ready"]
    }
    for field, scope in scaffolding.items():
        assert scope not in ready, f"{scope} became route-ready; unblock {field}"
        with pytest.raises(ProviderUnitRouteError):
            ProviderUnitRouteSwitches(**{field: True}).require_complete_routes()


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
