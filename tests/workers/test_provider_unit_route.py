from __future__ import annotations

import json
from collections.abc import Collection
from pathlib import Path

import pytest

from dev_health_ops.workers import provider_unit_route
from dev_health_ops.workers.provider_family_contract import WORK_ITEM_DATASETS
from dev_health_ops.workers.provider_unit_route import (
    ProviderUnitRouteError,
    is_atomic_provider_family_direct_alias,
    is_complete_github_work_item_family_claim,
    is_github_work_item_direct_alias,
    is_github_work_item_family_dataset,
    is_plannable,
    is_route_ready,
    routes_to_river,
    validate_provider_family_claim,
)


@pytest.fixture(autouse=True)
def _restore_matrix_contract_path():
    """Every test in this module may repoint the module-level matrix path at
    a fixture. Always restore the production path and drop any cached read
    afterwards so later test modules see the real checked-in contract."""

    original = provider_unit_route._MATRIX_CONTRACT_PATH
    try:
        yield
    finally:
        provider_unit_route._MATRIX_CONTRACT_PATH = original
        provider_unit_route.clear_matrix_cache()


# ---------------------------------------------------------------------------
# CHAOS-4054: routability is derived entirely from the checked-in capability
# matrix. There is no route-switch environment plane any more -- a pair
# routes to River exactly when the matrix marks it BOTH route_ready AND
# plannable.
# ---------------------------------------------------------------------------


def test_is_route_ready_reflects_the_checked_in_matrix() -> None:
    assert is_route_ready("launchdarkly", "feature-flags")
    # Same dataset name, different provider: both independently constructed
    # routes are ready in the aggregate matrix. Readiness is still exact pair
    # membership rather than a dataset-name wildcard.
    assert is_route_ready("gitlab", "feature-flags")
    assert not is_route_ready("not-a-provider", "feature-flags")
    # Case/whitespace must not matter -- callers pass live DB column values.
    assert is_route_ready(" LaunchDarkly ", "Feature-Flags")


def test_is_route_ready_fails_closed_for_unknown_pairs() -> None:
    assert not is_route_ready("acme", "widgets")
    assert is_route_ready("github", "commits")


def test_is_plannable_true_for_canonical_writer_identities() -> None:
    assert is_plannable("launchdarkly", "feature-flags")
    assert is_plannable("github", "prs")
    assert is_plannable("github", "cicd")
    assert is_plannable("github", "work-items")
    assert is_plannable("gitlab", "work-items")
    assert is_plannable("jira", "work-items")
    assert is_plannable("linear", "work-items")
    # PagerDuty's incident quartet is deliberately four independent claims,
    # not collapsed under an atomic family -- every member is plannable.
    for dataset in (
        "incidents",
        "incident-alerts",
        "incident-log-entries",
        "incident-notes",
    ):
        assert is_plannable("pagerduty", dataset)


@pytest.mark.parametrize(
    ("provider", "dataset"),
    [
        ("github", "pr-reviews"),
        ("github", "pr-comments"),
        ("github", "tests"),
        ("gitlab", "pr-reviews"),
        ("gitlab", "pr-comments"),
        ("gitlab", "tests"),
        ("github", "work-item-labels"),
        ("github", "work-item-projects"),
        ("github", "work-item-history"),
        ("github", "work-item-comments"),
        ("gitlab", "work-item-labels"),
        ("gitlab", "work-item-projects"),
        ("gitlab", "work-item-history"),
        ("gitlab", "work-item-comments"),
        ("jira", "work-item-labels"),
        ("jira", "work-item-projects"),
        ("jira", "work-item-history"),
        ("jira", "work-item-comments"),
        ("linear", "work-item-labels"),
        ("linear", "work-item-projects"),
        ("linear", "work-item-history"),
        ("linear", "work-item-comments"),
    ],
)
def test_alias_identities_are_route_ready_but_not_plannable(
    provider: str, dataset: str
) -> None:
    """The 22 alias identities: real capability/audit/watermark surfaces that
    fold into their family's canonical writer and are never independently
    plannable. This is the CHAOS-4054 successor to the deleted
    "mutually exclusive switches" tests -- exclusivity now lives in the
    matrix's ``plannable`` field rather than in runtime switch validation."""

    assert is_route_ready(provider, dataset)
    assert not is_plannable(provider, dataset)
    assert not routes_to_river(provider, dataset)


def test_routes_to_river_requires_both_ready_and_plannable() -> None:
    # Ready and plannable: routes.
    assert routes_to_river("github", "prs")
    assert routes_to_river("launchdarkly", "feature-flags")
    # Ready but alias (not plannable): does not route.
    assert is_route_ready("github", "pr-reviews")
    assert not routes_to_river("github", "pr-reviews")
    # Not even ready: does not route.
    assert not is_route_ready("acme", "widgets")
    assert not routes_to_river("acme", "widgets")


def test_matrix_ready_plannable_partition_matches_the_audit() -> None:
    """37 of the 59 route-ready pairs are plannable; the other 22 are the
    alias identities enumerated above. Both counts are independently
    asserted acceptance figures (CHAOS-4054), not a projection of whatever
    the generator currently emits."""

    matrix = json.loads(provider_unit_route._MATRIX_CONTRACT_PATH.read_text())
    ready = [pair for pair in matrix["pairs"] if pair.get("route_ready") is True]
    plannable = [pair for pair in ready if pair.get("plannable") is True]
    not_plannable = [pair for pair in ready if pair.get("plannable") is not True]

    assert len(ready) == 59
    assert len(plannable) == 37
    assert len(not_plannable) == 22
    for pair in ready:
        assert routes_to_river(pair["provider"], pair["dataset"]) is (pair in plannable)


# ---------------------------------------------------------------------------
# Fixture-contract seam: production never repoints _MATRIX_CONTRACT_PATH, but
# tests use it to prove the mechanism generalizes beyond the one checked-in
# contract.
# ---------------------------------------------------------------------------


def _write_matrix_fixture(
    path: Path,
    *,
    ready_plannable: Collection[tuple[str, str]] = (),
    ready_not_plannable: Collection[tuple[str, str]] = (),
) -> None:
    pairs = []
    for provider, dataset in sorted(ready_plannable):
        pairs.append(
            {
                "provider": provider,
                "dataset": dataset,
                "route_ready": True,
                "plannable": True,
            }
        )
    for provider, dataset in sorted(ready_not_plannable):
        pairs.append(
            {
                "provider": provider,
                "dataset": dataset,
                "route_ready": True,
                "plannable": False,
            }
        )
    # A not-ready pair alongside the ready ones proves the loader filters on
    # route_ready rather than treating "present in the file" as sufficient.
    pairs.append(
        {
            "provider": "github",
            "dataset": "commits",
            "route_ready": False,
            "plannable": True,
        }
    )
    path.write_text(json.dumps({"schema_version": 1, "pairs": pairs}))


def test_matrix_fixture_generalizes_to_a_hypothetical_second_ready_pair(
    tmp_path: Path,
) -> None:
    """Proves the mechanism, not just the checked-in pairs: pointing the
    loader at a fixture contract with TWO ready+plannable pairs makes BOTH
    routable through the exact same code path, with no edit to this module.
    """

    fixture = tmp_path / "matrix.json"
    _write_matrix_fixture(
        fixture,
        ready_plannable={
            ("launchdarkly", "feature-flags"),
            ("acme", "widgets"),
        },
    )
    provider_unit_route._MATRIX_CONTRACT_PATH = fixture
    provider_unit_route.clear_matrix_cache()

    assert routes_to_river("launchdarkly", "feature-flags")
    assert routes_to_river("acme", "widgets")
    # The explicitly not-ready row in the same fixture stays closed.
    assert not is_route_ready("github", "commits")
    # A pair entirely absent from the fixture also stays closed.
    assert not is_route_ready("jira", "incidents")


def test_matrix_fixture_ready_but_not_plannable_pair_never_routes(
    tmp_path: Path,
) -> None:
    """route_ready alone is never sufficient -- a fixture pair marked
    plannable: false stays closed no matter how the rest of the contract
    reads, exactly like the 22 real alias identities."""

    fixture = tmp_path / "matrix.json"
    _write_matrix_fixture(fixture, ready_not_plannable={("acme", "widgets")})
    provider_unit_route._MATRIX_CONTRACT_PATH = fixture
    provider_unit_route.clear_matrix_cache()

    assert is_route_ready("acme", "widgets")
    assert not is_plannable("acme", "widgets")
    assert not routes_to_river("acme", "widgets")


def test_unreadable_matrix_contract_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.json"
    provider_unit_route._MATRIX_CONTRACT_PATH = missing
    provider_unit_route.clear_matrix_cache()

    with pytest.raises(ProviderUnitRouteError):
        is_route_ready("github", "commits")


def test_malformed_matrix_contract_raises(tmp_path: Path) -> None:
    fixture = tmp_path / "matrix.json"
    fixture.write_text("not json")
    provider_unit_route._MATRIX_CONTRACT_PATH = fixture
    provider_unit_route.clear_matrix_cache()

    with pytest.raises(ProviderUnitRouteError):
        is_route_ready("github", "commits")


# ---------------------------------------------------------------------------
# CHAOS-3606 / CHAOS-4054: GitHub's five planner-collapsed work-item aliases
# are one Go family. Matrix readiness remains visible for every alias; only
# the canonical claim carrying every declared family flag is admissible.
# ---------------------------------------------------------------------------


_GITHUB_WORK_ITEM_FAMILY_FLAGS = {
    "family_dataset_work_items": True,
    "family_dataset_work_item_labels": True,
    "family_dataset_work_item_projects": True,
    "family_dataset_work_item_history": True,
    "family_dataset_work_item_comments": True,
}


def test_github_work_item_family_admission_requires_exact_boolean_flags() -> None:
    assert is_complete_github_work_item_family_claim(
        "github", "work-items", _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )
    for missing in _GITHUB_WORK_ITEM_FAMILY_FLAGS:
        flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        del flags[missing]
        assert not is_complete_github_work_item_family_claim(
            "github", "work-items", flags
        )
        flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        flags[missing] = False
        assert not is_complete_github_work_item_family_claim(
            "github", "work-items", flags
        )
    for value in ("true", "false", 1, 0):
        malformed_flags: dict[str, object] = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        malformed_flags["family_dataset_work_items"] = value
        assert not is_complete_github_work_item_family_claim(
            "github", "work-items", malformed_flags
        )
    flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
    flags["family_dataset_unexpected"] = True
    assert not is_complete_github_work_item_family_claim("github", "work-items", flags)
    assert not is_complete_github_work_item_family_claim(
        "github", "work-item-comments", _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )
    assert not is_complete_github_work_item_family_claim(
        "github", "prs", _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )


@pytest.mark.parametrize(
    ("provider", "canonical", "aliases", "complete_flags"),
    (
        pytest.param(
            provider,
            "work-items",
            (
                "work-item-labels",
                "work-item-projects",
                "work-item-history",
                "work-item-comments",
            ),
            _GITHUB_WORK_ITEM_FAMILY_FLAGS,
            id=f"{provider}-work-items",
        )
        for provider in ("github", "gitlab", "jira", "linear")
    ),
)
def test_provider_work_item_family_policy_is_exact_and_provider_neutral(
    provider: str,
    canonical: str,
    aliases: tuple[str, ...],
    complete_flags: dict[str, bool],
) -> None:
    assert validate_provider_family_claim(
        provider, canonical, complete_flags, strict_atomic=True
    )
    for alias in aliases:
        assert not validate_provider_family_claim(
            provider, alias, complete_flags, strict_atomic=True
        )
    for flag in complete_flags:
        missing = dict(complete_flags)
        del missing[flag]
        assert not validate_provider_family_claim(
            provider, canonical, missing, strict_atomic=True
        )
        false = dict(complete_flags)
        false[flag] = False
        assert not validate_provider_family_claim(
            provider, canonical, false, strict_atomic=True
        )
    unknown = dict(complete_flags)
    unknown["family_dataset_unknown"] = True
    assert not validate_provider_family_claim(
        provider, canonical, unknown, strict_atomic=True
    )


def test_neutral_work_item_family_matches_the_live_planner_boundary() -> None:
    assert (
        set(WORK_ITEM_DATASETS) == provider_unit_route._GITHUB_WORK_ITEM_FAMILY_DATASETS
    )


@pytest.mark.parametrize(
    "dataset",
    (
        "incidents",
        "incident-alerts",
        "incident-log-entries",
        "incident-notes",
    ),
)
def test_pagerduty_incident_family_preserves_d16_independent_claims(
    dataset: str,
) -> None:
    # PagerDuty currently plans four independent units. D16 forbids collapsing
    # that boundary inside the baseline port, so the neutral family catalog
    # identifies the relationship without imposing canonical/flag admission.
    assert validate_provider_family_claim("pagerduty", dataset, {}, strict_atomic=True)


@pytest.mark.parametrize("provider", ("gitlab", "jira", "linear"))
def test_default_off_work_item_family_keeps_legacy_claims_admissible(
    provider: str,
) -> None:
    assert validate_provider_family_claim(
        provider,
        "work-items",
        {"family_dataset_work_items": True},
        strict_atomic=False,
    )


def test_provider_family_policy_leaves_independent_routes_unchanged() -> None:
    assert validate_provider_family_claim(
        "github", "prs", {"sync_prs": True}, strict_atomic=True
    )
    assert validate_provider_family_claim(
        "github", "cicd", {"sync_cicd": True}, strict_atomic=True
    )
    assert validate_provider_family_claim(
        "github", "tests", {"sync_tests": True}, strict_atomic=True
    )


# ---------------------------------------------------------------------------
# Direct-alias predicates: used by sync_units._unroutable_reason to explain
# WHY an already-persisted claim is refused (as opposed to is_plannable,
# which governs whether the planner creates the claim in the first place).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("provider", "dataset", "expected"),
    (
        ("github", "work-item-labels", True),
        ("github", "work-item-projects", True),
        ("github", "work-item-history", True),
        ("github", "work-item-comments", True),
        ("github", "work-items", False),
        ("github", "prs", False),
        ("gitlab", "work-item-labels", False),
    ),
)
def test_is_github_work_item_direct_alias(
    provider: str, dataset: str, expected: bool
) -> None:
    assert is_github_work_item_direct_alias(provider, dataset) is expected


@pytest.mark.parametrize(
    ("provider", "dataset", "expected"),
    (
        ("github", "work-item-labels", True),
        ("gitlab", "work-item-comments", True),
        ("jira", "work-item-history", True),
        ("linear", "work-item-projects", True),
        ("github", "work-items", False),
        ("pagerduty", "incident-alerts", False),
        ("github", "prs", False),
    ),
)
def test_is_atomic_provider_family_direct_alias(
    provider: str, dataset: str, expected: bool
) -> None:
    assert is_atomic_provider_family_direct_alias(provider, dataset) is expected


@pytest.mark.parametrize(
    ("provider", "dataset", "expected"),
    (
        ("github", "work-items", True),
        ("github", "work-item-labels", True),
        ("github", "prs", False),
        ("gitlab", "work-items", False),
    ),
)
def test_is_github_work_item_family_dataset(
    provider: str, dataset: str, expected: bool
) -> None:
    assert is_github_work_item_family_dataset(provider, dataset) is expected
