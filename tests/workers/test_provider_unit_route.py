from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.workers import provider_unit_route
from dev_health_ops.workers.provider_family_contract import WORK_ITEM_DATASETS
from dev_health_ops.workers.provider_unit_route import (
    ProviderUnitRouteError,
    ProviderUnitRouteSwitches,
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


def test_route_switch_is_exact_and_independent() -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": "true"}
    )
    assert switches.routes_to_river("launchdarkly", "feature-flags")
    assert not switches.routes_to_river("launchdarkly", "work-items")
    assert not switches.routes_to_river("gitlab", "feature-flags")


@pytest.mark.parametrize(
    "name",
    (
        "WORKER_LINEAR_WORK_ITEMS_ENABLED",
        "WORKER_JIRA_WORK_ITEMS_ENABLED",
    ),
)
def test_incomplete_routes_fail_closed(name: str) -> None:
    with pytest.raises(ProviderUnitRouteError, match="incomplete"):
        ProviderUnitRouteSwitches.from_environment({name: "true"})


def test_invalid_switch_fails_closed_without_echoing_value() -> None:
    value = "secret-looking-invalid-value"
    with pytest.raises(ProviderUnitRouteError) as raised:
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": value}
        )
    assert value not in str(raised.value)


def test_jira_incidents_switch_is_the_second_required_key() -> None:
    assert ProviderUnitRouteSwitches.is_route_ready("jira", "incidents")
    assert not ProviderUnitRouteSwitches.from_environment({}).routes_to_river(
        "jira", "incidents"
    )
    assert ProviderUnitRouteSwitches.from_environment(
        {"WORKER_JIRA_INCIDENTS_ENABLED": "true"}
    ).routes_to_river("jira", "incidents")


# ---------------------------------------------------------------------------
# CHAOS-3123: the (github, repo-metadata) producer-side switch. Its Go
# counterpart is config.Config.WorkerGithubRepoMetadataEnabled, read from the
# same WORKER_GITHUB_REPO_METADATA_ENABLED name.
# ---------------------------------------------------------------------------


def test_github_repo_metadata_defaults_to_false() -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": "true"}
    )
    assert switches.github_repo_metadata is False


@pytest.mark.parametrize("value", sorted(provider_unit_route._TRUE))
def test_github_repo_metadata_parses_true_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_REPO_METADATA_ENABLED": value}
    )
    assert switches.github_repo_metadata is True


@pytest.mark.parametrize("value", sorted(provider_unit_route._FALSE))
def test_github_repo_metadata_parses_false_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_REPO_METADATA_ENABLED": value}
    )
    assert switches.github_repo_metadata is False


def test_github_repo_metadata_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_REPO_METADATA_ENABLED": "sometimes"}
        )


def test_github_repo_metadata_switch_is_the_second_required_key() -> None:
    """(github, repo-metadata) is route_ready in the checked-in matrix, but
    readiness alone must never move traffic -- routes_to_river only returns
    True once the GithubRepoMetadata switch is ALSO on. This is the half that
    matters: it proves the checked-in matrix cannot move traffic on its own.
    """

    assert ProviderUnitRouteSwitches.is_route_ready("github", "repo-metadata")

    off = ProviderUnitRouteSwitches.from_environment({})
    assert not off.routes_to_river("github", "repo-metadata")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_REPO_METADATA_ENABLED": "true"}
    )
    assert on.routes_to_river("github", "repo-metadata")


def test_github_repo_metadata_switch_does_not_open_gitlab_repo_metadata() -> None:
    """The two ready repository routes still require independent switches."""

    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_REPO_METADATA_ENABLED": "true"}
    )
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "repo-metadata")
    assert not switches.routes_to_river("gitlab", "repo-metadata")


def test_gitlab_repo_metadata_defaults_off_and_routes_independently() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    assert off.gitlab_repo_metadata is False
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "repo-metadata")
    assert not off.routes_to_river("gitlab", "repo-metadata")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_REPO_METADATA_ENABLED": "true"}
    )
    assert on.gitlab_repo_metadata is True
    assert on.routes_to_river("gitlab", "repo-metadata")
    assert not on.routes_to_river("github", "repo-metadata")


def test_gitlab_repo_metadata_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITLAB_REPO_METADATA_ENABLED": "sometimes"}
        )


def test_gitlab_commits_defaults_off_and_routes_independently() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    assert off.gitlab_commits is False
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "commits")
    assert not off.routes_to_river("gitlab", "commits")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_COMMITS_ENABLED": "true"}
    )
    assert on.gitlab_commits is True
    assert on.routes_to_river("gitlab", "commits")
    assert not on.routes_to_river("github", "commits")


def test_gitlab_commits_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITLAB_COMMITS_ENABLED": "sometimes"}
        )


def test_gitlab_commit_stats_defaults_off_and_routes_independently() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    assert off.gitlab_commit_stats is False
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "commit-stats")
    assert not off.routes_to_river("gitlab", "commit-stats")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_COMMIT_STATS_ENABLED": "true"}
    )
    assert on.gitlab_commit_stats is True
    assert on.routes_to_river("gitlab", "commit-stats")
    assert not on.routes_to_river("github", "commit-stats")


def test_gitlab_commit_stats_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITLAB_COMMIT_STATS_ENABLED": "sometimes"}
        )


def test_gitlab_complete_unit_aliases_route_independently() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    assert off.gitlab_cicd is False
    assert off.gitlab_tests is False
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "cicd")
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "tests")
    assert not off.routes_to_river("gitlab", "cicd")
    assert not off.routes_to_river("gitlab", "tests")

    cicd = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_CICD_ENABLED": "true"}
    )
    assert cicd.routes_to_river("gitlab", "cicd")
    assert not cicd.routes_to_river("gitlab", "tests")
    tests = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_TESTS_ENABLED": "true"}
    )
    assert tests.routes_to_river("gitlab", "tests")
    assert not tests.routes_to_river("gitlab", "cicd")


def test_gitlab_complete_unit_aliases_are_mutually_exclusive() -> None:
    with pytest.raises(ProviderUnitRouteError, match="mutually exclusive"):
        ProviderUnitRouteSwitches.from_environment(
            {
                "WORKER_GITLAB_CICD_ENABLED": "true",
                "WORKER_GITLAB_TESTS_ENABLED": "true",
            }
        )


def test_gitlab_cicd_invalid_value_fails_closed() -> None:
    for name in ("WORKER_GITLAB_CICD_ENABLED", "WORKER_GITLAB_TESTS_ENABLED"):
        with pytest.raises(ProviderUnitRouteError):
            ProviderUnitRouteSwitches.from_environment({name: "sometimes"})


def test_gitlab_incidents_defaults_off_and_routes_independently() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    assert off.gitlab_incidents is False
    assert ProviderUnitRouteSwitches.is_route_ready("gitlab", "incidents")
    assert not off.routes_to_river("gitlab", "incidents")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITLAB_INCIDENTS_ENABLED": "true"}
    )
    assert on.routes_to_river("gitlab", "incidents")
    assert not on.routes_to_river("jira", "incidents")


def test_gitlab_incidents_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITLAB_INCIDENTS_ENABLED": "sometimes"}
        )


# ---------------------------------------------------------------------------
# CHAOS-3122: the (github, prs) producer-side switch. Its Go counterpart is
# config.Config.WorkerGithubPRsEnabled, read from the same
# WORKER_GITHUB_PRS_ENABLED name.
# ---------------------------------------------------------------------------


def test_github_prs_defaults_to_false() -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": "true"}
    )
    assert switches.github_prs is False


@pytest.mark.parametrize("value", sorted(provider_unit_route._TRUE))
def test_github_prs_parses_true_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_PRS_ENABLED": value}
    )
    assert switches.github_prs is True


@pytest.mark.parametrize("value", sorted(provider_unit_route._FALSE))
def test_github_prs_parses_false_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_PRS_ENABLED": value}
    )
    assert switches.github_prs is False


def test_github_prs_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_PRS_ENABLED": "sometimes"}
        )


@pytest.mark.parametrize(
    ("dataset", "field", "environment_name"),
    [
        ("prs", "github_prs", "WORKER_GITHUB_PRS_ENABLED"),
        ("pr-reviews", "github_pr_reviews", "WORKER_GITHUB_PR_REVIEWS_ENABLED"),
        ("pr-comments", "github_pr_comments", "WORKER_GITHUB_PR_COMMENTS_ENABLED"),
    ],
)
def test_github_pr_social_routes_require_their_own_switch(
    dataset: str, field: str, environment_name: str
) -> None:
    assert ProviderUnitRouteSwitches.is_route_ready("github", dataset)
    off = ProviderUnitRouteSwitches.from_environment({})
    assert getattr(off, field) is False
    assert not off.routes_to_river("github", dataset)

    on = ProviderUnitRouteSwitches.from_environment({environment_name: "true"})
    assert getattr(on, field) is True
    assert on.routes_to_river("github", dataset)
    for other in {"prs", "pr-reviews", "pr-comments"} - {dataset}:
        assert not on.routes_to_river("github", other)


@pytest.mark.parametrize(
    "environment_name",
    ["WORKER_GITHUB_PR_REVIEWS_ENABLED", "WORKER_GITHUB_PR_COMMENTS_ENABLED"],
)
def test_github_pr_social_alias_invalid_values_fail_closed(
    environment_name: str,
) -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment({environment_name: "sometimes"})


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("WORKER_GITHUB_PRS_ENABLED", "WORKER_GITHUB_PR_REVIEWS_ENABLED"),
        ("WORKER_GITHUB_PRS_ENABLED", "WORKER_GITHUB_PR_COMMENTS_ENABLED"),
        ("WORKER_GITHUB_PR_REVIEWS_ENABLED", "WORKER_GITHUB_PR_COMMENTS_ENABLED"),
    ],
)
def test_github_pr_social_aliases_are_mutually_exclusive(left: str, right: str) -> None:
    with pytest.raises(ProviderUnitRouteError, match="mutually exclusive"):
        ProviderUnitRouteSwitches.from_environment({left: "true", right: "true"})


def test_github_cicd_defaults_to_false() -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": "true"}
    )
    assert switches.github_cicd is False


@pytest.mark.parametrize("value", sorted(provider_unit_route._TRUE))
def test_github_cicd_parses_true_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_CICD_ENABLED": value}
    )
    assert switches.github_cicd is True
    assert switches.routes_to_river("github", "cicd")


@pytest.mark.parametrize("value", sorted(provider_unit_route._FALSE))
def test_github_cicd_parses_false_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_CICD_ENABLED": value}
    )
    assert switches.github_cicd is False
    assert not switches.routes_to_river("github", "cicd")


def test_github_cicd_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_CICD_ENABLED": "sometimes"}
        )


def test_github_security_switch_is_the_second_required_key() -> None:
    assert ProviderUnitRouteSwitches.is_route_ready("github", "security")
    assert not ProviderUnitRouteSwitches.from_environment({}).routes_to_river(
        "github", "security"
    )
    assert ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_SECURITY_ENABLED": "true"}
    ).routes_to_river("github", "security")


def test_github_files_switch_is_the_second_required_key() -> None:
    assert ProviderUnitRouteSwitches.is_route_ready("github", "files")
    assert not ProviderUnitRouteSwitches.from_environment({}).routes_to_river(
        "github", "files"
    )
    assert ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_FILES_ENABLED": "true"}
    ).routes_to_river("github", "files")


def test_github_commit_stats_switch_routes_only_when_enabled() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_COMMIT_STATS_ENABLED": "true"}
    )
    assert off.github_commit_stats is False
    assert not off.routes_to_river("github", "commit-stats")
    assert on.github_commit_stats is True
    assert on.routes_to_river("github", "commit-stats")


def test_github_commit_stats_invalid_switch_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_COMMIT_STATS_ENABLED": "sometimes"}
        )


def test_github_blame_switch_routes_only_when_enabled() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_BLAME_ENABLED": "true"}
    )
    assert off.github_blame is False
    assert on.github_blame is True
    assert off.routes_to_river("github", "blame") is False
    assert on.routes_to_river("github", "blame") is True


def test_github_blame_invalid_switch_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_BLAME_ENABLED": "sometimes"}
        )


def test_github_tests_switch_routes_only_when_enabled() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_TESTS_ENABLED": "true"}
    )
    assert off.github_tests is False
    assert on.github_tests is True
    assert off.routes_to_river("github", "tests") is False
    assert on.routes_to_river("github", "tests") is True


def test_github_tests_invalid_switch_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_TESTS_ENABLED": "sometimes"}
        )


def test_github_cicd_and_tests_complete_unit_aliases_are_mutually_exclusive() -> None:
    with pytest.raises(ProviderUnitRouteError, match="mutually exclusive"):
        ProviderUnitRouteSwitches.from_environment(
            {
                "WORKER_GITHUB_CICD_ENABLED": "true",
                "WORKER_GITHUB_TESTS_ENABLED": "true",
            }
        )


# ---------------------------------------------------------------------------
# CHAOS-3606: GitHub's five planner-collapsed work-item aliases are one Go
# family. Matrix readiness remains visible for every alias; only canonical
# work-items can be admitted to the Go producer route.
# ---------------------------------------------------------------------------


_GITHUB_WORK_ITEM_FAMILY_FLAGS = {
    "family_dataset_work_items": True,
    "family_dataset_work_item_labels": True,
    "family_dataset_work_item_projects": True,
    "family_dataset_work_item_history": True,
    "family_dataset_work_item_comments": True,
}


def test_github_work_items_switch_routes_only_the_canonical_claim() -> None:
    off = ProviderUnitRouteSwitches.from_environment({})
    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_WORK_ITEMS_ENABLED": "true"}
    )
    datasets = (
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
    )
    for dataset in datasets:
        assert ProviderUnitRouteSwitches.is_route_ready("github", dataset)
        assert not off.routes_to_river("github", dataset)
    assert on.routes_to_river("github", "work-items")
    for alias in datasets[1:]:
        assert not on.routes_to_river("github", alias)


@pytest.mark.parametrize("value", sorted(provider_unit_route._TRUE))
def test_github_work_items_switch_parses_true_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_WORK_ITEMS_ENABLED": value}
    )
    assert switches.github_work_items is True
    assert switches.routes_to_river("github", "work-items")


@pytest.mark.parametrize("value", sorted(provider_unit_route._FALSE))
def test_github_work_items_switch_parses_false_spellings(value: str) -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_WORK_ITEMS_ENABLED": value}
    )
    assert switches.github_work_items is False
    assert not switches.routes_to_river("github", "work-items")


def test_github_work_items_switch_invalid_value_fails_closed() -> None:
    with pytest.raises(ProviderUnitRouteError):
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_GITHUB_WORK_ITEMS_ENABLED": "sometimes"}
        )


def test_github_work_item_family_admission_requires_exact_boolean_flags() -> None:
    assert provider_unit_route.is_complete_github_work_item_family_claim(
        "github", "work-items", _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )
    for missing in _GITHUB_WORK_ITEM_FAMILY_FLAGS:
        flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        del flags[missing]
        assert not provider_unit_route.is_complete_github_work_item_family_claim(
            "github", "work-items", flags
        )
        flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        flags[missing] = False
        assert not provider_unit_route.is_complete_github_work_item_family_claim(
            "github", "work-items", flags
        )
    for value in ("true", "false", 1, 0):
        malformed_flags: dict[str, object] = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
        malformed_flags["family_dataset_work_items"] = value
        assert not provider_unit_route.is_complete_github_work_item_family_claim(
            "github", "work-items", malformed_flags
        )
    flags = dict(_GITHUB_WORK_ITEM_FAMILY_FLAGS)
    flags["family_dataset_unexpected"] = True
    assert not provider_unit_route.is_complete_github_work_item_family_claim(
        "github", "work-items", flags
    )
    assert not provider_unit_route.is_complete_github_work_item_family_claim(
        "github", "work-item-comments", _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )
    assert not provider_unit_route.is_complete_github_work_item_family_claim(
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
    assert provider_unit_route.validate_provider_family_claim(
        provider, canonical, complete_flags, strict_atomic=True
    )
    for alias in aliases:
        assert not provider_unit_route.validate_provider_family_claim(
            provider, alias, complete_flags, strict_atomic=True
        )
    for flag in complete_flags:
        missing = dict(complete_flags)
        del missing[flag]
        assert not provider_unit_route.validate_provider_family_claim(
            provider, canonical, missing, strict_atomic=True
        )
        false = dict(complete_flags)
        false[flag] = False
        assert not provider_unit_route.validate_provider_family_claim(
            provider, canonical, false, strict_atomic=True
        )
    unknown = dict(complete_flags)
    unknown["family_dataset_unknown"] = True
    assert not provider_unit_route.validate_provider_family_claim(
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
    assert provider_unit_route.validate_provider_family_claim(
        "pagerduty", dataset, {}, strict_atomic=True
    )


@pytest.mark.parametrize("provider", ("gitlab", "jira", "linear"))
def test_default_off_work_item_family_keeps_legacy_claims_admissible(
    provider: str,
) -> None:
    assert provider_unit_route.validate_provider_family_claim(
        provider,
        "work-items",
        {"family_dataset_work_items": True},
        strict_atomic=False,
    )


def test_gitlab_family_catalog_does_not_invent_an_activation_switch() -> None:
    assert not provider_unit_route.provider_family_strict_admission_enabled(
        "gitlab",
        "work-items",
        {"WORKER_GITLAB_WORK_ITEMS_ENABLED": "true"},
    )


def test_provider_family_policy_leaves_independent_routes_unchanged() -> None:
    assert provider_unit_route.validate_provider_family_claim(
        "github", "prs", {"sync_prs": True}, strict_atomic=True
    )
    assert provider_unit_route.validate_provider_family_claim(
        "github", "cicd", {"sync_cicd": True}, strict_atomic=True
    )
    assert provider_unit_route.validate_provider_family_claim(
        "github", "tests", {"sync_tests": True}, strict_atomic=True
    )


# ---------------------------------------------------------------------------
# CHAOS-3131: routability is derived from the checked-in matrix, not from a
# hardcoded provider/dataset literal.
# ---------------------------------------------------------------------------


def test_is_route_ready_reflects_the_checked_in_matrix() -> None:
    assert ProviderUnitRouteSwitches.is_route_ready("launchdarkly", "feature-flags")
    # Same dataset name, different provider: the matrix marks gitlab's
    # feature-flags pair route_ready=false, so it must stay closed even
    # though the string "feature-flags" matches.
    assert not ProviderUnitRouteSwitches.is_route_ready("gitlab", "feature-flags")
    # Case/whitespace must not matter -- callers pass live DB column values.
    assert ProviderUnitRouteSwitches.is_route_ready(" LaunchDarkly ", "Feature-Flags")


def test_is_route_ready_fails_closed_for_unknown_pairs() -> None:
    assert not ProviderUnitRouteSwitches.is_route_ready("acme", "widgets")
    assert ProviderUnitRouteSwitches.is_route_ready("github", "commits")


def _write_matrix_fixture(path: Path, *, ready_pairs: set[tuple[str, str]]) -> None:
    pairs = []
    for provider, dataset in sorted(ready_pairs):
        pairs.append(
            {
                "provider": provider,
                "dataset": dataset,
                "route_ready": True,
            }
        )
    # A not-ready pair alongside the ready ones proves the loader filters on
    # route_ready rather than treating "present in the file" as sufficient.
    pairs.append({"provider": "github", "dataset": "commits", "route_ready": False})
    path.write_text(json.dumps({"schema_version": 1, "pairs": pairs}))


def test_is_route_ready_generalizes_to_a_hypothetical_second_ready_pair(
    tmp_path,
) -> None:
    """Proves the mechanism, not just the one production pair: pointing the
    loader at a fixture contract with TWO route_ready pairs makes BOTH
    routable through the exact same code path, with no edit to this module.
    """

    fixture = tmp_path / "matrix.json"
    _write_matrix_fixture(
        fixture,
        ready_pairs={
            ("launchdarkly", "feature-flags"),
            ("acme", "widgets"),
        },
    )
    provider_unit_route._MATRIX_CONTRACT_PATH = fixture
    provider_unit_route.clear_matrix_cache()

    assert ProviderUnitRouteSwitches.is_route_ready("launchdarkly", "feature-flags")
    assert ProviderUnitRouteSwitches.is_route_ready("acme", "widgets")
    # The explicitly not-ready row in the same fixture stays closed.
    assert not ProviderUnitRouteSwitches.is_route_ready("github", "commits")
    # A pair entirely absent from the fixture also stays closed.
    assert not ProviderUnitRouteSwitches.is_route_ready("jira", "incidents")


def test_a_hypothetical_ready_pair_still_requires_its_own_switch(tmp_path) -> None:
    """route_ready alone is never sufficient -- adding a matrix row cannot
    widen the routable surface by itself. ``acme/widgets`` has no matching
    field on ProviderUnitRouteSwitches, so it can never be enabled no matter
    how the matrix reads."""

    fixture = tmp_path / "matrix.json"
    _write_matrix_fixture(fixture, ready_pairs={("acme", "widgets")})
    provider_unit_route._MATRIX_CONTRACT_PATH = fixture
    provider_unit_route.clear_matrix_cache()

    assert ProviderUnitRouteSwitches.is_route_ready("acme", "widgets")
    switches = ProviderUnitRouteSwitches.from_environment({})
    assert not switches.routes_to_river("acme", "widgets")

    # Even a switch dataclass with every real field enabled cannot route it,
    # because none of those fields derive to "acme_widgets".
    all_enabled = ProviderUnitRouteSwitches(launchdarkly_feature_flags=True)
    assert not all_enabled.routes_to_river("acme", "widgets")


def test_switch_field_name_is_a_pure_naming_convention() -> None:
    assert (
        provider_unit_route._switch_field_name("launchdarkly", "feature-flags")
        == "launchdarkly_feature_flags"
    )
    assert (
        provider_unit_route._switch_field_name("Jira", "Work-Items")
        == "jira_work_items"
    )
