from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.workers import provider_unit_route
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
        "WORKER_JIRA_INCIDENTS_ENABLED",
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
    """Mirrors the Go-side TestGithubRepoMetadataSwitchDoesNotOpenGitLab:
    gitlab/repo-metadata shares the repo-metadata dataset name but has no
    native handler and stays route_ready=false in the matrix, so turning on
    the github switch must never widen gitlab's route open."""

    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_REPO_METADATA_ENABLED": "true"}
    )
    assert not ProviderUnitRouteSwitches.is_route_ready("gitlab", "repo-metadata")
    assert not switches.routes_to_river("gitlab", "repo-metadata")


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


def test_github_prs_is_not_yet_route_ready() -> None:
    """(github, prs) has a real Go handler (CHAOS-3122) but is deliberately
    NOT route_ready: codex H1 found that first_review_at/reviews_count/
    changes_requested_count on git_pull_requests are owned by Python's
    review-enrichment phase, which the Go handler does not perform, so it
    would always write fabricated zeros into columns it does not own. The
    switch exists and is wired end to end (so flipping RouteReady later is a
    one-line change, not new plumbing) but can never route traffic while the
    matrix says not-ready -- routes_to_river must stay False even with the
    switch on."""

    assert not ProviderUnitRouteSwitches.is_route_ready("github", "prs")

    on = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_PRS_ENABLED": "true"}
    )
    assert not on.routes_to_river("github", "prs")


def test_github_prs_switch_does_not_open_pr_reviews_or_pr_comments() -> None:
    """github/pr-reviews and github/pr-comments share the "prs" legacy target
    in Python (they are the same _sync_github_prs_to_store_async execution),
    but neither has its own Go handler yet and both stay route_ready=false in
    the matrix, so turning on GithubPRs must never widen either open."""

    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_GITHUB_PRS_ENABLED": "true"}
    )
    for dataset in ("pr-reviews", "pr-comments"):
        assert not ProviderUnitRouteSwitches.is_route_ready("github", dataset)
        assert not switches.routes_to_river("github", dataset)


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
