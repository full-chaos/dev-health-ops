"""Fail-closed transport gate for complete provider sync-unit routes.

Routability is derived from the checked-in provider capability matrix
(``contracts/provider-matrix/v1/matrix.json``) instead of a hardcoded
provider/dataset literal. A pair is routable only when BOTH hold:

  1. The matrix marks the pair ``route_ready`` (TRD: "A provider/dataset pair
     without a field here can never be enabled, which is why adding github,
     gitlab, and pagerduty descriptors ... cannot widen the live route
     surface.").
  2. Its declared environment switch (see ``_switch_field_name``) is enabled.

Neither condition is sufficient alone (PRD: "Declarative registry with
complete policy; unknown or incomplete kinds fail readiness."). Landing a new
``route_ready`` row in the matrix can never, by itself, move live traffic --
only a change that ALSO wires and enables the matching switch can. This keeps
CHAOS-3123..3127 (the per-provider readiness work) fully decoupled from any
edit in this module: flipping a matrix row and turning on its switch is
sufficient to route a pair, with no code change here.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path

_FALSE = frozenset({"", "0", "false", "no", "off"})
_TRUE = frozenset({"1", "true", "yes", "on"})

# src/dev_health_ops/workers/provider_unit_route.py -> repo root is 3 parents up.
_DEFAULT_MATRIX_CONTRACT_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "provider-matrix"
    / "v1"
    / "matrix.json"
)

# Reassignable only by tests (see tests/workers/test_provider_unit_route.py),
# so a hypothetical second route_ready pair can be exercised end to end
# through a fixture contract without ever editing the checked-in matrix.
# Production code never assigns to this.
_MATRIX_CONTRACT_PATH = _DEFAULT_MATRIX_CONTRACT_PATH


class ProviderUnitRouteError(ValueError):
    """Value-free rejection of an invalid or incomplete route switch."""


def _flag(environment: Mapping[str, str], name: str) -> bool:
    value = environment.get(name, "").strip().lower()
    if value in _FALSE:
        return False
    if value in _TRUE:
        return True
    raise ProviderUnitRouteError("provider unit route switch is invalid")


@cache
def _load_route_ready_pairs(path: Path) -> frozenset[tuple[str, str]]:
    """Read a capability matrix contract and return every pair it marks
    ``route_ready``.

    This is the single Python reader of the contract that Go's
    ``BuildProviderMatrix`` / ``TestProviderMatrixMatchesCheckedInContract``
    (``internal/providersync/capability_matrix_test.go``) produces and
    freezes byte-for-byte (CUT-08). A pair absent from the file, or present
    with ``route_ready: false``, can never be routable regardless of switch
    state -- this is the "unknown or incomplete kinds fail readiness" half of
    the PRD requirement.
    """

    raw = json.loads(path.read_text())
    return frozenset(
        (str(pair["provider"]).strip().lower(), str(pair["dataset"]).strip().lower())
        for pair in raw.get("pairs", ())
        if pair.get("route_ready") is True
    )


def _route_ready_pairs() -> frozenset[tuple[str, str]]:
    return _load_route_ready_pairs(_MATRIX_CONTRACT_PATH)


def clear_matrix_cache() -> None:
    """Drop the cached matrix read.

    Test-only: production reads the checked-in contract exactly once per
    process and never reloads it mid-run. Tests call this after repointing
    ``_MATRIX_CONTRACT_PATH`` at a fixture, or after restoring the default,
    so a stale cache entry from an earlier test can't leak in.
    """

    _load_route_ready_pairs.cache_clear()


def _switch_field_name(provider: str, dataset: str) -> str:
    """The ``ProviderUnitRouteSwitches`` field name a matrix pair's switch
    must use -- e.g. ``launchdarkly`` + ``feature-flags`` ->
    ``launchdarkly_feature_flags``.

    This is a naming convention, not a routing decision on its own: a pair is
    only enabled if a field of this name also exists on the dataclass AND is
    True. A provider/dataset combination with no matching field derives a
    name that resolves to nothing, so it fails closed by construction rather
    than through an explicit denylist.
    """

    return f"{provider.strip().lower()}_{dataset.strip().lower()}".replace("-", "_")


@dataclass(frozen=True, slots=True)
class ProviderUnitRouteSwitches:
    linear_work_items: bool = False
    jira_work_items: bool = False
    jira_incidents: bool = False
    launchdarkly_feature_flags: bool = False
    # github_repo_metadata is the producer half of the (github, repo-metadata)
    # gate (CHAOS-3123). Its Go counterpart is
    # config.Config.WorkerGithubRepoMetadataEnabled, read from the same
    # environment name, because a unit this gate routes to River is only
    # executed if the worker's CompleteRouteSwitches also enables the pair.
    github_repo_metadata: bool = False
    # Independent producer half of the native GitLab repository route. It
    # shares neither switch nor instance-normalization semantics with GitHub.
    gitlab_repo_metadata: bool = False
    # Independent producer half of the native GitLab commits route.
    gitlab_commits: bool = False
    # Independent producer half of the native GitLab aggregate commit-stat route.
    gitlab_commit_stats: bool = False
    # Mutually-exclusive aliases for one complete GitLab TestOps writer.
    gitlab_cicd: bool = False
    gitlab_tests: bool = False
    # github_prs is the producer half of the (github, prs) gate (CHAOS-3122,
    # following CHAOS-3123's precedent). Its Go counterpart is
    # config.Config.WorkerGithubPRsEnabled, read from the same
    # WORKER_GITHUB_PRS_ENABLED environment name.
    github_prs: bool = False
    github_cicd: bool = False
    github_commits: bool = False
    github_deployments: bool = False
    github_security: bool = False
    github_files: bool = False
    github_commit_stats: bool = False
    github_blame: bool = False
    github_tests: bool = False

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> ProviderUnitRouteSwitches:
        source = os.environ if environment is None else environment
        switches = cls(
            linear_work_items=_flag(source, "WORKER_LINEAR_WORK_ITEMS_ENABLED"),
            jira_work_items=_flag(source, "WORKER_JIRA_WORK_ITEMS_ENABLED"),
            jira_incidents=_flag(source, "WORKER_JIRA_INCIDENTS_ENABLED"),
            launchdarkly_feature_flags=_flag(
                source, "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED"
            ),
            github_repo_metadata=_flag(source, "WORKER_GITHUB_REPO_METADATA_ENABLED"),
            gitlab_repo_metadata=_flag(source, "WORKER_GITLAB_REPO_METADATA_ENABLED"),
            gitlab_commits=_flag(source, "WORKER_GITLAB_COMMITS_ENABLED"),
            gitlab_commit_stats=_flag(source, "WORKER_GITLAB_COMMIT_STATS_ENABLED"),
            gitlab_cicd=_flag(source, "WORKER_GITLAB_CICD_ENABLED"),
            gitlab_tests=_flag(source, "WORKER_GITLAB_TESTS_ENABLED"),
            github_prs=_flag(source, "WORKER_GITHUB_PRS_ENABLED"),
            github_cicd=_flag(source, "WORKER_GITHUB_CICD_ENABLED"),
            github_commits=_flag(source, "WORKER_GITHUB_COMMITS_ENABLED"),
            github_deployments=_flag(source, "WORKER_GITHUB_DEPLOYMENTS_ENABLED"),
            github_security=_flag(source, "WORKER_GITHUB_SECURITY_ENABLED"),
            github_files=_flag(source, "WORKER_GITHUB_FILES_ENABLED"),
            github_commit_stats=_flag(source, "WORKER_GITHUB_COMMIT_STATS_ENABLED"),
            github_blame=_flag(source, "WORKER_GITHUB_BLAME_ENABLED"),
            github_tests=_flag(source, "WORKER_GITHUB_TESTS_ENABLED"),
        )
        switches.require_complete_routes()
        return switches

    def require_complete_routes(self) -> None:
        if self.linear_work_items or self.jira_work_items:
            raise ProviderUnitRouteError("enabled provider unit route is incomplete")
        if self.github_cicd and self.github_tests:
            raise ProviderUnitRouteError(
                "github cicd and tests switches are mutually exclusive complete-unit aliases"
            )
        if self.gitlab_cicd and self.gitlab_tests:
            raise ProviderUnitRouteError(
                "gitlab cicd and tests switches are mutually exclusive complete-unit aliases"
            )

    def routes_to_river(self, provider: str, dataset: str) -> bool:
        self.require_complete_routes()
        return self.is_route_ready(provider, dataset) and self._switch_enabled(
            provider, dataset
        )

    def _switch_enabled(self, provider: str, dataset: str) -> bool:
        # getattr's default is what makes "no field declared for this pair"
        # resolve to False instead of raising -- the mechanism behind "a pair
        # can never be enabled by omission".
        return bool(getattr(self, _switch_field_name(provider, dataset), False))

    @staticmethod
    def is_route_ready(provider: str, dataset: str) -> bool:
        """Return whether the checked-in capability matrix marks this
        provider/dataset pair ``route_ready``.

        This is the only source of readiness in this module: nothing here
        hardcodes a provider/dataset literal, so a pair becomes eligible the
        moment the matrix says it is ready -- routability still requires its
        switch too (see ``routes_to_river``).
        """

        return (
            provider.strip().lower(),
            dataset.strip().lower(),
        ) in _route_ready_pairs()
