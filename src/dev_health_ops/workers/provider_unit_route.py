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

from dev_health_ops.contract_artifacts import contract_directory
from dev_health_ops.sync.planner import (
    _FAMILY_CANONICAL_DATASET_KEY,
    _WORK_ITEM_FAMILY_DATASET_ORDER,
    _family_dataset_flag,
)
from dev_health_ops.workers.provider_family_contract import (
    FamilyExecutionMode,
    atomic_provider_family_route_enabled,
    provider_family_policy,
    validate_provider_family_claim,
)

_FALSE = frozenset({"", "0", "false", "no", "off"})
_TRUE = frozenset({"1", "true", "yes", "on"})

_PROVIDER_ROUTES_PRESET_ENV = "GO_PROVIDER_ROUTES"
_DEV_HEALTH_ENV = "DEV_HEALTH_ENV"
_LOCAL_ALL_DISABLED_ALIASES = frozenset(
    {
        "github_pr_reviews",
        "github_pr_comments",
        "github_tests",
        "gitlab_pr_reviews",
        "gitlab_pr_comments",
        "gitlab_tests",
    }
)
_LOCAL_ALL_CANONICAL_ALTERNATIVES = {
    "github_prs": ("github_pr_reviews", "github_pr_comments"),
    "github_cicd": ("github_tests",),
    "gitlab_prs": ("gitlab_pr_reviews", "gitlab_pr_comments"),
    "gitlab_cicd": ("gitlab_tests",),
}
_LOCAL_ALL_ALIAS_CANONICAL = {
    alternative: canonical
    for canonical, alternatives in _LOCAL_ALL_CANONICAL_ALTERNATIVES.items()
    for alternative in alternatives
}

# Resolved through contract_artifacts rather than a per-file parents[N] count.
# The count here used to be 3, correct in a checkout and wrong in an installed
# distribution, where the same three hops land on the interpreter's lib
# directory instead of the repository root (CHAOS-3933).
_DEFAULT_MATRIX_CONTRACT_PATH = (
    contract_directory("provider-matrix", "v1") / "matrix.json"
)

# Reassignable only by tests (see tests/workers/test_provider_unit_route.py),
# so a hypothetical second route_ready pair can be exercised end to end
# through a fixture contract without ever editing the checked-in matrix.
# Production code never assigns to this.
_MATRIX_CONTRACT_PATH = _DEFAULT_MATRIX_CONTRACT_PATH

# The planner owns the exact family order and canonical claim identity. Import
# those values rather than copying five dataset names into this admission gate:
# a future planner alias cannot silently acquire a partially-wired switch here.
_GITHUB_WORK_ITEM_FAMILY_DATASETS = frozenset(_WORK_ITEM_FAMILY_DATASET_ORDER)
_GITHUB_WORK_ITEM_FAMILY_FLAGS = tuple(
    _family_dataset_flag(dataset) for dataset in _WORK_ITEM_FAMILY_DATASET_ORDER
)


class ProviderUnitRouteError(ValueError):
    """Value-free rejection of an invalid or incomplete route switch."""


def _flag(environment: Mapping[str, str], name: str) -> bool:
    value = environment.get(name, "").strip().lower()
    if value in _FALSE:
        return False
    if value in _TRUE:
        return True
    raise ProviderUnitRouteError("provider unit route switch is invalid")


def _provider_route_environment(
    environment: Mapping[str, str], field_names: tuple[str, ...]
) -> Mapping[str, str]:
    """Apply the local-only all-routes preset as per-switch defaults.

    Explicit switches win because ``setdefault`` never replaces a supplied
    value. Production remains default-off: any non-empty preset is rejected
    unless the process explicitly identifies its environment as local.
    """

    preset = environment.get(_PROVIDER_ROUTES_PRESET_ENV, "").strip().lower()
    if not preset:
        return environment
    if preset != "all":
        raise ProviderUnitRouteError("provider route preset must be empty or all")
    if environment.get(_DEV_HEALTH_ENV, "").strip().lower() != "local":
        raise ProviderUnitRouteError(
            "all provider routes preset requires local environment"
        )

    resolved = dict(environment)
    for field_name in field_names:
        environment_name = f"WORKER_{field_name.upper()}_ENABLED"
        alternative_selected = any(
            _flag(environment, f"WORKER_{alternative.upper()}_ENABLED")
            for alternative in _LOCAL_ALL_CANONICAL_ALTERNATIVES.get(field_name, ())
        )
        resolved.setdefault(
            environment_name,
            "false"
            if field_name in _LOCAL_ALL_DISABLED_ALIASES or alternative_selected
            else "true",
        )
    return resolved


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
    than through an explicit denylist. The one deliberate family mapping is
    Atomic work-item families: all five matrix identities share one switch,
    while only the canonical claim may acquire the Go writer. PagerDuty's
    incident datasets also share one rollout switch, but retain their four
    independent D16 claims.
    """

    normalized_provider = provider.strip().lower()
    normalized_dataset = dataset.strip().lower()
    policy = provider_family_policy(normalized_provider, normalized_dataset)
    if policy is not None and policy.mode is FamilyExecutionMode.ATOMIC_CANONICAL:
        return f"{normalized_provider}_{policy.canonical_dataset}".replace("-", "_")
    if normalized_provider == "pagerduty" and policy is not None:
        return "pagerduty_incidents"
    return f"{normalized_provider}_{normalized_dataset}".replace("-", "_")


def is_github_work_item_direct_alias(provider: str, dataset: str) -> bool:
    """Whether a pair is a malformed persisted alias, not a planner claim."""

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() in _GITHUB_WORK_ITEM_FAMILY_DATASETS
        and dataset.strip().lower() != _FAMILY_CANONICAL_DATASET_KEY
    )


def is_atomic_provider_family_direct_alias(provider: str, dataset: str) -> bool:
    """Whether a persisted claim is a non-canonical atomic-family alias."""

    normalized_provider = provider.strip().lower()
    normalized_dataset = dataset.strip().lower()
    policy = provider_family_policy(normalized_provider, normalized_dataset)
    return (
        policy is not None
        and policy.mode is FamilyExecutionMode.ATOMIC_CANONICAL
        and normalized_dataset != policy.canonical_dataset
    )


def is_github_work_item_family_dataset(provider: str, dataset: str) -> bool:
    """Whether a pair belongs to GitHub's planner-collapsed work-item family."""

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() in _GITHUB_WORK_ITEM_FAMILY_DATASETS
    )


def is_complete_github_work_item_family_claim(
    provider: str,
    dataset: str,
    processor_flags: Mapping[str, object] | None,
) -> bool:
    """Return whether a Go-admitted GitHub work-item unit is canonical and full.

    Planner output is exactly one canonical ``work-items`` unit carrying all
    five ordered ``family_dataset_*`` flags. The activation route cannot safely
    accept a subset: the Go route writes the complete sixteen-destination
    family, while completion fans watermarks back to every alias. A malformed
    direct alias or a partial canonical row is an ownership/configuration fault
    and is refused before the producer enqueues either runtime.
    """

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() == _FAMILY_CANONICAL_DATASET_KEY
        and validate_provider_family_claim(
            provider, dataset, processor_flags, strict_atomic=True
        )
    )


def provider_family_strict_admission_enabled(
    provider: str,
    dataset: str,
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Return whether the provider's atomic Go family switch is enabled.

    GitHub keeps its already-landed always-exact producer contract because its
    planner always emits all five ownership flags. Other providers remain on
    their D16 legacy claim shape while default-off; enabling their Go family
    switch makes strict validation apply before either transport stages work.
    """

    normalized_provider = provider.strip().lower()
    if normalized_provider == "github":
        return True
    return atomic_provider_family_route_enabled(provider, dataset, environment)


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
    gitlab_incidents: bool = False
    gitlab_deployments: bool = False
    gitlab_feature_flags: bool = False
    gitlab_files: bool = False
    gitlab_blame: bool = False
    gitlab_prs: bool = False
    gitlab_pr_reviews: bool = False
    gitlab_pr_comments: bool = False
    gitlab_security: bool = False
    # One switch for the canonical GitLab work-item claim. Direct aliases are
    # matrix identities only and cannot acquire a partial writer.
    gitlab_work_items: bool = False
    # PagerDuty catalog datasets retain independent claims. The incident
    # quartet shares a rollout switch without being collapsed into one claim.
    pagerduty_services: bool = False
    pagerduty_business_services: bool = False
    pagerduty_escalation_policies: bool = False
    pagerduty_schedules: bool = False
    pagerduty_on_calls: bool = False
    pagerduty_users: bool = False
    pagerduty_teams: bool = False
    pagerduty_incidents: bool = False
    # github_prs is the producer half of the (github, prs) gate (CHAOS-3122,
    # following CHAOS-3123's precedent). Its Go counterpart is
    # config.Config.WorkerGithubPRsEnabled, read from the same
    # WORKER_GITHUB_PRS_ENABLED environment name.
    github_prs: bool = False
    github_pr_reviews: bool = False
    github_pr_comments: bool = False
    github_cicd: bool = False
    github_commits: bool = False
    github_deployments: bool = False
    github_security: bool = False
    github_files: bool = False
    github_commit_stats: bool = False
    github_blame: bool = False
    github_tests: bool = False
    # One switch for all five GitHub work-item matrix aliases. Only planner's
    # canonical work-items claim is directly admissible; sibling aliases stay
    # route-ready for matrix/audit/watermark truth but never become partial
    # producer routes.
    github_work_items: bool = False
    # Internal marker for the validated local-only preset. It is not a direct
    # environment switch. It lets equivalent persisted alias identities use
    # the preset's one selected complete writer without widening explicit
    # deployment switches.
    local_all_routes: bool = False

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> ProviderUnitRouteSwitches:
        source = os.environ if environment is None else environment
        source = _provider_route_environment(source, tuple(cls.__dataclass_fields__))
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
            gitlab_incidents=_flag(source, "WORKER_GITLAB_INCIDENTS_ENABLED"),
            gitlab_deployments=_flag(source, "WORKER_GITLAB_DEPLOYMENTS_ENABLED"),
            gitlab_feature_flags=_flag(source, "WORKER_GITLAB_FEATURE_FLAGS_ENABLED"),
            gitlab_files=_flag(source, "WORKER_GITLAB_FILES_ENABLED"),
            gitlab_blame=_flag(source, "WORKER_GITLAB_BLAME_ENABLED"),
            gitlab_prs=_flag(source, "WORKER_GITLAB_PRS_ENABLED"),
            gitlab_pr_reviews=_flag(source, "WORKER_GITLAB_PR_REVIEWS_ENABLED"),
            gitlab_pr_comments=_flag(source, "WORKER_GITLAB_PR_COMMENTS_ENABLED"),
            gitlab_security=_flag(source, "WORKER_GITLAB_SECURITY_ENABLED"),
            gitlab_work_items=_flag(source, "WORKER_GITLAB_WORK_ITEMS_ENABLED"),
            pagerduty_services=_flag(source, "WORKER_PAGERDUTY_SERVICES_ENABLED"),
            pagerduty_business_services=_flag(
                source, "WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED"
            ),
            pagerduty_escalation_policies=_flag(
                source, "WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED"
            ),
            pagerduty_schedules=_flag(source, "WORKER_PAGERDUTY_SCHEDULES_ENABLED"),
            pagerduty_on_calls=_flag(source, "WORKER_PAGERDUTY_ON_CALLS_ENABLED"),
            pagerduty_users=_flag(source, "WORKER_PAGERDUTY_USERS_ENABLED"),
            pagerduty_teams=_flag(source, "WORKER_PAGERDUTY_TEAMS_ENABLED"),
            pagerduty_incidents=_flag(source, "WORKER_PAGERDUTY_INCIDENTS_ENABLED"),
            github_prs=_flag(source, "WORKER_GITHUB_PRS_ENABLED"),
            github_pr_reviews=_flag(source, "WORKER_GITHUB_PR_REVIEWS_ENABLED"),
            github_pr_comments=_flag(source, "WORKER_GITHUB_PR_COMMENTS_ENABLED"),
            github_cicd=_flag(source, "WORKER_GITHUB_CICD_ENABLED"),
            github_commits=_flag(source, "WORKER_GITHUB_COMMITS_ENABLED"),
            github_deployments=_flag(source, "WORKER_GITHUB_DEPLOYMENTS_ENABLED"),
            github_security=_flag(source, "WORKER_GITHUB_SECURITY_ENABLED"),
            github_files=_flag(source, "WORKER_GITHUB_FILES_ENABLED"),
            github_commit_stats=_flag(source, "WORKER_GITHUB_COMMIT_STATS_ENABLED"),
            github_blame=_flag(source, "WORKER_GITHUB_BLAME_ENABLED"),
            github_tests=_flag(source, "WORKER_GITHUB_TESTS_ENABLED"),
            github_work_items=_flag(source, "WORKER_GITHUB_WORK_ITEMS_ENABLED"),
            local_all_routes=(
                source.get(_PROVIDER_ROUTES_PRESET_ENV, "").strip().lower() == "all"
                and source.get(_DEV_HEALTH_ENV, "").strip().lower() == "local"
            ),
        )
        switches.require_complete_routes()
        return switches

    def require_complete_routes(self) -> None:
        if self.github_cicd and self.github_tests:
            raise ProviderUnitRouteError(
                "github cicd and tests switches are mutually exclusive complete-unit aliases"
            )
        if sum((self.github_prs, self.github_pr_reviews, self.github_pr_comments)) > 1:
            raise ProviderUnitRouteError(
                "github PR-social switches are mutually exclusive complete-unit aliases"
            )
        if self.gitlab_cicd and self.gitlab_tests:
            raise ProviderUnitRouteError(
                "gitlab cicd and tests switches are mutually exclusive complete-unit aliases"
            )
        if sum((self.gitlab_prs, self.gitlab_pr_reviews, self.gitlab_pr_comments)) > 1:
            raise ProviderUnitRouteError(
                "gitlab PR-social switches are mutually exclusive complete-unit aliases"
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
        if is_atomic_provider_family_direct_alias(provider, dataset):
            # Matrix readiness describes the all-five logical family. A direct
            # alias cannot be planned by the Python producer and cannot safely
            # execute the composite Go handler, so keep it at reconciliation
            # before any River enqueue rather than widening a partial writer.
            return False
        field_name = _switch_field_name(provider, dataset)
        if bool(getattr(self, field_name, False)):
            return True
        if self.local_all_routes:
            field_name = _LOCAL_ALL_ALIAS_CANONICAL.get(field_name, field_name)
        return bool(getattr(self, field_name, False))

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
