from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TeamProviderCapability:
    provider: str
    supports_org_drift_discovery: bool
    unsupported_reason: str | None = None


ORG_TEAM_DRIFT_CAPABILITIES: tuple[TeamProviderCapability, ...] = (
    TeamProviderCapability("github", True),
    TeamProviderCapability("gitlab", True),
    TeamProviderCapability("jira", True),
    TeamProviderCapability("linear", True),
    TeamProviderCapability("ms-teams", True),
)


def team_provider_capabilities() -> tuple[TeamProviderCapability, ...]:
    return ORG_TEAM_DRIFT_CAPABILITIES


def org_drift_capable_providers() -> tuple[str, ...]:
    return tuple(
        capability.provider
        for capability in ORG_TEAM_DRIFT_CAPABILITIES
        if capability.supports_org_drift_discovery
    )


# ---------------------------------------------------------------------------
# CHAOS-4323: per-category auto-import capability (teams / projects / members)
# ---------------------------------------------------------------------------
#
# ``ORG_TEAM_DRIFT_CAPABILITIES`` above is a single yes/no per provider ("can
# this provider do org-drift discovery at all"). The wizard's three
# independent checkboxes need a finer-grained answer per CATEGORY: a provider
# can be capable of team/member discovery while having no analog for one
# category at all. GitHub is the concrete case today -- its populator
# (``workers/team_autoimport_github.py``) has never had a "Projects" import;
# it only ever emits ``projects_imported=0`` and writes ownership via
# ``team_repo_ownership`` instead. This is the single source of truth both the
# API (rejects selecting an unsupported category) and the populators
# (independently clamp an unsupported category off, defense-in-depth even if
# a stale/bypassed config somehow carries one) read from.

CATEGORY_TEAMS = "teams"
CATEGORY_PROJECTS = "projects"
CATEGORY_MEMBERS = "members"
_AUTO_IMPORT_CATEGORIES = (CATEGORY_TEAMS, CATEGORY_PROJECTS, CATEGORY_MEMBERS)

# category -> the sync_options JSON key it corresponds to (matches
# team_autoimport_categories._CATEGORY_TO_SYNC_OPTION -- duplicated rather
# than imported to keep this module free of a workers/ dependency, since
# providers/ is imported by workers/, not the other way around).
_CATEGORY_TO_SYNC_OPTION = {
    CATEGORY_TEAMS: "auto_import_teams",
    CATEGORY_PROJECTS: "auto_import_projects",
    CATEGORY_MEMBERS: "auto_import_members",
}

_UNSUPPORTED_PROVIDER_REASON = (
    "provider does not support team/project/member auto-import"
)


@dataclass(frozen=True)
class AutoImportCapability:
    teams: bool
    projects: bool
    members: bool
    # category -> human-readable reason, present ONLY for a category this
    # dataclass marks unsupported (False). Never populated for a supported one.
    reasons: Mapping[str, str] = field(default_factory=dict)

    def supports(self, category: str) -> bool:
        return {
            CATEGORY_TEAMS: self.teams,
            CATEGORY_PROJECTS: self.projects,
            CATEGORY_MEMBERS: self.members,
        }[category]


_AUTO_IMPORT_CAPABILITIES: dict[str, AutoImportCapability] = {
    "github": AutoImportCapability(
        teams=True,
        projects=False,
        members=True,
        reasons={
            "projects": "GitHub attributes ownership via repos, not projects.",
        },
    ),
    "gitlab": AutoImportCapability(teams=True, projects=True, members=True),
    "jira": AutoImportCapability(teams=True, projects=True, members=True),
    "linear": AutoImportCapability(teams=True, projects=True, members=True),
}

_UNSUPPORTED_PROVIDER_CAPABILITY = AutoImportCapability(
    teams=False,
    projects=False,
    members=False,
    reasons={
        category: _UNSUPPORTED_PROVIDER_REASON for category in _AUTO_IMPORT_CATEGORIES
    },
)


def auto_import_capabilities(provider: str) -> AutoImportCapability:
    """The per-category auto-import capability for one provider.

    An unrecognized provider (no team_autoimport_<provider> populator) gets
    every category unsupported -- mirrors ``run_team_autoimport``'s own
    "no populator module" skip for the same providers.
    """

    return _AUTO_IMPORT_CAPABILITIES.get(
        provider.strip().lower(), _UNSUPPORTED_PROVIDER_CAPABILITY
    )


def all_auto_import_capabilities() -> dict[str, AutoImportCapability]:
    """Every known provider's capability, for a single-source-of-truth API read."""

    return dict(_AUTO_IMPORT_CAPABILITIES)


def unsupported_auto_import_categories(
    provider: str, sync_options: Mapping[str, object]
) -> dict[str, str]:
    """category -> reason, for every category ``sync_options`` requests (True)
    that ``provider`` cannot supply. Empty when every requested category is
    supported (including when nothing is requested at all)."""

    capability = auto_import_capabilities(provider)
    unsupported: dict[str, str] = {}
    for category, option_key in _CATEGORY_TO_SYNC_OPTION.items():
        if not bool(sync_options.get(option_key)):
            continue
        if not capability.supports(category):
            unsupported[category] = capability.reasons.get(
                category, _UNSUPPORTED_PROVIDER_REASON
            )
    return unsupported


def malformed_auto_import_category_values(
    sync_options: Mapping[str, object],
) -> dict[str, object]:
    """The subset of the three ``sync_options`` keys that ARE PRESENT but are
    not a strict ``bool`` (e.g. the string ``"false"``, ``1``, ``null``).

    CHAOS-4323 (codex adversarial-review): Python's own read of these flags
    (``team_autoimport_categories.import_categories_from_sync_options``) uses
    ``bool(value)`` truthiness -- the string ``"false"`` is truthy there. Go's
    ``native_post_sync.go`` dispatch gate instead compares the stored JSON
    text exactly to the literal ``'true'``. The same persisted config would
    then disagree between the two readers on whether to dispatch at all.
    Rejecting anything but ``true``/``false``/absent at every write boundary
    (create, update, batch create) means a malformed value can never reach
    either reader in the first place -- closing the gap at the source rather
    than trying to make two independent coercions agree.
    """

    return {
        option_key: sync_options[option_key]
        for option_key in _CATEGORY_TO_SYNC_OPTION.values()
        if option_key in sync_options and not isinstance(sync_options[option_key], bool)
    }
