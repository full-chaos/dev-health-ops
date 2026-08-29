"""Shared helpers for the CHAOS-4323 per-category auto-import split.

``SyncConfiguration.sync_options`` used to carry a single ``auto_import_teams``
boolean that drove teams, projects, AND members import as one unit. It is now
three independent booleans: ``auto_import_teams``, ``auto_import_projects``,
``auto_import_members`` (all default ``False``; existing rows were migrated
true -> all three true, false -> all three false by
``alembic/versions/0112_split_auto_import_teams_into_three_categories.py``).

CHAOS-4437: both the best-effort, user-facing path (``run_team_autoimport`` /
``run_post_sync_team_autoimport``) AND strict reference discovery
(``run_team_autoimport_strict``, used by backfill and by
``workers/reference_discovery.py``) now honour the three flags -- writing
teams/team_memberships/team_project_ownership rows for a category the org
disabled was never the point of reference discovery, only a side effect of
reusing the same ``populate()`` functions as the user-facing path (see
CHAOS-4430's proof). What reference discovery genuinely needs unconditionally
-- resolving dispatch-blocking reference team/sprint *keys* -- does NOT
require those write-side rows: sprint/cycle discovery in the Jira and Linear
populators is deliberately NOT gated on any category (see those modules'
``populate()``), and dispatch itself (``dispatch_sync_run``) only checks the
``SyncRunReferenceDiscovery`` ledger's ``status`` column, never CH team/sprint
rows directly -- the readback verifier is a self-consistency check against
whatever the populate summary claims, not an external requirement. So gating
the WRITE on the org's selection is safe; only the always-on reference-data
paths (sprints/cycles) must keep running regardless of category selection.
``resolve_import_categories`` below still defaults every category to
``True`` when ``import_categories`` is absent from scope, for callers that
predate this split entirely.
"""

from __future__ import annotations

from collections.abc import Mapping

CATEGORY_TEAMS = "teams"
CATEGORY_PROJECTS = "projects"
CATEGORY_MEMBERS = "members"

_CATEGORY_TO_SYNC_OPTION = {
    CATEGORY_TEAMS: "auto_import_teams",
    CATEGORY_PROJECTS: "auto_import_projects",
    CATEGORY_MEMBERS: "auto_import_members",
}


def import_categories_from_sync_options(
    sync_options: Mapping[str, object] | None,
) -> dict[str, bool]:
    """Read the three independent flags off a sync config's ``sync_options``.

    ``None`` means "no sync_options context at all" -- a caller that never
    had a config to read from (e.g. ``run_team_autoimport`` invoked with a
    scope that carries no ``sync_options`` key). That is treated as
    unrestricted: every category defaults ``True``, preserving the
    pre-CHAOS-4323 behavior for call sites that predate the split rather than
    silently importing nothing for them.

    An actual mapping (even ``{}``) is authoritative -- it came from a real
    ``SyncConfiguration.sync_options`` (a NOT NULL column, always a dict in
    production). Each category then defaults to ``False`` when its key is
    absent, matching the wizard's off-by-default checkboxes and the 0112
    migration's normalization of pre-existing rows.
    """

    if sync_options is None:
        return {category: True for category in _CATEGORY_TO_SYNC_OPTION}
    return {
        category: bool(sync_options.get(option_key, False))
        for category, option_key in _CATEGORY_TO_SYNC_OPTION.items()
    }


def resolve_import_categories(scope: Mapping[str, object]) -> dict[str, bool]:
    """Read the per-category selection a populator should honour from scope.

    ``scope["import_categories"]`` is set by both ``run_team_autoimport``
    (best-effort) and, since CHAOS-4437, ``run_team_autoimport_strict``
    (called by reference discovery AND by backfill's
    ``run_backfill_for_config``) -- every production caller now threads the
    org's real selection into the populator. When it is absent -- a caller
    that predates this split, or a direct test call -- every category
    defaults to ``True`` (unrestricted), matching behavior from before
    CHAOS-4323.
    """

    categories = scope.get("import_categories")
    if isinstance(categories, Mapping):
        return {
            CATEGORY_TEAMS: bool(categories.get(CATEGORY_TEAMS, True)),
            CATEGORY_PROJECTS: bool(categories.get(CATEGORY_PROJECTS, True)),
            CATEGORY_MEMBERS: bool(categories.get(CATEGORY_MEMBERS, True)),
        }
    return {CATEGORY_TEAMS: True, CATEGORY_PROJECTS: True, CATEGORY_MEMBERS: True}
