"""Shared helpers for the CHAOS-4323 per-category auto-import split.

``SyncConfiguration.sync_options`` used to carry a single ``auto_import_teams``
boolean that drove teams, projects, AND members import as one unit. It is now
three independent booleans: ``auto_import_teams``, ``auto_import_projects``,
``auto_import_members`` (all default ``False``; existing rows were migrated
true -> all three true, false -> all three false by
``alembic/versions/0112_split_auto_import_teams_into_three_categories.py``).

Only the best-effort, user-facing path (``run_team_autoimport`` /
``run_post_sync_team_autoimport``) honours the three flags. Strict reference
discovery (``run_team_autoimport_strict``, used by backfill and by
``workers/reference_discovery.py``) intentionally does NOT thread
``import_categories`` into the populator scope: reference discovery exists to
guarantee dispatch-blocking reference team/sprint keys resolve, not to reflect
a user's ClickHouse-attribution preference, so it keeps importing everything
it always has -- ``resolve_import_categories`` below defaults every category
to ``True`` when ``import_categories`` is absent from scope for exactly that
reason.
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

    ``scope["import_categories"]`` is only ever set by ``run_team_autoimport``
    (the non-strict, best-effort path). When it is absent -- strict reference
    discovery, backfill, or any caller that predates this split -- every
    category defaults to ``True`` so behavior is unchanged: full population,
    exactly as before CHAOS-4323.
    """

    categories = scope.get("import_categories")
    if isinstance(categories, Mapping):
        return {
            CATEGORY_TEAMS: bool(categories.get(CATEGORY_TEAMS, True)),
            CATEGORY_PROJECTS: bool(categories.get(CATEGORY_PROJECTS, True)),
            CATEGORY_MEMBERS: bool(categories.get(CATEGORY_MEMBERS, True)),
        }
    return {CATEGORY_TEAMS: True, CATEGORY_PROJECTS: True, CATEGORY_MEMBERS: True}
