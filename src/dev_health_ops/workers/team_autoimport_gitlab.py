from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.api.services.configuration.clickhouse_identity_drift import (
    split_memberships_for_review,
)
from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    project_provider_team_rows,
    project_team_rows_with_store,
)
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveredProject,
    TeamDiscoveryService,
)
from dev_health_ops.api.services.configuration.team_membership import (
    TeamMembershipService,
)
from dev_health_ops.metrics.schemas import (
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.team_capabilities import team_provider_capabilities
from dev_health_ops.storage.clickhouse import ClickHouseStore

logger = logging.getLogger(__name__)

PROVIDER = "gitlab"
PROVIDER_ACCESS_PRIORITY = 300
BASE_SPECIFICITY = 100
CHILD_SPECIFICITY_STEP = 10
DEFAULT_GITLAB_URL = "https://gitlab.com"
_REAL_CLICKHOUSE_SINK_TYPE = ClickHouseMetricsSink


def populate(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    return asyncio.run(
        _populate_async(
            org_id=org_id,
            credentials=credentials,
            scope=scope,
            team_store=kwargs.get("team_store"),
        )
    )


async def _populate_async(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    team_store: Any | None = None,
) -> dict[str, Any]:
    strict = bool(scope.get("strict_reference_discovery"))
    if not _provider_capable():
        if strict:
            raise ValueError(
                "GitLab is not import-capable for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="provider_not_import_capable")

    token = _first_string(credentials, "token", "access_token", "private_token")
    group_path = _gitlab_group(credentials=credentials, scope=scope)
    url = (
        _first_string(credentials, "url", "base_url", "gitlab_url")
        or DEFAULT_GITLAB_URL
    )
    if not token or not group_path:
        if strict:
            raise ValueError(
                "missing GitLab credentials or group for strict reference discovery"
            )
        return _zero_summary(
            org_id=org_id, reason="missing_gitlab_credentials_or_group"
        )

    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    try:
        result = await discovery.discover_gitlab(
            token=token,
            group_path=group_path,
            url=url,
        )
    except Exception as exc:
        if strict:
            raise
        logger.info(
            "Skipping GitLab team auto-import for org_id=%s group=%s: discovery failed: %s",
            org_id,
            group_path,
            exc,
        )
        return _zero_summary(org_id=org_id, reason="provider_discovery_skipped")

    teams = result.teams
    if not teams:
        return _zero_summary(org_id=org_id, reason="no_provider_teams")

    now = datetime.now(timezone.utc)
    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()
    team_rows = _team_rows(org_id=org_id, teams=teams, now=now)
    project_rows = _project_ownership_rows(org_id=org_id, teams=teams, now=now)
    # CHAOS-3380: import GitLab PROJECTS as their own catalog rows -- mirrors
    # Linear's CHAOS-3365 native project import -- so a GitLab project name
    # resolves as an Ask Dev subject. Sourced entirely from ``result.projects``
    # (the flat, recursive listing ``discover_gitlab`` already fetched -- no
    # separate network call here, so nothing in this block can fail on its
    # own; a total discovery failure is already handled by the try/except
    # around ``discover_gitlab`` above).
    #
    # CHAOS-3380 round 2 (Codex MEDIUM): a sync run only ever SYNCS the
    # projects its enabled ``IntegrationSource`` rows selected, but discovery
    # can see (and, before this filter, would catalog) every project the
    # credential has read access to -- an unselected project would become a
    # resolvable Ask Dev subject with retained-but-unsynced work items
    # queryable through it. ``source_external_ids`` (populated by the
    # reference-discovery scope, ``workers/reference_discovery.py``
    # ``_load_discovery_context``) carries exactly the enabled sources'
    # ``IntegrationSource.external_id`` values -- GitLab's numeric project id
    # (``sync/discovery.py._map_gitlab_tuple``'s own docstring: "the
    # canonical GitLab identifier"), the SAME id this worker mints the
    # catalog row's id from. When present, only discovered projects whose
    # numeric id is in that set are cataloged.
    #
    # Residual gap, reported rather than silently left: the more common
    # POST-SYNC trigger (``workers/team_autoimport.py.
    # run_post_sync_team_autoimport``) does not thread ``source_external_ids``
    # into scope today, so this filter is a no-op on that path -- closing it
    # there needs a change to that SHARED dispatcher (all 4 providers), out
    # of this ticket's scope.
    source_external_ids = _source_external_ids(scope)
    native_project_rows = _gitlab_project_catalog_rows(
        org_id=org_id,
        projects=result.projects,
        source_external_ids=source_external_ids,
        now=now,
    )
    native_projects_filtered_by_selection = len(result.projects) - len(
        native_project_rows
    )
    # CHAOS-3380 round 3 (Codex MEDIUM -- stale selected-source set reads as
    # complete): the filter above only ever narrows discovered -> selected.
    # A selected id that discovery did NOT return at all (the project was
    # deleted/renamed out of reach, access was revoked, or discovery's own
    # pagination bound was hit before reaching it) silently vanishes from
    # the catalog with no signal -- a first import writes nothing for it, a
    # re-import leaves whatever catalog row already existed stale, and
    # neither was visible in native_projects_complete, which only reflected
    # PAGINATION truncation, not a selected id going unaccounted for.
    # Computed BEFORE writing, so the summary is honest about this run even
    # though the (possibly partial) catalog write below still happens.
    discovered_ids = {project.id for project in result.projects}
    missing_selected_source_ids = (
        sorted(source_external_ids - discovered_ids)
        if source_external_ids is not None
        else []
    )
    # Truncation on EITHER walk (teams/repo_patterns or the flat projects
    # listing), OR a selected source discovery never returned, means the
    # catalog this run wrote may be a partial view -- a partial catalog must
    # never be recorded as a complete one.
    native_projects_complete = not result.truncated and not missing_selected_source_ids
    membership_rows, _, observed_team_ids = await _membership_rows(
        org_id=org_id,
        token=token,
        url=url,
        teams=teams,
        now=now,
        resolver=resolver,
        strict=strict,
    )
    sink = _sink(scope)
    membership_rows = await _split_memberships_for_review(
        org_id=org_id,
        rows=membership_rows,
        observed_team_ids=tuple(observed_team_ids),
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    member_roster = _roster_from_memberships(membership_rows)
    for team_row in team_rows:
        team_row["members"] = member_roster.get(str(team_row["id"]), [])

    await _project_team_rows(
        org_id=org_id,
        team_rows=team_rows,
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    sink.write_team_project_ownership(project_rows)
    sink.write_team_memberships(membership_rows)
    catalog_projects = _dedupe_projects(native_project_rows)
    if hasattr(sink, "write_projects"):
        sink.write_projects(catalog_projects)

    summary: dict[str, Any] = {
        "teams_imported": len(team_rows),
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [],
        "projects_imported": len({row.project_id for row in project_rows}),
        # The Ask Dev subject catalog rows written via write_projects(), distinct
        # from "projects_imported" above (team-ownership project paths).
        "native_projects_imported": len(catalog_projects),
        # False when either discovery walk (teams or the flat projects
        # listing) hit its pagination bound, OR a selected source id never
        # showed up in discovery at all -- so a partial catalog is never
        # recorded as a full one (mirrors Linear's native_projects_complete).
        "native_projects_complete": native_projects_complete,
        # Discovered-but-uncataloged projects, almost entirely from the
        # source-selection filter above (a malformed listing entry is
        # already dropped inside discover_gitlab itself). Observability for
        # the intersection, not a correctness signal on its own.
        "native_projects_filtered_by_selection": native_projects_filtered_by_selection,
        # A selected IntegrationSource id discovery never returned -- the
        # inverse gap from the filter above (selected but absent, not
        # discovered but unselected). Surfaced by id so a caller can act on
        # exactly which source is unaccounted for, not just a count.
        "native_projects_missing_selected_source_ids": missing_selected_source_ids,
        "members_imported": len({row.member_id for row in membership_rows}),
        "team_memberships_imported": len(membership_rows),
        "team_project_ownership_imported": len(project_rows),
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }
    if result.truncated:
        summary["warnings"] = list(result.warnings)
    return summary


def _provider_capable() -> bool:
    return any(
        capability.provider == PROVIDER and capability.supports_org_drift_discovery
        for capability in team_provider_capabilities()
    )


def _zero_summary(*, org_id: str, reason: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "provider": PROVIDER,
        "org_id": org_id,
        "reason": reason,
        "teams_imported": 0,
        "projects_imported": 0,
        "members_imported": 0,
        "team_memberships_imported": 0,
        "team_project_ownership_imported": 0,
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }


def _gitlab_group(
    *, credentials: Mapping[str, Any], scope: Mapping[str, Any]
) -> str | None:
    sync_options = _mapping(scope.get("sync_options"))
    return _first_string(
        credentials,
        "group_path",
        "group",
        "owner",
    ) or _first_string(sync_options, "group_path", "group", "owner")


def _team_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        provider_team_id = str(getattr(team, "provider_team_id"))
        team_id = _team_id(provider_team_id)
        rows.append(
            {
                "id": team_id,
                "name": str(getattr(team, "name", team_id)),
                "description": getattr(team, "description", None),
                "members": [],
                "project_keys": _strings(associations.get("repo_patterns")),
                "repo_patterns": [],
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": PROVIDER,
                "native_team_key": provider_team_id,
                "parent_team_id": _parent_team_id(provider_team_id, associations),
            }
        )
    return rows


def _project_ownership_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[TeamProjectOwnershipRecord]:
    parent_by_team = _parent_by_team(teams)
    rows: list[TeamProjectOwnershipRecord] = []
    seen: set[tuple[str, str]] = set()
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        provider_team_id = str(getattr(team, "provider_team_id"))
        team_id = _team_id(provider_team_id)
        specificity = BASE_SPECIFICITY + (
            _depth(team_id, parent_by_team) * CHILD_SPECIFICITY_STEP
        )
        for project_path in _strings(associations.get("repo_patterns")):
            key = (team_id, project_path)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                TeamProjectOwnershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    project_id=project_path,
                    project_key=project_path,
                    source="provider_access",
                    is_primary=0,
                    specificity=specificity,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows


def _project_id(org_id: str, provider: str, native_id: str) -> str:
    return f"{org_id}:{provider}:{native_id}"


def _source_external_ids(scope: Mapping[str, Any]) -> set[str] | None:
    """The enabled ``IntegrationSource.external_id`` set for this sync run.

    ``None`` (not an empty set) means "not scoped" -- catalog everything
    discovery can see, today's behavior for the common post-sync trigger
    path, which does not populate this key at all (see the caller's own
    comment). An explicit empty list, in contrast, means the reference-
    discovery scope enumerated zero enabled sources -- filter everything out,
    not everything in.
    """

    raw = scope.get("source_external_ids")
    if raw is None:
        return None
    return {str(value) for value in raw if str(value).strip()}


def _gitlab_project_catalog_rows(
    *,
    org_id: str,
    projects: Iterable[GitLabDiscoveredProject],
    source_external_ids: set[str] | None,
    now: datetime,
) -> list[ProjectRecord]:
    """Map discovered GitLab projects to Ask Dev subject catalog rows.

    CHAOS-3380 round 2 (Codex HIGH -- mutable path as canonical identity):
    ``id`` is GitLab's own IMMUTABLE numeric project id, prefixed like Jira's
    catalog id (``team_autoimport_jira._project_id``) rather than raw like
    Linear's -- because unlike Linear's project id, a GitLab project's PATH
    is mutable (rename, group transfer) while its numeric id never changes.
    ``project_key`` carries the CURRENT ``path_with_namespace`` instead.

    CHAOS-3380 round 3 (Codex HIGH -- incremental sync strands historical
    rows): a scope committed against this row's id resolves work items
    through ``native_status_change._project_identity_match``'s compatibility
    arm -- ``work_items.project_id = catalog.project_key`` -- NOT a
    project_key-to-project_key match. ``providers/gitlab/normalize.py``
    deliberately does NOT write anything onto ``work_items.project_key``
    (stays empty, exactly as before this ticket); it never needed to, since
    ``project_id`` already carries the raw path for every GitLab row, old and
    new alike (an ``updated_after`` incremental sync never rewrites a row
    that has not itself changed on the provider side). Comparing THAT
    directly against this catalog row's current path is what makes rename
    and incremental-sync history keep resolving without any change to
    ``work_items`` at all -- see below.

    Rename/transfer/reuse, made explicit rather than assumed correct:

    * RENAME or TRANSFER (path changes, numeric id doesn't): the NEXT sync's
      discovery reports the NEW path under the SAME numeric id, so this
      function updates the SAME catalog row's ``project_key`` in place (a
      new ReplacingMergeTree version, same ``(org_id, provider, id)`` key).
      Work items already ingested under the OLD path (``project_id`` baked
      in at ingestion time, and for GitLab the work_item_id itself is
      ALSO path-derived -- ``f"gitlab:{path}#{iid}"`` -- a pre-existing,
      out-of-scope characteristic of GitLab work-item identity) stop
      matching this catalog row until the provider sync re-ingests them
      under the new path. This ORPHANS old history rather than merging it
      into anything -- the fail-safe direction Codex asked for -- at the
      cost of a resolution gap until the next full provider sync catches
      up; a smaller ticket than reconciling path aliases across a rename.
    * PATH REUSE (an unrelated NEW project claims the exact string an OLD,
      deleted project used to hold): the two mint DISTINCT catalog rows
      (different numeric ids), so no two catalog rows ever collide. The
      residual risk is at the WORK-ITEM level, not the catalog level: OLD
      work items still on disk with the reused path baked into their own
      ``project_id`` (from before the delete) would match the NEW catalog
      row's ``project_key`` if they are never resynced -- because
      ``work_items.project_id`` records what a path meant AT INGESTION
      TIME, and nothing here rewrites historical rows when a path is
      reassigned elsewhere. Closing this fully needs GitLab's immutable
      numeric id threaded through ``work_items.project_id`` itself (not just
      the catalog), a materially larger provider-identity change deferred
      out of this ticket -- flagged for a follow-up rather than silently
      left as an assumed non-issue. (``ask_dev_project_subject_oracle.py``
      surfaces this same residual risk explicitly, per-row, as the
      ``"path_match_unverified"`` gap kind -- CHAOS-3380 round 3, MEDIUM 3.)
    """

    rows: list[ProjectRecord] = []
    for project in projects:
        if source_external_ids is not None and project.id not in source_external_ids:
            continue
        rows.append(
            ProjectRecord(
                id=_project_id(org_id, PROVIDER, project.id),
                org_id=org_id,
                provider=PROVIDER,
                project_key=project.path_with_namespace,
                name=project.name or project.path_with_namespace,
                is_active=0 if project.archived else 1,
                updated_at=now,
                last_synced=now,
                url=project.web_url,
            )
        )
    return rows


def _dedupe_projects(rows: list[ProjectRecord]) -> list[ProjectRecord]:
    return list({(row.org_id, row.provider, row.id): row for row in rows}.values())


async def _membership_rows(
    *,
    org_id: str,
    token: str,
    url: str,
    teams: Iterable[Any],
    now: datetime,
    resolver: IdentityResolver,
    strict: bool,
) -> tuple[list[TeamMembershipRecord], dict[str, list[str]], set[tuple[str, str]]]:
    service = TeamMembershipService(session=cast(Any, None), org_id=org_id)
    rows: list[TeamMembershipRecord] = []
    roster: dict[str, list[str]] = {}
    observed_team_ids: set[tuple[str, str]] = set()
    seen: set[tuple[str, str]] = set()
    for team in teams:
        group_path = str(getattr(team, "provider_team_id"))
        team_id = _team_id(group_path)
        try:
            members = await service.discover_members_gitlab(
                token=token,
                group_path=group_path,
                url=url,
            )
        except Exception as exc:
            if strict:
                raise
            logger.info(
                "Skipping GitLab membership import for org_id=%s group=%s: %s",
                org_id,
                group_path,
                exc,
            )
            continue
        observed_team_ids.add((PROVIDER, team_id))
        for member in members:
            raw_identity = str(getattr(member, "provider_identity", "")).strip()
            if not raw_identity:
                continue
            member_id = f"gl:{raw_identity}"
            key = (team_id, member_id)
            if key in seen:
                continue
            seen.add(key)
            # Resolve the member through the SAME org alias map an assignee uses:
            # facets[0] is the alias-resolved identity (canonical email when the
            # gitlab:<username> is aliased, else gitlab:<username>) — the facet a
            # no-email assignee resolves to. It goes into raw_provider_user_id
            # (member_id is the PK and keeps the bare username), identity_facets,
            # AND the teams.members roster so BOTH attribution paths match
            # aliased and non-aliased members (CHAOS-2609, CHAOS-2625).
            facets = resolver.membership_facets(
                provider=PROVIDER,
                username=raw_identity,
                email=getattr(member, "email", None),
            ) or [raw_identity]
            roster_for_team = roster.setdefault(team_id, [])
            for facet in facets:
                if facet not in roster_for_team:
                    roster_for_team.append(facet)
            rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=getattr(member, "email", None),
                    identity_facets=facets,
                    source="provider_access",
                    is_primary=0,
                    specificity=BASE_SPECIFICITY,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows, roster, observed_team_ids


def _sink(scope: Mapping[str, Any]) -> ClickHouseMetricsSink:
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("CLICKHOUSE_URI is required for GitLab team auto-import")
    return ClickHouseMetricsSink(dsn=dsn)


def _roster_from_memberships(
    rows: Iterable[TeamMembershipRecord],
) -> dict[str, list[str]]:
    roster: dict[str, list[str]] = {}
    for row in rows:
        roster_for_team = roster.setdefault(str(row.team_id), [])
        values = tuple(row.identity_facets) or tuple(
            value for value in (row.raw_provider_user_id, row.raw_email) if value
        )
        for value in values:
            if value and value not in roster_for_team:
                roster_for_team.append(str(value))
    return roster


async def _project_team_rows(
    *,
    org_id: str,
    team_rows: list[dict[str, Any]],
    sink: ClickHouseMetricsSink,
    team_store: Any | None,
    discovered_at: datetime,
) -> None:
    if team_store is not None:
        await project_team_rows_with_store(
            store=team_store,
            org_id=org_id,
            provider="gitlab",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        await project_provider_team_rows(
            dsn=sink.dsn,
            org_id=org_id,
            provider="gitlab",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    await sink.insert_teams(team_rows)


async def _split_memberships_for_review(
    *,
    org_id: str,
    rows: list[TeamMembershipRecord],
    observed_team_ids: Sequence[tuple[str, str]],
    sink: Any,
    team_store: Any | None,
    discovered_at: datetime,
) -> list[TeamMembershipRecord]:
    if team_store is not None:
        return await split_memberships_for_review(
            store=team_store,
            org_id=org_id,
            rows=rows,
            observed_team_ids=observed_team_ids,
            discovered_at=discovered_at,
        )
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        async with ClickHouseStore(sink.dsn) as store:
            store.org_id = org_id
            return await split_memberships_for_review(
                store=store,
                org_id=org_id,
                rows=rows,
                observed_team_ids=observed_team_ids,
                discovered_at=discovered_at,
            )
    return rows


def _team_id(provider_team_id: str) -> str:
    return f"gl:{provider_team_id.removeprefix('gl:')}"


def _parent_team_id(
    provider_team_id: str, associations: Mapping[str, Any]
) -> str | None:
    explicit_parent = _first_string(
        associations, "parent_team_id", "parent_provider_team_id"
    )
    if explicit_parent:
        return _team_id(explicit_parent)
    if "/" not in provider_team_id:
        return None
    return _team_id(provider_team_id.rsplit("/", 1)[0])


def _parent_by_team(teams: Iterable[Any]) -> dict[str, str]:
    parents: dict[str, str] = {}
    team_ids = {_team_id(str(getattr(team, "provider_team_id"))) for team in teams}
    for team in teams:
        provider_team_id = str(getattr(team, "provider_team_id"))
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(provider_team_id)
        parent = _parent_team_id(provider_team_id, associations)
        if parent and parent in team_ids:
            parents[team_id] = parent
    return parents


def _depth(team_id: str, parent_by_team: Mapping[str, str]) -> int:
    depth = 0
    current = team_id
    visited: set[str] = set()
    while current in parent_by_team and current not in visited:
        visited.add(current)
        current = parent_by_team[current]
        depth += 1
    return depth


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if not isinstance(value, Iterable):
        return []
    return [str(item) for item in value if str(item).strip()]


def _first_string(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
