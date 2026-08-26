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
    TeamDiscoveryService,
)
from dev_health_ops.api.services.configuration.team_membership import (
    TeamMembershipService,
)
from dev_health_ops.metrics.prometheus import (
    record_team_autoimport_roster_preservation_failed,
)
from dev_health_ops.metrics.schemas import TeamMembershipRecord, TeamRepoOwnershipRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.team_capabilities import (
    auto_import_capabilities,
    team_provider_capabilities,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.workers.team_autoimport_categories import (
    resolve_import_categories,
)

logger = logging.getLogger(__name__)

PROVIDER = "github"
PROVIDER_ACCESS_PRIORITY = 300
BASE_SPECIFICITY = 100
CHILD_SPECIFICITY_STEP = 10
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
    categories = resolve_import_categories(scope)
    # CHAOS-4323 defense-in-depth: AND against this provider's own capability
    # regardless of what scope requested -- GitHub has no "Projects" import
    # (projects_imported is always 0 below), so want_projects is always False
    # here even if a stale config or a bypassed caller set it True.
    capability = auto_import_capabilities(PROVIDER)
    want_teams = categories["teams"] and capability.teams
    want_projects = categories["projects"] and capability.projects
    want_members = categories["members"] and capability.members
    if not (want_teams or want_projects or want_members):
        return _zero_summary(org_id=org_id, reason="no_categories_selected")
    if not _provider_capable():
        if strict:
            raise ValueError(
                "GitHub is not import-capable for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="provider_not_import_capable")

    try:
        token = _github_access_token(credentials)
    except Exception as exc:
        if strict:
            raise
        logger.info(
            "Skipping GitHub team auto-import for org_id=%s: credential token exchange failed: %s",
            org_id,
            exc,
        )
        return _zero_summary(org_id=org_id, reason="provider_discovery_skipped")
    org_name = _github_org(credentials=credentials, scope=scope)
    if not token or not org_name:
        if strict:
            raise ValueError(
                "missing GitHub credentials or org for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="missing_github_credentials_or_org")

    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    try:
        teams = await discovery.discover_github(token=token, org_name=org_name)
    except Exception as exc:
        if strict:
            raise
        logger.info(
            "Skipping GitHub team auto-import for org_id=%s org=%s: discovery failed: %s",
            org_id,
            org_name,
            exc,
        )
        return _zero_summary(org_id=org_id, reason="provider_discovery_skipped")

    if not teams:
        return _zero_summary(org_id=org_id, reason="no_provider_teams")

    now = datetime.now(timezone.utc)
    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()
    team_rows = _team_rows(org_id=org_id, teams=teams, now=now)
    # GitHub has no "Projects" import concept at all -- auto_import_capabilities
    # ("github").projects is False, so want_projects is always False (see the
    # capability clamp above) and projects_imported stays 0 below. Repo
    # ownership is derived PURELY from team associations (no separate
    # "projects" discovery call exists), so it is a team-import artifact, not
    # a projects one -- gated on "teams", matching pre-CHAOS-4323 behavior
    # where the single auto_import_teams flag wrote both together.
    repo_rows = _repo_ownership_rows(org_id=org_id, teams=teams, now=now)
    if want_members:
        membership_rows, _, observed_team_ids = await _membership_rows(
            org_id=org_id,
            token=token,
            org_name=org_name,
            teams=teams,
            now=now,
            resolver=resolver,
            strict=strict,
        )
    else:
        membership_rows, observed_team_ids = [], set()
    sink = _sink(scope)
    membership_rows = await _split_memberships_for_review(
        org_id=org_id,
        rows=membership_rows,
        observed_team_ids=tuple(observed_team_ids),
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    # CHAOS-4323 round 2 (codex adversarial-review, HIGH): roster_write_safe
    # gates ONLY the team-dimension write (the row carrying "members"). A
    # roster-preservation read failure must never fall through to writing an
    # unconfirmed empty roster -- it skips that write entirely instead.
    roster_write_safe = True
    if want_members:
        member_roster = _roster_from_memberships(membership_rows)
        for team_row in team_rows:
            team_row["members"] = member_roster.get(str(team_row["id"]), [])
    elif want_teams:
        # A teams-only run (members off) must not erase a previously-imported
        # roster by writing an empty "members" list -- preserve whatever is
        # currently persisted.
        existing_members = _existing_team_members(
            sink=sink,
            org_id=org_id,
            provider=PROVIDER,
            team_ids=[str(row["id"]) for row in team_rows],
        )
        if existing_members is None:
            roster_write_safe = False
            record_team_autoimport_roster_preservation_failed(provider=PROVIDER)
        else:
            for team_row in team_rows:
                team_row["members"] = existing_members.get(str(team_row["id"]), [])

    if want_teams and roster_write_safe:
        await _project_team_rows(
            org_id=org_id,
            team_rows=team_rows,
            sink=sink,
            team_store=team_store,
            discovered_at=now,
        )
    if want_teams:
        sink.write_team_repo_ownership(repo_rows)
    if want_members:
        sink.write_team_memberships(membership_rows)

    return {
        "teams_imported": len(team_rows) if (want_teams and roster_write_safe) else 0,
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [],
        "projects_imported": 0,
        "members_imported": len({row.member_id for row in membership_rows})
        if want_members
        else 0,
        "team_memberships_imported": len(membership_rows) if want_members else 0,
        "team_project_ownership_imported": 0,
        "team_repo_ownership_imported": len(repo_rows) if want_teams else 0,
        "work_item_team_attributions_imported": 0,
        "roster_preservation_failed": not roster_write_safe,
    }


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


def _github_access_token(credentials: Mapping[str, Any]) -> str | None:
    token = _first_string(credentials, "token", "access_token", "github_token")
    if token:
        return token

    from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider
    from dev_health_ops.credentials.resolver import github_credentials_from_mapping

    github_credentials = github_credentials_from_mapping(dict(credentials))
    if github_credentials is None or not github_credentials.is_app_auth:
        return None
    if (
        github_credentials.app_id is None
        or github_credentials.private_key is None
        or github_credentials.installation_id is None
    ):
        return None
    return GitHubAppTokenProvider(
        app_id=github_credentials.app_id,
        private_key=github_credentials.private_key,
        installation_id=github_credentials.installation_id,
        api_base_url=github_credentials.base_url or "https://api.github.com",
    ).get_token()


def _github_org(
    *, credentials: Mapping[str, Any], scope: Mapping[str, Any]
) -> str | None:
    sync_options = _mapping(scope.get("sync_options"))
    return _first_string(
        credentials,
        "org",
        "organization",
        "org_name",
        "owner",
    ) or _first_string(sync_options, "org", "organization", "org_name", "owner")


def _team_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(str(getattr(team, "provider_team_id")))
        rows.append(
            {
                "id": team_id,
                "name": str(getattr(team, "name", team_id)),
                "description": getattr(team, "description", None),
                "members": [],
                "project_keys": [],
                "repo_patterns": _strings(associations.get("repo_patterns")),
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": PROVIDER,
                "native_team_key": str(getattr(team, "provider_team_id")),
                "parent_team_id": _parent_team_id(associations),
            }
        )
    return rows


def _repo_ownership_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[TeamRepoOwnershipRecord]:
    parent_by_team = _parent_by_team(teams)
    rows: list[TeamRepoOwnershipRecord] = []
    seen: set[tuple[str, str]] = set()
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(str(getattr(team, "provider_team_id")))
        specificity = BASE_SPECIFICITY + (
            _depth(team_id, parent_by_team) * CHILD_SPECIFICITY_STEP
        )
        for repo_full_name in _strings(associations.get("repo_patterns")):
            key = (team_id, repo_full_name)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                TeamRepoOwnershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    repo_full_name=repo_full_name,
                    match_type="exact",
                    source="provider_access",
                    is_primary=0,
                    specificity=specificity,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows


async def _membership_rows(
    *,
    org_id: str,
    token: str,
    org_name: str,
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
        team_slug = str(getattr(team, "provider_team_id"))
        team_id = _team_id(team_slug)
        try:
            members = await service.discover_members_github(
                token=token,
                org_name=org_name,
                team_slug=team_slug,
            )
        except Exception as exc:
            if strict:
                raise
            logger.info(
                "Skipping GitHub membership import for org_id=%s team=%s: %s",
                org_id,
                team_slug,
                exc,
            )
            continue
        observed_team_ids.add((PROVIDER, team_id))
        for member in members:
            raw_identity = str(getattr(member, "provider_identity", "")).strip()
            if not raw_identity:
                continue
            member_id = f"gh:{raw_identity}"
            key = (team_id, member_id)
            if key in seen:
                continue
            seen.add(key)
            # Resolve the member through the SAME org alias map an assignee uses:
            # facets[0] is the alias-resolved identity (canonical email when the
            # github:<login> is aliased, else github:<login>) — the facet a
            # no-email assignee resolves to. It goes into raw_provider_user_id
            # (member_id is the PK and keeps the bare login), identity_facets,
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
        raise ValueError("CLICKHOUSE_URI is required for GitHub team auto-import")
    return ClickHouseMetricsSink(dsn=dsn)


def _existing_team_members(
    *, sink: Any, org_id: str, provider: str, team_ids: list[str]
) -> dict[str, list[str]] | None:
    """Read of the CURRENTLY persisted roster for these teams, for a
    members-off run to carry forward instead of overwriting it with [].

    CHAOS-4323 round 2 (codex adversarial-review, HIGH): the first version of
    this helper swallowed a failed read and returned {}, which the caller
    then treated as "these teams truly have no members" and wrote — silently
    reproducing the exact data loss this fix exists to prevent, only now
    triggered by ClickHouse being unavailable/degraded instead of by the
    members flag. Returns ``None`` (never a substitute {}) whenever the
    current roster genuinely could not be confirmed, so the caller can fail
    CLOSED (skip the team-dimension write for this run) rather than write an
    empty roster it never actually verified was empty. An empty dict is only
    ever returned when there is nothing to look up (``team_ids`` empty) or
    the query genuinely found no matching rows (a real, confirmed answer).
    Same query shape as
    ``clickhouse_team_drift_projector.ClickHouseTeamDriftProjector._team_row``.

    CHAOS-4323 round 3 (codex adversarial-review, HIGH): deliberately does
    NOT filter on ``provider`` in SQL (``provider`` stays a parameter, used
    only for logging). ``migrations/clickhouse/002_teams.sql``'s
    ``ReplacingMergeTree(updated_at) ORDER BY (id)`` -- confirmed still true
    by ``024_add_org_id.sql``'s own comment that org_id was added as a
    plain column, NOT the sort key -- means ``teams`` deduplicates on ``id``
    ALONE. An admin edit of an existing team writes ``provider=""``
    (``clickhouse_team_admin.py``); if that write's ``updated_at`` is
    latest, ``FINAL`` returns that provider=""  row for the id, and a query
    that also filtered on this provider would see zero rows -- misread as
    "team doesn't exist yet" instead of "exists, under a different
    provider tag" -- and go on to write an empty roster, the exact bug this
    whole helper exists to prevent. Filtering only on ``id`` (matching the
    table's true identity) closes that gap.
    """
    if not team_ids:
        return {}
    if not hasattr(sink, "query_dicts"):
        logger.warning(
            "Cannot confirm existing team rosters for org_id=%s provider=%s "
            "(sink has no query_dicts) -- skipping the team-dimension write "
            "for this members-off run rather than risk erasing rosters",
            org_id,
            provider,
        )
        return None
    try:
        rows = sink.query_dicts(
            """
            SELECT id, members
            FROM teams FINAL
            WHERE org_id = {org_id:String}
              AND id IN {team_ids:Array(String)}
            """,
            {"org_id": org_id, "team_ids": team_ids},
        )
    except Exception:
        logger.warning(
            "Could not read existing team rosters for org_id=%s provider=%s "
            "-- skipping the team-dimension write for this members-off run "
            "rather than risk erasing rosters",
            org_id,
            provider,
            exc_info=True,
        )
        return None
    return {str(row.get("id")): list(row.get("members") or []) for row in rows}


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
            provider="github",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        await project_provider_team_rows(
            dsn=sink.dsn,
            org_id=org_id,
            provider="github",
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
    return f"gh:{provider_team_id.removeprefix('gh:')}"


def _parent_team_id(associations: Mapping[str, Any]) -> str | None:
    parent = _first_string(associations, "parent_team_id", "parent_provider_team_id")
    if parent is None:
        return None
    return _team_id(parent)


def _parent_by_team(teams: Iterable[Any]) -> dict[str, str]:
    parents: dict[str, str] = {}
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        parent = _parent_team_id(associations)
        if parent:
            parents[_team_id(str(getattr(team, "provider_team_id")))] = parent
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
