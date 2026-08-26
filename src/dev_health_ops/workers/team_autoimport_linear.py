from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Coroutine, Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, timezone
from threading import Thread
from typing import Any, Protocol, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

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
from dev_health_ops.credentials.resolver import linear_credentials_from_mapping
from dev_health_ops.metrics.prometheus import (
    record_team_autoimport_roster_preservation_failed,
)
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.work_items import Sprint
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.linear.client import LinearAuth, LinearClient
from dev_health_ops.providers.linear.normalize import linear_cycle_to_sprint
from dev_health_ops.providers.team_capabilities import auto_import_capabilities
from dev_health_ops.workers.team_autoimport_categories import (
    resolve_import_categories,
)

logger = logging.getLogger(__name__)

#: Placeholder timestamp for rows read back purely for comparison. Never written.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_T = TypeVar("_T")
_REAL_CLICKHOUSE_SINK_TYPE = ClickHouseMetricsSink


class _DimensionSink(Protocol):
    def write_projects(self, rows: Sequence[ProjectRecord]) -> None: ...
    def write_members(self, rows: Sequence[MemberRecord]) -> None: ...
    def write_team_memberships(self, rows: Sequence[TeamMembershipRecord]) -> None: ...
    def write_team_project_ownership(
        self, rows: Sequence[TeamProjectOwnershipRecord]
    ) -> None: ...
    def write_sprints(self, rows: Sequence[Sprint]) -> None: ...
    async def insert_teams(self, teams: list[dict[str, Any]]) -> None: ...
    def close(self) -> None: ...


def _run(coro: Coroutine[Any, Any, _T]) -> _T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: _T | None = None
    error: BaseException | None = None

    def _target() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(coro)
        except BaseException as exc:
            error = exc

    thread = Thread(target=_target)
    thread.start()
    thread.join()
    if error is not None:
        raise error
    return cast(_T, result)


def _sink_from_kwargs(
    scope: dict[str, Any], kwargs: dict[str, Any]
) -> tuple[_DimensionSink, bool]:
    injected = kwargs.get("sink")
    if injected is not None:
        return cast(_DimensionSink, injected), False
    return cast(_DimensionSink, ClickHouseMetricsSink(dsn=_clickhouse_dsn(scope))), True


def _clickhouse_dsn(scope: dict[str, Any]) -> str:
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("ClickHouse DSN is required for Linear team auto-import")
    return dsn


def _existing_team_members(
    *, sink: Any, org_id: str, provider: str, team_ids: list[str]
) -> dict[str, list[str]] | None:
    """Read of the CURRENTLY persisted roster for these teams, for a
    members-off run to carry forward instead of overwriting it with [].

    CHAOS-4323 round 2 (codex adversarial-review, HIGH): returns ``None``
    (never a substitute {}) whenever the current roster genuinely could not
    be confirmed, so the caller fails CLOSED (skips the team-dimension
    write) rather than write an empty roster it never actually verified was
    empty. Deliberately does NOT filter on ``provider`` in SQL (round 3:
    ``teams`` dedups on ``id`` alone -- an admin-edited provider="" row can
    otherwise hide the existing team from a provider-filtered query). See
    the identical helper's docstring in team_autoimport_github.py for the
    full rationale.
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


def _team_store_from_kwargs(sink: _DimensionSink, kwargs: dict[str, Any]) -> Any | None:
    return kwargs.get("team_store") or (
        sink if hasattr(sink, "insert_team_provider_observations") else None
    )


def _team_id(team: Any) -> str:
    return str(team.provider_team_id)


def _project_id(org_id: str, provider: str, project_key: str) -> str:
    return f"{org_id}:{provider}:{project_key}"


def _linear_team_row(
    *,
    org_id: str,
    team_id: str,
    name: str,
    description: str | None,
    project_keys: list[str],
    now: datetime,
) -> dict[str, Any]:
    """Build the concrete team dimension row written by Linear auto-import."""

    return {
        "id": team_id,
        "name": name,
        "description": description,
        "members": [],
        "project_keys": project_keys,
        "repo_patterns": [],
        "is_active": True,
        "updated_at": now,
        "org_id": org_id,
        "provider": "linear",
        "native_team_key": team_id,
        "parent_team_id": None,
    }


def _project_is_active(node: Mapping[str, Any]) -> int:
    """Activity for a Linear project is retirement, NOT lifecycle state.

    A project whose status is ``completed``/``canceled`` is still a legitimate
    thing to ask Ask Dev about, and ``scope_catalog`` filters the catalog on
    ``is_active = 1`` — so keying activity off lifecycle would make every
    finished project unresolvable the moment it finished. Lifecycle is carried
    in its own ``state`` column instead. Only a project Linear itself retired is
    inactive.

    Retirement has TWO distinct forms in Linear and both must count. ``archivedAt``
    is archival; ``trashed`` is the soft delete a project passes through before it
    is purged, and a trashed project is not an archived one. Reading only
    ``archivedAt`` would leave a deleted project resolvable indefinitely.

    This is a real predicate only because the worker asks for archived projects
    (``include_archived=True``). Under the connection's default they are simply
    absent from the response, and a worker that writes only what it is handed can
    never retire a row it stops being told about.
    """

    return 0 if (node.get("archivedAt") or node.get("trashed")) else 1


def _project_state(node: Mapping[str, Any]) -> str:
    """The project's lifecycle CATEGORY, from the non-deprecated ``status``.

    ``status.type`` is the closed vocabulary (backlog/planned/started/paused/
    completed/canceled) that the deprecated ``Project.state`` string used to
    carry. ``status.name`` is the workspace's own customisable label for that
    status and is deliberately NOT stored here: the ratified column set is three
    columns, and a category is what a consumer can reason about across
    workspaces. It is selected by the query so CHAOS-3368 can use it without a
    schema change.
    """

    status = node.get("status")
    if not isinstance(status, Mapping):
        return ""
    return str(status.get("type") or "")


def _project_target_date(value: Any) -> date | None:
    """Linear sends ``targetDate`` as a bare ``YYYY-MM-DD`` day, not an instant."""

    if not value:
        return None

    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _project_team_edges(node: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """Return the native Linear team id/key edges in producer order."""

    teams = node.get("teams")
    if not isinstance(teams, Mapping):
        return [], []
    nodes = teams.get("nodes")
    if not isinstance(nodes, list):
        return [], []
    team_ids: list[str] = []
    team_keys: list[str] = []
    for team in nodes:
        if not isinstance(team, Mapping):
            continue
        team_id = str(team.get("id") or "").strip()
        team_key = str(team.get("key") or "").strip()
        if team_id and team_id not in team_ids:
            team_ids.append(team_id)
        if team_key and team_key not in team_keys:
            team_keys.append(team_key)
    return team_ids, team_keys


def _project_lead(node: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Return the selected lead identity fields without widening the row."""

    lead = node.get("lead")
    if not isinstance(lead, Mapping):
        return None, None, None

    def _optional(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    return (
        _optional(lead.get("id")),
        _optional(lead.get("name")),
        _optional(lead.get("email")),
    )


def _linear_project_records(
    nodes: Iterable[Mapping[str, Any]], *, org_id: str, now: datetime
) -> list[ProjectRecord]:
    """Normalize Linear's ``PROJECTS_QUERY`` nodes into catalog project rows.

    ``id`` is the raw Linear project UUID — deliberately the SAME id space that
    ``work_items.project_id`` already carries — so a scope committed against one
    of these rows selects work items with no query change. ``project_key`` is
    ``None`` because a Linear project has no key; the team-derived
    ``{org}:linear:{team_key}`` attribution rows are a separate id space and are
    unaffected.
    """

    rows: list[ProjectRecord] = []
    for node in nodes:
        project_id = str(node.get("id") or "").strip()
        if not project_id:
            continue
        team_ids, team_keys = _project_team_edges(node)
        lead_id, lead_name, lead_email = _project_lead(node)
        rows.append(
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key=None,
                name=str(node.get("name") or project_id),
                is_active=_project_is_active(node),
                # Lifecycle stored, not interpreted (CHAOS-3365). Surfacing it
                # as an answer fact is CHAOS-3368; this only has to persist
                # Linear's own vocabulary faithfully rather than remap it.
                state=_project_state(node),
                target_date=_project_target_date(node.get("targetDate")),
                url=str(node.get("url") or ""),
                # OBSERVATION time, not Linear's mtime. ``updated_at`` is the
                # ReplacingMergeTree version column, and retirement tombstones
                # are stamped when the absence is observed, so versioning active
                # rows by provider mtime would mix two clocks in one ordering.
                # A project that came back after a false absence carries an
                # UNCHANGED, older mtime and would lose to its own tombstone
                # forever — a transient mistake made permanent. Versioning every
                # native row by observation time guarantees the opposite: the
                # most recent look at the world always wins.
                updated_at=now,
                last_synced=now,
                team_ids=team_ids,
                team_keys=team_keys,
                lead_id=lead_id,
                lead_name=lead_name,
                lead_email=lead_email,
            )
        )
    return rows


def _member_id(provider: str, provider_identity: str) -> str:
    return f"{provider}:{provider_identity.strip().lower()}"


def _provider_identities(provider: str, provider_identity: str) -> str:
    return json.dumps({provider: [provider_identity]}, sort_keys=True)


def _dedupe_projects(rows: list[ProjectRecord]) -> list[ProjectRecord]:
    return list({(row.org_id, row.provider, row.id): row for row in rows}.values())


def _dedupe_members(rows: list[MemberRecord]) -> list[MemberRecord]:
    return list({(row.org_id, row.member_id): row for row in rows}.values())


def _dedupe_memberships(
    rows: list[TeamMembershipRecord],
) -> list[TeamMembershipRecord]:
    return list(
        {
            (row.org_id, row.provider, row.team_id, row.member_id, row.source): row
            for row in rows
        }.values()
    )


def _dedupe_ownership(
    rows: list[TeamProjectOwnershipRecord],
) -> list[TeamProjectOwnershipRecord]:
    return list(
        {
            (row.org_id, row.provider, row.project_id, row.team_id, row.source): row
            for row in rows
        }.values()
    )


def populate(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    strict = bool(scope.get("strict_reference_discovery"))
    categories = resolve_import_categories(scope)
    # CHAOS-4323 defense-in-depth: AND against this provider's own capability
    # regardless of what scope requested (Linear supports all three today).
    capability = auto_import_capabilities("linear")
    want_teams = categories["teams"] and capability.teams
    want_projects = categories["projects"] and capability.projects
    want_members = categories["members"] and capability.members
    if not (want_teams or want_projects or want_members):
        return {
            "status": "skipped",
            "reason": "no_categories_selected",
            "projects_imported": 0,
            "native_projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }
    linear_credentials = linear_credentials_from_mapping(credentials)
    if linear_credentials is None:
        if strict:
            raise ValueError(
                "missing Linear credentials for strict reference discovery"
            )
        return {
            "status": "skipped",
            "reason": "missing_linear_credentials",
            "projects_imported": 0,
            "native_projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }

    now = datetime.now(timezone.utc)
    resolver = load_identity_resolver()
    discovery = TeamDiscoveryService(None, org_id)
    membership = TeamMembershipService(cast(AsyncSession, None), org_id)
    teams = _run(discovery.discover_linear(api_key=linear_credentials.api_key))

    team_rows: list[dict[str, Any]] = []
    project_rows: list[ProjectRecord] = []
    member_rows: list[MemberRecord] = []
    membership_rows: list[TeamMembershipRecord] = []
    ownership_rows: list[TeamProjectOwnershipRecord] = []
    sprint_rows: list[Sprint] = []

    for team in teams:
        team_id = _team_id(team)
        associations = dict(team.associations or {})
        project_keys = [str(key) for key in associations.get("project_keys", []) if key]
        if not project_keys:
            project_keys = [team_id]

        team_rows.append(
            _linear_team_row(
                org_id=org_id,
                team_id=team_id,
                name=team.name,
                description=team.description,
                project_keys=project_keys,
                now=now,
            )
        )

        for project_key in project_keys:
            project_rows.append(
                ProjectRecord(
                    id=_project_id(org_id, "linear", project_key),
                    org_id=org_id,
                    provider="linear",
                    project_key=project_key,
                    name=team.name,
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            )
            ownership_rows.append(
                TeamProjectOwnershipRecord(
                    org_id=org_id,
                    provider="linear",
                    team_id=team_id,
                    project_id=_project_id(org_id, "linear", project_key),
                    project_key=project_key,
                    source="native",
                    is_primary=1,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )

        discovered_members = (
            _run(
                membership.discover_members_linear(
                    api_key=linear_credentials.api_key,
                    team_key=team_id,
                )
            )
            if want_members
            else []
        )
        roster_facets: list[str] = []
        for member in discovered_members:
            member_id = _member_id("linear", member.provider_identity)
            facets = resolver.membership_facets(
                provider="linear",
                username=member.provider_identity,
                email=member.email,
            ) or [member.provider_identity]
            for facet in facets:
                if facet not in roster_facets:
                    roster_facets.append(facet)
            member_rows.append(
                MemberRecord(
                    org_id=org_id,
                    member_id=member_id,
                    name=member.display_name or member.provider_identity,
                    email=member.email,
                    provider_identities=_provider_identities(
                        "linear", member.provider_identity
                    ),
                    is_active=1,
                    updated_at=now,
                )
            )
            membership_rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider="linear",
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=member.email,
                    identity_facets=facets,
                    source="native",
                    is_primary=1,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )
        team_rows[-1]["members"] = roster_facets

    native_project_rows: list[ProjectRecord] = []
    # Starts FALSE and is set true only where enumeration demonstrably finished.
    # Defaulting to true meant a failure BEFORE the inner handler -- constructing
    # the client, for instance -- fell through the outer except with the flag
    # untouched, so a run that fetched nothing reported "0 projects, complete",
    # which reads as "this workspace has no projects".
    native_projects_complete = False
    try:
        with LinearClient(
            auth=LinearAuth(api_key=linear_credentials.api_key), org_id=org_id
        ) as client:
            # CHAOS-3365: import Linear PROJECTS as their own catalog rows so a
            # Linear project name resolves as an Ask Dev subject. Kept in its own
            # try so a cycles failure below cannot discard projects and a projects
            # failure cannot discard cycles — the two are independent fetches.
            # CHAOS-4323: gated on the "projects" category -- sprints/cycles
            # below stay unconditional (reference data, not a category).
            if want_projects:
                try:
                    # Accumulated one node at a time, NOT via a list comprehension
                    # over the generator: a failure part-way through pagination must
                    # keep the prefix already fetched instead of discarding every
                    # project because the last page 502'd.
                    #
                    # Archived projects are requested DELIBERATELY. Linear's
                    # connection omits them by default, so an archived project would
                    # just stop appearing and this worker -- which only writes the
                    # rows it is handed -- could never write the is_active=0 that
                    # retires it. The catalog would serve the stale subject forever.
                    for node in client.iter_projects(include_archived=True):
                        # Versioned at the OBSERVATION boundary, not at worker
                        # start. ``now`` is captured before team discovery and
                        # enumeration, which can take a long time; two overlapping
                        # runs for the same org would then be ordered by when they
                        # STARTED rather than by what they SAW. A run that started
                        # earlier but observed a project later would write a stale
                        # version and lose to the other run's tombstone, keeping a
                        # live project retired.
                        native_project_rows.extend(
                            _linear_project_records(
                                [node],
                                org_id=org_id,
                                now=datetime.now(timezone.utc),
                            )
                        )
                    # Only here: the iterator ran to exhaustion without raising.
                    native_projects_complete = True
                except Exception:
                    # Enumeration did not finish. Strict reference discovery fails
                    # the run; otherwise keep what arrived but leave the run marked
                    # INCOMPLETE, so a partial catalog is never recorded as a full
                    # one. A missing project would otherwise look identical to a
                    # project that does not exist.
                    logger.warning(
                        "Linear project enumeration for org_id=%s did not complete; "
                        "keeping %d project(s) already fetched and marking the run "
                        "incomplete. Absence-based retirement is skipped.",
                        org_id,
                        len(native_project_rows),
                        exc_info=True,
                    )
                    if strict:
                        raise
                project_rows.extend(native_project_rows)
            for team in teams:
                api_team = client.get_team_by_key(_team_id(team))
                if not api_team or not api_team.get("id"):
                    continue
                for cycle in client.iter_cycles(team_id=str(api_team["id"])):
                    sprint_rows.append(
                        replace(
                            linear_cycle_to_sprint(cycle),
                            native_team_key=_team_id(team),
                            org_id=org_id,
                        )
                    )
    except Exception:
        if strict:
            raise
        sprint_rows = []

    sink, should_close = _sink_from_kwargs(scope, kwargs)
    try:
        team_store = _team_store_from_kwargs(sink, kwargs)
        projects = _dedupe_projects(project_rows)
        members = _dedupe_members(member_rows)
        memberships = _dedupe_memberships(membership_rows)
        ownership = _dedupe_ownership(ownership_rows)
        review_store = team_store if team_store is not None else None
        if review_store is not None:
            memberships = _run(
                split_memberships_for_review(
                    store=review_store,
                    org_id=org_id,
                    rows=memberships,
                    observed_team_ids=(
                        _observed_team_ids("linear", team_rows) if want_members else ()
                    ),
                    discovered_at=now,
                )
            )
        elif isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
            from dev_health_ops.storage.clickhouse import ClickHouseStore

            async def split_with_store() -> list[TeamMembershipRecord]:
                async with ClickHouseStore(_clickhouse_dsn(scope)) as store:
                    store.org_id = org_id
                    return await split_memberships_for_review(
                        store=store,
                        org_id=org_id,
                        rows=memberships,
                        observed_team_ids=(
                            _observed_team_ids("linear", team_rows)
                            if want_members
                            else ()
                        ),
                        discovered_at=now,
                    )

            memberships = _run(split_with_store())
        # CHAOS-4323 round 2 (codex adversarial-review, HIGH): roster_write_safe
        # gates ONLY the team-dimension write (the row carrying "members"). A
        # roster-preservation read failure must never fall through to writing
        # an unconfirmed empty roster -- it skips that write entirely instead.
        roster_write_safe = True
        if want_members:
            _apply_roster(team_rows, memberships)
        elif want_teams:
            # A teams-only run (members off) must not erase a previously-
            # imported roster by writing an empty "members" list -- preserve
            # whatever is currently persisted.
            existing_members = _existing_team_members(
                sink=sink,
                org_id=org_id,
                provider="linear",
                team_ids=[str(row["id"]) for row in team_rows],
            )
            if existing_members is None:
                roster_write_safe = False
                record_team_autoimport_roster_preservation_failed(provider="linear")
            else:
                for team_row in team_rows:
                    team_row["members"] = existing_members.get(str(team_row["id"]), [])
        if want_teams and roster_write_safe:
            if team_store is not None:
                _run(
                    project_team_rows_with_store(
                        store=team_store,
                        org_id=org_id,
                        provider="linear",
                        team_rows=team_rows,
                        team_writer=sink.insert_teams,
                        discovered_at=now,
                    )
                )
            elif isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
                _run(
                    project_provider_team_rows(
                        dsn=_clickhouse_dsn(scope),
                        org_id=org_id,
                        provider="linear",
                        team_rows=team_rows,
                        team_writer=sink.insert_teams,
                        discovered_at=now,
                    )
                )
            else:
                _run(sink.insert_teams(team_rows))
        if want_projects:
            sink.write_projects(projects)
            sink.write_team_project_ownership(ownership)
        if want_members:
            sink.write_members(members)
            sink.write_team_memberships(memberships)
        if hasattr(sink, "write_sprints"):
            sink.write_sprints(sprint_rows)
    finally:
        if should_close:
            sink.close()

    return {
        "mode": scope.get("mode"),
        "teams_imported": len(team_rows) if (want_teams and roster_write_safe) else 0,
        "roster_preservation_failed": not roster_write_safe,
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [str(row.sprint_id) for row in sprint_rows],
        "projects_imported": len(projects) if want_projects else 0,
        # Counted off the deduped rows actually handed to the sink, not the
        # pre-dedupe fetch, so the number reports what was written.
        "native_projects_imported": sum(
            1 for row in projects if row.project_key is None
        )
        if want_projects
        else 0,
        # False when Linear's project connection could not be enumerated to the
        # end. Without this an incomplete discovery run reports exactly like a
        # complete one, and a project that simply never arrived is
        # indistinguishable from a project that does not exist.
        "native_projects_complete": native_projects_complete,
        "members_imported": len(members) if want_members else 0,
        "team_memberships_imported": len(memberships) if want_members else 0,
        "team_project_ownership_imported": len(ownership) if want_projects else 0,
        "sprints_imported": len(sprint_rows),
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }


def _apply_roster(
    team_rows: list[dict[str, Any]], rows: list[TeamMembershipRecord]
) -> None:
    roster: dict[str, list[str]] = {}
    for row in rows:
        roster_for_team = roster.setdefault(str(row.team_id), [])
        values = tuple(row.identity_facets) or tuple(
            value for value in (row.raw_provider_user_id, row.raw_email) if value
        )
        for value in values:
            if value and value not in roster_for_team:
                roster_for_team.append(str(value))
    for team_row in team_rows:
        team_row["members"] = roster.get(str(team_row["id"]), [])


def _observed_team_ids(
    provider: str, team_rows: list[dict[str, Any]]
) -> tuple[tuple[str, str], ...]:
    return tuple((provider, str(row["id"])) for row in team_rows)
