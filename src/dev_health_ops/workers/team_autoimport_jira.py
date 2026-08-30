from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Coroutine, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from threading import Thread
from typing import Any, Protocol, TypeVar, cast

import requests
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
from dev_health_ops.credentials.resolver import jira_credentials_from_mapping
from dev_health_ops.metrics.prometheus import (
    record_team_autoimport_reference_subitem_skipped,
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
from dev_health_ops.providers.jira.client import JiraAuth, JiraClient
from dev_health_ops.providers.jira.normalize import jira_sprint_payload_to_model
from dev_health_ops.providers.team_capabilities import auto_import_capabilities
from dev_health_ops.workers.team_autoimport_categories import (
    resolve_import_categories,
)

_T = TypeVar("_T")
_REAL_CLICKHOUSE_SINK_TYPE = ClickHouseMetricsSink


class _DimensionSink(Protocol):
    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]: ...
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
        raise ValueError("ClickHouse DSN is required for Jira team auto-import")
    return dsn


def _existing_team_members(
    *,
    sink: Any,
    org_id: str,
    provider: str,
    team_ids: list[str],
    sync_run_id: str | None = None,
) -> dict[str, list[str]] | None:
    """Read of the CURRENTLY persisted roster for these teams, for a
    members-off run to carry forward instead of overwriting it with [].

    CHAOS-4323 round 2 (codex adversarial-review, HIGH): returns ``None``
    (never a substitute {}) whenever the current roster genuinely could not
    be confirmed, so the caller fails CLOSED (skips the team-dimension
    write) rather than write an empty roster it never actually verified was
    empty. Deliberately does NOT filter on ``provider`` in SQL (round 3:
    ``teams`` dedups on ``id`` alone -- an admin-edited provider="" row can
    otherwise hide the existing team from a provider-filtered query).
    ``sync_run_id`` (round-3 follow-up) is included in the WARNING when the
    caller has one. See the identical helper's docstring in
    team_autoimport_github.py for the full rationale on both.
    """
    if not team_ids:
        return {}
    if not hasattr(sink, "query_dicts"):
        logging.getLogger(__name__).warning(
            "Cannot confirm existing team rosters for org_id=%s provider=%s "
            "sync_run_id=%s (sink has no query_dicts) -- skipping the "
            "team-dimension write for this members-off run rather than "
            "risk erasing rosters",
            org_id,
            provider,
            sync_run_id,
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
        logging.getLogger(__name__).warning(
            "Could not read existing team rosters for org_id=%s provider=%s "
            "sync_run_id=%s -- skipping the team-dimension write for this "
            "members-off run rather than risk erasing rosters",
            org_id,
            provider,
            sync_run_id,
            exc_info=True,
        )
        return None
    return {str(row.get("id")): list(row.get("members") or []) for row in rows}


def _team_store_from_kwargs(sink: _DimensionSink, kwargs: dict[str, Any]) -> Any | None:
    return kwargs.get("team_store") or (
        sink if hasattr(sink, "insert_team_provider_observations") else None
    )


def _project_id(org_id: str, provider: str, project_key: str) -> str:
    return f"{org_id}:{provider}:{project_key}"


def _skippable_jira_400_detail(exc: Exception) -> str | None:
    """Whether ``exc`` is the one known-benign per-item Jira failure this
    module isolates -- a plain HTTP 400 from the Jira Agile API -- and, if
    so, the best diagnostic detail available for the warning log.

    Returns ``None`` for everything else: 401/403 (auth), 429 (rate limit),
    5xx (server error), timeouts, connection errors, a 400 with no body or
    an unparseable/empty body, and any other exception. CHAOS-4357 round-2
    (codex P1, both rounds): the original per-board/per-project catch was a
    bare ``except Exception``, which under ``strict_reference_discovery=True``
    let a NON-skippable failure -- revoked credentials, a rate limit, an
    outage, or (round 2's finding) a plain 400 for an unrelated reason such
    as a malformed ``projectKeyOrId`` -- silently pass as if it were the
    expected "this board has no sprints" 400, handing back an incomplete
    reference set as a reported success. Strict discovery's contract is
    guaranteed-complete-or-raise (see
    ``test_strict_reference_discovery_fails_on_an_incomplete_enumeration``
    in ``tests/workers/test_team_autoimport_linear_projects.py``), so only
    this one documented, narrow shape may be swallowed.

    Jira's documented signal for "this board's type does not support
    sprints" is a 400 carrying a non-empty ``errorMessages`` array in the
    response body -- round 2 requires that array to actually be present
    (not merely a bare 400) before treating the failure as benign; a 400
    with no parseable body, or a body without ``errorMessages``, is treated
    as NOT skippable and propagates like any other unrecognized failure.
    """
    if not isinstance(exc, requests.HTTPError):
        return None
    response = exc.response
    if response is None or response.status_code != 400:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    messages = payload.get("errorMessages")
    if isinstance(messages, list) and messages:
        return "; ".join(str(message) for message in messages)
    return None


_JIRA_BOARD_CAPABLE_PROJECT_TYPES = frozenset({"software"})

# Jira Cloud's own three project template families, plus Product Discovery
# (a fourth, newer template) -- all confirmed to have no Agile boards.
# Codex review (CHAOS-4575, P2): an UNKNOWN or missing projectTypeKey must
# NOT be treated the same as a confirmed non-board-capable type -- that
# would silently skip a possibly board-capable project's real data under
# strict discovery's guaranteed-complete-or-raise contract (AGENTS.md
# checkpoint 12: "missing is not healthy"). Only this explicit, evidence-
# backed allowlist is skippable; anything else propagates.
_JIRA_NO_BOARD_PROJECT_TYPES = frozenset(
    {"service_desk", "business", "product_discovery"}
)


def _jira_project_type_key(client: Any, *, project_key: str) -> str:
    """Look up ``project_key``'s ``projectTypeKey`` ("software",
    "service_desk", "business") via a single-project GET, so board discovery
    can gate on project TYPE rather than guessing from a 400's text.

    CHAOS-4575 (live executed evidence, org 70d529e0, 2026-08-30): ``GET
    /rest/agile/1.0/board?projectKeyOrId=SUP`` returned ``400
    {"errorMessages": ["You must have the browse project permission to view
    this project."]}`` even though the credential is a personal API token
    with full-workspace access (confirmed with chris) -- ``GET
    /rest/api/3/project/SUP`` on the SAME credential returned
    ``projectTypeKey: "service_desk"``. That 400's wording is misleading:
    the Software Agile Boards API answers it for ANY non-software project
    regardless of permissions, because boards are a software-project concept
    -- Service Management and Business (Jira Work Management) projects never
    have one. Treating the message text as a permission signal (an earlier
    version of this fix did exactly that) would have been right for the
    wrong reason and missed every other non-software project whose 400
    happens to word it differently.
    """
    project = client.get_project(project_key=project_key)
    return str((project or {}).get("projectTypeKey") or "").strip().lower()


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


def _load_jira_legacy_links(
    sink: _DimensionSink,
    *,
    org_id: str,
) -> list[dict[str, Any]]:
    try:
        return sink.query_dicts(
            """
            SELECT project_key, ops_team_id, project_name, ops_team_name, updated_at
            FROM jira_project_ops_team_links FINAL
            WHERE org_id = {org_id:String}
            """,
            {"org_id": org_id},
        )
    except Exception:
        return []


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
    # regardless of what scope requested (Jira supports all three today).
    capability = auto_import_capabilities("jira")
    want_teams = categories["teams"] and capability.teams
    want_projects = categories["projects"] and capability.projects
    want_members = categories["members"] and capability.members
    # CHAOS-4437 (codex review, P1): do NOT take this early exit in strict
    # mode. Sprint/cycle discovery below is unconditional reference data
    # (not gated on any category -- see team_autoimport_categories.py's
    # module docstring), needed to resolve dispatch-blocking sprint keys
    # even when an org has disabled every writable category. Returning here
    # skipped that discovery entirely, silently leaving sprint references
    # stale/missing while dispatch proceeded anyway. The best-effort
    # (non-strict) path still short-circuits here to avoid a wasted API call
    # when the user disabled everything.
    if not strict and not (want_teams or want_projects or want_members):
        return {
            "status": "skipped",
            "reason": "no_categories_selected",
            "teams_imported": 0,
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }
    jira_credentials = jira_credentials_from_mapping(credentials)
    if jira_credentials is None:
        if strict:
            raise ValueError("missing Jira credentials for strict reference discovery")
        return {
            "status": "skipped",
            "reason": "missing_jira_credentials",
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }

    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(None, org_id)
    membership = TeamMembershipService(cast(AsyncSession, None), org_id)
    try:
        teams = _run(
            discovery.discover_jira(
                email=jira_credentials.email,
                api_token=jira_credentials.api_token,
                url=jira_credentials.base_url,
            )
        )
    except Exception as exc:
        if strict:
            raise
        # Mirror github/gitlab: a discovery failure (e.g. HTTP 403) skips the
        # import INTERNALLY without writing anything, so a manual ownership row
        # is never clobbered and the sync stays successful (CHAOS-2609).
        logging.getLogger(__name__).info(
            "Skipping Jira team auto-import for org_id=%s: discovery failed: %s",
            org_id,
            exc,
        )
        return {
            "status": "skipped",
            "reason": "provider_discovery_skipped",
            "teams_imported": 0,
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
            "team_repo_ownership_imported": 0,
            "work_item_team_attributions_imported": 0,
        }

    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()

    team_rows: list[dict[str, Any]] = []
    project_rows: list[ProjectRecord] = []
    member_rows: list[MemberRecord] = []
    membership_rows: list[TeamMembershipRecord] = []
    ownership_rows: list[TeamProjectOwnershipRecord] = []
    sprint_rows: list[Sprint] = []

    for team in teams:
        team_id = str(team.provider_team_id)
        associations = dict(team.associations or {})
        project_keys = [str(key) for key in associations.get("project_keys", []) if key]
        if not project_keys:
            project_keys = [team_id]

        team_rows.append(
            {
                "id": team_id,
                "name": team.name,
                "description": team.description,
                "members": [],
                "project_keys": project_keys,
                "repo_patterns": [],
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": "jira",
                "native_team_key": team_id,
                "parent_team_id": None,
            }
        )

        for project_key in project_keys:
            project_rows.append(
                ProjectRecord(
                    id=_project_id(org_id, "jira", project_key),
                    org_id=org_id,
                    provider="jira",
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
                    provider="jira",
                    team_id=team_id,
                    project_id=_project_id(org_id, "jira", project_key),
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
                membership.discover_members_jira_bulk(
                    email=jira_credentials.email,
                    api_token=jira_credentials.api_token,
                    url=jira_credentials.base_url,
                    project_keys=project_keys,
                )
            )
            if want_members
            else []
        )
        # Resolve each member through the SAME org alias map an assignee uses:
        # facets[0] is the alias-resolved identity (canonical email when the
        # accountId is aliased, else jira:accountid:<id>) — the facet a no-email
        # assignee resolves to. raw_provider_user_id stores it (member_id PK keeps
        # the jira:<id> form); identity_facets and the roster carry all facets so
        # BOTH attribution paths match aliased and non-aliased members
        # (CHAOS-2609, CHAOS-2625).
        roster_facets: list[str] = []
        for member in discovered_members:
            member_id = _member_id("jira", member.provider_identity)
            facets = resolver.membership_facets(
                provider="jira",
                account_id=member.provider_identity,
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
                        "jira", member.provider_identity
                    ),
                    is_active=1,
                    updated_at=now,
                )
            )
            membership_rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider="jira",
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=member.email,
                    identity_facets=facets,
                    source="native",
                    is_primary=1 if member.role == "lead" else 0,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )
        team_rows[-1]["members"] = roster_facets

    try:
        client = JiraClient(
            auth=JiraAuth(
                base_url=jira_credentials.base_url,
                email=jira_credentials.email,
                api_token=jira_credentials.api_token,
            ),
            org_id=org_id,
        )
        try:
            for project_key in {
                row.project_key for row in project_rows if row.project_key
            }:
                # CHAOS-4357 round 2 (codex P1) left board LISTING
                # un-isolated: a generic 400 there is ambiguous between
                # "this project just has no boards" and "this
                # projectKeyOrId is malformed", and swallowing either could
                # silently omit a real project's reference data while
                # strict discovery still reports success. CHAOS-4575 closes
                # the actual gap without reopening that ambiguity: check the
                # project's TYPE first (a single, cheap GET) and skip board
                # discovery only for a project type on the explicit
                # confirmed-no-boards allowlist (_JIRA_NO_BOARD_PROJECT_TYPES)
                # -- an unrecognized type raises instead of silently skipping
                # (codex P2), and a software project's board-listing 400
                # (malformed key or anything else) still propagates to the
                # outer strict/non-strict handler below, unchanged.
                project_type = _jira_project_type_key(client, project_key=project_key)
                if project_type in _JIRA_NO_BOARD_PROJECT_TYPES:
                    # Codex review (CHAOS-4575, delta round): this is NOT a
                    # sub-item that failed or is incomplete -- a service_desk/
                    # business/product_discovery project structurally has no
                    # boards, by design, every time. Incrementing
                    # reference_subitem_skipped_total here (as the sprint-
                    # listing 400 branch below correctly does for a REAL
                    # failure) would corrupt that counter's meaning ("data
                    # for this sub-item is incomplete") with routine,
                    # complete-by-design non-applicability. Log at INFO, not
                    # WARNING, for the same reason.
                    logging.getLogger(__name__).info(
                        "Jira board discovery not applicable for org_id=%s "
                        "project_key=%s: project type %r has no Agile boards",
                        org_id,
                        project_key,
                        project_type,
                    )
                    continue
                if project_type not in _JIRA_BOARD_CAPABLE_PROJECT_TYPES:
                    # Codex review (CHAOS-4575, P2): an unrecognized or
                    # missing projectTypeKey is NOT the same as a confirmed
                    # non-board-capable type -- assuming "no boards" here
                    # could silently drop a real software project's data.
                    # Strict discovery's guaranteed-complete-or-raise
                    # contract requires this to propagate as a failure, not
                    # a silent skip.
                    raise RuntimeError(
                        "unrecognized Jira project type "
                        f"{project_type or 'unknown'!r} for project_key={project_key!r}"
                    )
                boards = list(client.iter_boards(project_key=project_key))
                for board in boards:
                    board_id = board.get("id")
                    if board_id is None:
                        continue
                    try:
                        for payload in client.iter_board_sprints(board_id=board_id):
                            sprint = jira_sprint_payload_to_model(payload)
                            if sprint:
                                sprint_rows.append(replace(sprint, org_id=org_id))
                    except Exception as exc:
                        # CHAOS-4357: Jira answers 400 for a board that does not
                        # support sprints (kanban-only boards, or a board that
                        # changed shape) -- that is a per-board fact, not an
                        # org-wide failure. Skip THAT board's sprints (logging a
                        # warning + counting it) and continue with the rest;
                        # never let one board's 400 kill the whole populate,
                        # strict mode included.
                        #
                        # Round 2 (codex P1): only that documented 400 shape is
                        # skippable -- 401/403/429/5xx/timeouts/connection
                        # errors propagate to the outer strict/non-strict
                        # handler below, same as before this isolation existed.
                        detail = _skippable_jira_400_detail(exc)
                        if detail is None:
                            raise
                        logging.getLogger(__name__).warning(
                            "Jira sprint listing failed for org_id=%s board_id=%s: %s",
                            org_id,
                            board_id,
                            detail,
                        )
                        record_team_autoimport_reference_subitem_skipped(
                            provider="jira", kind="board_sprints"
                        )
                        continue
        finally:
            client.close()
    except Exception:
        if strict:
            raise
        sprint_rows = []

    sink, should_close = _sink_from_kwargs(scope, kwargs)
    try:
        team_store = _team_store_from_kwargs(sink, kwargs)
        if want_projects:
            for link in _load_jira_legacy_links(sink, org_id=org_id):
                project_key = str(link.get("project_key") or "")
                ops_team_id = str(link.get("ops_team_id") or "")
                if not project_key or not ops_team_id:
                    continue
                project_id = _project_id(org_id, "jira", project_key)
                if not any(row.id == project_id for row in project_rows):
                    project_rows.append(
                        ProjectRecord(
                            id=project_id,
                            org_id=org_id,
                            provider="jira",
                            project_key=project_key,
                            name=str(link.get("project_name") or project_key),
                            is_active=1,
                            updated_at=now,
                            last_synced=now,
                        )
                    )
                ownership_rows.append(
                    TeamProjectOwnershipRecord(
                        org_id=org_id,
                        provider="jira",
                        team_id=ops_team_id,
                        project_id=project_id,
                        project_key=project_key,
                        source="jira_legacy",
                        is_primary=1,
                        specificity=90,
                        priority=20,
                        valid_from=now,
                        updated_at=now,
                    )
                )

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
                        _observed_team_ids("jira", team_rows) if want_members else ()
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
                            _observed_team_ids("jira", team_rows)
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
                provider="jira",
                team_ids=[str(row["id"]) for row in team_rows],
                sync_run_id=(
                    str(scope.get("sync_run_id"))
                    if scope.get("sync_run_id") is not None
                    else None
                ),
            )
            if existing_members is None:
                roster_write_safe = False
                record_team_autoimport_roster_preservation_failed(provider="jira")
            else:
                for team_row in team_rows:
                    team_row["members"] = existing_members.get(str(team_row["id"]), [])
        if want_teams and roster_write_safe:
            if team_store is not None:
                _run(
                    project_team_rows_with_store(
                        store=team_store,
                        org_id=org_id,
                        provider="jira",
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
                        provider="jira",
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

    jira_legacy_count = sum(1 for row in ownership if row.source == "jira_legacy")
    return {
        "mode": scope.get("mode"),
        "teams_imported": len(team_rows) if (want_teams and roster_write_safe) else 0,
        "roster_preservation_failed": not roster_write_safe,
        # CHAOS-4437: only claim keys actually written this call -- the
        # readback verifier polls ClickHouse for exactly these native_team_key
        # values and fails the whole reference-discovery run (blocking
        # dispatch) if a claimed-but-skipped team never lands. Sprints stay
        # unconditional: sprint writes are already unconditional above
        # (CHAOS-4323: "reference data, not a category"), so the claim always
        # matches what was written.
        "reference_team_keys": (
            [str(row["native_team_key"]) for row in team_rows]
            if (want_teams and roster_write_safe)
            else []
        ),
        "reference_sprint_ids": [str(row.sprint_id) for row in sprint_rows],
        "projects_imported": len(projects) if want_projects else 0,
        "members_imported": len(members) if want_members else 0,
        "team_memberships_imported": len(memberships) if want_members else 0,
        "team_project_ownership_imported": len(ownership) if want_projects else 0,
        "sprints_imported": len(sprint_rows),
        "jira_legacy_project_ownership_imported": jira_legacy_count
        if want_projects
        else 0,
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
