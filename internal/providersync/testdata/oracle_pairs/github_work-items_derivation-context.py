"""Live Python oracle for work-item attribution context and linked donors."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import io
import json
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.metrics.compute_work_items import (
        build_linked_issue_team_resolver,
        resolve_team_attribution,
    )
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
    from dev_health_ops.providers.teams import build_project_key_resolver

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_COMPUTE_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/compute_work_items.py"


def _time(value: str | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _work_item(raw: dict[str, Any]) -> WorkItem:
    repo_id = raw.get("RepoID")
    return WorkItem(
        work_item_id=str(raw["WorkItemID"]),
        provider=raw["Provider"],
        title=str(raw.get("Title") or raw["WorkItemID"]),
        type="issue",
        status="todo",
        status_raw="open",
        repo_id=uuid.UUID(repo_id) if repo_id else None,
        native_team_key=raw.get("NativeTeamKey"),
        project_key=raw.get("ProjectKey"),
        project_id=raw.get("ProjectID"),
        project_name=raw.get("ProjectName"),
        assignees=list(raw.get("Assignees") or []),
        created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
        org_id=str(raw.get("OrgID") or ""),
    )


def _loader_row(raw: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in fields:
        raw_key = {
            "team_id": "TeamID",
            "project_id": "ProjectID",
            "repo_id": "RepoID",
            "member_id": "MemberID",
            "raw_provider_user_id": "RawProviderUserID",
            "scope_id": "ScopeID",
        }.get(field, "".join(part.title() for part in field.split("_")))
        value = raw.get(raw_key)
        row[field] = _time(value) if field == "updated_at" and value else value
    return row


class _ContextQueryClient:
    """Fake ClickHouse boundary; production loader SQL and mapping stay live."""

    def __init__(self, facts: dict[str, Any], *, org_id: str, as_of: datetime) -> None:
        self.facts = facts
        self.org_id = org_id
        self.as_of = as_of
        self.seen: set[str] = set()

    def _identities_from_members(self) -> list[dict[str, Any]]:
        """CHAOS-4321: synthesize `identities` rows FROM the oracle case's
        `Facts.Members` list (still the shared Go/Python fixture format --
        the OUTPUT shape loadMembers/member_rows used to query directly, now
        the shared oracle test data's input shape instead). One
        `identities` row per distinct MemberID, with its facets grouped by
        Provider and every TeamID it appears under unioned into `team_ids`
        -- reproduces exactly the admin-authored data that would make the
        production loader emit the SAME member_by_identity candidates the
        pre-CHAOS-4321 team_memberships-shaped fixture rows did.
        """
        by_member: dict[str, dict[str, Any]] = {}
        for raw in self.facts.get("Members") or []:
            member_id = str(raw.get("MemberID") or "")
            if not member_id:
                continue
            entry = by_member.setdefault(
                member_id,
                {
                    "canonical_id": member_id,
                    "email": raw.get("RawEmail"),
                    "provider_identities": {},
                    "team_ids": [],
                    "updated_at": raw.get("UpdatedAt"),
                },
            )
            team_id = str(raw.get("TeamID") or "")
            if team_id and team_id not in entry["team_ids"]:
                entry["team_ids"].append(team_id)
            provider = str(raw.get("Provider") or "")
            if provider:
                raw_ids = list(raw.get("IdentityFacets") or [])
                if raw.get("RawProviderUserID"):
                    raw_ids.append(raw["RawProviderUserID"])
                bucket = entry["provider_identities"].setdefault(provider, [])
                for value in raw_ids:
                    if value and value not in bucket:
                        bucket.append(value)
        return [
            {
                "canonical_id": entry["canonical_id"],
                "email": entry["email"],
                "provider_identities": json.dumps(entry["provider_identities"]),
                "team_ids": entry["team_ids"],
                "updated_at": _time(entry["updated_at"])
                if entry["updated_at"]
                else None,
            }
            for entry in by_member.values()
        ]

    def _admin_teams_from_members(self) -> list[dict[str, Any]]:
        """CHAOS-4321: synthesize `teams` rows FROM the same `Facts.Members`
        list, one row per distinct TeamID -- the identities-side `team_ids`
        union above already accounts for every membership these Facts
        describe, so `members`/`manual_members` are left empty here.

        Note: this fake does NOT yet exercise Facts.UntypedMembers/
        Facts.ProviderUntypedMembers (the teams.manual_members-override /
        teams.members-fallback untyped-facet paths added by CHAOS-4321) --
        that gap predates this ticket's fix (member_by_untyped_facet's oracle
        coverage was already absent) and is covered instead by pure-Go/
        pure-Python unit tests on both sides
        (TestGitHubWorkItemDerivationTwoLayerMembershipResolution's (e)-(j)
        subtests; test_chaos_4321_ownership_only_attribution.py)."""
        by_team: dict[str, dict[str, Any]] = {}
        for raw in self.facts.get("Members") or []:
            team_id = str(raw.get("TeamID") or "")
            if not team_id:
                continue
            by_team.setdefault(
                team_id,
                {
                    "id": team_id,
                    "name": str(raw.get("TeamName") or team_id),
                    "members": [],
                    "manual_members": [],
                },
            )
        return list(by_team.values())

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM identities FINAL" in query:
            if (
                params.get("org_id") != self.org_id
                or "org_id = {org_id:String}" not in query
            ):
                raise AssertionError("identities query lost its tenant fence")
            if "is_active = 1" not in query:
                raise AssertionError("identities query lost its active fence")
            self.seen.add("identities")
            return self._identities_from_members()
        if "FROM teams FINAL" in query:
            if (
                params.get("org_id") != self.org_id
                or "org_id = {org_id:String}" not in query
            ):
                raise AssertionError("admin teams query lost its tenant fence")
            if "is_active = 1" not in query:
                raise AssertionError("admin teams query lost its active fence")
            self.seen.add("admin_teams")
            return self._admin_teams_from_members()
        if (
            params.get("org_id") != self.org_id
            or "o.org_id = {org_id:String}" not in query
        ):
            raise AssertionError("team attribution query lost its tenant fence")
        if params.get("as_of") != self.as_of.replace(tzinfo=None):
            raise AssertionError("team attribution query lost its exact as-of value")
        if "team_project_ownership" in query:
            self._assert_deterministic_team_join(query, "g.project_id")
            self.seen.add("projects")
            return [
                _loader_row(
                    raw,
                    (
                        "provider",
                        "team_id",
                        "team_name",
                        "project_id",
                        "project_key",
                        "is_primary",
                        "specificity",
                        "priority",
                        "updated_at",
                    ),
                )
                for raw in self.facts.get("Projects") or []
            ]
        if "team_repo_ownership" in query:
            self._assert_deterministic_team_join(query, "g.repo_full_name")
            self.seen.add("repos")
            return [
                _loader_row(
                    raw,
                    (
                        "provider",
                        "team_id",
                        "team_name",
                        "repo_id",
                        "repo_full_name",
                        "is_primary",
                        "specificity",
                        "priority",
                        "updated_at",
                    ),
                )
                for raw in self.facts.get("Repos") or []
            ]
        if "team_memberships" in query:
            # CHAOS-4321 (chris, 08:30 PT): the FALLBACK membership layer --
            # provider auto-import, restored unchanged from before this
            # ticket. Sourced from Facts.ProviderMembers (NOT Facts.Members,
            # which is the admin/override layer synthesized into
            # identities/teams via _identities_from_members above).
            self._assert_deterministic_team_join(query, "g.member_id")
            self.seen.add("provider_members")
            return [
                _loader_row(
                    raw,
                    (
                        "provider",
                        "team_id",
                        "team_name",
                        "member_id",
                        "raw_provider_user_id",
                        "raw_email",
                        "identity_facets",
                        "is_primary",
                        "specificity",
                        "priority",
                        "updated_at",
                    ),
                )
                for raw in self.facts.get("ProviderMembers") or []
            ]
        if "manual_attribution_fallbacks" in query:
            self.seen.add("manual")
            return [
                _loader_row(
                    raw,
                    (
                        "provider",
                        "scope_type",
                        "scope_id",
                        "team_id",
                        "team_name",
                        "reason",
                        "priority",
                    ),
                )
                for raw in self.facts.get("ManualFallbacks") or []
            ]
        raise AssertionError(f"unexpected team-attribution query: {query}")

    @staticmethod
    def _assert_deterministic_team_join(query: str, order_key: str) -> None:
        if "argMax(name, (updated_at, last_synced, name))" not in query:
            raise AssertionError("team name join does not collapse duplicate versions")
        if f"ORDER BY g.provider, {order_key}, g.team_id" not in query:
            raise AssertionError("team attribution rows have no stable ordering")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    facts = case.get("Facts") or {}
    as_of = _time(case.get("AsOf") or "2026-08-04T12:00:00Z")
    org_id = str(case["Subject"].get("OrgID") or "")
    work_items = [_work_item(raw) for raw in case.get("WorkItems") or []]
    dependencies = [
        WorkItemDependency(
            source_work_item_id=str(raw["source_work_item_id"]),
            target_work_item_id=str(raw["target_work_item_id"]),
            relationship_type=str(raw["relationship_type"]),
            relationship_type_raw=str(raw.get("relationship_type_raw") or ""),
            relationship_semantics_version=str(
                raw.get("relationship_semantics_version") or "canonical-blocks.v2"
            ),
            last_synced=_time(raw.get("last_synced")),
            org_id=str(raw.get("org_id") or ""),
        )
        for raw in case.get("Dependencies") or []
    ]
    teams = [
        {
            "id": str(raw.get("TeamID") or ""),
            "name": str(raw.get("TeamName") or raw.get("TeamID") or ""),
            "project_keys": list(raw.get("ProjectKeys") or []),
        }
        for raw in facts.get("Teams") or []
    ]
    project_key_resolver = build_project_key_resolver(teams)
    query_client = _ContextQueryClient(facts, org_id=org_id, as_of=as_of)
    context = asyncio.run(
        ClickHouseDataLoader(query_client, org_id=org_id).load_team_attribution_context(
            as_of=as_of
        )
    )
    if query_client.seen != {
        "projects",
        "repos",
        "identities",
        "admin_teams",
        "provider_members",
        "manual",
    }:
        raise AssertionError(
            f"production team-attribution loader coverage incomplete: {query_client.seen}"
        )
    linked = build_linked_issue_team_resolver(
        work_items=work_items,
        dependencies=dependencies,
        project_key_resolver=project_key_resolver,
        attribution_context=context,
    )
    subject = _work_item(case["Subject"])
    _, _, candidates = resolve_team_attribution(
        subject,
        team_resolver=None,
        project_key_resolver=project_key_resolver,
        linked_issue_resolver=linked,
        attribution_context=context,
    )
    rows = [dataclasses.asdict(candidate) for candidate in candidates]
    fields = dataclass_field_names(
        _COMPUTE_SOURCE.read_text(), "TeamAttributionCandidate"
    )
    return {field: [row[field] for row in rows] for field in fields}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/derivation-context",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _COMPUTE_SOURCE.read_text(), "TeamAttributionCandidate"
        ),
    )
)
