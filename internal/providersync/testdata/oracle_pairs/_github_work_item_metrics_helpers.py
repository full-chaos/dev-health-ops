"""Shared live-production wiring for the work-item metric triplet oracle pairs.

The three pairs (metrics-daily, user-metrics-daily, cycle-times) are three
projections of ONE production call: ``compute_work_item_metrics_daily`` returns
all three lists together, and they are only meaningful against each other
(a cycle-time row and the group percentile it feeds come from the same item).
Running the real function once per case here, and letting each pair select its
own list, keeps the three comparisons consistent by construction rather than by
three copies of the same fixture wiring agreeing by luck.

Nothing about the compared values is reimplemented: the team resolvers, the
attribution context, the linked-issue donor index, and the metric computation
are all the live production functions. Only the ClickHouse transport under
``ClickHouseDataLoader`` is faked, exactly as the derivation-context pair does,
and its SQL and row mapping stay live.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import pathlib
import uuid
from datetime import date, datetime, timezone
from typing import Any

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
        compute_work_item_metrics_daily,
    )
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.models.work_items import (
        WorkItem,
        WorkItemDependency,
        WorkItemStatusTransition,
    )
    from dev_health_ops.providers.teams import build_project_key_resolver

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
COMPUTE_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/compute_work_items.py"
SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"

# The compute contract's own default. Declared here so the three pairs state the
# same reason in one place rather than three drifting copies.
ORG_ID_EXCLUSION = (
    "compute_work_item_metrics_daily never sets org_id: every record dataclass "
    "defaults it to '' and the ClickHouse sink injects the tenant from sink "
    "context at insert time (sinks/clickhouse/core.py:553-560). The Go port has "
    "no sink-level context to inject from, so it stamps the frozen claim's "
    "OrgID at compute time. The persisted value is compared for real by the "
    "ClickHouse readback integration test, not here."
)


def parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _required_time(value: Any) -> datetime:
    parsed = parse_time(value)
    if parsed is None:
        raise ValueError("a required timestamp was null in the oracle case")
    return parsed


def _work_item(raw: dict[str, Any]) -> WorkItem:
    repo_id = raw.get("repo_id")
    story_points = raw.get("story_points")
    return WorkItem(
        work_item_id=str(raw["work_item_id"]),
        provider=raw["provider"],
        title=str(raw.get("title") or raw["work_item_id"]),
        type=raw.get("type") or "issue",
        status=raw.get("status") or "todo",
        status_raw=raw.get("status_raw"),
        repo_id=uuid.UUID(repo_id) if repo_id else None,
        native_team_key=raw.get("native_team_key"),
        project_key=raw.get("project_key"),
        project_id=raw.get("project_id"),
        project_name=raw.get("project_name"),
        assignees=list(raw.get("assignees") or []),
        created_at=_required_time(raw["created_at"]),
        updated_at=_required_time(raw.get("updated_at") or raw["created_at"]),
        started_at=parse_time(raw.get("started_at")),
        completed_at=parse_time(raw.get("completed_at")),
        closed_at=parse_time(raw.get("closed_at")),
        labels=list(raw.get("labels") or []),
        story_points=None if story_points is None else float(story_points),
        org_id=str(raw.get("org_id") or ""),
    )


def _transition(raw: dict[str, Any]) -> WorkItemStatusTransition:
    return WorkItemStatusTransition(
        work_item_id=str(raw["work_item_id"]),
        provider=raw["provider"],
        occurred_at=_required_time(raw["occurred_at"]),
        from_status_raw=raw.get("from_status_raw"),
        to_status_raw=raw.get("to_status_raw"),
        from_status=raw.get("from_status") or "todo",
        to_status=raw["to_status"],
        actor=raw.get("actor"),
        org_id=str(raw.get("org_id") or ""),
    )


def _dependency(raw: dict[str, Any]) -> WorkItemDependency:
    return WorkItemDependency(
        source_work_item_id=str(raw["source_work_item_id"]),
        target_work_item_id=str(raw["target_work_item_id"]),
        relationship_type=str(raw["relationship_type"]),
        relationship_type_raw=str(raw.get("relationship_type_raw") or ""),
        relationship_semantics_version=str(
            raw.get("relationship_semantics_version") or "canonical-blocks.v2"
        ),
        last_synced=_required_time(raw.get("last_synced") or "1970-01-01T00:00:00Z"),
        org_id=str(raw.get("org_id") or ""),
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
        row[field] = parse_time(value) if field == "updated_at" and value else value
    return row


def _identities_and_teams_from_members(
    members: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Translate Go-shaped ``githubWorkItemDerivationMemberFact`` fixture rows
    into the ``identities``/``teams`` rows CHAOS-4321's loader now reads.

    See the near-identical copy in ``_github_work_item_derived_helpers.py``
    for the full rationale (this module's LANE NOTE explains the deliberate,
    temporary duplication of the fake query client).
    """
    identities_by_canonical: dict[str, dict[str, Any]] = {}
    team_names: dict[str, str] = {}
    for raw in members:
        team_id = str(raw.get("TeamID") or "")
        if team_id and team_id not in team_names:
            team_names[team_id] = str(raw.get("TeamName") or team_id)
        canonical_id = str(raw.get("MemberID") or "")
        if not canonical_id:
            continue
        entry = identities_by_canonical.setdefault(
            canonical_id,
            {
                "canonical_id": canonical_id,
                "email": None,
                "provider_identities": {},
                "team_ids": [],
                "updated_at": raw.get("UpdatedAt"),
            },
        )
        if not entry["email"] and raw.get("RawEmail"):
            entry["email"] = raw.get("RawEmail")
        provider = str(raw.get("Provider") or "")
        if provider:
            facets = entry["provider_identities"].setdefault(provider, [])
            for facet in raw.get("IdentityFacets") or []:
                facet_str = str(facet)
                if facet_str and facet_str not in facets:
                    facets.append(facet_str)
        if team_id and team_id not in entry["team_ids"]:
            entry["team_ids"].append(team_id)
    identities_rows = [
        {
            "canonical_id": data["canonical_id"],
            "email": data["email"],
            "provider_identities": json.dumps(data["provider_identities"]),
            "team_ids": data["team_ids"],
            "updated_at": parse_time(data["updated_at"])
            if data["updated_at"]
            else None,
        }
        for data in identities_by_canonical.values()
    ]
    teams_rows = [
        {"id": team_id, "name": name, "members": []}
        for team_id, name in team_names.items()
    ]
    return identities_rows, teams_rows


class _ContextQueryClient:
    """Fake ClickHouse boundary; production loader SQL and mapping stay live."""

    def __init__(self, facts: dict[str, Any], *, org_id: str, as_of: datetime) -> None:
        self.facts = facts
        self.org_id = org_id
        self.as_of = as_of
        self.seen: set[str] = set()

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM identities FINAL" in query:
            if params.get("org_id") != self.org_id:
                raise AssertionError("identities query lost its tenant fence")
            self.seen.add("identities")
            identities_rows, _ = _identities_and_teams_from_members(
                self.facts.get("Members") or []
            )
            return identities_rows
        if "FROM teams FINAL" in query:
            if params.get("org_id") != self.org_id:
                raise AssertionError("admin teams query lost its tenant fence")
            self.seen.add("teams")
            _, teams_rows = _identities_and_teams_from_members(
                self.facts.get("Members") or []
            )
            return teams_rows
        if (
            params.get("org_id") != self.org_id
            or "o.org_id = {org_id:String}" not in query
        ):
            raise AssertionError("team attribution query lost its tenant fence")
        if params.get("as_of") != self.as_of.replace(tzinfo=None):
            raise AssertionError("team attribution query lost its exact as-of value")
        if "team_project_ownership" in query:
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
            # identities/teams above).
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


def compute_triplet(case: dict[str, Any]) -> tuple[list[Any], list[Any], list[Any]]:
    """Run the real per-day compute for one case and return its three lists."""
    facts = case.get("Facts") or {}
    org_id = str(case.get("OrgID") or "")
    as_of = _required_time(case.get("AsOf") or case["ComputedAt"])
    computed_at = _required_time(case["ComputedAt"])
    day_value = str(case["Day"])
    day = date.fromisoformat(day_value)

    work_items = [_work_item(raw) for raw in case.get("WorkItems") or []]
    transitions = [_transition(raw) for raw in case.get("Transitions") or []]
    dependencies = [_dependency(raw) for raw in case.get("Dependencies") or []]

    # Donor subjects are the previously persisted work items an inheritable
    # dependency edge can point at -- job_work_items.py builds the resolver from
    # `donor_by_id.values()`, a merge of fetched donors with the freshly synced
    # rows where fresh wins. loadGitHubWorkItemDerivationContext merges the same
    # two sets the same way, so this must too: passing only `work_items` would
    # make cross-provider inheritance untestable, and passing both lists
    # unmerged would double-count a row that is in each.
    donor_subjects: dict[str, WorkItem] = {}
    for raw in case.get("Donors") or []:
        donor = _work_item(raw)
        donor_subjects[donor.work_item_id] = donor
    for item in work_items:
        donor_subjects[item.work_item_id] = item

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
    attribution_context = asyncio.run(
        ClickHouseDataLoader(query_client, org_id=org_id).load_team_attribution_context(
            as_of=as_of
        )
    )
    if query_client.seen != {
        "projects",
        "repos",
        "identities",
        "teams",
        "provider_members",
        "manual",
    }:
        raise AssertionError(
            f"production team-attribution loader coverage incomplete: {query_client.seen}"
        )
    linked_issue_resolver = build_linked_issue_team_resolver(
        work_items=list(donor_subjects.values()),
        dependencies=dependencies,
        project_key_resolver=project_key_resolver,
        attribution_context=attribution_context,
    )
    return compute_work_item_metrics_daily(
        day=day,
        work_items=work_items,
        transitions=transitions,
        computed_at=computed_at,
        project_key_resolver=project_key_resolver,
        linked_issue_resolver=linked_issue_resolver,
        attribution_context=attribution_context,
    )


def columns(records: list[Any], fields: frozenset[str]) -> dict[str, list[Any]]:
    """Transpose an ordered record list into one column per production field.

    The generic comparator needs a top-level object per case; column lists keep
    the production ORDER comparable (a reordering shows up as every column
    diverging at once) while `fields` coming from the production dataclass keeps
    the shape exhaustive when a field is added or removed upstream.
    """
    return {
        field: [_json_safe(getattr(record, field)) for record in records]
        for field in sorted(fields)
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return value
