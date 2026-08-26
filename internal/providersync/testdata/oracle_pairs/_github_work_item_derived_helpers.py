"""Shared live-production wiring for the derived work-item destination pairs.

Three destinations are covered here -- estimate_coverage_metrics_daily,
work_item_team_attributions and work_item_state_durations_daily. All three
resolve a work item's team through the SAME production cascade
(``resolve_team_attribution``, the project-key resolver, the linked-issue donor
index and the ClickHouse-loaded attribution context), so building that cascade
once per case and letting each pair call its own production function keeps the
three comparisons consistent by construction rather than by three copies of the
same wiring agreeing by luck.

Nothing about the compared values is reimplemented. The team resolvers, the
attribution context, the linked-issue donor index and all three compute
functions are the live production functions. Only the ClickHouse transport
under ``ClickHouseDataLoader`` is faked, exactly as the derivation-context pair
does, and its SQL and row mapping stay live.

LANE NOTE (derived-lane): the metric-triplet lane's
``_github_work_item_metrics_helpers`` carries a near-identical copy of the case
decoders, the fake query client and ``columns``. That duplication is
deliberate and temporary -- that lane is unmerged and its files may still move,
so importing from it would couple this lane to an unreviewed head. Whichever
lands second collapses the two modules into one.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import pathlib
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.analytics.investment import InvestmentClassifier
    from dev_health_ops.metrics.compute_work_item_state_durations import (
        compute_work_item_state_durations_daily,
    )
    from dev_health_ops.metrics.compute_work_items import (
        build_linked_issue_team_resolver,
        compute_estimate_coverage_metrics_daily,
        compute_work_item_team_attributions,
    )
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.metrics.work_item_engine_destinations import (
        compute_work_item_engine_destinations_daily,
    )
    from dev_health_ops.models.work_items import (
        WorkItem,
        WorkItemDependency,
        WorkItemStatusTransition,
    )
    from dev_health_ops.providers.status_mapping import load_status_mapping
    from dev_health_ops.providers.teams import build_project_key_resolver
    from dev_health_ops.utils.cli import resolve_date_range

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"
STATUS_MAPPING_SOURCE = REPO_ROOT / "src/dev_health_ops/config/status_mapping.yaml"
INVESTMENT_CONFIG_SOURCE = REPO_ROOT / "src/dev_health_ops/config/investment_areas.yaml"
JOB_WORK_ITEMS_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/job_work_items.py"

# The older derived-surface producers still leave org_id blank and rely on the
# ClickHouse sink to inject it. The three engine destinations added by this lane
# pass org_id explicitly and therefore do not use this exclusion.
ORG_ID_EXCLUSION = (
    "the Python compute function leaves org_id blank and the ClickHouse sink "
    "injects the tenant from sink context at insert time; its Go peer stamps "
    "the frozen claim tenant before persistence"
)


def parse_time(value: Any) -> datetime | None:
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
        # `or "todo"` would coerce an explicit "" to "todo" and MASK the
        # from_status fallback production Python actually has
        # (compute_work_item_state_durations.py:86 uses
        # `first.from_status if first.from_status else item.status`). Absent
        # key still defaults to "todo"; an explicit "" is passed through so the
        # fallback can be exercised on both sides.
        from_status=("todo" if "from_status" not in raw else raw["from_status"]),
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


_PROJECT_FIELDS = (
    "provider",
    "team_id",
    "team_name",
    "project_id",
    "project_key",
    "is_primary",
    "specificity",
    "priority",
    "updated_at",
)
_REPO_FIELDS = (
    "provider",
    "team_id",
    "team_name",
    "repo_id",
    "repo_full_name",
    "is_primary",
    "specificity",
    "priority",
    "updated_at",
)
_MEMBER_FIELDS = (
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
)
_MANUAL_FIELDS = (
    "provider",
    "scope_type",
    "scope_id",
    "team_id",
    "team_name",
    "reason",
    "priority",
)


def _identities_and_teams_from_members(
    members: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Translate Go-shaped ``githubWorkItemDerivationMemberFact`` fixture rows
    into the ``identities``/``teams`` rows CHAOS-4321's loader now reads.

    Oracle fixtures are authored once, as ``Facts.Members`` (Provider, TeamID,
    TeamName, MemberID, RawEmail, IdentityFacets, ...) -- the SAME shape both
    languages' `resolve()` cascades consume. Since CHAOS-4321 moved the
    ClickHouse SOURCE of that data from `team_memberships` to `identities`/
    `teams`, this fake transport must synthesize the new tables' shape from
    the SAME fixture rather than requiring every oracle pair to author two
    parallel fixtures for one concept -- an identity's canonical_id is its
    MemberID; its team_ids is every TeamID it appears under; its
    provider_identities groups IdentityFacets by Provider.
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
    # Note: this fake does NOT yet exercise Facts.UntypedMembers/
    # Facts.ProviderUntypedMembers (the teams.manual_members-override /
    # teams.members-fallback untyped-facet paths added by CHAOS-4321) --
    # that gap predates this ticket's fix (member_by_untyped_facet's oracle
    # coverage was already absent) and is covered instead by pure-Go/
    # pure-Python unit tests on both sides
    # (TestGitHubWorkItemDerivationTwoLayerMembershipResolution's (e)-(j)
    # subtests; test_chaos_4321_ownership_only_attribution.py).
    teams_rows = [
        {"id": team_id, "name": name, "members": [], "manual_members": []}
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
        # CHAOS-4321: `identities`/`teams` are admin-authored catalogs, not
        # bitemporal ownership facts -- their queries carry only org_id/limit,
        # no as_of, and no `o.`-aliased subquery. Checked before the generic
        # ownership-query fence below, which requires both.
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
                _loader_row(raw, _PROJECT_FIELDS)
                for raw in self.facts.get("Projects") or []
            ]
        if "team_repo_ownership" in query:
            self.seen.add("repos")
            return [
                _loader_row(raw, _REPO_FIELDS) for raw in self.facts.get("Repos") or []
            ]
        if "team_memberships" in query:
            # CHAOS-4321 (chris, 08:30 PT): the FALLBACK membership layer --
            # provider auto-import, restored unchanged from before this
            # ticket. Sourced from Facts.ProviderMembers (NOT Facts.Members,
            # which is the admin/override layer synthesized into
            # identities/teams above).
            self.seen.add("provider_members")
            return [
                _loader_row(raw, _MEMBER_FIELDS)
                for raw in self.facts.get("ProviderMembers") or []
            ]
        if "manual_attribution_fallbacks" in query:
            self.seen.add("manual")
            return [
                _loader_row(raw, _MANUAL_FIELDS)
                for raw in self.facts.get("ManualFallbacks") or []
            ]
        raise AssertionError(f"unexpected team-attribution query: {query}")


class DerivedCase:
    """One decoded case plus the live production resolver cascade it needs."""

    def __init__(self, case: dict[str, Any]) -> None:
        facts = case.get("Facts") or {}
        self.org_id = str(case.get("OrgID") or "")
        self.as_of = _required_time(case.get("AsOf") or case["ComputedAt"])
        self.computed_at = _required_time(case["ComputedAt"])
        self.days = _case_days(case)
        self.day = self.days[0]
        self.work_items = [_work_item(raw) for raw in case.get("WorkItems") or []]
        self.transitions = [_transition(raw) for raw in case.get("Transitions") or []]
        dependencies = [_dependency(raw) for raw in case.get("Dependencies") or []]

        # Donor subjects are the previously persisted work items an inheritable
        # dependency edge can point at. job_work_items.py builds the resolver
        # from `donor_by_id.values()`, a merge of fetched donors with the
        # freshly synced rows where fresh wins, and
        # loadGitHubWorkItemDerivationContext merges the same two sets the same
        # way -- so this must too. Passing only `work_items` would make
        # cross-provider inheritance untestable; passing both lists unmerged
        # would double-count a row present in each.
        donor_subjects: dict[str, WorkItem] = {}
        for raw in case.get("Donors") or []:
            donor = _work_item(raw)
            donor_subjects[donor.work_item_id] = donor
        for item in self.work_items:
            donor_subjects[item.work_item_id] = item

        teams = [
            {
                "id": str(raw.get("TeamID") or ""),
                "name": str(raw.get("TeamName") or raw.get("TeamID") or ""),
                "project_keys": list(raw.get("ProjectKeys") or []),
            }
            for raw in facts.get("Teams") or []
        ]
        self.project_key_resolver = build_project_key_resolver(teams)
        query_client = _ContextQueryClient(facts, org_id=self.org_id, as_of=self.as_of)
        self.attribution_context = asyncio.run(
            ClickHouseDataLoader(
                query_client, org_id=self.org_id
            ).load_team_attribution_context(as_of=self.as_of)
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
                "production team-attribution loader coverage incomplete: "
                f"{query_client.seen}"
            )
        self.linked_issue_resolver = build_linked_issue_team_resolver(
            work_items=list(donor_subjects.values()),
            dependencies=dependencies,
            project_key_resolver=self.project_key_resolver,
            attribution_context=self.attribution_context,
        )

    def _resolver_kwargs(self) -> dict[str, Any]:
        return {
            "project_key_resolver": self.project_key_resolver,
            "linked_issue_resolver": self.linked_issue_resolver,
            "attribution_context": self.attribution_context,
        }

    def estimate_coverage(self) -> list[Any]:
        return compute_estimate_coverage_metrics_daily(
            day=self.day,
            work_items=self.work_items,
            computed_at=self.computed_at,
            **self._resolver_kwargs(),
        )

    def team_attributions(self) -> list[Any]:
        return compute_work_item_team_attributions(
            work_items=self.work_items,
            computed_at=self.computed_at,
            **self._resolver_kwargs(),
        )

    def state_durations(self) -> list[Any]:
        return compute_work_item_state_durations_daily(
            day=self.day,
            work_items=self.work_items,
            transitions=self.transitions,
            computed_at=self.computed_at,
            **self._resolver_kwargs(),
        )

    def engine_destinations(self) -> tuple[list[Any], list[Any], list[Any]]:
        """Execute the exact production helper called by job_work_items."""

        return compute_work_item_engine_destinations_daily(
            day=self.day,
            work_items=self.work_items,
            computed_at=self.computed_at,
            org_id=self.org_id,
            status_mapping=load_status_mapping(STATUS_MAPPING_SOURCE),
            investment_classifier=InvestmentClassifier(INVESTMENT_CONFIG_SOURCE),
            **self._resolver_kwargs(),
        )

    def engine_destinations_all_days(
        self,
    ) -> tuple[list[Any], list[Any], list[Any]]:
        """Mirror the live job's per-day call while reusing one resolver context."""

        _assert_engine_helper_called_inside_job_loop()
        issue_types: list[Any] = []
        classifications: list[Any] = []
        metrics: list[Any] = []
        for day in self.days:
            self.day = day
            daily_issue_types, daily_classifications, daily_metrics = (
                self.engine_destinations()
            )
            issue_types.extend(daily_issue_types)
            classifications.extend(daily_classifications)
            metrics.extend(daily_metrics)
        return issue_types, classifications, metrics


def _case_days(case: dict[str, Any]) -> list[date]:
    raw_days = case.get("Days")
    if raw_days:
        return [date.fromisoformat(str(value)) for value in raw_days]
    if case.get("SinceAt") and case.get("BeforeAt"):
        since = _required_time(case["SinceAt"])
        before = _required_time(case["BeforeAt"])
        if since.time() != datetime.min.time() or before.time() != datetime.min.time():
            raise AssertionError("window oracle requires midnight-aligned date flags")
        end_day, backfill_days = resolve_date_range(
            argparse.Namespace(
                since=since.date(),
                before=before.date(),
                backfill=1,
                day=None,
                date=None,
            )
        )
        start_day = end_day - timedelta(days=backfill_days - 1)
        return [start_day + timedelta(days=index) for index in range(backfill_days)]
    return [date.fromisoformat(str(case["Day"]))]


def _assert_engine_helper_called_inside_job_loop() -> None:
    """Fail if the production job stops calling the compared helper per day."""

    lines = JOB_WORK_ITEMS_SOURCE.read_text().splitlines()
    loop_indices = [
        index for index, line in enumerate(lines) if line.strip() == "for d in days:"
    ]
    if len(loop_indices) != 1:
        raise AssertionError(
            f"expected exactly one work-item day loop, found {len(loop_indices)}"
        )
    loop_index = loop_indices[0]
    loop_indent = len(lines[loop_index]) - len(lines[loop_index].lstrip())
    for line in lines[loop_index + 1 :]:
        if line.strip() and len(line) - len(line.lstrip()) <= loop_indent:
            break
        if "compute_work_item_engine_destinations_daily(" in line:
            return
    raise AssertionError(
        "compute_work_item_engine_destinations_daily is no longer called inside "
        "job_work_items.py's per-day loop"
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
