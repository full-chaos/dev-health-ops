from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal, TypedDict

from dev_health_ops.metrics.schemas import (
    EstimateCoverageMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemTeamAttributionRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemDependency,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.teams import (
    LinkedIssueTeamResolver,
    ProjectKeyTeamResolver,
    TeamResolver,
    normalize_team_id,
    normalize_team_name,
)
from dev_health_ops.utils.datetime import to_utc


def _utc_day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    if percentile <= 0:
        return float(min(values))
    if percentile >= 100:
        return float(max(values))
    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    rank = (len(sorted_vals) - 1) * (float(percentile) / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _earliest_utc(*values: datetime | None) -> datetime | None:
    timestamps = [to_utc(value) for value in values if value is not None]
    return min(timestamps) if timestamps else None


def _resolve_team(
    team_resolver: TeamResolver | None, identity: str | None
) -> tuple[str | None, str | None]:
    if team_resolver is None:
        return None, None
    return team_resolver.resolve(identity)


TeamAttributionSource = Literal[
    "native_team",
    "issue_project",
    "project_ownership",
    "repo_ownership",
    "assignee_membership",
    "linked_issue",
    "manual_fallback",
    "unassigned",
]


@dataclass(frozen=True)
class TeamAttributionCandidate:
    source: TeamAttributionSource
    team_id: str | None
    team_name: str | None
    confidence: str
    evidence: str
    is_primary: int = 0
    specificity: int = 0
    priority: int = 0
    updated_at: datetime = field(
        default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc)
    )


@dataclass(frozen=True)
class ManualFallbackRule:
    """An explicit `manual_attribution_fallbacks` row (rank 6, never an override).

    Used only when no native/imported/linked source resolved. `scope_type` is one
    of repo | project | member | issue_key_prefix.
    """

    provider: str
    scope_type: str
    scope_id: str
    team_id: str
    team_name: str
    reason: str = ""
    priority: int = 100


@dataclass(frozen=True)
class TeamAttributionContext:
    project_by_id: dict[tuple[str, str], list[TeamAttributionCandidate]] = field(
        default_factory=dict
    )
    project_by_key: dict[tuple[str, str], list[TeamAttributionCandidate]] = field(
        default_factory=dict
    )
    repo_by_id: dict[tuple[str, str], list[TeamAttributionCandidate]] = field(
        default_factory=dict
    )
    repo_by_name: dict[tuple[str, str], list[TeamAttributionCandidate]] = field(
        default_factory=dict
    )
    member_by_identity: dict[tuple[str, str], list[TeamAttributionCandidate]] = field(
        default_factory=dict
    )
    manual_fallbacks: list[ManualFallbackRule] = field(default_factory=list)


# CHAOS-2600: deterministic staged precedence. linked_issue is a TRUE FALLBACK
# (rank 5) below every native/imported fact — it never overrides ownership or
# assignee membership; it only beats manual_fallback and unassigned. native_team
# stays top. See docs/architecture/team-attribution.md §0.
_SOURCE_ORDER: dict[TeamAttributionSource, int] = {
    "native_team": 0,
    "issue_project": 1,
    "project_ownership": 2,
    "repo_ownership": 3,
    "assignee_membership": 4,
    "linked_issue": 5,
    "manual_fallback": 6,
    "unassigned": 7,
}

# Sources a work item may pass on when it acts as a linked-issue *donor*. A donor
# may only contribute a team it earned from a first-class attribution fact
# (sources 0-4). manual_fallback (rank 6) and unassigned are excluded so a
# fallback rule — especially the provider-neutral `issue_key_prefix` scope —
# can never be laundered into rank-5 `linked_issue` provenance on a dependent
# item. linked_issue itself is absent here because donor resolution runs with
# `linked_issue_resolver=None` (no transitive inheritance). See
# docs/architecture/team-attribution.md §0.
_DONOR_SOURCES: frozenset[TeamAttributionSource] = frozenset(
    {
        "native_team",
        "issue_project",
        "project_ownership",
        "repo_ownership",
        "assignee_membership",
    }
)


def _identity_key(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _candidate_sort_key(
    candidate: TeamAttributionCandidate,
) -> tuple[int, int, int, float, str]:
    updated_at = to_utc(candidate.updated_at)
    return (
        -int(candidate.is_primary),
        -int(candidate.specificity),
        int(candidate.priority),
        -updated_at.timestamp(),
        candidate.team_id or "",
    )


def _ranked(
    candidates: Sequence[TeamAttributionCandidate],
) -> list[TeamAttributionCandidate]:
    return sorted(candidates, key=_candidate_sort_key)


def _dedupe_candidates(
    candidates: Sequence[TeamAttributionCandidate],
) -> list[TeamAttributionCandidate]:
    seen: set[tuple[object, ...]] = set()
    deduped: list[TeamAttributionCandidate] = []
    for candidate in candidates:
        key = (
            candidate.source,
            candidate.team_id,
            candidate.team_name,
            candidate.confidence,
            candidate.evidence,
            candidate.is_primary,
            candidate.specificity,
            candidate.priority,
            to_utc(candidate.updated_at),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _context_candidates(
    mapping: dict[tuple[str, str], list[TeamAttributionCandidate]],
    provider: str,
    key: object,
) -> list[TeamAttributionCandidate]:
    if key is None:
        return []
    return list(mapping.get((provider, str(key)), []))


def _native_team_candidate(
    item: WorkItem,
    project_key_resolver: ProjectKeyTeamResolver | None,
) -> TeamAttributionCandidate | None:
    if project_key_resolver is None or not item.native_team_key:
        return None
    native_key = str(item.native_team_key)
    team_id, team_name = project_key_resolver.resolve(native_key)
    if team_id is None:
        return None
    return TeamAttributionCandidate(
        source="native_team",
        team_id=team_id,
        team_name=team_name or team_id,
        confidence="high",
        evidence=f"native_team_key={native_key}",
        is_primary=1,
        specificity=100,
    )


def _issue_project_candidate(
    item: WorkItem,
    project_key_resolver: ProjectKeyTeamResolver | None,
) -> TeamAttributionCandidate | None:
    """The issue's OWN native project key -> owning team (a native WTI fact).

    Distinct from imported ``project_ownership`` (the ``team_project_ownership``
    edges supplied via ``attribution_context``). CHAOS-2600 ranks this as
    ``issue_project`` (rank 1), just below ``native_team``.
    """
    if project_key_resolver is None:
        return None
    keys = [item.work_scope_id or ""]
    if item.project_key:
        project_key = str(item.project_key)
        if project_key not in keys:
            keys.append(project_key)
    for key in keys:
        team_id, team_name = project_key_resolver.resolve(key)
        if team_id is not None:
            return TeamAttributionCandidate(
                source="issue_project",
                team_id=team_id,
                team_name=team_name or team_id,
                confidence="high",
                evidence=f"issue_project_key={key}",
                is_primary=1,
                specificity=50,
            )
    return None


_ISSUE_KEY_RE = re.compile(r"^([A-Za-z]{2,})-\d+$")


def _issue_key_prefix(item: WorkItem) -> str | None:
    """The item's own issue-key prefix, e.g. linear:CHAOS-5 -> CHAOS. None for PRs/MRs."""
    wid = item.work_item_id or ""
    if ":" not in wid:
        return None
    match = _ISSUE_KEY_RE.match(wid.split(":", 1)[1].strip())
    return match.group(1).upper() if match else None


def _manual_fallback_candidates(
    item: WorkItem, fallbacks: Sequence[ManualFallbackRule]
) -> list[TeamAttributionCandidate]:
    """Build manual_fallback candidates (rank 6) matching the item's scope.

    The precedence loop ensures these only become primary when no native/imported/
    linked source matched — manual fallback is never an override. An ``issue_key_prefix``
    rule matches the item's OWN key prefix; it is provider-neutral (a prefix spans
    providers) and is NEVER linked_issue inheritance.
    """
    if not fallbacks:
        return []
    repo_ids = {str(v).strip() for v in (item.repo_id, item.project_id) if v}
    project_ids = {
        str(v).strip()
        for v in (item.project_id, item.project_key, item.work_scope_id)
        if v
    }
    member_ids = {_identity_key(a) for a in item.assignees if a}
    prefix = _issue_key_prefix(item)
    out: list[TeamAttributionCandidate] = []
    for rule in fallbacks:
        # A rule matches its own provider, or any provider when its provider is blank.
        # issue_key_prefix is provider-neutral, so it is not provider-gated.
        if (
            rule.provider
            and rule.provider != item.provider
            and rule.scope_type != "issue_key_prefix"
        ):
            continue
        scope_id = rule.scope_id.strip()
        if rule.scope_type == "repo":
            matched = scope_id in repo_ids
        elif rule.scope_type == "project":
            matched = scope_id in project_ids
        elif rule.scope_type == "member":
            matched = _identity_key(scope_id) in member_ids
        elif rule.scope_type == "issue_key_prefix":
            matched = prefix is not None and prefix == scope_id.upper()
        else:
            matched = False
        if matched:
            out.append(
                TeamAttributionCandidate(
                    source="manual_fallback",
                    team_id=rule.team_id,
                    team_name=rule.team_name or rule.team_id,
                    confidence="manual",
                    evidence=f"manual_fallback:{rule.scope_type}={rule.scope_id}"
                    + (f" ({rule.reason})" if rule.reason else ""),
                    is_primary=1,
                    priority=rule.priority,
                )
            )
    return out


def resolve_team_attribution(
    item: WorkItem,
    team_resolver: TeamResolver | None,
    project_key_resolver: ProjectKeyTeamResolver | None,
    linked_issue_resolver: LinkedIssueTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> tuple[str | None, str | None, list[TeamAttributionCandidate]]:
    candidates_by_source: dict[
        TeamAttributionSource, list[TeamAttributionCandidate]
    ] = {
        "native_team": [],
        "issue_project": [],
        "project_ownership": [],
        "repo_ownership": [],
        "assignee_membership": [],
        "linked_issue": [],
        "manual_fallback": [],
        "unassigned": [],
    }

    native = _native_team_candidate(item, project_key_resolver)
    if native is not None:
        candidates_by_source["native_team"].append(native)

    if linked_issue_resolver is not None:
        linked_team_id, linked_team_name = linked_issue_resolver.resolve(
            item.work_item_id
        )
        if linked_team_id is not None:
            candidates_by_source["linked_issue"].append(
                TeamAttributionCandidate(
                    source="linked_issue",
                    team_id=linked_team_id,
                    team_name=linked_team_name or linked_team_id,
                    confidence="medium",
                    evidence=f"linked_issue={item.work_item_id}",
                    is_primary=1,
                    specificity=90,
                )
            )

    issue_project = _issue_project_candidate(item, project_key_resolver)
    if issue_project is not None:
        candidates_by_source["issue_project"].append(issue_project)

    if attribution_context is not None:
        candidates_by_source["project_ownership"].extend(
            _context_candidates(
                attribution_context.project_by_id, item.provider, item.project_id
            )
        )
        # Deconflict against issue_project: `team_project_ownership` keyed on the SAME
        # project key is the imported representation of the same "project key -> team" fact
        # the issue_project candidate already emitted (via project_key_resolver). Suppress the
        # duplicate-team row so one fact yields one provenance row; a genuinely DIFFERENT team
        # claimed by-key is a real lower-precedence signal and is kept.
        _issue_project_teams = {
            c.team_id for c in candidates_by_source["issue_project"] if c.team_id
        }
        candidates_by_source["project_ownership"].extend(
            candidate
            for candidate in _context_candidates(
                attribution_context.project_by_key, item.provider, item.project_key
            )
            if candidate.team_id not in _issue_project_teams
        )
        candidates_by_source["repo_ownership"].extend(
            _context_candidates(
                attribution_context.repo_by_id, item.provider, item.repo_id
            )
        )
        candidates_by_source["repo_ownership"].extend(
            _context_candidates(
                attribution_context.repo_by_name, item.provider, item.project_id
            )
        )
        for assignee_identity in item.assignees:
            candidates_by_source["assignee_membership"].extend(
                _context_candidates(
                    attribution_context.member_by_identity,
                    item.provider,
                    _identity_key(assignee_identity),
                )
            )

    assignee: str | None = item.assignees[0] if item.assignees else None
    team_id, team_name = _resolve_team(team_resolver, assignee)
    if team_id is not None:
        candidates_by_source["assignee_membership"].append(
            TeamAttributionCandidate(
                source="assignee_membership",
                team_id=team_id,
                team_name=team_name or team_id,
                confidence="medium",
                evidence=f"assignee={assignee}",
                is_primary=1,
                specificity=50,
            )
        )

    if attribution_context is not None and attribution_context.manual_fallbacks:
        candidates_by_source["manual_fallback"].extend(
            _manual_fallback_candidates(item, attribution_context.manual_fallbacks)
        )

    primary: TeamAttributionCandidate | None = None
    rows: list[TeamAttributionCandidate] = []
    for source in sorted(candidates_by_source, key=lambda s: _SOURCE_ORDER[s]):
        ranked = _ranked(_dedupe_candidates(candidates_by_source[source]))
        if primary is None and ranked:
            primary = ranked[0]
        rows.extend(ranked)

    if primary is None:
        primary = TeamAttributionCandidate(
            source="unassigned",
            team_id=None,
            team_name=None,
            confidence="none",
            evidence="no_candidate",
            is_primary=1,
        )
        rows.append(primary)

    marked_rows = [
        TeamAttributionCandidate(
            source=c.source,
            team_id=c.team_id,
            team_name=c.team_name,
            confidence=c.confidence,
            evidence=c.evidence,
            is_primary=1 if c is primary else 0,
            specificity=c.specificity,
            priority=c.priority,
            updated_at=c.updated_at,
        )
        for c in rows
    ]
    return primary.team_id, primary.team_name, marked_rows


def resolve_base_team(
    item: WorkItem,
    team_resolver: TeamResolver | None,
    project_key_resolver: ProjectKeyTeamResolver | None,
) -> tuple[str | None, str | None]:
    """Resolve a work item's team via scope key, then project_key, then assignee.

    Returns ``(None, None)`` when no mapping matches (i.e. the item would
    normalize to ``unassigned``). This is the first three attribution tiers;
    linked-issue inheritance is layered on top by callers. Shared by the
    per-day metrics loop and :func:`build_linked_issue_team_resolver` so both
    apply identical rules — an item only becomes an inheritance donor if it
    resolves to a real team here.
    """
    team_id, team_name, _ = resolve_team_attribution(
        item,
        team_resolver,
        project_key_resolver,
        linked_issue_resolver=None,
        attribution_context=None,
    )
    if team_id == "unassigned":
        return None, None
    return team_id, team_name


# Relationship types that mean "this item does (or duplicates) the work of the
# linked issue" — the only edges from which it is sound to inherit a team. A
# blocking relationship (`blocks`/`blocked_by`/`is_blocked_by`) connects items
# that are frequently owned by *different* teams, so it must NOT drive
# attribution. `external_issue_key` is the provider-neutral cross-provider edge
# emitted by the PR parsers (a PR closing a Linear/Jira issue).
_INHERITABLE_RELATIONSHIP_TYPES = frozenset(
    {"relates_to", "relates", "duplicates", "external_issue_key"}
)


def build_linked_issue_team_resolver(
    *,
    work_items: Sequence[WorkItem],
    dependencies: Sequence[WorkItemDependency],
    team_resolver: TeamResolver | None = None,
    project_key_resolver: ProjectKeyTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> LinkedIssueTeamResolver:
    """Build the cross-item team-inheritance fallback from dependency edges.

    Any work item that resolves to a real team via :func:`resolve_base_team`
    becomes an attribution *donor*. A linked work item that resolves to no
    team inherits the first donor team reachable through a
    ``work_item_dependencies`` edge where it is the ``source`` (PRs are the
    source of their ``fixes``/``closes``/``relates_to`` edges).

    ``extkey:KEY`` targets — emitted by the PR parsers for cross-provider
    references such as a Linear/Jira issue key mentioned in a PR body or
    branch name — are matched against Linear/Jira work-item keys so a GitHub
    PR can inherit from the Linear issue it closes even though the two live in
    different providers.
    """
    donor_team: dict[str, tuple[str, str]] = {}
    base_resolved: dict[str, str | None] = {}
    # Issue KEY (e.g. CHAOS-2400, PROJ-12) -> work_item_id, for resolving the
    # provider-neutral ``extkey:`` targets emitted by PR parsers. ``extkey``
    # carries no provider, so if the SAME key exists in both Linear and Jira it
    # is genuinely ambiguous — those keys are tracked separately and never
    # resolve, so an ambiguous link is dropped rather than guessed.
    key_index: dict[str, str] = {}
    ambiguous_keys: set[str] = set()

    for item in work_items:
        wid = item.work_item_id
        native = _native_team_candidate(item, project_key_resolver)
        team_id, team_name, marked = resolve_team_attribution(
            item,
            team_resolver,
            project_key_resolver,
            linked_issue_resolver=None,
            attribution_context=attribution_context,
        )
        base_resolved[wid] = native.team_id if native is not None else None
        primary_source = next((r.source for r in marked if r.is_primary), None)
        # Only register a donor when its *primary* team came from a first-class
        # fact (sources 0-4). A team earned via manual_fallback must never be
        # relabeled as rank-5 linked_issue inheritance on a dependent item.
        if team_id and primary_source in _DONOR_SOURCES:
            donor_team[wid] = (team_id, team_name or team_id)
        if item.provider in ("linear", "jira") and ":" in wid:
            k = wid.split(":", 1)[1].strip().upper()
            if k in ambiguous_keys:
                continue
            existing = key_index.get(k)
            if existing is not None and existing != wid:
                del key_index[k]
                ambiguous_keys.add(k)  # same key in two providers — ambiguous
            else:
                key_index[k] = wid

    def _canonical_target(target_id: str) -> str | None:
        if target_id.startswith("extkey:"):
            # Missing or ambiguous keys both return None → no inheritance.
            return key_index.get(target_id.split(":", 1)[1].strip().upper())
        return target_id

    def _recency(dep: WorkItemDependency) -> float:
        last = dep.last_synced
        try:
            return last.timestamp() if last is not None else 0.0
        except (ValueError, OverflowError, OSError):
            return 0.0

    # Collapse to one edge per (source, target), keeping the latest by
    # last_synced — so a relationship-type change (e.g. relates_to -> blocked_by)
    # supersedes the stale row instead of leaving an old inheritable edge alive
    # alongside the new one. On identical timestamps the lexicographically
    # smaller relationship_type wins, which deterministically prefers the safer
    # blocking type (`blocked_by`/`blocks` < `relates_to`) over inheriting.
    latest_edge: dict[tuple[str, str], WorkItemDependency] = {}
    for dep in dependencies:
        pair = (dep.source_work_item_id, dep.target_work_item_id)
        cur = latest_edge.get(pair)
        if (
            cur is None
            or _recency(dep) > _recency(cur)
            or (
                _recency(dep) == _recency(cur)
                and dep.relationship_type < cur.relationship_type
            )
        ):
            latest_edge[pair] = dep

    # Collect every valid donor candidate per source, then pick deterministically
    # so attribution never depends on edge/storage order (ClickHouse rows have no
    # inherent ordering). When a source links several team-attributed issues, the
    # lexicographically smallest canonical target wins — a stable, run-independent
    # tiebreak. The common case is a single donor, where the choice is moot.
    candidates: dict[str, list[tuple[str, tuple[str, str]]]] = {}
    for dep in latest_edge.values():
        # Only "does-the-work-of"/duplicate links transfer a team; skip blocking
        # and other relationships that routinely span teams.
        if dep.relationship_type not in _INHERITABLE_RELATIONSHIP_TYPES:
            continue
        source_id = dep.source_work_item_id
        # Only items that resolve to no team of their own need a donor; never
        # override a real attribution. (missing => treat as unresolved.)
        if base_resolved.get(source_id) is not None:
            continue
        target_id = _canonical_target(dep.target_work_item_id)
        if not target_id:
            continue
        donor = donor_team.get(target_id)
        if donor is not None:
            candidates.setdefault(source_id, []).append((target_id, donor))

    inherited: dict[str, tuple[str, str]] = {
        source_id: min(cands, key=lambda c: c[0])[1]
        for source_id, cands in candidates.items()
    }
    return LinkedIssueTeamResolver(_inherited=inherited)


WAIT_STATUSES = {
    "backlog",
    "todo",
    "waiting",
    "blocked",
    "review_requested",
    "waiting_for_review",
}


def _calculate_flow_breakdown(
    item: WorkItem, transitions: list[WorkItemStatusTransition]
) -> tuple[float, float]:
    if not item.started_at or not item.completed_at:
        return 0.0, 0.0

    start_utc = to_utc(item.started_at)
    end_utc = to_utc(item.completed_at)

    if start_utc >= end_utc:
        return 0.0, 0.0

    # Sort transitions by time
    sorted_trans = sorted(transitions, key=lambda x: x.occurred_at)

    # Filter transitions relevant to the cycle time window [started_at, completed_at]
    # Actually, we need to know the state *starting* from started_at.
    # We find the last transition *before* started_at to know initial state.

    # Simple approach: walk through time from start to end.
    current_status = "unknown"

    # Find initial status at started_at
    # Iterate backwards or keep track.
    # Assuming 'in_progress' is the start state if started_at is present.
    # But let's look at transitions before started_at.
    for t in sorted_trans:
        t_utc = to_utc(t.occurred_at)
        if t_utc <= start_utc:
            current_status = t.to_status
        else:
            break

    # If explicitly started, and status is unknown or todo, maybe default to 'active' (in_progress)?
    # Usually started_at corresponds to a transition to In Progress.
    if current_status in ("unknown", "todo", "backlog"):
        current_status = "in_progress"

    active_seconds = 0.0
    wait_seconds = 0.0

    last_time = start_utc

    # Iterate transitions that happen *within* the window
    for t in sorted_trans:
        t_utc = to_utc(t.occurred_at)
        if t_utc <= start_utc:
            continue
        if t_utc >= end_utc:
            break

        # Add duration of previous state
        duration = (t_utc - last_time).total_seconds()
        if current_status.lower() in WAIT_STATUSES:
            wait_seconds += duration
        else:
            active_seconds += duration

        # Update state and time
        current_status = t.to_status
        last_time = t_utc

    # Add final segment from last transition to completed_at
    duration = (end_utc - last_time).total_seconds()
    if duration > 0:
        if current_status.lower() in WAIT_STATUSES:
            wait_seconds += duration
        else:
            active_seconds += duration

    return active_seconds / 3600.0, wait_seconds / 3600.0


class GroupBucket(TypedDict):
    team_name: str
    items_started: int
    items_completed: int
    items_started_unassigned: int
    items_completed_unassigned: int
    wip_count: int
    wip_unassigned: int
    wip_age_hours: list[float]
    lead_hours: list[float]
    cycle_hours: list[float]
    bug_completed: int
    story_points_completed: float
    new_bugs: int
    new_items: int
    weekly_throughput: int
    predictability_score: float


class EstimateCoverageBucket(TypedDict):
    team_name: str
    estimated_count: int
    unestimated_count: int


class UserBucket(TypedDict):
    team_name: str
    items_started: int
    items_completed: int
    wip_count: int
    cycle_hours: list[float]


def compute_work_item_metrics_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    transitions: Sequence[WorkItemStatusTransition],
    computed_at: datetime,
    team_resolver: TeamResolver | None = None,
    project_key_resolver: ProjectKeyTeamResolver | None = None,
    linked_issue_resolver: LinkedIssueTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> tuple[
    list[WorkItemMetricsDailyRecord],
    list[WorkItemUserMetricsDailyRecord],
    list[WorkItemCycleTimeRecord],
]:
    """
    Compute work tracking metrics for a single UTC day.

    Inputs must be WorkItems with:
    - created_at, updated_at always set
    - started_at/completed_at best-effort derived (may be None)

    Null behavior:
    - cycle-time percentiles ignore items missing started_at or completed_at
    - WIP metrics ignore items missing started_at
    """
    start, end = _utc_day_window(day)
    computed_at_utc = to_utc(computed_at)

    # Aggregations keyed by (provider, work_scope_id, team_id).
    by_group: dict[tuple[str, str, str | None], GroupBucket] = {}
    by_user: dict[tuple[str, str, str, str | None], UserBucket] = {}

    cycle_time_records: list[WorkItemCycleTimeRecord] = []

    # Pre-index transitions by work_item_id for faster lookup
    transitions_by_item: dict[str, list[WorkItemStatusTransition]] = {}
    for t in transitions:
        transitions_by_item.setdefault(t.work_item_id, []).append(t)

    for item in work_items:
        work_scope_id = item.work_scope_id or ""
        created_at = to_utc(item.created_at)
        started_at = to_utc(item.started_at) if item.started_at else None
        completed_at = to_utc(item.completed_at) if item.completed_at else None
        terminal_at = _earliest_utc(item.completed_at, item.closed_at)

        # Ignore items that don't exist yet on this day.
        if created_at >= end:
            continue

        assignee = item.assignees[0] if item.assignees else None
        team_id, team_name, _ = resolve_team_attribution(
            item,
            team_resolver,
            project_key_resolver,
            linked_issue_resolver=linked_issue_resolver,
            attribution_context=attribution_context,
        )
        team_id_norm = normalize_team_id(team_id)
        team_name_norm = normalize_team_name(team_name)

        started_today = started_at is not None and start <= started_at < end
        completed_today = completed_at is not None and start <= completed_at < end
        wip_end_of_day = (
            started_at is not None
            and started_at < end
            and (terminal_at is None or terminal_at >= end)
        )

        # Only emit a bucket for groups/users that have activity for this day.
        # However, for Phase 2 metrics (new items), we also need to account for items created today even if not started/completed.
        created_today = start <= created_at < end

        # We need to process if there's any activity or existence relevant to metrics
        relevant_activity = (
            started_today or completed_today or wip_end_of_day or created_today
        )
        if not relevant_activity:
            continue

        group_key = (item.provider, work_scope_id, team_id_norm)
        bucket = by_group.get(group_key)
        if bucket is None:
            new_bucket: GroupBucket = {
                "team_name": team_name_norm,
                "items_started": 0,
                "items_completed": 0,
                "items_started_unassigned": 0,
                "items_completed_unassigned": 0,
                "wip_count": 0,
                "wip_unassigned": 0,
                "wip_age_hours": [],
                "lead_hours": [],
                "cycle_hours": [],
                "bug_completed": 0,
                "story_points_completed": 0.0,
                # Phase 2 metrics counters
                "new_bugs": 0,
                "new_items": 0,
                "weekly_throughput": 0,
                "predictability_score": 0.0,
            }
            by_group[group_key] = new_bucket
            bucket = new_bucket

        user_identity = assignee or "unassigned"
        # User bucket (primary assignee or 'unassigned').
        if user_identity:
            user_key = (item.provider, work_scope_id, user_identity, team_id_norm)
            ub = by_user.get(user_key)
            if ub is None:
                new_user_bucket: UserBucket = {
                    "team_name": team_name_norm,
                    "items_started": 0,
                    "items_completed": 0,
                    "wip_count": 0,
                    "cycle_hours": [],
                }
                by_user[user_key] = new_user_bucket
                ub = new_user_bucket
        else:
            user_key = None
            ub = None

        # Phase 2: Creation stats
        if created_today:
            bucket["new_items"] += 1
            if item.type == "bug":
                bucket["new_bugs"] += 1

        # Phase 2: Weekly Throughput (Completed in last 7 days)
        # Window: [end - 7 days, end)
        week_start = end - timedelta(days=7)
        if completed_at is not None and week_start <= completed_at < end:
            bucket["weekly_throughput"] += 1

        # Started today.
        if started_today:
            bucket["items_started"] += 1
            if assignee is None:
                bucket["items_started_unassigned"] += 1
            if ub is not None:
                ub["items_started"] += 1

        # Completed today.
        if completed_today:
            # We already know completed_at is not None
            assert completed_at is not None
            bucket["items_completed"] += 1
            if assignee is None:
                bucket["items_completed_unassigned"] += 1
            if item.type == "bug":
                bucket["bug_completed"] += 1
            if item.story_points is not None:
                try:
                    bucket["story_points_completed"] += float(item.story_points)
                except Exception:
                    # Ignore invalid story_points values for this work item but log for diagnostics.
                    logging.getLogger(__name__).warning(
                        "Failed to convert story_points for work item %s: %r",
                        getattr(item, "work_item_id", None),
                        item.story_points,
                    )

            if ub is not None:
                ub["items_completed"] += 1

            lead_hours = (completed_at - created_at).total_seconds() / 3600.0
            bucket["lead_hours"].append(float(lead_hours))

            cycle_duration_hours: float | None = None
            active_hours = None
            wait_hours = None
            flow_efficiency = None

            if started_at is not None:
                cycle_duration_hours = (
                    completed_at - started_at
                ).total_seconds() / 3600.0
                bucket["cycle_hours"].append(float(cycle_duration_hours))
                if ub is not None:
                    ub["cycle_hours"].append(float(cycle_duration_hours))

            # Calculate flow breakdown if cycle_hours is available
            if cycle_duration_hours is not None and cycle_duration_hours > 0:
                item_transitions = transitions_by_item.get(item.work_item_id, [])
                calculated_active_h, calculated_wait_h = _calculate_flow_breakdown(
                    item, item_transitions
                )

                # If no transitions recorded between start and complete, assume 100% active.
                if calculated_active_h + calculated_wait_h == 0:
                    active_hours = cycle_duration_hours
                    wait_hours = 0.0
                else:
                    active_hours = calculated_active_h
                    wait_hours = calculated_wait_h

                flow_efficiency = (
                    (active_hours / (active_hours + wait_hours))
                    if (active_hours + wait_hours) > 0
                    else 0.0
                )

            cycle_time_records.append(
                WorkItemCycleTimeRecord(
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    day=completed_at.date(),
                    work_scope_id=work_scope_id,
                    team_id=normalize_team_id(team_id),
                    team_name=team_name_norm,
                    assignee=assignee,
                    type=item.type,
                    status=item.status,
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=float(cycle_duration_hours)
                    if cycle_duration_hours is not None
                    else None,
                    lead_time_hours=float(lead_hours),
                    active_time_hours=float(active_hours)
                    if active_hours is not None
                    else None,
                    wait_time_hours=float(wait_hours)
                    if wait_hours is not None
                    else None,
                    flow_efficiency=float(flow_efficiency)
                    if flow_efficiency is not None
                    else None,
                    computed_at=computed_at_utc,
                )
            )

        # WIP (Active at end of day)
        if wip_end_of_day:
            # started_at is not None, checked by wip_end_of_day
            assert started_at is not None
            bucket["wip_count"] += 1
            if assignee is None:
                bucket["wip_unassigned"] += 1
            age_hours = (end - started_at).total_seconds() / 3600.0
            bucket["wip_age_hours"].append(float(age_hours))
            if ub is not None:
                ub["wip_count"] += 1

    group_records: list[WorkItemMetricsDailyRecord] = []
    for (provider, work_scope_id, team_id), bucket in sorted(
        by_group.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2] or ""))
    ):
        items_completed = bucket["items_completed"]
        bug_completed = bucket["bug_completed"]
        bug_ratio = (bug_completed / items_completed) if items_completed else 0.0
        cycle_hour_values: list[float] = bucket["cycle_hours"]
        lead_hour_values: list[float] = bucket["lead_hours"]
        wip_age_values: list[float] = bucket["wip_age_hours"]

        new_bugs = bucket["new_bugs"]
        new_items = bucket["new_items"]
        defect_rate = (new_bugs / new_items) if new_items else 0.0

        throughput_7d = bucket["weekly_throughput"]
        wip_val = bucket["wip_count"]
        # If throughput is 0, we can't divide. If WIP is > 0 and throughput is 0,
        # congestion is technically infinite. We'll cap or use 0.
        denominator = max(1.0, float(throughput_7d))
        wip_congestion = float(wip_val) / denominator

        # Predictability Proxy: Completion Rate (Completed / (Completed + Remaining))
        # This indicates how effectively the team clears its plate.
        total_load = float(items_completed + wip_val)
        predictability = (
            (float(items_completed) / total_load) if total_load > 0 else 0.0
        )

        group_records.append(
            WorkItemMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                team_id=normalize_team_id(team_id),
                team_name=bucket["team_name"],
                items_started=bucket["items_started"],
                items_completed=items_completed,
                items_started_unassigned=bucket["items_started_unassigned"],
                items_completed_unassigned=bucket["items_completed_unassigned"],
                wip_count_end_of_day=bucket["wip_count"],
                wip_unassigned_end_of_day=bucket["wip_unassigned"],
                cycle_time_p50_hours=float(_percentile(cycle_hour_values, 50.0))
                if cycle_hour_values
                else None,
                cycle_time_p90_hours=float(_percentile(cycle_hour_values, 90.0))
                if cycle_hour_values
                else None,
                lead_time_p50_hours=float(_percentile(lead_hour_values, 50.0))
                if lead_hour_values
                else None,
                lead_time_p90_hours=float(_percentile(lead_hour_values, 90.0))
                if lead_hour_values
                else None,
                wip_age_p50_hours=float(_percentile(wip_age_values, 50.0))
                if wip_age_values
                else None,
                wip_age_p90_hours=float(_percentile(wip_age_values, 90.0))
                if wip_age_values
                else None,
                bug_completed_ratio=float(bug_ratio),
                story_points_completed=float(bucket["story_points_completed"]),
                # Phase 2 metrics
                new_bugs_count=new_bugs,
                new_items_count=new_items,
                defect_intro_rate=defect_rate,
                wip_congestion_ratio=wip_congestion,
                predictability_score=predictability,
                computed_at=computed_at_utc,
            )
        )

    user_records: list[WorkItemUserMetricsDailyRecord] = []
    for (provider, work_scope_id, user_identity, team_id), user_bucket in sorted(
        by_user.items(),
        key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], str(kv[0][3] or "")),
    ):
        user_cycle_hours: list[float] = user_bucket["cycle_hours"]
        user_records.append(
            WorkItemUserMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                user_identity=user_identity,
                team_id=normalize_team_id(team_id),
                team_name=user_bucket["team_name"],
                items_started=user_bucket["items_started"],
                items_completed=user_bucket["items_completed"],
                wip_count_end_of_day=user_bucket["wip_count"],
                cycle_time_p50_hours=float(_percentile(user_cycle_hours, 50.0))
                if user_cycle_hours
                else None,
                cycle_time_p90_hours=float(_percentile(user_cycle_hours, 90.0))
                if user_cycle_hours
                else None,
                computed_at=computed_at_utc,
            )
        )

    return group_records, user_records, cycle_time_records


def compute_estimate_coverage_metrics_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    computed_at: datetime,
    team_resolver: TeamResolver | None = None,
    project_key_resolver: ProjectKeyTeamResolver | None = None,
    linked_issue_resolver: LinkedIssueTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> list[EstimateCoverageMetricsDailyRecord]:
    _start, end = _utc_day_window(day)
    computed_at_utc = to_utc(computed_at)
    by_group: dict[tuple[str, str, str | None], EstimateCoverageBucket] = {}

    for item in work_items:
        created_at = to_utc(item.created_at)
        terminal_at = _earliest_utc(item.completed_at, item.closed_at)
        if created_at >= end:
            continue

        team_id, team_name, _ = resolve_team_attribution(
            item,
            team_resolver,
            project_key_resolver,
            linked_issue_resolver=linked_issue_resolver,
            attribution_context=attribution_context,
        )
        team_id_norm = normalize_team_id(team_id)
        team_name_norm = normalize_team_name(team_name)
        key = (item.provider, item.work_scope_id or "", team_id_norm)
        bucket = by_group.get(key)
        if bucket is None:
            new_bucket: EstimateCoverageBucket = {
                "team_name": team_name_norm,
                "estimated_count": 0,
                "unestimated_count": 0,
            }
            by_group[key] = new_bucket
            bucket = new_bucket

        if terminal_at is not None and terminal_at < end:
            continue

        if item.story_points is None:
            bucket["unestimated_count"] += 1
        else:
            bucket["estimated_count"] += 1

    records: list[EstimateCoverageMetricsDailyRecord] = []
    for (provider, work_scope_id, team_id), bucket in sorted(
        by_group.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2] or ""))
    ):
        estimated_count = bucket["estimated_count"]
        unestimated_count = bucket["unestimated_count"]
        backlog_size = estimated_count + unestimated_count
        ratio = (float(estimated_count) / float(backlog_size)) if backlog_size else None
        records.append(
            EstimateCoverageMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                team_id=normalize_team_id(team_id),
                team_name=bucket["team_name"],
                estimated_count=estimated_count,
                unestimated_count=unestimated_count,
                backlog_size=backlog_size,
                ratio=ratio,
                computed_at=computed_at_utc,
            )
        )
    return records


def compute_work_item_team_attributions(
    *,
    work_items: Sequence[WorkItem],
    computed_at: datetime,
    team_resolver: TeamResolver | None = None,
    project_key_resolver: ProjectKeyTeamResolver | None = None,
    linked_issue_resolver: LinkedIssueTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> list[WorkItemTeamAttributionRecord]:
    computed_at_utc = to_utc(computed_at)
    records: list[WorkItemTeamAttributionRecord] = []
    for item in work_items:
        _, _, candidates = resolve_team_attribution(
            item,
            team_resolver,
            project_key_resolver,
            linked_issue_resolver=linked_issue_resolver,
            attribution_context=attribution_context,
        )
        for candidate in candidates:
            records.append(
                WorkItemTeamAttributionRecord(
                    repo_id=item.repo_id,
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    team_id=candidate.team_id,
                    team_name=candidate.team_name,
                    source=candidate.source,
                    is_primary=candidate.is_primary,
                    confidence=candidate.confidence,
                    evidence=candidate.evidence,
                    computed_at=computed_at_utc,
                    org_id=item.org_id,
                )
            )
    return records
