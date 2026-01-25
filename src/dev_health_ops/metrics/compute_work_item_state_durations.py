from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from dev_health_ops.metrics.schemas import WorkItemStateDurationDailyRecord
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemStatusCategory,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.teams import TeamResolver


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_team(
    team_resolver: Optional[TeamResolver], identity: Optional[str]
) -> Tuple[str, str]:
    if team_resolver is None:
        return "unassigned", "Unassigned"
    team_id, team_name = team_resolver.resolve(identity)
    return team_id or "unassigned", team_name or "Unassigned"


def _segment_statuses(
    *,
    item: WorkItem,
    transitions: Sequence[WorkItemStatusTransition],
    computed_at: datetime,
) -> List[Tuple[WorkItemStatusCategory, datetime, datetime]]:
    """
    Build best-effort status segments for a work item.

    Segments are (status_category, start_at, end_at) in UTC.

    End of the final segment is:
    - completed_at if present
    - else updated_at
    - else computed_at (fallback)
    """
    created_at = _to_utc(item.created_at)
    completed_at = _to_utc(item.completed_at) if item.completed_at else None
    # For open items, assume the last known status persists through `computed_at`.
    end_of_item = completed_at or _to_utc(computed_at)

    ordered = sorted(list(transitions), key=lambda t: _to_utc(t.occurred_at))
    if not ordered:
        # No history; cannot infer per-status durations.
        return []

    segments: List[Tuple[WorkItemStatusCategory, datetime, datetime]] = []

    first = ordered[0]
    current_status: WorkItemStatusCategory = first.from_status or item.status  # type: ignore[assignment]
    current_start = created_at

    for tr in ordered:
        tr_at = _to_utc(tr.occurred_at)
        if tr_at <= current_start:
            current_status = tr.to_status
            current_start = tr_at
            continue

        segments.append((current_status, current_start, tr_at))
        current_status = tr.to_status
        current_start = tr_at

    if end_of_item > current_start:
        segments.append((current_status, current_start, end_of_item))

    # Drop invalid or zero segments.
    return [(s, a, b) for (s, a, b) in segments if b > a]


def compute_work_item_state_durations_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    transitions: Sequence[WorkItemStatusTransition],
    computed_at: datetime,
    team_resolver: Optional[TeamResolver] = None,
) -> List[WorkItemStateDurationDailyRecord]:
    """
    Compute per-day time-in-state totals from status transitions.

    Output rows are keyed for Grafana aggregation:
    (day, provider, work_scope_id, team_id, status)

    Null/missing behavior:
    - if an item has no status transitions, it contributes no time-in-state rows.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    transitions_by_id: Dict[str, List[WorkItemStatusTransition]] = defaultdict(list)
    for tr in transitions:
        transitions_by_id[tr.work_item_id].append(tr)

    totals: Dict[Tuple[str, str, str, str], float] = defaultdict(float)
    items_seen: Dict[Tuple[str, str, str, str], set] = defaultdict(set)
    team_name_by_key: Dict[Tuple[str, str, str], str] = {}

    for item in work_items:
        item_transitions = transitions_by_id.get(item.work_item_id) or []
        if not item_transitions:
            continue

        assignee = item.assignees[0] if item.assignees else None
        team_id, team_name = _resolve_team(team_resolver, assignee)
        work_scope_id = item.work_scope_id or ""
        team_name_by_key[(item.provider, work_scope_id, team_id)] = team_name

        for status, seg_start, seg_end in _segment_statuses(
            item=item, transitions=item_transitions, computed_at=computed_at_utc
        ):
            overlap_start = max(start, seg_start)
            overlap_end = min(end, seg_end)
            if overlap_end <= overlap_start:
                continue
            hours = (overlap_end - overlap_start).total_seconds() / 3600.0
            key = (item.provider, work_scope_id, team_id, str(status))
            totals[key] += float(hours)
            items_seen[key].add(item.work_item_id)

    rows: List[WorkItemStateDurationDailyRecord] = []
    for (provider, work_scope_id, team_id, status), total_hours in sorted(
        totals.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], kv[0][3])
    ):
        team_name = team_name_by_key.get((provider, work_scope_id, team_id), "")
        avg_wip = float(total_hours) / 24.0
        rows.append(
            WorkItemStateDurationDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                team_id=team_id,
                team_name=team_name,
                status=status,
                duration_hours=float(total_hours),
                items_touched=len(
                    items_seen.get((provider, work_scope_id, team_id, status), set())
                ),
                avg_wip=avg_wip,
                computed_at=computed_at_utc,
            )
        )

    return rows
