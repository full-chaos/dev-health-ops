from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple, TypedDict

from dev_health_ops.metrics.schemas import (
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition
from dev_health_ops.providers.teams import TeamResolver
import logging


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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


def _resolve_team(
    team_resolver: Optional[TeamResolver], identity: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    if team_resolver is None:
        return None, None
    return team_resolver.resolve(identity)


WAIT_STATUSES = {
    "backlog",
    "todo",
    "waiting",
    "blocked",
    "review_requested",
    "waiting_for_review",
}


def _calculate_flow_breakdown(
    item: WorkItem, transitions: List[WorkItemStatusTransition]
) -> Tuple[float, float]:
    if not item.started_at or not item.completed_at:
        return 0.0, 0.0

    start_utc = _to_utc(item.started_at)
    end_utc = _to_utc(item.completed_at)

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
        t_utc = _to_utc(t.occurred_at)
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
        t_utc = _to_utc(t.occurred_at)
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
    wip_age_hours: List[float]
    lead_hours: List[float]
    cycle_hours: List[float]
    bug_completed: int
    story_points_completed: float
    new_bugs: int
    new_items: int
    weekly_throughput: int
    predictability_score: float


class UserBucket(TypedDict):
    team_name: str
    items_started: int
    items_completed: int
    wip_count: int
    cycle_hours: List[float]


def compute_work_item_metrics_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    transitions: Sequence[WorkItemStatusTransition],
    computed_at: datetime,
    team_resolver: Optional[TeamResolver] = None,
) -> Tuple[
    List[WorkItemMetricsDailyRecord],
    List[WorkItemUserMetricsDailyRecord],
    List[WorkItemCycleTimeRecord],
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
    computed_at_utc = _to_utc(computed_at)

    # Aggregations keyed by (provider, work_scope_id, team_id).
    by_group: Dict[Tuple[str, str, Optional[str]], GroupBucket] = {}
    by_user: Dict[Tuple[str, str, str, Optional[str]], UserBucket] = {}

    cycle_time_records: List[WorkItemCycleTimeRecord] = []

    # Pre-index transitions by work_item_id for faster lookup
    transitions_by_item: Dict[str, List[WorkItemStatusTransition]] = {}
    for t in transitions:
        transitions_by_item.setdefault(t.work_item_id, []).append(t)

    for item in work_items:
        work_scope_id = item.work_scope_id or ""
        created_at = _to_utc(item.created_at)
        started_at = _to_utc(item.started_at) if item.started_at else None
        completed_at = _to_utc(item.completed_at) if item.completed_at else None

        # Ignore items that don't exist yet on this day.
        if created_at >= end:
            continue

        assignee = item.assignees[0] if item.assignees else None
        team_id, team_name = _resolve_team(team_resolver, assignee)
        team_id_norm = team_id or "unassigned"
        team_name_norm = team_name or "Unassigned"

        started_today = started_at is not None and start <= started_at < end
        completed_today = completed_at is not None and start <= completed_at < end
        wip_end_of_day = (
            started_at is not None
            and started_at < end
            and (completed_at is None or completed_at >= end)
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
            bucket = {
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
            by_group[group_key] = bucket

        user_identity = assignee or "unassigned"
        # User bucket (primary assignee or 'unassigned').
        if user_identity:
            user_key = (item.provider, work_scope_id, user_identity, team_id_norm)
            ub = by_user.get(user_key)
            if ub is None:
                ub = {
                    "team_name": team_name_norm,
                    "items_started": 0,
                    "items_completed": 0,
                    "wip_count": 0,
                    "cycle_hours": [],
                }
                by_user[user_key] = ub
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

            cycle_hours = None
            active_hours = None
            wait_hours = None
            flow_efficiency = None

            if started_at is not None:
                cycle_hours = (completed_at - started_at).total_seconds() / 3600.0
                bucket["cycle_hours"].append(float(cycle_hours))
                if ub is not None:
                    ub["cycle_hours"].append(float(cycle_hours))

            # Calculate flow breakdown if cycle_hours is available
            if cycle_hours is not None and cycle_hours > 0:
                item_transitions = transitions_by_item.get(item.work_item_id, [])
                calculated_active_h, calculated_wait_h = _calculate_flow_breakdown(
                    item, item_transitions
                )

                # If no transitions recorded between start and complete, assume 100% active.
                if calculated_active_h + calculated_wait_h == 0:
                    active_hours = cycle_hours
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
                    day=completed_at.date(),  # type: ignore
                    work_scope_id=work_scope_id,
                    team_id=team_id or "unassigned",
                    team_name=team_name_norm,
                    assignee=assignee,
                    type=item.type,
                    status=item.status,
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=float(cycle_hours)
                    if cycle_hours is not None
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

    group_records: List[WorkItemMetricsDailyRecord] = []
    for (provider, work_scope_id, team_id), bucket in sorted(
        by_group.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2] or ""))
    ):
        items_completed = bucket["items_completed"]
        bug_completed = bucket["bug_completed"]
        bug_ratio = (bug_completed / items_completed) if items_completed else 0.0
        cycle_hours = bucket["cycle_hours"]
        lead_hours = bucket["lead_hours"]
        wip_ages = bucket["wip_age_hours"]

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
                team_id=team_id or "unassigned",
                team_name=bucket["team_name"],
                items_started=bucket["items_started"],
                items_completed=items_completed,
                items_started_unassigned=bucket["items_started_unassigned"],
                items_completed_unassigned=bucket["items_completed_unassigned"],
                wip_count_end_of_day=bucket["wip_count"],
                wip_unassigned_end_of_day=bucket["wip_unassigned"],
                cycle_time_p50_hours=float(_percentile(cycle_hours, 50.0))
                if cycle_hours
                else None,
                cycle_time_p90_hours=float(_percentile(cycle_hours, 90.0))
                if cycle_hours
                else None,
                lead_time_p50_hours=float(_percentile(lead_hours, 50.0))
                if lead_hours
                else None,
                lead_time_p90_hours=float(_percentile(lead_hours, 90.0))
                if lead_hours
                else None,
                wip_age_p50_hours=float(_percentile(wip_ages, 50.0))
                if wip_ages
                else None,
                wip_age_p90_hours=float(_percentile(wip_ages, 90.0))
                if wip_ages
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

    user_records: List[WorkItemUserMetricsDailyRecord] = []
    for (provider, work_scope_id, user_identity, team_id), bucket in sorted(
        by_user.items(),
        key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], str(kv[0][3] or "")),
    ):
        cycle_hours = bucket["cycle_hours"]
        user_records.append(
            WorkItemUserMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                user_identity=user_identity,
                team_id=team_id or "unassigned",
                team_name=bucket["team_name"],
                items_started=bucket["items_started"],
                items_completed=bucket["items_completed"],
                wip_count_end_of_day=bucket["wip_count"],
                cycle_time_p50_hours=float(_percentile(cycle_hours, 50.0))
                if cycle_hours
                else None,
                cycle_time_p90_hours=float(_percentile(cycle_hours, 90.0))
                if cycle_hours
                else None,
                computed_at=computed_at_utc,
            )
        )

    return group_records, user_records, cycle_time_records
