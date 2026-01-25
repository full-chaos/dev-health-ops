from __future__ import annotations

import math
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Sequence

from dev_health_ops.metrics.schemas import (
    UserMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
    ICLandscapeRollingRecord,
)


def _percentile_rank(values: Sequence[float], value: float) -> float:
    """
    Compute percentile rank of a value within a distribution.
    Returns 0..1
    """
    if not values:
        return 0.5  # Default middle if no context

    sorted_vals = sorted(values)
    n = len(sorted_vals)
    # Find rank
    # simple rank: number of values <= value / n
    count_less = sum(1 for v in sorted_vals if v < value)
    count_equal = sum(1 for v in sorted_vals if v == value)

    # Kind='weak': fraction of scores <= x
    rank = (count_less + 0.5 * count_equal) / n
    return rank


def compute_ic_metrics_daily(
    git_metrics: List[UserMetricsDailyRecord],
    wi_metrics: List[WorkItemUserMetricsDailyRecord],
    team_map: Dict[str, str],
) -> List[UserMetricsDailyRecord]:
    """
    Merge Git-based user metrics and WorkItem-based user metrics into a unified record.
    Also populates the new IC fields (identity_id, loc_touched, delivery_units, etc).
    """

    # Process Git metrics
    git_map: Dict[str, UserMetricsDailyRecord] = {}
    for r in git_metrics:
        identity = r.author_email  # Assuming normalized already
        git_map[identity] = r

    # Process WI metrics
    # TODO: Find a way to map JIRA/LinearB/whatever to git commits more reliably.
    # Currently we rely on identity_mapping.yaml to normalize identities separately
    # for Git (via compute.py) and WorkItems (via compute_work_items.py).
    wi_map: Dict[str, WorkItemUserMetricsDailyRecord] = {}
    for r in wi_metrics:
        # wi metrics might be per-provider, we aggregate per user
        identity = r.user_identity
        existing = wi_map.get(identity)
        if existing:
            # Aggregate if multiple providers for same user
            # We need to create a new record summing up
            wi_map[identity] = WorkItemUserMetricsDailyRecord(
                day=r.day,
                provider="mixed",
                work_scope_id="mixed",
                user_identity=identity,
                team_id=r.team_id,
                team_name=r.team_name,
                items_started=existing.items_started + r.items_started,
                items_completed=existing.items_completed + r.items_completed,
                wip_count_end_of_day=existing.wip_count_end_of_day
                + r.wip_count_end_of_day,
                cycle_time_p50_hours=max(
                    existing.cycle_time_p50_hours or 0, r.cycle_time_p50_hours or 0
                ),  # Crude aggregation
                cycle_time_p90_hours=max(
                    existing.cycle_time_p90_hours or 0, r.cycle_time_p90_hours or 0
                ),
                computed_at=r.computed_at,
            )
        else:
            wi_map[identity] = r

    all_identities = set(git_map.keys()) | set(wi_map.keys())

    results: List[UserMetricsDailyRecord] = []

    for identity in all_identities:
        g = git_map.get(identity)
        w = wi_map.get(identity)

        # Use git record as base if exists, else synthesize
        if g:
            base = g
        else:
            # Create a zero-filled base record using properties from w
            base = UserMetricsDailyRecord(
                repo_id=uuid.uuid4(),  # We don't have a repo_id for pure WI users easily, use random or dummy
                day=w.day if w else date.today(),
                author_email=identity,
                commits_count=0,
                loc_added=0,
                loc_deleted=0,
                files_changed=0,
                large_commits_count=0,
                avg_commit_size_loc=0.0,
                prs_authored=0,
                prs_merged=0,
                avg_pr_cycle_hours=0.0,
                median_pr_cycle_hours=0.0,
                computed_at=datetime.now(timezone.utc),
            )

        # Resolve team
        team_id = team_map.get(identity) or base.team_id

        # Compute derived fields
        loc_touched = base.loc_added + base.loc_deleted

        wi_completed = w.items_completed if w else 0
        wi_active = w.wip_count_end_of_day if w else 0

        delivery_units = base.prs_merged + wi_completed

        # Create new record with extended fields
        # We assume the schema change allows passing these to __init__ or we use replace()
        # Since dataclasses are frozen, we use __init__ with all fields.
        # But standard dataclasses don't support .replace() easily unless we use dataclasses.replace
        from dataclasses import replace

        new_record = replace(
            base,
            identity_id=identity,
            team_id=team_id,
            loc_touched=loc_touched,
            prs_opened=base.prs_authored,  # Map prs_authored to prs_opened
            work_items_completed=wi_completed,
            work_items_active=wi_active,
            delivery_units=delivery_units,
            cycle_p50_hours=base.median_pr_cycle_hours,
            cycle_p90_hours=base.pr_cycle_p90_hours,
        )

        results.append(new_record)

    return results


def compute_ic_landscape_rolling(
    as_of_day: date,
    rolling_stats: List[Dict[str, Any]],
    team_map: Dict[str, str],
) -> List[ICLandscapeRollingRecord]:
    """
    Compute landscape coordinates for the 3 maps.

    Maps:
    1. churn_throughput: X=log(loc_touched), Y=delivery_units
    2. cycle_throughput: X=log(cycle_p50), Y=delivery_units
    3. wip_throughput: X=wip_max, Y=delivery_units

    Normalization is per-team.
    """

    # 1. Prepare raw values
    # Structure: identity -> { map_name -> {x_raw, y_raw} }

    computed_at = datetime.now(timezone.utc)

    records: List[ICLandscapeRollingRecord] = []

    # Enrich stats with team_id from map if missing
    enriched_stats = []
    for row in rolling_stats:
        identity = row.get("identity_id") or ""
        team_id = row.get("team_id")
        if not team_id:
            if identity and identity != "unknown":
                team_id = team_map.get(identity, "unassigned")

        # Ensure numeric types
        churn = float(row.get("churn_loc_30d") or 0)
        delivery = float(row.get("delivery_units_30d") or 0)
        cycle = float(row.get("cycle_p50_30d_hours") or 0)
        wip = float(row.get("wip_max_30d") or 0)

        # Map 1: Churn vs Throughput
        x1 = math.log1p(churn)
        y1 = delivery  # Throughput is delivery units

        # Map 2: Cycle vs Throughput
        x2 = math.log1p(cycle)
        y2 = delivery

        # Map 3: WIP vs Throughput
        x3 = wip
        y3 = delivery

        enriched_stats.append(
            {
                "identity_id": identity,
                "team_id": team_id,
                "stats": {
                    "churn_loc_30d": churn,
                    "delivery_units_30d": delivery,
                    "cycle_p50_30d_hours": cycle,
                    "wip_max_30d": wip,
                },
                "maps": {
                    "churn_throughput": (x1, y1),
                    "cycle_throughput": (x2, y2),
                    "wip_throughput": (x3, y3),
                },
            }
        )

    # 2. Group by team for normalization
    by_team: Dict[str, List[Any]] = {}
    for item in enriched_stats:
        by_team.setdefault(item["team_id"], []).append(item)

    # 3. Compute norms and build records
    for team_id, items in by_team.items():
        # Collect vectors for each map
        vectors = {
            "churn_throughput": {"x": [], "y": []},
            "cycle_throughput": {"x": [], "y": []},
            "wip_throughput": {"x": [], "y": []},
        }

        for item in items:
            for map_name, (x, y) in item["maps"].items():
                vectors[map_name]["x"].append(x)
                vectors[map_name]["y"].append(y)

        # Compute ranks
        for item in items:
            identity = item["identity_id"]
            stats = item["stats"]

            for map_name, (x, y) in item["maps"].items():
                x_vec = vectors[map_name]["x"]
                y_vec = vectors[map_name]["y"]

                x_norm = _percentile_rank(x_vec, x)
                y_norm = _percentile_rank(y_vec, y)

                records.append(
                    ICLandscapeRollingRecord(
                        repo_id=uuid.UUID(
                            int=0
                        ),  # Placeholder, landscape is cross-repo usually?
                        # Actually schema requires repo_id. We might use the one from input if available,
                        # or just a dummy one if aggregating across repos.
                        # The rolling stats query grouped by identity_id (cross-repo).
                        as_of_day=as_of_day,
                        identity_id=identity,
                        team_id=team_id,
                        map_name=map_name,
                        x_raw=x,
                        y_raw=y,
                        x_norm=x_norm,
                        y_norm=y_norm,
                        churn_loc_30d=int(stats["churn_loc_30d"]),
                        delivery_units_30d=int(stats["delivery_units_30d"]),
                        cycle_p50_30d_hours=stats["cycle_p50_30d_hours"],
                        wip_max_30d=int(stats["wip_max_30d"]),
                        computed_at=computed_at,
                    )
                )

    return records
