from __future__ import annotations

import math
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Sequence, Optional

from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    FileMetricsRecord,
    FileHotspotDaily,
    FileComplexitySnapshot,
)


def compute_file_hotspots(
    *,
    repo_id: uuid.UUID,
    day: date,
    window_stats: Sequence[CommitStatRow],
    computed_at: datetime,
) -> List[FileMetricsRecord]:
    """
    Compute file hotspot scores based on a window of commit stats.

    Formula (from docs 1.3.2):
    hotspot_raw = α*log(1 + churn_f) + β*contributors_f + γ*commit_count_f
    Weights: α=0.4, β=0.3, γ=0.3
    """
    file_map: Dict[str, Dict[str, Any]] = {}

    for row in window_stats:
        if row["repo_id"] != repo_id:
            continue

        path = row.get("file_path")
        if not path:
            continue

        if path not in file_map:
            file_map[path] = {
                "churn": 0,
                "authors": set(),
                "commits": set(),
            }

        stats = file_map[path]
        additions = max(0, int(row.get("additions") or 0))
        deletions = max(0, int(row.get("deletions") or 0))
        stats["churn"] += additions + deletions

        author = (
            row.get("author_email") or row.get("author_name") or "unknown"
        ).strip()
        stats["authors"].add(author)
        stats["commits"].add(row["commit_hash"])

    records: List[FileMetricsRecord] = []
    alpha, beta, gamma = 0.4, 0.3, 0.3

    for path, stats in file_map.items():
        churn = stats["churn"]
        contributors = len(stats["authors"])
        commits_count = len(stats["commits"])

        # Formula from docs 1.3.2
        hotspot_score = (
            (alpha * math.log1p(churn))
            + (beta * contributors)
            + (gamma * commits_count)
        )

        records.append(
            FileMetricsRecord(
                repo_id=repo_id,
                day=day,
                path=path,
                churn=churn,
                contributors=contributors,
                commits_count=commits_count,
                hotspot_score=float(hotspot_score),
                computed_at=computed_at,
            )
        )

    # Sort by hotspot score descending
    return sorted(records, key=lambda r: r.hotspot_score, reverse=True)


def compute_file_risk_hotspots(
    *,
    repo_id: uuid.UUID,
    day: date,
    window_stats: Sequence[CommitStatRow],
    complexity_map: Dict[str, FileComplexitySnapshot],
    blame_map: Optional[Dict[str, float]] = None,
    computed_at: datetime,
) -> List[FileHotspotDaily]:
    """
    Compute risk score merging churn (30d) and complexity.

    risk_score = z(churn) + z(complexity)

    Blame concentration can be provided (e.g., derived from git blame data).
    """
    # 1. Aggregate churn per file
    churn_map: Dict[str, Dict[str, int]] = {}
    for row in window_stats:
        if row["repo_id"] != repo_id:
            continue
        path = row.get("file_path")
        if not path:
            continue

        if path not in churn_map:
            churn_map[path] = {"churn": 0, "commits": 0}

        adds = max(0, int(row.get("additions") or 0))
        dels = max(0, int(row.get("deletions") or 0))
        churn_map[path]["churn"] += adds + dels
        churn_map[path]["commits"] += 1

    # 2. Merge keys (union of churned files and complex files)
    all_files = set(churn_map.keys()) | set(complexity_map.keys())

    data = []
    for f in all_files:
        c_stats = churn_map.get(f, {"churn": 0, "commits": 0})
        comp = complexity_map.get(f)

        churn_val = c_stats["churn"]
        comp_val = comp.cyclomatic_total if comp else 0

        data.append(
            {
                "path": f,
                "churn": churn_val,
                "commits": c_stats["commits"],
                "complexity": comp_val,
                "comp_obj": comp,
            }
        )

    if not data:
        return []

    # 3. Compute Z-scores
    # Helper to compute z-scores for a list of values
    def get_z_scores(values: List[float]) -> List[float]:
        if not values:
            return []
        n = len(values)
        if n < 2:
            return [0.0] * n
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        stdev = math.sqrt(variance)
        if stdev == 0:
            return [0.0] * n
        return [(x - mean) / stdev for x in values]

    churns = [float(d["churn"]) for d in data]
    complexities = [float(d["complexity"]) for d in data]

    z_churn = get_z_scores(churns)
    z_comp = get_z_scores(complexities)

    results = []
    for i, d in enumerate(data):
        risk = z_churn[i] + z_comp[i]

        comp_obj = d["comp_obj"]
        blame_concentration = None
        if blame_map:
            blame_concentration = blame_map.get(d["path"])

        results.append(
            FileHotspotDaily(
                repo_id=repo_id,
                day=day,
                file_path=d["path"],
                churn_loc_30d=d["churn"],
                churn_commits_30d=d["commits"],
                cyclomatic_total=comp_obj.cyclomatic_total if comp_obj else 0,
                cyclomatic_avg=comp_obj.cyclomatic_avg if comp_obj else 0.0,
                blame_concentration=blame_concentration,
                risk_score=risk,
                computed_at=computed_at,
            )
        )

    # Sort by risk descending
    return sorted(results, key=lambda x: x.risk_score, reverse=True)
