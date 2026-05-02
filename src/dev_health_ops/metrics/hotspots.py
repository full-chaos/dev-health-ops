from __future__ import annotations

import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import TypedDict

from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    FileComplexitySnapshot,
    FileHotspotDaily,
    FileMetricsRecord,
)


class FileStats(TypedDict):
    churn: int
    authors: set[str]
    commits: set[str]


class ChurnStats(TypedDict):
    churn: int
    commits: int


@dataclass(frozen=True)
class RiskHotspotInput:
    path: str
    churn: int
    commits: int
    complexity: int
    complexity_snapshot: FileComplexitySnapshot | None


def compute_file_hotspots(
    *,
    repo_id: uuid.UUID,
    day: date,
    window_stats: Sequence[CommitStatRow],
    computed_at: datetime,
) -> list[FileMetricsRecord]:
    """
    Compute file hotspot scores based on a window of commit stats.

    Formula (from docs 1.3.2):
    hotspot_raw = α*log(1 + churn_f) + β*contributors_f + γ*commit_count_f
    Weights: α=0.4, β=0.3, γ=0.3
    """
    file_map: dict[str, FileStats] = {}

    for row in window_stats:
        if row["repo_id"] != repo_id:
            continue

        path = row.get("file_path")
        if not path:
            continue

        if path not in file_map:
            file_map[path] = {"churn": 0, "authors": set(), "commits": set()}

        stats = file_map[path]
        additions = max(0, int(row.get("additions") or 0))
        deletions = max(0, int(row.get("deletions") or 0))
        stats["churn"] += additions + deletions

        author = (
            row.get("author_email") or row.get("author_name") or "unknown"
        ).strip()
        stats["authors"].add(author)
        stats["commits"].add(row["commit_hash"])

    records: list[FileMetricsRecord] = []
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
    complexity_map: dict[str, FileComplexitySnapshot],
    blame_map: dict[str, float] | None = None,
    computed_at: datetime,
) -> list[FileHotspotDaily]:
    """
    Compute risk score merging churn (30d) and complexity.

    risk_score = z(churn) + z(complexity)

    Blame concentration can be provided (e.g., derived from git blame data).
    """
    # 1. Aggregate churn per file
    churn_map: dict[str, ChurnStats] = {}
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

    data: list[RiskHotspotInput] = []
    for f in all_files:
        c_stats = churn_map.get(f, {"churn": 0, "commits": 0})
        comp = complexity_map.get(f)

        churn_val = c_stats["churn"]
        comp_val = comp.cyclomatic_total if comp else 0

        data.append(
            RiskHotspotInput(
                path=f,
                churn=churn_val,
                commits=c_stats["commits"],
                complexity=comp_val,
                complexity_snapshot=comp,
            )
        )

    if not data:
        return []

    # 3. Compute Z-scores
    # Helper to compute z-scores for a list of values
    def get_z_scores(values: list[float]) -> list[float]:
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

    churns = [float(item.churn) for item in data]
    complexities = [float(item.complexity) for item in data]

    z_churn = get_z_scores(churns)
    z_comp = get_z_scores(complexities)

    results: list[FileHotspotDaily] = []
    for i, item in enumerate(data):
        risk = z_churn[i] + z_comp[i]

        comp_obj = item.complexity_snapshot
        blame_concentration = None
        if blame_map:
            blame_concentration = blame_map.get(item.path)

        results.append(
            FileHotspotDaily(
                repo_id=repo_id,
                day=day,
                file_path=item.path,
                churn_loc_30d=item.churn,
                churn_commits_30d=item.commits,
                cyclomatic_total=comp_obj.cyclomatic_total if comp_obj else 0,
                cyclomatic_avg=comp_obj.cyclomatic_avg if comp_obj else 0.0,
                blame_concentration=blame_concentration,
                risk_score=risk,
                computed_at=computed_at,
            )
        )

    # Sort by risk descending
    return sorted(results, key=lambda x: x.risk_score, reverse=True)
