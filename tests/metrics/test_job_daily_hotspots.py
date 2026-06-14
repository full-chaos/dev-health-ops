"""CHAOS-2376: file_hotspot_daily live compute + git_blame default backfill.

Two seams are proven here without a live ClickHouse / GitHub / GitLab:

(a) ``job_daily`` loads the latest complexity snapshot per file and feeds it
    into ``compute_file_risk_hotspots`` so ``file_hotspot_daily`` is written on
    the live daily path (not only by fixtures).
(b) the default ``backfill_missing and sync_git`` onboarding backfills in the
    GitHub / GitLab processors pass ``include_blame=True`` so the Ownership-risk
    tab is populated for normal OAuth orgs.
"""

from __future__ import annotations

import ast
import textwrap
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics import job_daily
from dev_health_ops.metrics.hotspots import compute_file_risk_hotspots
from dev_health_ops.metrics.schemas import CommitStatRow, FileComplexitySnapshot

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)

PROCESSORS_DIR = Path(job_daily.__file__).resolve().parent.parent / "processors"


# ---------------------------------------------------------------------------
# (a) complexity-snapshot loader maps ClickHouse rows -> FileComplexitySnapshot
# ---------------------------------------------------------------------------


class _ComplexitySink:
    """Minimal sink returning canned ``file_complexity_snapshots`` rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.queries.append((query, parameters))
        return self._rows


def test_load_complexity_map_for_repo_maps_rows() -> None:
    repo_id = uuid.uuid4()
    sink = _ComplexitySink(
        [
            {
                "file_path": "app/core.py",
                "language": "python",
                "loc": 120,
                "functions_count": 4,
                "cyclomatic_total": 31,
                "cyclomatic_avg": 7.75,
                "high_complexity_functions": 2,
                "very_high_complexity_functions": 1,
            },
            # A blank path is dropped (defensive against malformed rows).
            {"file_path": "", "cyclomatic_total": 99, "cyclomatic_avg": 9.0},
        ]
    )

    result = job_daily._load_complexity_map_for_repo(
        primary_sink=sink,
        org_id="acme",
        repo_id=repo_id,
        day=DAY,
    )

    assert set(result) == {"app/core.py"}
    snap = result["app/core.py"]
    assert isinstance(snap, FileComplexitySnapshot)
    assert snap.cyclomatic_total == 31
    assert snap.cyclomatic_avg == 7.75
    assert snap.org_id == "acme"
    # Scoped to repo/day/org with a latest-compute argMax read.
    query, params = sink.queries[0]
    assert params["repo_id"] == str(repo_id)
    assert params["day"] == DAY
    assert params["org_id"] == "acme"
    assert "argMax" in query
    assert "file_complexity_snapshots" in query


def test_load_complexity_map_swallows_query_failure() -> None:
    class _Boom:
        def query_dicts(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
            raise RuntimeError("table missing")

    # A missing/unmigrated table must not abort the daily job.
    result = job_daily._load_complexity_map_for_repo(
        primary_sink=_Boom(),
        org_id="acme",
        repo_id=uuid.uuid4(),
        day=DAY,
    )
    assert result == {}


def test_risk_hotspot_seam_merges_complexity_and_churn() -> None:
    """The loaded complexity map + churn window produce ranked hotspot rows."""
    repo_id = uuid.uuid4()
    complexity_map = job_daily._load_complexity_map_for_repo(
        primary_sink=_ComplexitySink(
            [
                {
                    "file_path": "hot.py",
                    "language": "python",
                    "loc": 300,
                    "functions_count": 10,
                    "cyclomatic_total": 80,
                    "cyclomatic_avg": 8.0,
                    "high_complexity_functions": 5,
                    "very_high_complexity_functions": 2,
                },
                {
                    "file_path": "calm.py",
                    "language": "python",
                    "loc": 40,
                    "functions_count": 2,
                    "cyclomatic_total": 3,
                    "cyclomatic_avg": 1.5,
                    "high_complexity_functions": 0,
                    "very_high_complexity_functions": 0,
                },
            ]
        ),
        org_id="acme",
        repo_id=repo_id,
        day=DAY,
    )

    window_stats: list[CommitStatRow] = [
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "file_path": "hot.py",
            "additions": 200,
            "deletions": 100,
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": NOW,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
        {
            "repo_id": repo_id,
            "commit_hash": "c2",
            "file_path": "calm.py",
            "additions": 1,
            "deletions": 0,
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": NOW,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
    ]

    rows = compute_file_risk_hotspots(
        repo_id=repo_id,
        day=DAY,
        window_stats=window_stats,
        complexity_map=complexity_map,
        computed_at=NOW,
    )

    assert {r.file_path for r in rows} == {"hot.py", "calm.py"}
    # High churn + high complexity ranks first.
    assert rows[0].file_path == "hot.py"
    assert rows[0].cyclomatic_total == 80
    assert rows[0].risk_score > rows[1].risk_score


# ---------------------------------------------------------------------------
# (b) default onboarding backfills pass include_blame=True
# ---------------------------------------------------------------------------


def _backfill_include_blame_values(source_path: Path, func_name: str) -> list[bool]:
    """Return the ``include_blame`` literal passed at each call to *func_name*.

    Only inspects keyword args that are constant booleans; a call that omits
    ``include_blame`` contributes ``True`` (the function default).
    """
    tree = ast.parse(source_path.read_text())
    values: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if not (isinstance(target, ast.Name) and target.id == func_name):
            continue
        kw = next(
            (k for k in node.keywords if k.arg == "include_blame"),
            None,
        )
        if kw is None:
            values.append(True)
            continue
        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, bool):
            values.append(kw.value.value)
    return values


def test_github_backfill_calls_request_blame() -> None:
    values = _backfill_include_blame_values(
        PROCESSORS_DIR / "github.py", "_backfill_github_missing_data"
    )
    assert values, "expected at least one _backfill_github_missing_data call"
    assert all(values), (
        "GitHub onboarding backfill must request blame so the Ownership-risk "
        f"tab is populated (CHAOS-2376); saw include_blame values: {values}"
    )


def test_gitlab_backfill_calls_request_blame() -> None:
    values = _backfill_include_blame_values(
        PROCESSORS_DIR / "gitlab.py", "_backfill_gitlab_missing_data"
    )
    assert values, "expected at least one _backfill_gitlab_missing_data call"
    assert all(values), (
        "GitLab onboarding backfill must request blame so the Ownership-risk "
        f"tab is populated (CHAOS-2376); saw include_blame values: {values}"
    )


def test_backfill_default_include_blame_is_true() -> None:
    """The processor helper signatures default include_blame to True."""
    for path, func in (
        (PROCESSORS_DIR / "github.py", "_backfill_github_missing_data"),
        (PROCESSORS_DIR / "gitlab.py", "_backfill_gitlab_missing_data"),
    ):
        tree = ast.parse(textwrap.dedent(path.read_text()))
        fn = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == func
        )
        default_by_arg = dict(
            zip(
                [a.arg for a in fn.args.args][-len(fn.args.defaults) :],
                fn.args.defaults,
            )
        )
        include_blame_default = default_by_arg["include_blame"]
        assert isinstance(include_blame_default, ast.Constant)
        assert include_blame_default.value is True
