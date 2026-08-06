"""CHAOS-3219: conflicting-evidence fixture rows for the ask-dev-world.

Realizes ``sources.json``'s ``conflicting`` terminal state and the corpus
registry's ``trust.conflicting-evidence`` family: two CI signals for the
same repository that disagree.

Honesty note on schema fidelity: ``models.git.CiPipelineRun``
(``ci_pipeline_runs``) has no ``commit_sha``/ref column at all -- a pipeline
run is keyed by ``(repo_id, run_id)`` only, so "two runs against the exact
same commit" cannot be expressed at this table's own grain. The realization
here is the closest honest analog the real schema supports: two runs for the
SAME repo, queued within the same short window, with opposite ``status``
values -- two contemporaneous CI signals for the same repository that
disagree, which is what ``trust.conflicting-evidence``'s question shape
("Is meridian/web-app's CI passing?") actually asks about. Documented here
rather than silently claiming commit-level precision the schema cannot back.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True, slots=True)
class ConflictingCiRunPair:
    repo_id: uuid.UUID
    run_id_success: str
    run_id_failed: str
    queued_at: datetime
    org_id: str


def build_conflicting_ci_runs(
    *, repo_id: uuid.UUID, org_id: str, as_of: datetime, seed_label: str
) -> ConflictingCiRunPair:
    """Two same-repo, same-window, opposite-conclusion CI runs.

    ``as_of`` anchors ``queued_at`` -- always caller-supplied (the world's
    pinned ``now`` or a repo-specific deterministic offset of it), never
    ``datetime.now()``.
    """

    return ConflictingCiRunPair(
        repo_id=repo_id,
        run_id_success=f"conflict-{seed_label}-success",
        run_id_failed=f"conflict-{seed_label}-failed",
        queued_at=as_of,
        org_id=org_id,
    )


def to_postgres_ci_pipeline_runs(pair: ConflictingCiRunPair) -> list[Any]:
    """The pair as :class:`models.git.CiPipelineRun` ORM rows."""

    from dev_health_ops.models.git import CiPipelineRun

    started = pair.queued_at + timedelta(minutes=1)
    finished_ok = started + timedelta(minutes=8)
    finished_bad = started + timedelta(minutes=5)
    return [
        CiPipelineRun(
            repo_id=pair.repo_id,
            run_id=pair.run_id_success,
            status="success",
            queued_at=pair.queued_at,
            started_at=started,
            finished_at=finished_ok,
        ),
        CiPipelineRun(
            repo_id=pair.repo_id,
            run_id=pair.run_id_failed,
            status="failed",
            queued_at=pair.queued_at + timedelta(minutes=2),
            started_at=started + timedelta(minutes=2),
            finished_at=finished_bad,
        ),
    ]


def to_clickhouse_extended_rows(
    pair: ConflictingCiRunPair, *, team_id: str | None, service_id: str
) -> list[dict[str, Any]]:
    """The pair as ``PipelineRunExtendedRow``-shaped dicts for ClickHouse.

    Mirrors ``fixtures/generators/pipelines.py``
    ``generate_pipeline_run_extended_rows``'s row shape exactly, so it can be
    handed straight to :meth:`ClickHouseStore.insert_testops_pipeline_runs`
    alongside a repo's normally-generated rows.
    """

    started = pair.queued_at + timedelta(minutes=1)
    finished_ok = started + timedelta(minutes=8)
    finished_bad = started + timedelta(minutes=5)
    base = {
        "repo_id": pair.repo_id,
        "provider": "github_actions",
        "retry_count": 0,
        "team_id": team_id,
        "service_id": service_id,
        "org_id": pair.org_id,
    }
    return [
        {
            **base,
            "run_id": pair.run_id_success,
            "status": "success",
            "queued_at": pair.queued_at,
            "started_at": started,
            "finished_at": finished_ok,
        },
        {
            **base,
            "run_id": pair.run_id_failed,
            "status": "failed",
            "queued_at": pair.queued_at + timedelta(minutes=2),
            "started_at": started + timedelta(minutes=2),
            "finished_at": finished_bad,
        },
    ]
