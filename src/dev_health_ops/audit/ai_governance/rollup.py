"""Daily coverage rollups for AI governance."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone
from uuid import UUID

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceArtifact,
    AIGovernanceCoverageDaily,
)
from dev_health_ops.audit.ai_governance.policy import evaluate_artifact


def rollup_coverage_daily(
    artifacts: Iterable[AIGovernanceArtifact],
    *,
    day: date,
    computed_at: datetime | None = None,
) -> list[AIGovernanceCoverageDaily]:
    """Produce team/repo coverage rows for AI governance."""
    grouped: dict[tuple[str, str | None, UUID | None], list[AIGovernanceArtifact]] = {}
    for artifact in artifacts:
        if not artifact.ai_detected or artifact.observed_at.date() != day:
            continue
        key = (artifact.org_id, artifact.team_id, artifact.repo_id)
        grouped.setdefault(key, []).append(artifact)

    rows: list[AIGovernanceCoverageDaily] = []
    row_computed_at = computed_at or datetime.now(timezone.utc)
    for (org_id, team_id, repo_id), group in sorted(
        grouped.items(),
        key=lambda item: (item[0][0], item[0][1] or "", str(item[0][2])),
    ):
        rows.append(
            AIGovernanceCoverageDaily(
                org_id=org_id,
                team_id=team_id,
                repo_id=repo_id,
                day=day,
                ai_artifacts=len(group),
                declared_artifacts=sum(1 for a in group if a.declared_ai),
                human_reviewed_prs=sum(
                    1
                    for a in group
                    if a.subject_type == "pull_request" and a.human_reviewed is True
                ),
                security_scanned_prs=sum(
                    1
                    for a in group
                    if a.subject_type == "pull_request" and a.security_scanned is True
                ),
                in_policy_artifacts=sum(1 for a in group if not evaluate_artifact(a)),
                computed_at=row_computed_at,
            )
        )
    return rows
