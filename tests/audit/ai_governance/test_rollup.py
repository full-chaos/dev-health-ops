from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID

from dev_health_ops.audit.ai_governance import (
    AIGovernanceArtifact,
    rollup_coverage_daily,
)

DAY = date(2026, 5, 18)
NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
REPO = UUID("11111111-1111-1111-1111-111111111111")


def test_rollup_coverage_counts_team_and_repo_scope() -> None:
    rows = rollup_coverage_daily(
        [
            AIGovernanceArtifact(
                org_id="org-1",
                team_id="team-1",
                repo_id=REPO,
                subject_type="pull_request",
                subject_id="1",
                observed_at=NOW,
                ai_detected=True,
                declared_ai=True,
                human_reviewed=True,
                security_scanned=True,
            ),
            AIGovernanceArtifact(
                org_id="org-1",
                team_id="team-1",
                repo_id=REPO,
                subject_type="pull_request",
                subject_id="2",
                observed_at=NOW,
                ai_detected=True,
                declared_ai=False,
                human_reviewed=False,
                security_scanned=False,
            ),
            AIGovernanceArtifact(
                org_id="org-1",
                team_id="team-1",
                repo_id=REPO,
                subject_type="commit",
                subject_id="abc",
                observed_at=NOW,
                ai_detected=False,
            ),
        ],
        day=DAY,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.org_id == "org-1"
    assert row.team_id == "team-1"
    assert row.repo_id == REPO
    assert row.ai_artifacts == 2
    assert row.declared_artifacts == 1
    assert row.human_reviewed_prs == 1
    assert row.security_scanned_prs == 1
    assert row.in_policy_artifacts == 1
    assert row.declaration_coverage == 0.5
    assert row.in_policy_coverage == 0.5
