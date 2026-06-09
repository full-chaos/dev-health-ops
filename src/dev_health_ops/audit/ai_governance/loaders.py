"""Typed ClickHouse query helpers for AI governance."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any
from uuid import UUID

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceArtifact,
    AIGovernanceCoverageDaily,
    AIGovernanceViolation,
    ToolAllowlistStatus,
)


@dataclass(frozen=True)
class AIGovernanceViolationQueryRow:
    org_id: str
    team_id: str | None
    repo_id: UUID | None
    rule_id: str
    severity: str
    subject_type: str
    subject_id: str
    observed_at: datetime
    evidence: str


class AIGovernanceLoader:
    """Query AI governance inputs and outputs from ClickHouse."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def load_artifacts_for_day(
        self, *, org_id: str, day: date
    ) -> list[AIGovernanceArtifact]:
        start = datetime.combine(day, time.min, tzinfo=timezone.utc)
        end = datetime.combine(day, time.max, tzinfo=timezone.utc)
        rows = self.client.query_dicts(
            _ARTIFACTS_SQL,
            {"org_id": org_id, "start": start, "end": end},
        )
        return [_artifact_from_row(row) for row in rows]

    async def load_coverage(
        self,
        *,
        org_id: str,
        start_day: date,
        end_day: date,
        team_id: str | None = None,
        repo_id: UUID | None = None,
    ) -> list[AIGovernanceCoverageDaily]:
        from dev_health_ops.api.queries.client import query_dicts

        rows = await query_dicts(
            self.client,
            _COVERAGE_SQL,
            {
                "org_id": org_id,
                "start_day": start_day,
                "end_day": end_day,
                "team_id": team_id or "",
                "repo_id": str(repo_id) if repo_id is not None else "",
            },
        )
        return [_coverage_from_row(row) for row in rows]

    async def load_violations(
        self,
        *,
        org_id: str,
        start_day: date,
        end_day: date,
        team_id: str | None = None,
        repo_id: UUID | None = None,
        limit: int = 500,
    ) -> list[AIGovernanceViolationQueryRow]:
        from dev_health_ops.api.queries.client import query_dicts

        rows = await query_dicts(
            self.client,
            _VIOLATIONS_SQL,
            {
                "org_id": org_id,
                "start_day": start_day,
                "end_day": end_day,
                "team_id": team_id or "",
                "repo_id": str(repo_id) if repo_id is not None else "",
                "limit": limit,
            },
        )
        return [
            AIGovernanceViolationQueryRow(
                org_id=str(row["org_id"]),
                team_id=_optional_str(row.get("team_id")),
                repo_id=_optional_uuid(row.get("repo_id")),
                rule_id=str(row["rule_id"]),
                severity=str(row["severity"]),
                subject_type=str(row["subject_type"]),
                subject_id=str(row["subject_id"]),
                observed_at=_datetime(row["observed_at"]),
                evidence=str(row.get("evidence") or "{}"),
            )
            for row in rows
        ]


def build_governance_rows_for_day(
    client: Any, *, org_id: str, day: date
) -> tuple[list[AIGovernanceViolation], list[AIGovernanceCoverageDaily]]:
    """Load raw artifacts and compute policy events plus coverage rows."""
    from dev_health_ops.audit.ai_governance.policy import evaluate_artifacts
    from dev_health_ops.audit.ai_governance.rollup import rollup_coverage_daily

    artifacts = AIGovernanceLoader(client).load_artifacts_for_day(
        org_id=org_id, day=day
    )
    return evaluate_artifacts(artifacts), rollup_coverage_daily(artifacts, day=day)


def _artifact_from_row(row: dict[str, Any]) -> AIGovernanceArtifact:
    tool_name = _optional_str(row.get("tool_name"))
    return AIGovernanceArtifact(
        org_id=str(row["org_id"]),
        team_id=_optional_str(row.get("team_id")),
        repo_id=_optional_uuid(row.get("repo_id")),
        subject_type=str(row["subject_type"]),
        subject_id=str(row["subject_id"]),
        observed_at=_datetime(row["observed_at"]),
        ai_detected=bool(row.get("ai_detected", True)),
        declared_ai=bool(row.get("declared_ai", False)),
        human_reviewed=_optional_bool(row.get("human_reviewed")),
        sensitive_repo=bool(row.get("sensitive_repo", False)),
        repo_allows_ai=bool(row.get("repo_allows_ai", True)),
        security_scanned=_optional_bool(row.get("security_scanned")),
        license_or_dependency_finding=bool(
            row.get("license_or_dependency_finding", False)
        ),
        tool_name=tool_name,
        model_name=_optional_str(row.get("model_name")),
        tool_allowlist_status=_allowlist_status(row.get("tool_allowlist_status")),
        evidence={
            "source": _optional_str(row.get("source")),
            "kind": _optional_str(row.get("kind")),
            "confidence": row.get("confidence"),
            "artifact_url": _optional_str(row.get("artifact_url")),
        },
    )


def _coverage_from_row(row: dict[str, Any]) -> AIGovernanceCoverageDaily:
    return AIGovernanceCoverageDaily(
        org_id=str(row["org_id"]),
        team_id=_optional_str(row.get("team_id")),
        repo_id=_optional_uuid(row.get("repo_id")),
        day=_date(row["day"]),
        ai_artifacts=int(row.get("ai_artifacts") or 0),
        declared_artifacts=int(row.get("declared_artifacts") or 0),
        human_reviewed_prs=int(row.get("human_reviewed_prs") or 0),
        security_scanned_prs=int(row.get("security_scanned_prs") or 0),
        in_policy_artifacts=int(row.get("in_policy_artifacts") or 0),
        computed_at=_datetime(row["computed_at"]),
    )


def _optional_uuid(value: object) -> UUID | None:
    if value in (None, ""):
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _optional_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _allowlist_status(value: object) -> ToolAllowlistStatus:
    if value in (None, ""):
        return ToolAllowlistStatus.UNKNOWN
    try:
        return ToolAllowlistStatus(str(value))
    except ValueError:
        return ToolAllowlistStatus.UNKNOWN


def _datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


_ARTIFACTS_SQL = """
SELECT
    toString(a.org_id) AS org_id,
    CAST(NULL, 'Nullable(String)') AS team_id,
    a.repo_id AS repo_id,
    a.subject_type AS subject_type,
    a.subject_id AS subject_id,
    a.observed_at AS observed_at,
    1 AS ai_detected,
    a.source IN ('manual', 'pr_label', 'commit_trailer') AS declared_ai,
    if(a.subject_type = 'pull_request', pr.reviews_count > 0, NULL) AS human_reviewed,
    0 AS sensitive_repo,
    1 AS repo_allows_ai,
    if(a.subject_type = 'pull_request', scan.scan_count > 0, NULL) AS security_scanned,
    finding.finding_count > 0 AS license_or_dependency_finding,
    JSONExtractString(a.evidence, 'tool_name') AS tool_name,
    JSONExtractString(a.evidence, 'model_name') AS model_name,
    multiIf(
        allow_exact.status != '', allow_exact.status,
        allow_wild.status != '', allow_wild.status,
        'unknown'
    ) AS tool_allowlist_status,
    a.source AS source,
    a.kind AS kind,
    a.confidence AS confidence,
    '' AS artifact_url
FROM ai_attribution_resolved AS a
LEFT JOIN git_pull_requests AS pr
    ON a.repo_id = pr.repo_id
    AND a.subject_type = 'pull_request'
    AND a.subject_id = toString(pr.number)
LEFT JOIN (
    SELECT repo_id, count() AS scan_count
    FROM ci_pipeline_runs FINAL
    WHERE lower(coalesce(status, '')) IN ('success', 'passed', 'completed')
    GROUP BY repo_id
) AS scan ON a.repo_id = scan.repo_id
LEFT JOIN (
    SELECT repo_id, count() AS finding_count
    FROM security_alerts FINAL
    WHERE lower(coalesce(source, '')) IN ('dependabot', 'gitlab_dependency', 'dependency_scanning')
    GROUP BY repo_id
) AS finding ON a.repo_id = finding.repo_id
-- Allowlist precedence (CHAOS-2209): an exact tool+model row beats a
-- wildcard row, and each side is deduplicated to its latest version
-- (argMax over computed_at — the table is a ReplacingMergeTree) AND to one
-- row per join key, so an artifact can never fan out into multiple
-- coverage events from overlapping policy rows.
-- Wildcard means nullIf(model_name, '') IS NULL: legacy '' rows are
-- wildcard, never exact — JSONExtractString yields '' for missing model
-- evidence, so a '' "exact" key would phantom-match every artifact that
-- lacks model evidence. Note migration 038's ORDER BY ifNull(model_name,'')
-- makes NULL and '' the SAME dedup key (rows can replace each other on
-- merge); fixing that needs a schema migration — follow-up ticket.
LEFT JOIN (
    SELECT
        org_id,
        tool_name,
        model_name AS model_key,
        argMax(status, computed_at) AS status
    FROM ai_tool_allowlist
    WHERE nullIf(model_name, '') IS NOT NULL
    GROUP BY org_id, tool_name, model_key
) AS allow_exact
    ON toString(a.org_id) = allow_exact.org_id
    AND JSONExtractString(a.evidence, 'tool_name') = allow_exact.tool_name
    AND JSONExtractString(a.evidence, 'model_name') = allow_exact.model_key
LEFT JOIN (
    SELECT
        org_id,
        tool_name,
        argMax(status, computed_at) AS status
    FROM ai_tool_allowlist
    WHERE nullIf(model_name, '') IS NULL
    GROUP BY org_id, tool_name
) AS allow_wild
    ON toString(a.org_id) = allow_wild.org_id
    AND JSONExtractString(a.evidence, 'tool_name') = allow_wild.tool_name
WHERE toString(a.org_id) = {org_id:String}
  AND a.observed_at >= {start:DateTime64(3, 'UTC')}
  AND a.observed_at <= {end:DateTime64(3, 'UTC')}
"""

_COVERAGE_SQL = """
SELECT
    org_id,
    team_id,
    repo_id,
    day,
    argMax(ai_artifacts, coverage.computed_at) AS ai_artifacts,
    argMax(declared_artifacts, coverage.computed_at) AS declared_artifacts,
    argMax(human_reviewed_prs, coverage.computed_at) AS human_reviewed_prs,
    argMax(security_scanned_prs, coverage.computed_at) AS security_scanned_prs,
    argMax(in_policy_artifacts, coverage.computed_at) AS in_policy_artifacts,
    max(coverage.computed_at) AS computed_at
FROM ai_governance_coverage_daily AS coverage
WHERE org_id = {org_id:String}
  AND day >= {start_day:Date}
  AND day <= {end_day:Date}
  AND ({team_id:String} = '' OR team_id = {team_id:String})
  AND ({repo_id:String} = '' OR toString(repo_id) = {repo_id:String})
GROUP BY org_id, team_id, repo_id, day
ORDER BY day, team_id, repo_id
"""

_VIOLATIONS_SQL = """
SELECT
    org_id,
    team_id,
    repo_id,
    rule_id,
    severity,
    subject_type,
    subject_id,
    observed_at,
    evidence
FROM ai_policy_events FINAL
WHERE org_id = {org_id:String}
  AND toDate(observed_at) >= {start_day:Date}
  AND toDate(observed_at) <= {end_day:Date}
  AND ({team_id:String} = '' OR team_id = {team_id:String})
  AND ({repo_id:String} = '' OR toString(repo_id) = {repo_id:String})
ORDER BY observed_at DESC
LIMIT {limit:UInt32}
"""


__all__: Sequence[str] = [
    "AIGovernanceLoader",
    "AIGovernanceViolationQueryRow",
    "build_governance_rows_for_day",
]
