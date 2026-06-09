"""ClickHouse sink methods for AI governance records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceCoverageDaily,
    AIGovernanceViolation,
    AIToolAllowlistEntry,
)
from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


POLICY_EVENT_COLUMNS = [
    "event_id",
    "org_id",
    "team_id",
    "repo_id",
    "rule_id",
    "severity",
    "subject_type",
    "subject_id",
    "observed_at",
    "evidence",
    "computed_at",
]

ALLOWLIST_COLUMNS = [
    "org_id",
    "tool_name",
    "model_name",
    "status",
    "reason",
    "updated_at",
    "computed_at",
]

COVERAGE_COLUMNS = [
    "org_id",
    "team_id",
    "repo_id",
    "day",
    "ai_artifacts",
    "declared_artifacts",
    "human_reviewed_prs",
    "security_scanned_prs",
    "in_policy_artifacts",
    "computed_at",
]


def _policy_event_row(event: AIGovernanceViolation) -> list[object]:
    return [
        str(event.event_id),
        event.org_id,
        event.team_id,
        str(event.repo_id) if event.repo_id is not None else None,
        str(event.rule_id),
        str(event.severity),
        event.subject_type,
        event.subject_id,
        _dt_to_clickhouse_datetime(event.observed_at),
        event.evidence_json(),
        _dt_to_clickhouse_datetime(event.computed_at),
    ]


def _allowlist_row(entry: AIToolAllowlistEntry) -> list[object]:
    return [
        entry.org_id,
        entry.tool_name,
        entry.model_name,
        str(entry.status),
        entry.reason,
        _dt_to_clickhouse_datetime(entry.updated_at),
        _dt_to_clickhouse_datetime(entry.computed_at),
    ]


def _coverage_row(row: AIGovernanceCoverageDaily) -> list[object]:
    return [
        row.org_id,
        row.team_id,
        str(row.repo_id) if row.repo_id is not None else None,
        row.day,
        row.ai_artifacts,
        row.declared_artifacts,
        row.human_reviewed_prs,
        row.security_scanned_prs,
        row.in_policy_artifacts,
        _dt_to_clickhouse_datetime(row.computed_at),
    ]


class AIGovernanceMixin(_ClickHouseSinkBase):
    """Mixin for AI governance policy events and coverage rows."""

    def write_ai_policy_events(
        self,
        events: Sequence[AIGovernanceViolation],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not events:
            return
        for chunk in _chunked(list(events), batch_size):
            self.client.insert(
                "ai_policy_events",
                [_policy_event_row(event) for event in chunk],
                column_names=POLICY_EVENT_COLUMNS,
            )

    def write_ai_governance_coverage_daily(
        self,
        rows: Sequence[AIGovernanceCoverageDaily],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not rows:
            return
        for chunk in _chunked(list(rows), batch_size):
            self.client.insert(
                "ai_governance_coverage_daily",
                [_coverage_row(row) for row in chunk],
                column_names=COVERAGE_COLUMNS,
            )

    def write_ai_tool_allowlist(
        self,
        entries: Sequence[AIToolAllowlistEntry],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """Persist allowlist policy entries (CHAOS-2209 admin-seeded path)."""
        if not entries:
            return
        for chunk in _chunked(list(entries), batch_size):
            self.client.insert(
                "ai_tool_allowlist",
                [_allowlist_row(entry) for entry in chunk],
                column_names=ALLOWLIST_COLUMNS,
            )
