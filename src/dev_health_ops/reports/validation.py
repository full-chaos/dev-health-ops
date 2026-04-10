"""Structured validation for report planner requests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from dev_health_ops.reports.resolver import EntityResolution, MetricResolution


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    field: str
    value: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[ValidationIssue] = field(default_factory=list)


def validate_plan_request(
    *,
    metric_resolution: MetricResolution,
    entity_resolution: EntityResolution,
    time_range_start: date | None,
    time_range_end: date | None,
    invalid_reasons: list[str],
) -> ValidationResult:
    errors: list[ValidationIssue] = []

    for term in metric_resolution.unresolved_terms:
        errors.append(
            ValidationIssue(
                code="unsupported_metric",
                field="metrics",
                message=f"Unsupported metric request: {term}",
                value=term,
            )
        )

    if not (
        entity_resolution.team_ids
        or entity_resolution.repo_ids
        or entity_resolution.service_ids
    ):
        errors.append(
            ValidationIssue(
                code="empty_scope",
                field="scope",
                message="Prompt did not resolve to any known team, repo, or service.",
            )
        )

    if invalid_reasons or (
        time_range_start and time_range_end and time_range_start > time_range_end
    ):
        errors.append(
            ValidationIssue(
                code="invalid_time_range",
                field="time_range",
                message="Prompt contains an invalid or unsupported time range.",
            )
        )

    return ValidationResult(ok=not errors, errors=errors)
