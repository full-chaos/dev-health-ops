"""Canonical AI governance rule evaluators."""

from __future__ import annotations

from collections.abc import Iterable

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceArtifact,
    AIGovernanceViolation,
    AIPolicyRule,
    AIPolicySeverity,
    ToolAllowlistStatus,
)

RULE_SEVERITY: dict[AIPolicyRule, AIPolicySeverity] = {
    AIPolicyRule.MISSING_AI_DECLARATION: AIPolicySeverity.WARNING,
    AIPolicyRule.MISSING_HUMAN_REVIEW: AIPolicySeverity.HIGH,
    AIPolicyRule.SENSITIVE_REPO_DISALLOWED: AIPolicySeverity.CRITICAL,
    AIPolicyRule.DISALLOWED_TOOL: AIPolicySeverity.HIGH,
    AIPolicyRule.MISSING_SECURITY_SCAN: AIPolicySeverity.HIGH,
    AIPolicyRule.NEW_LICENSE_FINDING_FROM_AI_PR: AIPolicySeverity.HIGH,
}


def evaluate_artifact(artifact: AIGovernanceArtifact) -> list[AIGovernanceViolation]:
    """Evaluate one artifact against the canonical AI governance registry."""
    if not artifact.ai_detected:
        return []

    violations: list[AIGovernanceViolation] = []
    if not artifact.declared_ai:
        violations.append(_violation(artifact, AIPolicyRule.MISSING_AI_DECLARATION))
    if artifact.subject_type == "pull_request" and artifact.human_reviewed is not True:
        violations.append(_violation(artifact, AIPolicyRule.MISSING_HUMAN_REVIEW))
    if artifact.sensitive_repo and not artifact.repo_allows_ai:
        violations.append(_violation(artifact, AIPolicyRule.SENSITIVE_REPO_DISALLOWED))
    if artifact.tool_allowlist_status == ToolAllowlistStatus.DISALLOWED:
        violations.append(_violation(artifact, AIPolicyRule.DISALLOWED_TOOL))
    if (
        artifact.subject_type == "pull_request"
        and artifact.security_scanned is not True
    ):
        violations.append(_violation(artifact, AIPolicyRule.MISSING_SECURITY_SCAN))
    if artifact.license_or_dependency_finding:
        violations.append(
            _violation(artifact, AIPolicyRule.NEW_LICENSE_FINDING_FROM_AI_PR)
        )
    return violations


def evaluate_artifacts(
    artifacts: Iterable[AIGovernanceArtifact],
) -> list[AIGovernanceViolation]:
    """Evaluate multiple artifacts and flatten policy events."""
    violations: list[AIGovernanceViolation] = []
    for artifact in artifacts:
        violations.extend(evaluate_artifact(artifact))
    return violations


def _violation(
    artifact: AIGovernanceArtifact, rule_id: AIPolicyRule
) -> AIGovernanceViolation:
    evidence: dict[str, object] = {
        "subject_type": artifact.subject_type,
        "subject_id": artifact.subject_id,
        "tool_name": artifact.tool_name,
        "model_name": artifact.model_name,
        "tool_allowlist_status": str(artifact.tool_allowlist_status),
        "artifact_evidence": artifact.evidence,
    }
    return AIGovernanceViolation(
        org_id=artifact.org_id,
        team_id=artifact.team_id,
        repo_id=artifact.repo_id,
        rule_id=rule_id,
        severity=RULE_SEVERITY[rule_id],
        subject_type=artifact.subject_type,
        subject_id=artifact.subject_id,
        observed_at=artifact.observed_at,
        evidence=evidence,
    )
