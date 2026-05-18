from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from dev_health_ops.audit.ai_governance import (
    AIGovernanceArtifact,
    AIPolicyRule,
    ToolAllowlistStatus,
    evaluate_artifact,
)

NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)


def _artifact(**overrides: object) -> AIGovernanceArtifact:
    values: dict[str, Any] = {
        "org_id": "org-1",
        "team_id": "team-1",
        "repo_id": uuid4(),
        "subject_type": "pull_request",
        "subject_id": "42",
        "observed_at": NOW,
        "ai_detected": True,
        "declared_ai": True,
        "human_reviewed": True,
        "sensitive_repo": False,
        "repo_allows_ai": True,
        "security_scanned": True,
        "license_or_dependency_finding": False,
        "tool_allowlist_status": ToolAllowlistStatus.ALLOWED,
    }
    values.update(overrides)
    return AIGovernanceArtifact(**values)


def _rules(artifact: AIGovernanceArtifact) -> set[AIPolicyRule]:
    return {violation.rule_id for violation in evaluate_artifact(artifact)}


def test_clean_artifact_has_no_violations() -> None:
    assert evaluate_artifact(_artifact()) == []


def test_missing_data_for_non_ai_artifact_is_clean() -> None:
    assert evaluate_artifact(_artifact(ai_detected=False, human_reviewed=None)) == []


def test_missing_ai_declaration_trips() -> None:
    assert _rules(_artifact(declared_ai=False)) == {AIPolicyRule.MISSING_AI_DECLARATION}


def test_missing_ai_declaration_clean() -> None:
    assert AIPolicyRule.MISSING_AI_DECLARATION not in _rules(
        _artifact(declared_ai=True)
    )


def test_missing_ai_declaration_missing_data() -> None:
    assert AIPolicyRule.MISSING_AI_DECLARATION in _rules(_artifact(declared_ai=False))


def test_missing_human_review_trips() -> None:
    assert AIPolicyRule.MISSING_HUMAN_REVIEW in _rules(_artifact(human_reviewed=False))


def test_missing_human_review_clean() -> None:
    assert AIPolicyRule.MISSING_HUMAN_REVIEW not in _rules(
        _artifact(human_reviewed=True)
    )


def test_missing_human_review_missing_data() -> None:
    assert AIPolicyRule.MISSING_HUMAN_REVIEW in _rules(_artifact(human_reviewed=None))


def test_sensitive_repo_disallowed_trips() -> None:
    assert AIPolicyRule.SENSITIVE_REPO_DISALLOWED in _rules(
        _artifact(sensitive_repo=True, repo_allows_ai=False)
    )


def test_sensitive_repo_disallowed_clean() -> None:
    assert AIPolicyRule.SENSITIVE_REPO_DISALLOWED not in _rules(
        _artifact(sensitive_repo=True, repo_allows_ai=True)
    )


def test_sensitive_repo_disallowed_missing_data() -> None:
    assert AIPolicyRule.SENSITIVE_REPO_DISALLOWED not in _rules(
        _artifact(sensitive_repo=False, repo_allows_ai=False)
    )


def test_disallowed_tool_trips() -> None:
    assert AIPolicyRule.DISALLOWED_TOOL in _rules(
        _artifact(tool_allowlist_status=ToolAllowlistStatus.DISALLOWED)
    )


def test_disallowed_tool_clean() -> None:
    assert AIPolicyRule.DISALLOWED_TOOL not in _rules(
        _artifact(tool_allowlist_status=ToolAllowlistStatus.ALLOWED)
    )


def test_disallowed_tool_missing_data() -> None:
    assert AIPolicyRule.DISALLOWED_TOOL not in _rules(
        _artifact(tool_allowlist_status=ToolAllowlistStatus.UNKNOWN)
    )


def test_missing_security_scan_trips() -> None:
    assert AIPolicyRule.MISSING_SECURITY_SCAN in _rules(
        _artifact(security_scanned=False)
    )


def test_missing_security_scan_clean() -> None:
    assert AIPolicyRule.MISSING_SECURITY_SCAN not in _rules(
        _artifact(security_scanned=True)
    )


def test_missing_security_scan_missing_data() -> None:
    assert AIPolicyRule.MISSING_SECURITY_SCAN in _rules(
        _artifact(security_scanned=None)
    )


def test_license_or_dependency_finding_trips() -> None:
    assert AIPolicyRule.NEW_LICENSE_FINDING_FROM_AI_PR in _rules(
        _artifact(license_or_dependency_finding=True)
    )


def test_license_or_dependency_finding_clean() -> None:
    assert AIPolicyRule.NEW_LICENSE_FINDING_FROM_AI_PR not in _rules(
        _artifact(license_or_dependency_finding=False)
    )


def test_license_or_dependency_finding_missing_data() -> None:
    assert AIPolicyRule.NEW_LICENSE_FINDING_FROM_AI_PR not in _rules(
        _artifact(license_or_dependency_finding=False)
    )
