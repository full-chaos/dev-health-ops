"""Live-Python oracle for the ai_governance native port (CHAOS-4285).

Runs the PRODUCTION functions -- audit.ai_governance.policy.evaluate_artifacts
and audit.ai_governance.rollup.rollup_coverage_daily -- over a pinned fixture
and prints the result as JSON on the last stdout line. The Go side builds the
byte-identical fixture and compares.

Run only through ci/check_go.sh's live-python-oracles verb, which also checks
this run's proof marker. See compute_test.go's runPythonOracle.

# What is deliberately NOT compared

event_id. Python's AIGovernanceViolation.event_id is uuid4() (models.py:110) --
a fresh random value on every construction -- so it cannot be compared against
anything, by this oracle or any other. The Go port replaces it with a value
derived from the rest of the ORDER BY key (design.md Q1, approved by team-lead
09-04); that derivation is pinned by TestDeriveEventIDIsStableAndKeyDependent
and by the frozen golden, NOT here. Comparing every OTHER column against live
Python is exactly what this oracle is for, and excluding a column Python
randomises is not a weakened assertion -- there is no Python answer to assert
against.

computed_at is likewise excluded: it is datetime.now(timezone.utc) on both
sides. That is the standing rot-guard rule (compare the PAYLOAD, never
provenance/environment fields), not a special case for this family.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from uuid import UUID

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceArtifact,
    ToolAllowlistStatus,
)
from dev_health_ops.audit.ai_governance.policy import evaluate_artifacts
from dev_health_ops.audit.ai_governance.rollup import rollup_coverage_daily

DAY = date(2026, 9, 3)
ORG = "70d529e0-3c06-4597-8480-794fd02328b6"

REPO_A = UUID("d4f322ad-2102-1fbf-8425-7400573194f7")
# Chosen so its string form sorts BEFORE the literal "None" that str(None)
# produces for a null repo, while REPO_A's sorts after -- pinning
# rollup.py:32-35's sort-key behaviour, which no count assertion could catch.
REPO_B = UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")


def artifact(**overrides: object) -> AIGovernanceArtifact:
    base: dict[str, object] = {
        "org_id": ORG,
        "subject_type": "pull_request",
        "subject_id": "1",
        "observed_at": datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc),
        "team_id": None,
        "repo_id": REPO_A,
        "ai_detected": True,
        "declared_ai": True,
        "human_reviewed": True,
        "sensitive_repo": False,
        "repo_allows_ai": True,
        "security_scanned": True,
        "license_or_dependency_finding": False,
        "tool_name": "copilot",
        "model_name": "gpt-4o",
        "tool_allowlist_status": ToolAllowlistStatus.ALLOWED,
        "evidence": {
            "source": "pr_label",
            "kind": "ai_assisted",
            "confidence": 0.949999988079071,  # float32(0.95) widened, as the driver hands it over
            "artifact_url": None,
        },
    }
    base.update(overrides)
    return AIGovernanceArtifact(**base)  # type: ignore[arg-type]


ARTIFACTS = [
    # 1. Fully compliant PR -> zero violations, counts toward in_policy.
    artifact(subject_id="1"),
    # 2. Undeclared PR -> MISSING_AI_DECLARATION only.
    artifact(subject_id="2", declared_ai=False),
    # 3. human_reviewed is None (UNKNOWN, not False) -> MISSING_HUMAN_REVIEW.
    #    Pins `is not True`, which `is False` would get wrong.
    artifact(subject_id="3", human_reviewed=None),
    # 4. security_scanned None -> MISSING_SECURITY_SCAN, same three-valued rule.
    artifact(subject_id="4", security_scanned=None),
    # 5. Non-PR subject with both PR-only signals absent: proves the
    #    subject_type guard, i.e. NEITHER PR rule fires for a commit.
    artifact(
        subject_id="abc123",
        subject_type="commit",
        human_reviewed=None,
        security_scanned=None,
    ),
    # 6. Disallowed tool.
    artifact(subject_id="6", tool_allowlist_status=ToolAllowlistStatus.DISALLOWED),
    # 7. Sensitive repo that disallows AI -> SENSITIVE_REPO_DISALLOWED.
    artifact(subject_id="7", sensitive_repo=True, repo_allows_ai=False),
    # 8. Sensitive repo that ALLOWS AI -> no violation. The negative control
    #    for 7: without it, a port that ignored repo_allows_ai would pass.
    artifact(subject_id="8", sensitive_repo=True, repo_allows_ai=True),
    # 9. License/dependency finding.
    artifact(subject_id="9", license_or_dependency_finding=True),
    # 10. Every rule at once -> pins the six-rule APPEND ORDER, not just the set.
    artifact(
        subject_id="10",
        declared_ai=False,
        human_reviewed=False,
        sensitive_repo=True,
        repo_allows_ai=False,
        tool_allowlist_status=ToolAllowlistStatus.DISALLOWED,
        security_scanned=False,
        license_or_dependency_finding=True,
    ),
    # 11/12. team_id None vs "" are DIFFERENT group keys but the SAME sort key
    #     (`team_id or ""`), so their relative order is decided by dict
    #     insertion order alone -- a stability requirement, not a comparison.
    artifact(subject_id="11", team_id=None, repo_id=REPO_B),
    artifact(subject_id="12", team_id="", repo_id=REPO_B),
    # 13. Null repo_id -> sorts as the literal string "None".
    artifact(subject_id="13", repo_id=None),
    # 14. Non-null team, for a third distinct group.
    artifact(subject_id="14", team_id="team-alpha"),
    # 15. Different DAY -> excluded from the rollup, INCLUDED in violations
    #     (evaluate_artifacts has no day filter; only rollup does).
    artifact(
        subject_id="15",
        declared_ai=False,
        observed_at=datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc),
    ),
    # 16. ai_detected False -> excluded from BOTH.
    artifact(subject_id="16", ai_detected=False, declared_ai=False),
    # 17. Absent tool/model evidence: _optional_str turns "" into None, so the
    #     persisted evidence JSON carries nulls, not empty strings.
    artifact(subject_id="17", tool_name=None, model_name=None),
    # 18. confidence 0.0 -- must stay 0.0 in the evidence JSON, NOT collapse to
    #     null the way an empty string would. Guards against applying
    #     _optional_str to a numeric field.
    artifact(
        subject_id="18",
        evidence={
            "source": "pr_body",
            "kind": "ai_assisted",
            "confidence": 0.0,
            "artifact_url": None,
        },
    ),
]


def main() -> None:
    violations = evaluate_artifacts(ARTIFACTS)
    coverage = rollup_coverage_daily(
        ARTIFACTS,
        day=DAY,
        computed_at=datetime(2026, 9, 4, 0, 0, 0, tzinfo=timezone.utc),
    )
    print(
        json.dumps(
            {
                "violations": [
                    {
                        "org_id": v.org_id,
                        "team_id": v.team_id,
                        "repo_id": str(v.repo_id) if v.repo_id is not None else None,
                        "rule_id": str(v.rule_id),
                        "severity": str(v.severity),
                        "subject_type": v.subject_type,
                        "subject_id": v.subject_id,
                        "observed_at": v.observed_at.isoformat(),
                        # The exact persisted bytes, which is what the Go side
                        # must reproduce via pythonparity.MarshalPythonJSONSorted.
                        "evidence": v.evidence_json(),
                    }
                    for v in violations
                ],
                "coverage": [
                    {
                        "org_id": c.org_id,
                        "team_id": c.team_id,
                        "repo_id": str(c.repo_id) if c.repo_id is not None else None,
                        "day": c.day.isoformat(),
                        "ai_artifacts": c.ai_artifacts,
                        "declared_artifacts": c.declared_artifacts,
                        "human_reviewed_prs": c.human_reviewed_prs,
                        "security_scanned_prs": c.security_scanned_prs,
                        "in_policy_artifacts": c.in_policy_artifacts,
                    }
                    for c in coverage
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
