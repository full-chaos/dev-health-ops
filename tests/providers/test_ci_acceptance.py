from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from dev_health_ops.providers.ci_acceptance import project_checks

REPO_ID = UUID("11111111-1111-1111-1111-111111111111")
NOW = datetime(2026, 7, 28, tzinfo=UTC)


@pytest.mark.parametrize(
    ("provider", "required_names", "expected"),
    [
        ("github_actions", {"test"}, {"test": "required", "lint": "optional"}),
        ("gitlab_ci", {"pipeline"}, {"pipeline": "required", "test": "optional"}),
        ("jenkins", None, {"test": "unknown"}),
        ("buildkite", set(), {"test": "optional"}),
    ],
)
def test_provider_neutral_requirement_matrix(
    provider: str,
    required_names: set[str] | None,
    expected: dict[str, str],
) -> None:
    jobs = [{"name": name, "status": "success"} for name in expected]
    rows = project_checks(
        repo_id=REPO_ID,
        org_id="org-a",
        run_id="run-1",
        provider=provider,
        observed_at=NOW,
        jobs=jobs,
        required_names=required_names,
        provenance="provider.policy",
    )

    assert {row["check_name"]: row["requirement"] for row in rows} == expected
    assert all(row["rule_version"] == "ci-acceptance.v1" for row in rows)


def test_missing_required_check_is_unknown_not_passed() -> None:
    rows = project_checks(
        repo_id=REPO_ID,
        org_id="org-a",
        run_id="run-1",
        provider="github_actions",
        observed_at=NOW,
        jobs=[{"name": "lint", "status": "success"}],
        required_names={"acceptance"},
        provenance="github.branch_protection.required_status_checks",
    )

    by_name = {row["check_name"]: row for row in rows}
    assert by_name["acceptance"]["requirement"] == "required"
    assert by_name["acceptance"]["result"] == "unknown"


def test_green_pipeline_with_skipped_required_work_does_not_false_pass() -> None:
    rows = project_checks(
        repo_id=REPO_ID,
        org_id="org-a",
        run_id="run-1",
        provider="github_actions",
        observed_at=NOW,
        jobs=[
            {"name": "pipeline", "status": "success"},
            {"name": "acceptance", "status": "skipped"},
        ],
        required_names={"acceptance"},
        provenance="github.branch_protection.required_status_checks",
    )

    acceptance = next(row for row in rows if row["check_name"] == "acceptance")
    assert acceptance["requirement"] == "required"
    assert acceptance["result"] == "skipped"
