from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
)
from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.status_change_service import (
    ChangeCategory,
    ChangeSummaryRequest,
    CompletionState,
    StatusChangeService,
    StatusResultState,
    StatusSnapshotRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


def _scope(
    kind: DirectScope = DirectScope.ISSUE,
    *,
    entity_id: str | None = None,
    repositories: list[str] | None = None,
) -> DevScope:
    entity_type = {
        DirectScope.ISSUE: EntityType.ISSUE,
        DirectScope.PROJECT: EntityType.PROJECT,
        DirectScope.PULL_REQUEST: EntityType.PULL_REQUEST,
        DirectScope.WORK_UNIT: EntityType.WORK_UNIT,
    }[kind]
    entity_id = entity_id or ("issue-1" if kind is DirectScope.ISSUE else "repo-a#pr7")
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=kind,
        repositories=repositories or ["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=entity_type,
                entity_id=entity_id,
                display_label=entity_id,
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _deployment_row(
    status: str = "success", *, pr_number: int = 7, repository_id: str = "repo-a"
) -> dict[str, Any]:
    return {
        "repository_id": repository_id,
        "entity_id": "deployment-1",
        "display_label": "Production deployment",
        "status": status,
        "environment": "production",
        "pr_number": pr_number,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_native_work_unit_status_uses_canonical_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_sql: list[str] = []
    observed_params: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        observed_params.append(_params)
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return [
                {
                    "node_type": "issue",
                    "node_id": "linear:DONE-1",
                    "last_synced": NOW,
                },
                {
                    "node_type": "pr",
                    "node_id": "repo-a#pr7",
                    "last_synced": NOW,
                },
            ]
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY
    assert result.actual.reason_codes == ()
    assert result.children[0].entity_id == "linear:DONE-1"
    assert result.children[0].required is True
    assert result.pull_requests[0].required is True
    assert any("work_unit_membership" in sql for sql in observed_sql)
    assert any(
        params.get("member_issue_ids") == ["linear:DONE-1"]
        and params.get("member_pr_ids") == ["repo-a#pr7"]
        for params in observed_params
    )


@pytest.mark.asyncio
async def test_native_work_unit_without_complete_membership_run_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert "SELECT max(completed_at) AS last_synced" in sql
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert result.source_refs[0].source_system == "work_units"
    assert result.source_refs[0].freshness.value == "unknown"


@pytest.mark.asyncio
async def test_native_empty_complete_work_unit_membership_is_not_source_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return []
        pytest.fail(f"empty complete membership must not query downstream: {sql}")

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_release_evidence_missing",)
    assert result.source_refs[0].source_system == "work_units"
    assert result.source_refs[0].freshness.value == "fresh"


@pytest.mark.asyncio
async def test_native_linked_open_unreviewed_pr_blocks_issue_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "open",
                    "review_state": None,
                    "changes_requested": 0,
                    "merged": 0,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.pull_requests[0].required is True
    assert result.actual.reason_codes == (
        "required_pull_request_unmerged",
        "required_review_unresolved",
    )


@pytest.mark.asyncio
async def test_native_linked_merged_unreviewed_pr_still_blocks_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": None,
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.reason_codes == ("required_review_unresolved",)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ("jira", "github", "gitlab", "linear"))
async def test_native_issue_reader_applies_same_membership_rule_for_every_provider(
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
) -> None:
    observed_params: list[dict[str, Any]] = []
    parent_id = f"{provider}:parent"
    child_id = f"{provider}:child"

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_params.append(params)
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": parent_id,
                    "title": "Parent",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "work_item_id": child_id,
                    "title": "Child",
                    "status": "in_progress",
                    "parent_id": parent_id,
                    "updated_at": NOW,
                    "last_synced": NOW,
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id=parent_id), as_of=NOW),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.NOT_READY
    assert "required_child_incomplete" in result.actual.reason_codes
    assert "child_requirement_unknown" not in result.actual.reason_codes
    assert result.children[0].required is True
    assert any(
        ref.source_system == "work_graph" and ref.freshness.value == "fresh"
        for ref in result.source_refs
    )
    assert observed_params
    assert all(params["org_id"] == "org-a" for params in observed_params)


@pytest.mark.asyncio
async def test_native_open_incoming_blocker_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return [
                {
                    "entity_id": "jira:BLOCK-1",
                    "display_label": "Open blocker",
                    "status": "in_progress",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "jira:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                _pull_request_row(
                    number=7, state="merged", review_state="APPROVED", merged=1
                )
            ]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires this.
            return [_deployment_row(repository_id="repo-x", pr_number=7)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="jira:DONE-1"), as_of=NOW),
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.reason_codes == ("open_blocker",)
    assert result.blockers[0].entity_id == "jira:BLOCK-1"


@pytest.mark.asyncio
async def test_native_done_issue_is_ready_after_fresh_zero_blocker_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                _pull_request_row(
                    number=7, state="merged", review_state="APPROVED", merged=1
                )
            ]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires this.
            return [_deployment_row(repository_id="repo-x", pr_number=7)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY
    assert result.actual.reason_codes == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_source", ("pull_requests", "deployments"))
async def test_native_source_failure_cannot_masquerade_as_empty_optional_data(
    monkeypatch: pytest.MonkeyPatch,
    failing_source: str,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if failing_source == "pull_requests" and "FROM git_pull_requests" in sql:
            raise RuntimeError("pull request source unavailable")
        if failing_source == "deployments" and "FROM deployments" in sql:
            raise RuntimeError("deployment source unavailable")
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.DEGRADED
    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert ("required_release_evidence_missing" in result.actual.reason_codes) is (
        failing_source == "deployments"
    )
    assert any(
        ref.source_system == failing_source and ref.freshness.value == "unavailable"
        for ref in result.source_refs
    )


@pytest.mark.asyncio
async def test_native_missing_release_evidence_is_never_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.INSUFFICIENT_EVIDENCE
    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_release_evidence_missing",)


@pytest.mark.asyncio
async def test_multi_repo_blocker_watermark_requires_complete_scope_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            marker_sql.append(sql)
            return []  # one missing repo means the aggregate HAVING returns no row
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "jira:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(
            _scope(entity_id="jira:DONE-1", repositories=["repo-a", "repo-b"]),
            as_of=NOW,
        ),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert marker_sql
    assert "countDistinctIf" in marker_sql[0]
    assert "length({repository_ids:Array(String)})" in marker_sql[0]


@pytest.mark.asyncio
async def test_native_pr_ci_never_invents_required_check_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST)),
    )

    assert result.ci[0].required is None
    assert result.ci[0].skipped_required_work is None
    assert result.actual.state is CompletionState.INDETERMINATE
    assert "ci_requirement_unknown" in result.actual.reason_codes
    assert any(
        "cannot prove required work ran" in warning for warning in result.warnings
    )


@pytest.mark.asyncio
async def test_native_pr_green_pipeline_with_skipped_required_check_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#acceptance",
                    "display_label": "acceptance",
                    "requirement": "required",
                    "conclusion": "skipped",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST)),
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert "required_ci_work_skipped" in result.actual.reason_codes
    assert result.ci[0].required is True
    assert result.ci[0].skipped_required_work is True


@pytest.mark.parametrize(
    ("older_result", "newer_result", "expected_state"),
    (
        ("failed", "passed", CompletionState.READY),
        ("passed", "failed", CompletionState.NOT_READY),
    ),
)
@pytest.mark.asyncio
async def test_native_pr_assesses_only_the_latest_ci_run_as_a_unit(
    monkeypatch: pytest.MonkeyPatch,
    older_result: str,
    newer_result: str,
    expected_state: CompletionState,
) -> None:
    older = NOW - timedelta(hours=1)
    observed_ci_params: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            observed_ci_params.append(params)
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new",
                    "display_label": "New CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-old",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-old",
                    "display_label": "Old CI",
                    "conclusion": "failure",
                    "observed_at": older,
                    "last_synced": older,
                },
            ]
        if "FROM ci_acceptance_checks" in sql:
            observed_ci_params.append(params)
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": newer_result,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new#optional",
                    "display_label": "optional",
                    "requirement": "optional",
                    "conclusion": "failed",
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-old",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-old#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": older_result,
                    "observed_at": older,
                    "last_synced": older,
                },
            ]
        if "FROM deployments" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "entity_id": "deployment-1",
                    "status": "success",
                    "environment": "production",
                    "pr_number": 7,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM operational_incidents" in sql:
            return [
                {
                    "entity_id": "incident-1",
                    "display_label": "Resolved incident",
                    "status": "resolved",
                    "active": 0,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST), as_of=NOW),
    )

    assert result.actual.state is expected_state
    assert {fact.entity_id for fact in result.ci} == {
        "repo-a#ci-new#required",
        "repo-a#ci-new#optional",
    }
    assert ("required_ci_not_passing" in result.actual.reason_codes) is (
        newer_result == "failed"
    )
    assert observed_ci_params
    assert all(params["org_id"] == "org-a" for params in observed_ci_params)
    assert all(params["as_of"] == NOW for params in observed_ci_params)


@pytest.mark.asyncio
async def test_native_reader_preserves_the_completion_assessment_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_limits: list[int] = []

    async def fake_query(
        _client: object, _sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_limits.append(int(params["limit"]))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert observed_limits
    # CHAOS-3297 s2 round 2 (codex HIGH): _WORK_ITEMS_SQL requests one
    # sentinel row beyond the bound (1,001) so truncation can be detected
    # even though the declared parent consumes one row of the shared
    # budget -- every other query still binds exactly the bound.
    assert set(observed_limits) == {1_000, 1_001}


@pytest.mark.asyncio
async def test_native_parent_inclusive_source_cap_never_fabricates_a_ready_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 2 (codex HIGH) regression: _WORK_ITEMS_SQL fetches
    the declared parent AND its children from one query sharing a single
    LIMIT. Simulate a Done parent with 999 newer completed children plus one
    OLDER incomplete child that falls off the fetch window (1 parent + 1000
    children = 1001 rows total, one more than the 1,000-item bound). Before
    this fix, ``len(children)`` would be exactly 999 (< 1,000), so
    ``assessment_source_limit_reached`` never fired and the omitted
    incomplete child let the service report a fabricated 999/999
    READY/COMPLETE. The fix must detect the truncation via the sentinel row
    and never present that as a trustworthy ratio.
    """

    def _work_item_row(
        work_item_id: str, status: str, *, updated_at: datetime, parent_id: str | None
    ) -> dict[str, Any]:
        return {
            "repository_id": "repo-a",
            "work_item_id": work_item_id,
            "title": work_item_id,
            "status": status,
            "parent_id": parent_id,
            "project_id": None,
            "project_key": None,
            "updated_at": updated_at,
            "last_synced": NOW,
        }

    parent = _work_item_row("issue-1", "done", updated_at=NOW, parent_id=None)
    newest_children = [
        _work_item_row(
            f"child-{index:04d}",
            "done",
            updated_at=NOW - timedelta(minutes=index),
            parent_id="issue-1",
        )
        for index in range(999)
    ]
    # The oldest row -- by ``ORDER BY ... updated_at DESC``, this is the
    # very last row and the first one a LIMIT cuts off.
    oldest_incomplete_child = _work_item_row(
        "child-oldest",
        "in_progress",
        updated_at=NOW - timedelta(days=365),
        parent_id="issue-1",
    )
    all_work_item_rows = [parent, *newest_children, oldest_incomplete_child]

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if (
            "SELECT toString(repo_id) AS repository_id, work_item_id, title, status,"
            in sql
        ):
            return all_work_item_rows[: int(params["limit"])]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    # Regression: the pre-fix code reported 999/999 READY/COMPLETE here.
    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None


@pytest.mark.asyncio
async def test_native_mixed_issue_pr_membership_truncation_never_fabricates_a_ready_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 3 (codex HIGH) regression: _WORK_UNIT_MEMBERS_SQL
    mixes issue and PR members in ONE query sharing a single LIMIT, then
    splits them post-fetch by node_type. Simulate 500 issue members + 502
    PR members (1,002 total rows against the 1,000-item bound) -- neither
    the resulting ``member_issue_ids`` (500) nor ``member_pr_ids`` (500,
    after the shared LIMIT truncates to 1,000 total) ever reaches 1,000
    alone, so before this fix neither downstream arm's own length check
    could ever detect the drop of the last (oldest, by node_id) PR member.
    The fix must detect the truncation via the membership sentinel and
    never present a fabricated denominator or a false READY/COMPLETE.
    """
    issue_members = [
        {
            "node_type": "issue",
            "node_id": f"linear:ISSUE-{index:04d}",
            "last_synced": NOW,
        }
        for index in range(500)
    ]
    pr_members = [
        {
            "node_type": "pr",
            "node_id": f"repo-a#pr{index:04d}",
            "last_synced": NOW,
        }
        for index in range(502)
    ]
    # _WORK_UNIT_MEMBERS_SQL orders by (node_type, node_id) ASC -- issues
    # ('issue') sort before PRs ('pr'), so a 1,000-row LIMIT keeps every
    # issue and the first 500 PRs, dropping the last 2 PRs entirely.
    all_membership_rows = issue_members + pr_members

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return all_membership_rows[: int(params["limit"])]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(
            _scope(DirectScope.WORK_UNIT, entity_id="work-unit-1"), max_items=100
        ),
    )

    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None


@pytest.mark.asyncio
async def test_native_high_churn_ci_never_hides_another_prs_failing_latest_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 5 (codex HIGH) exact repro: _CI_SQL orders by
    ``observed_at DESC`` (a global, per-EVENT bound) and only collapses to
    latest-run-per-PR AFTER the fetch. PR A has 1,000 newer CI runs (all
    passing); PR B has a single, much OLDER, FAILING latest run. The global
    bound admits every PR A run and none of PR B's -- PR B's run is never
    fetched at all, so the latest-run collapse has nothing to recover it
    from, and neither PR A's nor PR B's post-collapse row count ever
    reaches the 1,000-item bound (there's exactly one collapsed row per
    PR). Before this fix the service reported a clean READY/COMPLETE
    covering only PR A; the fix must detect the truncation via the
    sentinel and never present that as trustworthy.
    """
    ci_runs_for_pr_a = [
        {
            "repository_id": "repo-a",
            "run_id": f"run-a-{index:04d}",
            "pr_number": 1,
            "entity_id": f"repo-a#ci#run-a-{index:04d}",
            "display_label": "CI",
            "conclusion": "success",
            "observed_at": NOW - timedelta(minutes=index),
            "last_synced": NOW,
        }
        for index in range(1_000)
    ]
    # PR B's only run: far older than every PR A run, and failing.
    ci_run_for_pr_b = {
        "repository_id": "repo-a",
        "run_id": "run-b-old",
        "pr_number": 2,
        "entity_id": "repo-a#ci#run-b-old",
        "display_label": "CI",
        "conclusion": "failure",
        "observed_at": NOW - timedelta(days=365),
        "last_synced": NOW,
    }
    all_ci_rows = ci_runs_for_pr_a + [ci_run_for_pr_b]

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": number,
                    "entity_id": f"repo-a#pr{number}",
                    "display_label": f"PR {number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
                for number in (1, 2)
            ]
        if "FROM ci_pipeline_runs" in sql:
            return all_ci_rows[: int(params["limit"])]
        if "FROM ci_acceptance_checks" in sql:
            # Only PR A's latest run has a matching required-check
            # classification -- PR B's run is never fetched in the first
            # place, so it can never have one either; that's the point.
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-a-0000",
                    "pr_number": 1,
                    "entity_id": "repo-a#ci#run-a-0000#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            # pr_number=1 so the (repository_id, pr_number) pair filter
            # admits it as PR A's release evidence -- isolates the
            # assertions below to the CI truncation mechanism, not an
            # unrelated missing-release-evidence confound.
            return [_deployment_row(pr_number=1)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    # Regression: the pre-fix code reported READY/COMPLETE covering only
    # PR A's (passing) latest run, PR B's failing run never having been
    # fetched at all.
    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes


@pytest.mark.asyncio
async def test_native_change_reader_returns_only_canonical_observed_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        if "FROM work_graph_edges" in sql:
            return [
                {
                    "change_id": "edge-1",
                    "source_type": "issue",
                    "source_id": "issue-1",
                    "edge_type": "implements",
                    "target_type": "pr",
                    "target_id": "repo-a#pr7",
                    "provenance": "native",
                    "confidence": 1.0,
                    "observed_at": NOW - timedelta(minutes=30),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))
    request = ChangeSummaryRequest(
        scope=_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await service.change_summary("org-a", "permission-v1", request)

    assert [change.change_id for change in result.changes] == [
        result.changes[0].change_id,
        "edge-1",
    ]
    assert result.changes[1].relationship_chain == (
        "issue-1",
        "implements",
        "repo-a#pr7",
    )
    assert all(change.claim_kind.value == "observed" for change in result.changes)


def _org_scope(*, team_ids: list[str] | None = None) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.ORGANIZATION,
        repositories=[],
        entity_refs=[],
        team_ids=team_ids or [],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
async def test_organization_scope_status_snapshot_enumerates_repos_natively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3255: organization scope must not read as an empty repo set.

    ``DevScope.repositories``/``entity_refs`` are empty for organization
    scope (the wire contract forbids attaching entities to it), so the
    native source must re-derive the authorized repository set itself from
    ``org_id`` rather than reading the (empty) bounded scope fields.
    """
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM repos FINAL" in sql:
            assert params == {"org_id": "org-a"}
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        if "FROM git_pull_requests AS pr" in sql:
            return [_pull_request_row()]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope()),
    )

    deployment_calls = [item for item in observed if "FROM deployments" in item["sql"]]
    pull_request_calls = [
        item for item in observed if "FROM git_pull_requests AS pr" in item["sql"]
    ]
    assert pull_request_calls, "expected the pull_requests read to execute"
    assert deployment_calls, "expected the deployments read to execute"
    assert pull_request_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." not in result.warnings
    )
    # Assert the SQL text itself carries the organization branch, not just
    # that a mocked row came back: a mock that returns rows unconditionally
    # (ignoring the real WHERE clause) would still pass even if the
    # 'organization' branch were deleted from _PULL_REQUESTS_SQL/
    # _DEPLOYMENTS_SQL (CHAOS-3255 follow-up — this is the regression the
    # prior HIGH finding actually shipped, and a table-name-only mock
    # cannot detect it re-appearing).
    assert "IN ('organization', 'repository')" in pull_request_calls[0]["sql"]
    assert "IN ('organization', 'repository')" in deployment_calls[0]["sql"]
    assert result.pull_requests, "organization scope must surface PR facts"
    assert result.deployments, "organization scope must surface deployment facts"


@pytest.mark.asyncio
async def test_organization_scope_change_summary_enumerates_repos_natively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM repos FINAL" in sql:
            assert params == {"org_id": "org-a"}
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        if "FROM work_graph_edges" in sql:
            return [
                {
                    "change_id": "edge-1",
                    "source_type": "issue",
                    "source_id": "issue-1",
                    "edge_type": "implements",
                    "target_type": "pr",
                    "target_id": "repo-x#pr7",
                    "provenance": "native",
                    "confidence": 1.0,
                    "observed_at": NOW - timedelta(minutes=30),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    request = ChangeSummaryRequest(
        scope=_org_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", request)

    transitions_calls = [
        item for item in observed if "FROM work_item_transitions" in item["sql"]
    ]
    relationship_calls = [
        item for item in observed if "FROM work_graph_edges" in item["sql"]
    ]
    assert transitions_calls
    assert relationship_calls
    assert transitions_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    assert "Observed-change scope was not widened." not in result.warnings
    # CHAOS-3255 follow-up: _TRANSITIONS_SQL/_RELATIONSHIPS_SQL previously had
    # no 'organization' branch, so change_summary.v1 silently dropped status
    # transitions and work-graph relationships even with a full repo set.
    # Assert the SQL text itself, not just the mocked row: a table-name-only
    # mock returns rows regardless of the real WHERE clause and would not
    # catch the 'organization' branch being deleted again.
    assert "IN ('organization', 'repository')" in transitions_calls[0]["sql"]
    assert "IN ('organization', 'repository')" in relationship_calls[0]["sql"]
    change_ids = {change.change_id for change in result.changes}
    assert any(
        change.entity_id == "issue-1" and change.before == "in_progress"
        for change in result.changes
    ), "organization scope must surface status transition changes"
    assert "edge-1" in change_ids, (
        "organization scope must surface relationship changes"
    )


@pytest.mark.asyncio
async def test_organization_scope_with_no_authorized_repos_is_explicit_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM repos FINAL" in sql:
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope()),
    )

    # Zero authorized repositories must surface as explicit degraded/unavailable
    # evidence, never as a silently-complete or partial answer.
    assert result.state is StatusResultState.DEGRADED
    assert result.declared is None
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


@pytest.mark.asyncio
async def test_team_filtered_organization_scope_is_never_widened_to_the_full_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A team filter narrows organization scope; no native query here applies
    it, so organization-native enumeration must not kick in and silently
    return every repository in the org instead of respecting the filter."""

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM repos FINAL" in sql:
            # If this executes, the org-native repository derivation
            # incorrectly ran despite the team filter.
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope(team_ids=["team-a"])),
    )

    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


# ---------------------------------------------------------------------------
# CHAOS-3303 — a committed team subject re-derives owned repositories from
# team_repo_ownership at query time and executes real team-scoped reads.
#
# N0 (test_chaos_3301_controls.py) already proves the fail-closed floor
# using a bare ``object()`` client, which raises before any query can run --
# that control stays valid unchanged (the new team-repo lookup also raises
# against that fake and is caught the same way). These tests are the
# positive/negative pair the CHAOS-3303 planning brief calls "must flip
# GREEN": a team WITH real ownership rows gets real facts, and a team whose
# ownership query genuinely returns zero rows (as opposed to a client
# failure) still fails closed, never silently empty.
# ---------------------------------------------------------------------------


def _team_scope(*, team_id: str = "team-platform") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM,
                entity_id=team_id,
                display_label="Platform",
            )
        ],
        team_ids=[team_id],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
async def test_team_scope_status_snapshot_re_derives_owned_repos_and_executes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM team_repo_ownership" in sql:
            assert params == {
                "org_id": "org-a",
                "team_id": "team-platform",
                "as_of": NOW,
            }
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        # _PULL_REQUESTS_SQL now embeds the canonical-primary-attribution
        # subquery INSIDE its own single query string (no separate
        # query_dicts round trip for it) -- matching on
        # "work_item_team_attributions" here would incorrectly intercept
        # that composite query itself, since the substring appears within
        # it. Match the outer query's own unique marker instead.
        if "FROM git_pull_requests AS pr" in sql:
            return [_pull_request_row()]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires deployments to
            # genuinely trace through an admitted PR, not merely share a
            # bare PR number.
            return [_deployment_row(repository_id="repo-x", pr_number=3)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )

    pull_request_calls = [
        item for item in observed if "FROM git_pull_requests AS pr" in item["sql"]
    ]
    deployment_calls = [item for item in observed if "FROM deployments" in item["sql"]]
    assert pull_request_calls, "expected the pull_requests read to execute"
    assert deployment_calls, "expected the deployments read to execute"
    assert pull_request_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    # Assert the SQL text itself carries the canonical-primary-attribution
    # team branch, not just that a mocked row came back -- a table-name-only
    # mock would still pass even if 'team' were dropped from the
    # disjunction, or if it fell back to the coarser repository-membership
    # arm (CHAOS-3303 round 2's own regression).
    assert "IN ('issue', 'project', 'team')" in pull_request_calls[0]["sql"]
    assert "work_item_team_attributions" in pull_request_calls[0]["sql"]
    assert "is_primary = 1" in pull_request_calls[0]["sql"]
    assert result.pull_requests, "team scope must surface pull-request facts"
    assert result.deployments, "team scope must surface deployment facts"
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." not in result.warnings
    )
    # A team has no single declared/children completion tree; this is
    # structural (see TEAM_NOT_APPLICABLE_SOURCES), never a data gap.
    assert result.declared is None
    assert result.children == ()
    assert result.blockers == ()
    # declared_optional now includes TEAM (status_change_service.py), so real
    # fresh evidence with no declared status is COMPLETE, not
    # INSUFFICIENT_EVIDENCE -- the end-to-end proof this issue's health
    # services depend on.
    assert result.state is StatusResultState.COMPLETE


@pytest.mark.asyncio
async def test_team_scope_never_queries_declared_work_items_or_blockers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Structural, not a data gap: _WORK_ITEMS_SQL/_BLOCKERS_SQL must never
    even be attempted for a team subject, exactly like organization/
    repository scope already never attempts them.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-x"}]
        if "FROM work_items FINAL" in sql:
            raise AssertionError("_WORK_ITEMS_SQL must not run for a team subject")
        if "FROM work_graph_edges" in sql and "blocker" in sql:
            raise AssertionError("_BLOCKERS_SQL must not run for a team subject")
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )
    assert result.declared is None


@pytest.mark.asyncio
async def test_team_scope_change_summary_re_derives_owned_repos_and_executes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM team_repo_ownership" in sql:
            assert params == {
                "org_id": "org-a",
                "team_id": "team-platform",
                "as_of": NOW,
            }
            return [{"repository_id": "repo-x"}]
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    request = ChangeSummaryRequest(
        scope=_team_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", request)

    transitions_calls = [
        item for item in observed if "FROM work_item_transitions" in item["sql"]
    ]
    assert transitions_calls
    assert transitions_calls[0]["params"]["repository_ids"] == ["repo-x"]
    assert "work_item_team_attributions" in transitions_calls[0]["sql"]
    assert "is_primary = 1" in transitions_calls[0]["sql"]
    assert any(
        change.entity_id == "issue-1" and change.before == "in_progress"
        for change in result.changes
    ), "team scope must surface status transition changes"
    assert "Observed-change scope was not widened." not in result.warnings


@pytest.mark.asyncio
async def test_team_scope_with_zero_owned_repositories_is_explicit_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuinely empty team_repo_ownership result (not a client failure --
    see N0 for that case) must still fail closed, never silently empty.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return []
        raise AssertionError(
            "no fact read may run once the team has zero owned repositories"
        )

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )

    assert result.declared is None
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


# ---------------------------------------------------------------------------
# CHAOS-3303 round 2 (Codex HIGH, 2026-08-02): repository co-location is not
# team ownership. A PARENT team with team_repo_ownership access to a shared
# repository must NEVER receive facts whose canonical PRIMARY work-item
# attribution belongs to a different (here, CHILD) team -- every one of the
# nine team arms must exclude them. Parametrized across all nine arms (not
# sampled) per the round-2 directive; the CHILD-team run is the required
# positive control proving the mock (and the underlying plumbing) genuinely
# discriminates by team_id rather than always returning empty.
#
# The mock cannot execute the embedded work_item_team_attributions subquery
# (it interprets SQL by table-name substring, not by running it) -- so, like
# every other structural regression check already in this file (see the
# CHAOS-3255 'organization' branch assertions above), each case ALSO asserts
# the query's own SQL text still contains the canonical-attribution join
# text, so a future edit that silently reverts to bare repository-membership
# admission is caught even though the mock's behavioral check alone could
# not detect it.
# ---------------------------------------------------------------------------

_PARENT_TEAM_ID = "team-parent"
_CHILD_TEAM_ID = "team-child"
_SHARED_REPO_ID = "repo-shared"
_CHILD_WORK_ITEM_ID = "work-item-child-owned"


def _team_scope_for(team_id: str) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM, entity_id=team_id, display_label=team_id
            )
        ],
        team_ids=[team_id],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _generic_delivery_change_row() -> dict[str, Any]:
    """The row shape every _*_CHANGES_SQL query feeds into the SAME generic
    ``ClickHouseStatusChangeSource._delivery_changes`` mapper -- one shape
    covers all five delivery-projection cases regardless of which table the
    real SQL selects from.
    """

    return {
        "change_id": f"change-{_CHILD_WORK_ITEM_ID}",
        "entity_id": f"{_SHARED_REPO_ID}#pr7",
        "display_label": "Child-owned delivery event",
        "before_value": None,
        "after_value": "changed",
        "observed_at": NOW - timedelta(hours=1),
        "last_synced": NOW,
    }


def _make_team_repo_only_fake_query(*, target_marker: str, target_row: dict[str, Any]):
    """A fake_query that admits ``target_row`` for the query matching
    ``target_marker`` ONLY when ``params["team_id"] == _CHILD_TEAM_ID`` --
    the parent (mere repository co-location) always sees an empty result
    for that same arm, proving exclusion rather than a client-side crash or
    an accidentally-always-empty mock.
    """

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": _SHARED_REPO_ID}]
        if target_marker in sql:
            if params.get("team_id") == _CHILD_TEAM_ID:
                return [target_row]
            return []
        return []

    return fake_query


_STATUS_SNAPSHOT_CASE = (
    "pull_requests",
    "reviews.review_state AS review_state",
    lambda: _pull_request_row(),
    lambda result: result.pull_requests,
)

_CHANGE_SUMMARY_CASES = (
    (
        "transitions",
        "FROM work_item_transitions AS transition FINAL",
        lambda: {
            "entity_id": _CHILD_WORK_ITEM_ID,
            "display_label": "Child work item",
            "from_status": "in_progress",
            "to_status": "done",
            "observed_at": NOW - timedelta(hours=1),
            "last_synced": NOW,
        },
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.STATUS
        ],
    ),
    (
        "relationships",
        "edge_id AS change_id",
        lambda: {
            "change_id": "edge-child-1",
            "source_type": "issue",
            "source_id": _CHILD_WORK_ITEM_ID,
            "edge_type": "implements",
            "target_type": "pull_request",
            "target_id": f"{_SHARED_REPO_ID}#pr7",
            "provenance": "native",
            "confidence": 1.0,
            "observed_at": NOW - timedelta(hours=1),
            "last_synced": NOW,
        },
        lambda result: [
            c
            for c in result.changes
            if c.category
            in {
                ChangeCategory.RELATIONSHIP,
                ChangeCategory.BLOCKER,
                ChangeCategory.DEPENDENCY,
            }
        ],
    ),
    (
        "pull_request_changes",
        "'#state#'",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.PULL_REQUEST
        ],
    ),
    (
        "review_changes",
        "FROM git_pull_request_reviews AS review FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.REVIEW
        ],
    ),
    (
        "ci_changes",
        "FROM ci_pipeline_runs AS run FINAL",
        _generic_delivery_change_row,
        lambda result: [c for c in result.changes if c.category is ChangeCategory.CI],
    ),
    (
        "deployment_changes",
        "FROM deployments AS deployment FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.DEPLOYMENT
        ],
    ),
    (
        "incident_changes",
        "INNER JOIN deployments AS deployment FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.INCIDENT
        ],
    ),
)


@pytest.mark.asyncio
async def test_team_arm_excludes_child_owned_facts_pull_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _label, marker, row_factory, extract = _STATUS_SNAPSHOT_CASE
    row = row_factory()

    parent_query = _make_team_repo_only_fake_query(target_marker=marker, target_row=row)
    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", parent_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope_for(_PARENT_TEAM_ID)),
    )
    assert extract(parent_result) == (), (
        "a team with mere repository co-location must not receive facts "
        "canonically owned by a different team"
    )

    observed_sql: list[str] = []

    async def child_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        return await parent_query(_client, sql, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", child_query
    )
    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for(_CHILD_TEAM_ID))
    )
    assert extract(child_result), (
        "positive control: the canonically-owning team must receive the fact "
        "-- proves the mock discriminates rather than always excluding"
    )
    target_sql = next(sql for sql in observed_sql if marker in sql)
    assert "work_item_team_attributions" in target_sql
    assert "is_primary = 1" in target_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,marker,row_factory,extract",
    _CHANGE_SUMMARY_CASES,
    ids=[case[0] for case in _CHANGE_SUMMARY_CASES],
)
async def test_team_arm_excludes_child_owned_facts_change_summary(
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    marker: str,
    row_factory: Any,
    extract: Any,
) -> None:
    del label
    row = row_factory()

    def _request(team_id: str) -> ChangeSummaryRequest:
        return ChangeSummaryRequest(
            scope=_team_scope_for(team_id),
            current_start=NOW - timedelta(days=7),
            current_end=NOW,
            comparison_start=NOW - timedelta(days=14),
            comparison_end=NOW - timedelta(days=7),
        )

    parent_query = _make_team_repo_only_fake_query(target_marker=marker, target_row=row)
    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", parent_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request(_PARENT_TEAM_ID))
    assert extract(parent_result) == [], (
        "a team with mere repository co-location must not receive facts "
        "canonically owned by a different team"
    )

    observed_sql: list[str] = []

    async def child_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        return await parent_query(_client, sql, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", child_query
    )
    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request(_CHILD_TEAM_ID))
    assert extract(child_result), (
        "positive control: the canonically-owning team must receive the fact "
        "-- proves the mock discriminates rather than always excluding"
    )
    target_sql = next(sql for sql in observed_sql if marker in sql)
    assert "work_item_team_attributions" in target_sql
    assert "is_primary = 1" in target_sql


@pytest.mark.asyncio
async def test_team_arm_excludes_child_owned_facts_deployments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ninth arm: _DEPLOYMENTS_SQL has no canonical-attribution join of
    its own -- team-scoped deployments are admitted only through an already
    team-owned PR (pr_numbers, derived from the now-correctly-filtered
    _PULL_REQUESTS_SQL rows). Exclusion is therefore proven end to end: the
    parent's canonically-excluded PR never contributes a PR number, so its
    deployment is never admitted either, despite sharing the same repo.
    """

    child_pr_number = 77

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": _SHARED_REPO_ID}]
        if "reviews.review_state AS review_state" in sql:
            if params.get("team_id") == _CHILD_TEAM_ID:
                return [_pull_request_row(number=child_pr_number)]
            return []
        if "FROM deployments FINAL" in sql:
            if child_pr_number in (params.get("pr_numbers") or []):
                # _pull_request_row's repository_id is hardcoded "repo-x"
                # (not parametrized) -- match it here so the round-3
                # pair-admission fix sees a genuinely matching pair.
                return [
                    _deployment_row(repository_id="repo-x", pr_number=child_pr_number)
                ]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope_for(_PARENT_TEAM_ID)),
    )
    assert parent_result.deployments == (), (
        "a team with mere repository co-location must not receive deployment "
        "facts reachable only through a PR canonically owned by a different team"
    )

    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for(_CHILD_TEAM_ID))
    )
    assert child_result.deployments, (
        "positive control: the canonically-owning team must receive the "
        "deployment via its own admitted PR"
    )


def _pull_request_row(
    *,
    number: int = 3,
    state: str = "open",
    review_state: str | None = None,
    changes_requested: int = 0,
    merged: int = 0,
) -> dict[str, Any]:
    return {
        "repository_id": "repo-x",
        "number": number,
        "entity_id": f"repo-x#pr{number}",
        "display_label": f"PR {number}",
        "state": state,
        "review_state": review_state,
        "changes_requested": changes_requested,
        "merged": merged,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_deployment_admission_uses_repo_pr_pairs_not_flattened_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (HIGH): _DEPLOYMENTS_SQL's ``ifNull(pull_request_number,
    0) IN {pr_numbers}`` arm matches a bare, cross-repository-flattened PR
    NUMBER -- with two repos in the team's accessible set, repo A's
    team-owned PR #77 must not admit repo B's UNRELATED, differently-owned
    PR #77 deployment, nor any incident reachable only through it.
    """

    repo_a, repo_b = "repo-a-owned", "repo-b-other"
    collision_number = 77

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": repo_a}, {"repository_id": repo_b}]
        if "SELECT 1 AS found" in sql:
            return []
        if "reviews.review_state AS review_state" in sql:
            # Only repo A's PR is canonically team-owned; repo B's
            # same-numbered PR is a genuinely different, unrelated pull
            # request this team never owned (round 2's canonical-attribution
            # join already excludes it -- exercised by the parent/child
            # tests above).
            return [
                {
                    "repository_id": repo_a,
                    "number": collision_number,
                    "entity_id": f"{repo_a}#pr{collision_number}",
                    "display_label": f"PR {collision_number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments FINAL" in sql:
            # Simulates the SQL's own (repository-agnostic) admission: with
            # pr_numbers=[77], ifNull(pull_request_number, 0) IN {77}
            # matches BOTH repos' deployments -- the collision this fix
            # must resolve on the Python side, by (repository_id,
            # pr_number) PAIR rather than bare number.
            return [
                {
                    "repository_id": repo_a,
                    "entity_id": "deploy-a",
                    "display_label": "Deploy A",
                    "status": "success",
                    "environment": "production",
                    "pr_number": collision_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": repo_b,
                    "entity_id": "deploy-b",
                    "display_label": "Deploy B (unrelated PR, same number)",
                    "status": "success",
                    "environment": "production",
                    "pr_number": collision_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
            ]
        if "FROM operational_incidents" in sql:
            deployment_pairs = params.get("deployment_pairs") or []
            rows: list[dict[str, Any]] = []
            if (repo_a, "deploy-a") in deployment_pairs:
                rows.append(
                    {
                        "entity_id": "incident-a",
                        "display_label": "Incident A",
                        "status": "resolved",
                        "active": False,
                        "observed_at": NOW,
                        "last_synced": NOW,
                    }
                )
            if (repo_b, "deploy-b") in deployment_pairs:
                rows.append(
                    {
                        "entity_id": "incident-b",
                        "display_label": (
                            "Incident reachable only via the wrongly-"
                            "admitted deployment"
                        ),
                        "status": "resolved",
                        "active": False,
                        "observed_at": NOW,
                        "last_synced": NOW,
                    }
                )
            return rows
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-1"))
    )

    deployment_ids_seen = {d.entity_id for d in result.deployments}
    assert deployment_ids_seen == {"deploy-a"}, (
        "repo B's unrelated same-numbered-PR deployment must be excluded "
        "by the (repository_id, pr_number) pair check"
    )
    incident_ids_seen = {i.entity_id for i in result.incidents}
    assert incident_ids_seen == {"incident-a"}, (
        "an incident reachable ONLY through the wrongly-admitted deployment "
        "must never propagate to the wrong team"
    )


@pytest.mark.asyncio
async def test_team_attribution_subquery_bounds_reassignment_by_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (HIGH): the canonical-attribution subquery's
    max(computed_at) must be bounded by as_of -- a work item reassigned
    from team A to team B at t2 must not rewrite a t1 query's result in
    either direction: team A's t1 snapshot must keep the in-window facts,
    and team B's t1 snapshot must not retroactively gain them.
    """

    t1 = NOW - timedelta(days=3)
    t2 = NOW

    def _admit(team_id: str | None, as_of: datetime | None) -> bool:
        if as_of is None or team_id is None:
            return False
        if as_of < t2:
            return team_id == "team-a"
        return team_id == "team-b"

    observed_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-1"}]
        if "FROM work_item_transitions AS transition FINAL" in sql:
            observed_sql.append(sql)
            if _admit(params.get("team_id"), params.get("as_of")):
                return [
                    {
                        "entity_id": "wi-reassigned",
                        "display_label": "Reassigned item",
                        "from_status": "in_progress",
                        "to_status": "done",
                        "observed_at": NOW - timedelta(hours=1),
                        "last_synced": NOW,
                    }
                ]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )

    async def _status_changes(team_id: str, *, current_end: datetime) -> list[Any]:
        request = ChangeSummaryRequest(
            scope=_team_scope_for(team_id),
            current_start=current_end - timedelta(days=1),
            current_end=current_end,
            comparison_start=current_end - timedelta(days=2),
            comparison_end=current_end - timedelta(days=1),
        )
        result = await StatusChangeService(
            ClickHouseStatusChangeSource(object(), now=NOW)
        ).change_summary("org-a", "permission-v1", request)
        return [c for c in result.changes if c.category is ChangeCategory.STATUS]

    team_a_at_t1 = await _status_changes("team-a", current_end=t1)
    team_b_at_t1 = await _status_changes("team-b", current_end=t1)
    team_a_at_t2 = await _status_changes("team-a", current_end=t2)
    team_b_at_t2 = await _status_changes("team-b", current_end=t2)

    assert team_a_at_t1, "team A's t1 snapshot must keep the item's in-window facts"
    assert not team_b_at_t1, (
        "team B must not retroactively gain facts it did not own at t1"
    )
    assert not team_a_at_t2, (
        "team A must not keep the item's facts after a real reassignment at t2"
    )
    assert team_b_at_t2, (
        "team B must see the item once genuinely reassigned to it at t2"
    )

    assert observed_sql, "the transitions arm must have actually been queried"
    assert all("computed_at <=" in sql for sql in observed_sql), (
        "the canonical-attribution subquery must bound max(computed_at) by "
        "as_of, not take a global maximum"
    )


@pytest.mark.asyncio
async def test_team_with_only_unlinked_repo_activity_discloses_coverage_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (MEDIUM): a team whose accessible repos contain ONLY
    unlinked delivery facts (standalone PR/deployment activity with no
    canonical work-item chain) must not resolve a clean READY/COMPLETE with
    zero attributed facts and no disclosure -- the exclusion itself (repo
    access != ownership) is correct, but the silent completeness is not.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-unlinked-only"}]
        if "SELECT 1 AS found" in sql:
            return [{"found": 1}]
        return []  # every canonically-scoped read finds nothing

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-solo"))
    )

    assert result.pull_requests == ()
    assert result.deployments == ()
    assert result.actual.state is CompletionState.INDETERMINATE, (
        "a coverage gap must never resolve as READY"
    )
    assert result.state is StatusResultState.DEGRADED, (
        "a coverage gap must never resolve as a clean COMPLETE"
    )
    assert any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), "the coverage gap must be disclosed in warnings, not silently absorbed"


@pytest.mark.asyncio
async def test_team_with_genuinely_no_repo_activity_stays_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the coverage-gap disclosure above: a team whose
    accessible repos have NO activity at all (not even unlinked) must not
    be forced into a false coverage-gap disclosure -- the existence check
    itself must discriminate, not fire unconditionally for every empty team.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-genuinely-empty"}]
        return []  # including "SELECT 1 AS found": nothing exists at all

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-quiet"))
    )

    assert not any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), "a genuinely empty team must not be wrongly flagged with a coverage gap"


@pytest.mark.asyncio
async def test_incident_propagation_is_scoped_by_repository_not_bare_deployment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (HIGH): deployment IDs are only unique PER REPO in the
    schema. Round 3's pair filter correctly excludes repo B's deployment
    from ``deployments``, but _INCIDENTS_SQL matched incident edges on the
    bare ``edge.deployment_id`` -- an incident edge on the EXCLUDED
    (repo-b, deployment_id) pair still leaked into the team snapshot
    because the same deployment_id string collides across repos and
    repo B remains in the team's authorized repository_ids.
    """

    repo_a, repo_b = "repo-a-owned", "repo-b-other"
    shared_deployment_id = "42"  # deliberately identical across both repos
    pr_number = 5

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": repo_a}, {"repository_id": repo_b}]
        if "SELECT 1 AS found" in sql:
            return []
        if "reviews.review_state AS review_state" in sql:
            return [
                {
                    "repository_id": repo_a,
                    "number": pr_number,
                    "entity_id": f"{repo_a}#pr{pr_number}",
                    "display_label": f"PR {pr_number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments FINAL" in sql:
            return [
                {
                    "repository_id": repo_a,
                    "entity_id": shared_deployment_id,
                    "display_label": "Deploy repo A",
                    "status": "success",
                    "environment": "production",
                    "pr_number": pr_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": repo_b,
                    "entity_id": shared_deployment_id,
                    "display_label": "Deploy repo B (unrelated, same ID)",
                    "status": "success",
                    "environment": "production",
                    "pr_number": pr_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
            ]
        if "FROM operational_incidents" in sql:
            # Discriminate by which admission shape the code actually
            # sends: the fixed code passes repo-scoped (repo_id,
            # deployment_id) PAIRS; the pre-fix code passed a bare
            # deployment_id list that cannot tell repo A's admitted "42"
            # apart from repo B's excluded "42".
            deployment_pairs = params.get("deployment_pairs")
            if deployment_pairs is not None:
                if (repo_b, shared_deployment_id) in deployment_pairs:
                    return [_incident_row("incident-leak")]
                return []
            deployment_ids = params.get("deployment_ids") or []
            if shared_deployment_id in deployment_ids:
                return [_incident_row("incident-leak")]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-1"))
    )

    assert {d.entity_id for d in result.deployments} == {shared_deployment_id}
    assert result.incidents == (), (
        "an incident edge on the excluded (repo-b, deployment-id) pair "
        "must not leak into the team snapshot merely because the bare "
        "deployment_id collides with an admitted deployment in another repo"
    )


def _incident_row(entity_id: str) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "display_label": "Leaked incident",
        "status": "resolved",
        "active": False,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_coverage_probe_failure_fails_closed_not_silently_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (HIGH): a TimeoutError/query error on the coverage
    probe collapsed to an empty result -- the SAME shape as "genuinely
    nothing found" -- silently restoring the exact false-confidence
    READY/COMPLETE state the round-3 probe exists to prevent. A probe
    FAILURE must disclose exactly like a probe that finds activity.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-flaky"}]
        if "SELECT 1 AS found" in sql:
            raise TimeoutError("coverage probe timed out")
        return []  # PRs, deployments genuinely empty

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-flaky"))
    )

    assert result.actual.state is CompletionState.INDETERMINATE, (
        "a failed coverage probe must never resolve as READY"
    )
    assert result.state is StatusResultState.DEGRADED, (
        "a failed coverage probe must never resolve as a clean COMPLETE"
    )
    assert any(
        "coverage probe" in warning or "cannot rule out" in warning
        for warning in result.warnings
    ), "the probe failure itself must be disclosed, not silently absorbed"


@pytest.mark.asyncio
async def test_unlinked_activity_probe_is_bounded_by_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (MEDIUM): _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL had no
    as_of predicate -- a pull request or deployment created strictly AFTER
    the snapshot's as_of would still trip the probe, falsely degrading a
    historical (as_of=t1) snapshot with activity that had not happened yet
    at t1. The probe's SELECT list has no timestamp column to filter
    client-side (it only returns ``1 AS found``), so this can only be
    enforced inside the SQL text itself -- verified structurally, mirroring
    the as_of-bound structural check used for the round-3 canonical-
    attribution subquery.
    """

    observed_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-solo"}]
        if "SELECT 1 AS found" in sql:
            observed_sql.append(sql)
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-solo"))
    )

    assert observed_sql, "the unlinked-activity probe must have actually been queried"
    for sql in observed_sql:
        pr_clause, _, deployment_clause = sql.partition("UNION ALL")
        assert "created_at <= {as_of" in pr_clause, (
            "the pull-request arm of the probe must bound by as_of, "
            "mirroring the root _PULL_REQUESTS_SQL bound"
        )
        assert "<= {as_of" in deployment_clause, (
            "the deployment arm of the probe must bound by as_of, "
            "mirroring the root _DEPLOYMENTS_SQL bound"
        )


@pytest.mark.asyncio
async def test_partial_unlinked_activity_alongside_linked_facts_stays_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ratified policy decision (round 4, team-lead, 2026-08-02): the
    coverage-gap probe fires ONLY when a team's attributed facts
    (pull_requests AND deployment_rows) are COMPLETELY empty -- see
    _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL's docstring. A team with at least
    one genuinely attributed pull request in ANY repo stays a clean,
    undisclosed result even if a DIFFERENT repo in its accessible set has
    purely unlinked activity of its own. Partial-gap detection (an
    unlinked-specific count query distinguishing "some repos have
    unattributed activity" from "all of them do") is explicitly deferred,
    not a defect -- this test pins the shipped behavior so any future
    change to it is deliberate.
    """

    repo_attributed, repo_unlinked_only = "repo-attributed", "repo-unlinked-only"
    probe_calls = 0

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        nonlocal probe_calls
        if "FROM team_repo_ownership" in sql:
            return [
                {"repository_id": repo_attributed},
                {"repository_id": repo_unlinked_only},
            ]
        if "reviews.review_state AS review_state" in sql:
            return [
                {
                    "repository_id": repo_attributed,
                    "number": 1,
                    "entity_id": f"{repo_attributed}#pr1",
                    "display_label": "PR 1",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "SELECT 1 AS found" in sql:
            probe_calls += 1
        return []  # deployments empty; probe must never be invoked (guard is False)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-mixed"))
    )

    # Codex round 5 (MEDIUM): a fake probe that unconditionally returns []
    # cannot distinguish "the probe was never invoked" (the policy this
    # test claims to pin) from "the probe was invoked and happened to find
    # nothing" -- only an invocation count can. This is the actual pin;
    # the warnings assertion below is necessary but not sufficient on its
    # own.
    assert probe_calls == 0, (
        "the coverage-gap probe must not be invoked at all when the team "
        "already has attributed facts -- the all-empty-only guard must "
        "short-circuit before the probe query, not merely produce no "
        "warning from it"
    )
    assert not any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), (
        "partial coverage (linked facts present, unattributed activity "
        "elsewhere in the team's repos) is the documented, deferred "
        "policy -- must NOT be flagged in this round"
    )
