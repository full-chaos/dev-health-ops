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


def _deployment_row(status: str = "success", *, pr_number: int = 7) -> dict[str, Any]:
    return {
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
        if "FROM deployments" in sql:
            return [_deployment_row()]
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
    assert set(observed_limits) == {1_000}


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
                return [_deployment_row(pr_number=child_pr_number)]
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


def _pull_request_row(*, number: int = 3) -> dict[str, Any]:
    return {
        "repository_id": "repo-x",
        "number": number,
        "entity_id": f"repo-x#pr{number}",
        "display_label": f"PR {number}",
        "state": "open",
        "review_state": None,
        "changes_requested": 0,
        "merged": 0,
        "observed_at": NOW,
        "last_synced": NOW,
    }
