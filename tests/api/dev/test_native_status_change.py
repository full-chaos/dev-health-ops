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


def _deployment_row(status: str = "success") -> dict[str, Any]:
    return {
        "entity_id": "deployment-1",
        "display_label": "Production deployment",
        "status": status,
        "environment": "production",
        "pr_number": 7,
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


def _pull_request_row() -> dict[str, Any]:
    return {
        "repository_id": "repo-x",
        "number": 3,
        "entity_id": "repo-x#pr3",
        "display_label": "PR 3",
        "state": "open",
        "review_state": None,
        "changes_requested": 0,
        "merged": 0,
        "observed_at": NOW,
        "last_synced": NOW,
    }
