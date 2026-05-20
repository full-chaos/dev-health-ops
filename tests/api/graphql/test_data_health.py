from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers import data_health
from dev_health_ops.models.settings import IdentityMapping, JobRun, SyncConfiguration

pytestmark = pytest.mark.anyio


class _ScalarResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows

    def first(self) -> Any | None:
        return self._rows[0] if self._rows else None


class _ExecuteResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def scalars(self) -> _ScalarResult:
        return _ScalarResult(self._rows)


class _Session:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    async def execute(self, _stmt: Any) -> _ExecuteResult:
        return _ExecuteResult(self.rows)


def _context(**kwargs: Any) -> GraphQLContext:
    ctx = GraphQLContext(
        org_id="org-1",
        db_url="clickhouse://localhost/default",
        client=kwargs.pop("client", object()),
        user=cast(Any, SimpleNamespace(role="admin", is_superuser=False)),
    )
    for key, value in kwargs.items():
        setattr(ctx, key, value)
    return ctx


async def test_connectors_reads_existing_sync_history(monkeypatch: pytest.MonkeyPatch):
    config = SyncConfiguration(
        name="GitHub Main",
        provider="github",
        org_id="org-1",
        sync_targets=["acme/app"],
    )
    config.last_sync_at = datetime(2026, 5, 20, tzinfo=UTC)
    config.last_sync_success = False
    config.last_sync_error = "rate limited"
    config.last_sync_stats = {"rows_ingested": 42}

    run = JobRun(job_id=UUID("00000000-0000-0000-0000-000000000001"))
    run.completed_at = datetime(2026, 5, 20, 1, tzinfo=UTC)
    run.result = {"stage": "fetch"}
    monkeypatch.setattr(
        data_health, "_latest_job_run", lambda *_args: _async_value(run)
    )

    result = await data_health.resolve_connectors(
        _context(db_session=_Session([config]))
    )

    assert len(result) == 1
    assert result[0].provider == "github"
    assert result[0].scope == "acme/app"
    assert result[0].rows_ingested == 42
    assert result[0].last_failure is not None
    assert result[0].last_failure.message == "rate limited"
    assert result[0].last_failure.stage == "fetch"


async def test_identity_mapping_surfaces_unmapped_and_alias_suggestions(
    monkeypatch: pytest.MonkeyPatch,
):
    async def fake_query_dicts(
        _context: GraphQLContext, _sql: str, _params: dict[str, Any]
    ):
        return [
            {
                "provider": "git",
                "identity": "sam@example.com",
                "display_name": "Sam",
                "observed_count": 7,
            },
            {
                "provider": "git",
                "identity": "alex@example.com",
                "display_name": "Alex",
                "observed_count": 3,
            },
        ]

    mapped = [
        IdentityMapping(
            canonical_id="sam@corp.test",
            org_id="org-1",
            email="sam@corp.test",
            provider_identities={"git": ["sam@corp.test"]},
            team_ids=["team-a"],
        )
    ]

    monkeypatch.setattr(data_health, "_query_dicts", fake_query_dicts)
    monkeypatch.setattr(
        data_health, "_mapped_identities", lambda *_args, **_kw: _async_value(mapped)
    )

    result = await data_health.resolve_identity_mapping(_context(), "team-a")

    assert result.unmapped_count == 2
    assert [item.email for item in result.unmapped_identities] == [
        "sam@example.com",
        "alex@example.com",
    ]
    assert result.suggested_aliases[0].suggested_canonical_id == "sam@corp.test"


async def test_mapping_coverage_reports_missing_repositories(
    monkeypatch: pytest.MonkeyPatch,
):
    async def fake_coverage_rows(_context: GraphQLContext, *, org_id: str, team: str):
        assert org_id == "org-1"
        assert team == "team-a"
        return (
            [
                {"repo_name": "api", "total": 2, "covered": 2},
                {"repo_name": "web", "total": 1, "covered": 0},
            ],
            [{"repo_name": "api", "total": 4, "covered": 4}],
        )

    monkeypatch.setattr(data_health, "_coverage_rows", fake_coverage_rows)

    result = await data_health.resolve_mapping_coverage(_context(), "team-a")

    assert result.deployments.total_repos == 2
    assert result.deployments.covered_repos == 1
    assert result.deployments.coverage_pct == 50.0
    assert result.deployments.missing[0].repo_name == "web"
    assert result.work_items.coverage_pct == 100.0


async def test_metric_lineage_uses_registry_and_argmax(monkeypatch: pytest.MonkeyPatch):
    captured_sql: list[str] = []

    async def fake_query_dicts(_client: Any, sql: str, params: dict[str, Any]):
        captured_sql.append(sql)
        assert params["org_id"] == "org-1"
        return [{"computed_at": datetime(2026, 5, 20, 2, tzinfo=UTC), "row_count": 9}]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await data_health.resolve_metric_lineage(
        _context(), "team-a", "throughput"
    )

    assert result is not None
    assert result.metric_id == "throughput"
    assert result.source_tables == ["work_item_metrics_daily"]
    assert result.compute_window.kind == "daily"
    assert result.row_count == 9
    assert "argMax(computed_at, computed_at)" in captured_sql[0]


async def _async_value(value: Any) -> Any:
    return value
