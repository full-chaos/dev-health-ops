"""Unit tests proving retry_count is wired through the BASE sync_cicd path.

Regression coverage for CHAOS-2380: testops_pipeline_metrics_daily.rerun_rate
was flat 0% because ci_pipeline_runs.retry_count was only populated by the
extended TestOps path (gated behind the non-default sync_tests target). These
tests pin the seam end-to-end on the base path:

  GitHub run_attempt  -> _fetch_github_workflow_runs_sync
                      -> build_ci_pipeline_run(retry_count=...)
                      -> CiPipelineRun.retry_count
                      -> CicdMixin.insert_ci_pipeline_runs row + update_columns

so real orgs syncing only sync_cicd get a non-zero rerun_rate, and a re-sync of
the same run_id overwrites (does not duplicate) the row.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.processors.base_git import build_ci_pipeline_run
from dev_health_ops.processors.github import _fetch_github_workflow_runs_sync
from dev_health_ops.processors.gitlab import _fetch_gitlab_pipelines_sync
from dev_health_ops.storage.mixins.cicd import CicdMixin

_STARTED = datetime(2023, 1, 1, 0, 1, 0, tzinfo=timezone.utc)


def _gh_run(run_id: str, *, run_attempt: object = "unset") -> SimpleNamespace:
    """Build a minimal GitHub WorkflowRun-like object.

    Uses SimpleNamespace (not Mock) so an omitted run_attempt is genuinely
    absent rather than auto-vivified into a truthy Mock.
    """
    fields: dict[str, object] = {
        "id": run_id,
        "conclusion": "success",
        "status": "completed",
        "created_at": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "run_started_at": _STARTED,
        "updated_at": datetime(2023, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
    }
    if run_attempt != "unset":
        fields["run_attempt"] = run_attempt
    return SimpleNamespace(**fields)


def test_build_ci_pipeline_run_carries_retry_count():
    """build_ci_pipeline_run threads retry_count onto the model (default 0)."""
    run = build_ci_pipeline_run(
        repo_id=None,
        run_id="1",
        status="success",
        queued_at=None,
        started_at=_STARTED,
        finished_at=None,
        retry_count=2,
    )
    assert run.retry_count == 2

    defaulted = build_ci_pipeline_run(
        repo_id=None,
        run_id="2",
        status="success",
        queued_at=None,
        started_at=_STARTED,
        finished_at=None,
    )
    assert defaulted.retry_count == 0


def test_github_workflow_run_maps_run_attempt_to_retry_count():
    """run_attempt=3 -> retry_count=2 on the base sync_cicd path."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("100", run_attempt=3)])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert len(runs) == 1
    assert runs[0].run_id == "100"
    assert runs[0].retry_count == 2


def test_github_workflow_run_first_attempt_is_zero_retries():
    """run_attempt=1 (first, non-retried) -> retry_count=0."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("101", run_attempt=1)])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert runs[0].retry_count == 0


def test_github_workflow_run_absent_run_attempt_defaults_to_zero():
    """Missing run_attempt -> retry_count=0 (no crash, no false positives)."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("102")])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert runs[0].retry_count == 0


def test_gitlab_pipeline_retry_count_defaults_to_zero():
    """GitLab pipelines have no clean attempt counter -> retry_count=0."""
    from unittest.mock import Mock

    pipeline = Mock()
    pipeline.id = 1
    pipeline.status = "success"
    pipeline.created_at = "2023-01-01T00:00:00Z"
    pipeline.started_at = "2023-01-01T00:01:00Z"
    pipeline.finished_at = "2023-01-01T00:05:00Z"

    gl_project = Mock()
    gl_project.pipelines.list.return_value = [pipeline]

    pipelines = _fetch_gitlab_pipelines_sync(
        gl_project, repo_id=None, max_pipelines=10, since=None
    )

    assert len(pipelines) == 1
    assert pipelines[0].retry_count == 0


class _CapturingStore(CicdMixin):
    """Minimal CicdMixin host that captures the _upsert_many payload.

    Drives the REAL production storage seam (CicdMixin.insert_ci_pipeline_runs)
    so the test proves retry_count actually reaches the persisted row and the
    upsert update set — not a mocked-away stand-in. Class attributes mirror the
    sibling ``_DummyStore`` in tests/test_storage_mixins.py to satisfy the
    SQLAlchemyStoreMixinProtocol members (none are exercised by this path).
    """

    session = None
    _ci_pipeline_runs_table = "ci_pipeline_runs"
    _ci_job_runs_table = "ci_job_runs"
    _work_items_table = "work_items"
    _work_item_transitions_table = "work_item_transitions"
    _work_item_dependencies_table = "work_item_dependencies"
    _work_graph_issue_pr_table = "work_graph_issue_pr"
    _work_graph_pr_commit_table = "work_graph_pr_commit"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def _insert_for_dialect(self, model: Any) -> Any:  # pragma: no cover - unused
        return None

    async def _upsert_many(
        self,
        model: Any,
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
    ) -> None:
        self.calls.append(
            {
                "model": model,
                "rows": rows,
                "conflict_columns": conflict_columns,
                "update_columns": update_columns,
            }
        )


@pytest.mark.asyncio
async def test_cicd_mixin_persists_retry_count_and_overwrites_on_resync() -> None:
    """retry_count reaches the upsert row AND is in update_columns.

    Inclusion in update_columns is what makes a re-sync of the same
    (repo_id, run_id) OVERWRITE retry_count rather than leave a stale 0 — the
    table is ReplacingMergeTree on ClickHouse and an upsert on Postgres, so
    idempotency hinges on retry_count being an updated column, not a new row.
    """
    store = _CapturingStore()

    obj_run = build_ci_pipeline_run(
        repo_id=None,
        run_id="run-obj",
        status="success",
        queued_at=None,
        started_at=_STARTED,
        finished_at=None,
        retry_count=2,
    )
    dict_run = {
        "repo_id": None,
        "run_id": "run-dict",
        "status": "success",
        "started_at": _STARTED,
        "retry_count": 3,
    }

    await store.insert_ci_pipeline_runs(cast(Any, [obj_run, dict_run]))

    payload = store.calls[0]
    rows = {r["run_id"]: r for r in payload["rows"]}
    assert rows["run-obj"]["retry_count"] == 2
    assert rows["run-dict"]["retry_count"] == 3
    # Overwrite-on-resync guarantee: retry_count must be in the update set so a
    # later sync of the same run_id replaces a stale value (no duplicate row).
    assert "retry_count" in payload["update_columns"]
    assert payload["conflict_columns"] == ["repo_id", "run_id"]


@pytest.mark.asyncio
async def test_cicd_mixin_defaults_missing_retry_count_to_zero() -> None:
    """A row lacking retry_count persists as 0 (no crash, no false reruns)."""
    store = _CapturingStore()

    await store.insert_ci_pipeline_runs(
        cast(
            Any,
            [
                {
                    "repo_id": None,
                    "run_id": "run-bare",
                    "status": "success",
                    "started_at": _STARTED,
                }
            ],
        )
    )

    assert store.calls[0]["rows"][0]["retry_count"] == 0


# ---------------------------------------------------------------------------
# Migration 0010 — Postgres schema parity for retry_count (CHAOS-2380 round-2)
#
# The git data-plane tables (ci_pipeline_runs, deployments, …) are bootstrapped
# via Base.metadata.create_all, NOT by the Alembic 0001 initial schema. create_all
# only creates *missing tables*; it never adds a *column* to an existing table.
# So an already-provisioned deployment whose ci_pipeline_runs predates retry_count
# would have the base sync_cicd INSERT/ON CONFLICT (which now references
# retry_count) fail the whole batch. Migration 0010 closes that gap idempotently.
# These tests drive the migration's real upgrade()/downgrade() bodies via Alembic's
# Operations context against an "old-schema" table to prove existing deployments
# upgrade cleanly before the insert path runs.
# ---------------------------------------------------------------------------


def _load_migration_0010():
    import importlib.util
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dev_health_ops"
        / "alembic"
        / "versions"
        / "0010_add_ci_pipeline_runs_retry_count.py"
    )
    spec = importlib.util.spec_from_file_location("_mig_0010", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _columns(conn, table: str) -> set[str]:
    import sqlalchemy as sa

    return {col["name"] for col in sa.inspect(conn).get_columns(table)}


def _run_in_op_context(conn, fn) -> None:
    """Bind Alembic ``op`` to ``conn`` and invoke a migration upgrade/downgrade.

    Mirrors how ``op.get_bind()`` resolves inside a live ``alembic upgrade`` run,
    so the migration's real body (table-existence guard + add/drop column) is
    exercised end-to-end rather than reimplemented in the test.
    """
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    ctx = MigrationContext.configure(conn)
    with Operations.context(ctx):
        fn()


def test_migration_0010_chain_links_to_0009():
    """0010 must follow 0009 so the head advances (deploy ordering)."""
    mig = _load_migration_0010()
    assert mig.revision == "0010"
    assert mig.down_revision == "0009"


def test_migration_0010_adds_column_to_old_schema_table():
    """An existing ci_pipeline_runs WITHOUT retry_count gains it on upgrade,
    and the base-path INSERT referencing retry_count then succeeds."""
    import sqlalchemy as sa

    mig = _load_migration_0010()
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        # Simulate an old (pre-retry_count) deployment's table.
        conn.exec_driver_sql(
            "CREATE TABLE ci_pipeline_runs ("
            "  repo_id TEXT, run_id TEXT, status TEXT, started_at TEXT,"
            "  PRIMARY KEY (repo_id, run_id)"
            ")"
        )
        assert "retry_count" not in _columns(conn, "ci_pipeline_runs")

        _run_in_op_context(conn, mig.upgrade)

        assert "retry_count" in _columns(conn, "ci_pipeline_runs")
        # The exact statement shape the base sync_cicd path emits must now work
        # against the upgraded table (this is what previously failed the batch).
        conn.exec_driver_sql(
            "INSERT INTO ci_pipeline_runs "
            "(repo_id, run_id, status, started_at, retry_count) "
            "VALUES ('r', '1', 'success', '2023-01-01', 2)"
        )
        # NOT NULL DEFAULT 0: a row omitting retry_count still lands as 0.
        conn.exec_driver_sql(
            "INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at) "
            "VALUES ('r', '2', 'success', '2023-01-01')"
        )
        rows: dict[str, int] = {
            str(run_id): int(retry_count)
            for run_id, retry_count in conn.exec_driver_sql(
                "SELECT run_id, retry_count FROM ci_pipeline_runs ORDER BY run_id"
            ).fetchall()
        }
        assert rows == {"1": 2, "2": 0}


def test_migration_0010_is_idempotent_when_column_present():
    """Re-running upgrade (or a fresh create_all deploy that already has the
    column) is a no-op — never a duplicate-column error."""
    import sqlalchemy as sa

    mig = _load_migration_0010()
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE ci_pipeline_runs ("
            "  repo_id TEXT, run_id TEXT,"
            "  retry_count INTEGER NOT NULL DEFAULT 0,"
            "  PRIMARY KEY (repo_id, run_id)"
            ")"
        )
        # Should not raise even though the column already exists.
        _run_in_op_context(conn, mig.upgrade)
        assert "retry_count" in _columns(conn, "ci_pipeline_runs")


def test_migration_0010_noop_when_table_absent():
    """Fresh DB where create_all has not yet materialised the table: upgrade is
    a no-op (the table will later be created already containing the column)."""
    import sqlalchemy as sa

    mig = _load_migration_0010()
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        # No ci_pipeline_runs table at all.
        _run_in_op_context(conn, mig.upgrade)
        assert "ci_pipeline_runs" not in sa.inspect(conn).get_table_names()


def test_migration_0010_downgrade_drops_column():
    """Downgrade removes the column when present and is a no-op otherwise."""
    import sqlalchemy as sa

    mig = _load_migration_0010()
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE ci_pipeline_runs ("
            "  repo_id TEXT, run_id TEXT,"
            "  retry_count INTEGER NOT NULL DEFAULT 0,"
            "  PRIMARY KEY (repo_id, run_id)"
            ")"
        )
        _run_in_op_context(conn, mig.downgrade)
        assert "retry_count" not in _columns(conn, "ci_pipeline_runs")
        # Idempotent downgrade: column already gone -> no error.
        _run_in_op_context(conn, mig.downgrade)
