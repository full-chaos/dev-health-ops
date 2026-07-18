import uuid
from datetime import date, datetime, timezone

import pytest

# Import connectors first to defuse the providers._base <-> connectors circular
# import so this module collects/runs in isolation (CHAOS-2370 pattern).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.metrics import job_dora
from dev_health_ops.metrics.compute_dora import compute_dora_metrics_daily


def test_run_dora_metrics_job_rejects_sqlite():
    """DORA metrics job should reject non-ClickHouse backends (CHAOS-641)."""
    with pytest.raises(ValueError, match="Only ClickHouse is supported"):
        job_dora.run_dora_metrics_job(
            db_url="sqlite:///test.db",
            day=date(2025, 1, 1),
            backfill_days=1,
            repo_id=uuid.uuid4(),
            repo_name="group/project",
            org_id="test-org",
        )


class _FakeClickHouseSink:
    """Captures DORA writes and serves seeded deployments/incidents.

    Mirrors the real ClickHouse sink surface used by run_dora_metrics_job:
    query_dicts(), ensure_tables(), write_dora_metrics(), close().
    """

    def __init__(self, deployments=None, incidents=None):
        self._deployments = deployments or []
        self._incidents = incidents or []
        self.org_id = ""
        self.written = []

    def query_dicts(self, query, parameters):
        if "FROM deployments" in query:
            return self._deployments
        if "FROM incidents" in query:
            return self._incidents
        raise AssertionError(f"Unexpected query: {query}")

    def ensure_tables(self):
        return None

    def write_dora_metrics(self, rows):
        self.written.extend(rows)

    def close(self):
        return None


def test_run_dora_metrics_job_github_org_no_gitlab_token(monkeypatch):
    """CHAOS-2382: a GitHub-only org (no GITLAB_TOKEN) must NOT raise and must
    compute DORA rows from already-synced ClickHouse deployments."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)

    repo_id = uuid.uuid4()
    deployments = [
        {
            "repo_id": repo_id,
            "deployment_id": "d1",
            "status": "success",
            "environment": "production",
            "started_at": datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            "deployed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            "merged_at": datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc),
            "pull_request_number": 42,
        },
        {
            "repo_id": repo_id,
            "deployment_id": "d2",
            "status": "failed",
            "environment": "production",
            "started_at": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc),
            "deployed_at": datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc),
            "merged_at": None,
            "pull_request_number": None,
        },
    ]
    incidents = [
        {
            "repo_id": repo_id,
            "incident_id": "i1",
            "status": "resolved",
            "started_at": datetime(2025, 1, 1, 13, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2025, 1, 1, 14, 0, tzinfo=timezone.utc),
        },
    ]

    sink = _FakeClickHouseSink(deployments=deployments, incidents=incidents)
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    # No auth / GITLAB_TOKEN passed: must not raise.
    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        org_id="a78c1a6a-0000-0000-0000-000000000000",
    )

    by_metric = {row.metric_name: row.value for row in sink.written}
    assert by_metric["deployment_frequency"] == 2.0
    # 1 failed of 2 deployments
    assert by_metric["change_failure_rate"] == pytest.approx(0.5)
    # lead time: deployed 10:00 - merged 08:00 = 2h = 7200s (only d1 has merged_at)
    assert by_metric["lead_time_for_changes"] == pytest.approx(7200.0)
    # MTTR: resolved 14:00 - started 13:00 = 1h = 3600s
    assert by_metric["time_to_restore_service"] == pytest.approx(3600.0)


def test_run_dora_metrics_job_uses_mapped_pagerduty_projection_once(monkeypatch):
    """A canonical PagerDuty incident joins its mapped repo without double counting."""
    repo_id = uuid.uuid4()
    deployment = {
        "repo_id": repo_id,
        "deployment_id": "d1",
        "status": "success",
        "environment": "production",
        "started_at": datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
        "deployed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
        "merged_at": None,
        "pull_request_number": None,
    }
    canonical_projection = {
        "repo_id": repo_id,
        "incident_id": "pd-incident",
        "status": "resolved",
        "started_at": datetime(2025, 1, 1, 13, 0, tzinfo=timezone.utc),
        "resolved_at": datetime(2025, 1, 1, 14, 0, tzinfo=timezone.utc),
        "last_synced": datetime(2025, 1, 1, 14, 0, tzinfo=timezone.utc),
    }
    sink = _CapturingClickHouseSink(
        deployments=[deployment],
        incidents=[canonical_projection, canonical_projection],
    )
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda _db_url: sink)

    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        org_id="a78c1a6a-0000-0000-0000-000000000000",
    )

    by_metric = {row.metric_name: row.value for row in sink.written}
    assert by_metric["time_to_restore_service"] == pytest.approx(3600.0)
    incident_query = next(
        query for query, _params in sink.queries if "FROM incidents" in query
    )
    assert "operational_incidents" in incident_query
    assert "operational_service_repository_mappings" in incident_query


def test_run_dora_metrics_job_no_deployments_writes_nothing(monkeypatch):
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    sink = _FakeClickHouseSink(deployments=[], incidents=[])
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        org_id="test-org",
    )

    assert sink.written == []


def test_run_dora_metrics_job_filters_to_requested_metrics(monkeypatch):
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    repo_id = uuid.uuid4()
    deployments = [
        {
            "repo_id": repo_id,
            "deployment_id": "d1",
            "status": "success",
            "environment": "production",
            "started_at": datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            "deployed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            "merged_at": None,
            "pull_request_number": None,
        },
    ]
    sink = _FakeClickHouseSink(deployments=deployments, incidents=[])
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        metrics="deployment_frequency",
        org_id="test-org",
    )

    assert {row.metric_name for row in sink.written} == {"deployment_frequency"}


class _CapturingClickHouseSink(_FakeClickHouseSink):
    """Records every query + params so repo scoping can be asserted."""

    def __init__(self, deployments=None, incidents=None):
        super().__init__(deployments=deployments, incidents=incidents)
        self.queries: list[tuple[str, dict]] = []

    def query_dicts(self, query, parameters):
        self.queries.append((query, dict(parameters)))
        return super().query_dicts(query, parameters)


def test_run_dora_metrics_job_repo_name_scopes_both_queries(monkeypatch):
    """CHAOS-2382 round-2: a ``repo_name``-only run must constrain BOTH the
    deployment and incident ClickHouse queries via an org-scoped ``repos``
    subquery. Regression for the no-ship where ``--repo-name`` was accepted but
    silently dropped, letting a scoped run write DORA rows for every repo."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    sink = _CapturingClickHouseSink(deployments=[], incidents=[])
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    org_id = "a78c1a6a-0000-0000-0000-000000000000"
    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        repo_name="owner/repo",
        org_id=org_id,
    )

    dep_q = next((q, p) for q, p in sink.queries if "FROM deployments" in q)
    inc_q = next((q, p) for q, p in sink.queries if "FROM incidents" in q)
    for query, params in (dep_q, inc_q):
        # The repo-name subquery filter must be present and org-scoped so a
        # cross-tenant name collision cannot leak another org's repo.
        assert "repo_id IN (" in query
        assert "FROM repos" in query
        assert "repo = {repo_name:String}" in query
        assert "org_id = {org_id:String}" in query
        assert params["repo_name"] == "owner/repo"
        assert params["org_id"] == org_id


def test_run_dora_metrics_job_repo_id_takes_precedence(monkeypatch):
    """When both repo_id and repo_name are passed, repo_id wins (direct filter)
    and no unscoped repos subquery is emitted."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    sink = _CapturingClickHouseSink(deployments=[], incidents=[])
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    repo_id = uuid.uuid4()
    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        repo_id=repo_id,
        repo_name="owner/repo",
        org_id="test-org",
    )

    for query, params in sink.queries:
        assert "repo_id = {repo_id:UUID}" in query
        assert "FROM repos" not in query
        assert params["repo_id"] == str(repo_id)
        assert "repo_name" not in params


def test_run_dora_metrics_job_no_repo_scope_omits_filter(monkeypatch):
    """An unscoped (org-wide) run must not emit any repo filter clause."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    sink = _CapturingClickHouseSink(deployments=[], incidents=[])
    monkeypatch.setattr(job_dora, "ClickHouseMetricsSink", lambda db_url: sink)

    job_dora.run_dora_metrics_job(
        db_url="clickhouse://localhost:8123/default",
        day=date(2025, 1, 1),
        backfill_days=1,
        org_id="test-org",
    )

    for query, params in sink.queries:
        assert "AND repo_id = {repo_id:UUID}" not in query
        assert "repo = {repo_name:String}" not in query
        assert "repo_id" not in params
        assert "repo_name" not in params


def test_compute_dora_metrics_daily_seconds_units():
    """Lead time and MTTR are emitted in seconds (GitLab-API parity)."""
    repo_id = uuid.uuid4()
    computed_at = datetime(2025, 1, 2, tzinfo=timezone.utc)
    rows = compute_dora_metrics_daily(
        day=date(2025, 1, 1),
        deployments=[
            {
                "repo_id": repo_id,
                "deployment_id": "d1",
                "status": "success",
                "environment": "production",
                "started_at": datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                "deployed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                "merged_at": datetime(2025, 1, 1, 7, 0, tzinfo=timezone.utc),
            },
        ],
        incidents=[],
        computed_at=computed_at,
    )
    by_metric = {row.metric_name: row.value for row in rows}
    # 10:00 - 07:00 = 3h = 10800s
    assert by_metric["lead_time_for_changes"] == pytest.approx(10800.0)
    assert by_metric["deployment_frequency"] == 1.0
    assert by_metric["change_failure_rate"] == 0.0
    assert "time_to_restore_service" not in by_metric


@pytest.mark.parametrize(
    "failed_status",
    ["failure", "failed", "error", "canceled", "FAILURE", " Failed "],
)
def test_compute_dora_metrics_daily_counts_failed_status_variants(failed_status):
    """CHAOS-2382 round-3: every provider's failed-deployment vocabulary must
    contribute to change_failure_rate. GitHub persists ``status='failure'``
    (raw GitHub Deployment ``state``); GitLab persists ``status='failed'``.
    Regression for the no-ship where a GitHub ``status='failure'`` row was
    counted as a success, driving CFR toward 0 and hiding failed changes."""
    repo_id = uuid.uuid4()
    rows = compute_dora_metrics_daily(
        day=date(2025, 1, 1),
        deployments=[
            {
                "repo_id": repo_id,
                "deployment_id": "d-ok",
                "status": "success",
                "environment": "production",
                "started_at": datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                "deployed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                "merged_at": None,
            },
            {
                "repo_id": repo_id,
                "deployment_id": "d-bad",
                "status": failed_status,
                "environment": "production",
                "started_at": datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2025, 1, 1, 11, 30, tzinfo=timezone.utc),
                "deployed_at": datetime(2025, 1, 1, 11, 30, tzinfo=timezone.utc),
                "merged_at": None,
            },
        ],
        incidents=[],
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    by_metric = {row.metric_name: row.value for row in rows}
    assert by_metric["deployment_frequency"] == 2.0
    # 1 failed of 2 total -> CFR must be 0.5, never 0.0.
    assert by_metric["change_failure_rate"] == pytest.approx(0.5)


def test_compute_dora_metrics_daily_github_failure_not_counted_as_success():
    """Pin the exact Codex regression at the compute layer: a single
    GitHub-style ``status='failure'`` deployment yields CFR == 1.0, not 0.0."""
    repo_id = uuid.uuid4()
    rows = compute_dora_metrics_daily(
        day=date(2025, 1, 1),
        deployments=[
            {
                "repo_id": repo_id,
                "deployment_id": "gh-1",
                "status": "failure",  # raw GitHub Deployment state
                "environment": "production",
                "started_at": datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
                "finished_at": None,
                "deployed_at": datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
                "merged_at": None,
            },
        ],
        incidents=[],
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    by_metric = {row.metric_name: row.value for row in rows}
    assert by_metric["change_failure_rate"] == pytest.approx(1.0)


def test_compute_dora_metrics_daily_mixed_provider_failures_per_repo():
    """A two-repo mix where one repo's failure uses the GitHub vocabulary
    ('failure') and the other uses GitLab's ('failed') must classify BOTH as
    failed — proving the failed-status set is provider-agnostic across repos
    in a single org-wide run."""
    gh_repo = uuid.uuid4()
    gl_repo = uuid.uuid4()
    started = datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc)
    finished = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)
    rows = compute_dora_metrics_daily(
        day=date(2025, 1, 1),
        deployments=[
            {
                "repo_id": gh_repo,
                "deployment_id": "gh-1",
                "status": "failure",  # GitHub vocabulary
                "environment": "production",
                "started_at": started,
                "finished_at": finished,
                "deployed_at": finished,
                "merged_at": None,
            },
            {
                "repo_id": gl_repo,
                "deployment_id": "gl-1",
                "status": "failed",  # GitLab vocabulary
                "environment": "production",
                "started_at": started,
                "finished_at": finished,
                "deployed_at": finished,
                "merged_at": None,
            },
        ],
        incidents=[],
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    cfr = {
        str(row.repo_id): row.value
        for row in rows
        if row.metric_name == "change_failure_rate"
    }
    assert cfr[str(gh_repo)] == pytest.approx(1.0)
    assert cfr[str(gl_repo)] == pytest.approx(1.0)


def test_compute_dora_metrics_daily_ignores_out_of_window():
    repo_id = uuid.uuid4()
    rows = compute_dora_metrics_daily(
        day=date(2025, 1, 1),
        deployments=[
            {
                "repo_id": repo_id,
                "deployment_id": "d1",
                "status": "success",
                "environment": "production",
                "started_at": None,
                "finished_at": None,
                "deployed_at": datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc),
                "merged_at": None,
            },
        ],
        incidents=[],
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    assert rows == []
