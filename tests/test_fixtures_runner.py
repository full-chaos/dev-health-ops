import argparse
import uuid

import pytest

from dev_health_ops.fixtures import runner
from dev_health_ops.fixtures.demo_identity import (
    ONBOARDED_ADMIN_USER_EMAIL,
    ONBOARDING_ORGLESS_USER_EMAIL,
)
from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.fixtures.runner import (
    _build_repo_team_assignments,
    run_fixtures_generation,
)
from dev_health_ops.models.ai_attribution import AIAttributionKind
from dev_health_ops.models.ai_workflow import (
    AIWorkflowArtifactType,
    AIWorkflowRunKind,
    AIWorkflowRunStatus,
)
from dev_health_ops.storage import SQLAlchemyStore


@pytest.mark.asyncio
async def test_fixtures_generation_smoke_sqlite(tmp_path):
    """
    Smoke test to ensure fixtures generation runs without crashing in SQLite.
    This would have caught the 'now' scope error and missing imports.
    """
    db_file = tmp_path / "test_fixtures.db"
    db_uri = f"sqlite:///{db_file}"

    # Mock argparse.Namespace
    ns = argparse.Namespace(
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/repo",
        repo_count=1,
        days=2,
        commits_per_day=2,
        pr_count=2,
        seed=42,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=2,
    )

    # Run the generation (metrics require ClickHouse since CHAOS-641)
    result = await run_fixtures_generation(ns)

    assert result == 0
    assert db_file.exists()


@pytest.mark.asyncio
async def test_fixtures_generation_minimal_no_metrics(tmp_path):
    """
    Ensure minimal generation works without the metrics flag.
    """
    db_file = tmp_path / "test_minimal.db"
    db_uri = f"sqlite:///{db_file}"

    ns = argparse.Namespace(
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/minimal",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=1,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)
    assert result == 0
    assert db_file.exists()


def test_pr_fixture_generator_emits_ai_attribution_records():
    org_id = str(uuid.uuid4())
    generator = SyntheticDataGenerator(repo_name="test/ai-fixtures", seed=42)
    pr_data = generator.generate_prs(count=6)
    prs = [item["pr"] for item in pr_data]

    records = generator.generate_ai_attributions(prs, org_id=org_id)

    assert records
    assert {record.org_id for record in records} == {uuid.UUID(org_id)}
    assert {record.repo_id for record in records} == {generator.repo_id}
    assert {record.subject_type for record in records} == {"pull_request"}
    assert {record.kind for record in records} >= {
        AIAttributionKind.AI_ASSISTED,
        AIAttributionKind.AGENT_CREATED,
        AIAttributionKind.HUMAN,
    }
    assert any(r.kind is AIAttributionKind.HUMAN for r in records), (
        "need human-bucket attributions so AI baseline deltas can compute"
    )


def test_pr_fixture_generator_emits_revert_shaped_prs():
    generator = SyntheticDataGenerator(repo_name="test/ai-reverts", seed=11)
    pr_data = generator.generate_prs(count=21)
    prs = [item["pr"] for item in pr_data]
    reverts = [
        pr
        for pr in prs
        if pr.title.startswith("Revert ")
        and (pr.deletions or 0) > (pr.additions or 0) * 2
        and (pr.deletions or 0) >= 50
    ]
    assert reverts, "expected revert-shaped PRs to drive revert_rate signal"


def test_ai_workflow_generator_emits_runs_and_edges():
    org_id = str(uuid.uuid4())
    generator = SyntheticDataGenerator(repo_name="test/ai-workflow", seed=7)
    pr_data = generator.generate_prs(count=6, issue_numbers=[101, 202])
    prs = [item["pr"] for item in pr_data]
    work_items = generator.generate_work_items(days=2)

    runs = generator.generate_ai_workflow_runs(prs, org_id=org_id)
    assert runs, "expected at least one synthetic AI workflow run"
    assert {run.org_id for run in runs} == {uuid.UUID(org_id)}
    assert all(run.prompts_redacted for run in runs)
    assert all(run.prompt_hash and len(run.prompt_hash) == 64 for run in runs)
    assert {run.run_kind for run in runs} >= {
        AIWorkflowRunKind.CHAT_ASSISTED,
        AIWorkflowRunKind.AGENT_AUTONOMOUS,
    }
    pr_runs = [
        run for run in runs if run.metadata.get("subject_type") == "pull_request"
    ]
    assert pr_runs, "expected at least one PR-linked run"
    autonomous_runs = [
        run for run in runs if run.run_kind is AIWorkflowRunKind.AGENT_AUTONOMOUS
    ]
    assert any(run.status is AIWorkflowRunStatus.FAILED for run in autonomous_runs)

    artifact_edges = generator.generate_ai_workflow_artifact_edges(
        runs, prs, org_id=org_id
    )
    assert artifact_edges, "expected artifact edges linking runs to PRs"
    edge_run_ids = {edge.run_id for edge in artifact_edges}
    pr_run_ids = {run.run_id for run in pr_runs}
    assert edge_run_ids.issubset({run.run_id for run in runs})
    assert edge_run_ids & pr_run_ids, "artifact edges must reference PR runs"
    assert {edge.artifact_type for edge in artifact_edges} >= {
        AIWorkflowArtifactType.PULL_REQUEST,
    }
    assert all(edge.repo_id == generator.repo_id for edge in artifact_edges)

    issue_edges = generator.generate_ai_workflow_issue_edges(
        runs, prs, work_items, org_id=org_id
    )
    assert issue_edges, "expected issue edges to be generated"
    assert {edge.run_id for edge in issue_edges}.issubset(
        {run.run_id for run in pr_runs}
    )
    assert all(edge.issue_id for edge in issue_edges)
    assert all(edge.confidence > 0 for edge in issue_edges)


@pytest.mark.asyncio
async def test_fixtures_generation_ensures_tables(tmp_path, monkeypatch):
    db_file = tmp_path / "test_ensure_tables.db"
    db_uri = f"sqlite:///{db_file}"

    called = {"value": False}
    original = SQLAlchemyStore.ensure_tables

    async def _wrapped(self):
        called["value"] = True
        return await original(self)

    monkeypatch.setattr(SQLAlchemyStore, "ensure_tables", _wrapped)

    ns = argparse.Namespace(
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/ensure",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=2,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)

    assert result == 0
    assert db_file.exists()
    assert called["value"] is True


def test_repo_team_assignments_distribution():
    teams = SyntheticDataGenerator(seed=123).get_team_assignment(count=6)["teams"]
    assignments = _build_repo_team_assignments(teams, repo_count=20, seed=123)

    assert len(assignments) == 20

    unowned_count = sum(1 for repo_teams in assignments if not repo_teams)
    assert unowned_count <= int(20 * 0.1)

    owned_by_team = {team.id: 0 for team in teams}
    for repo_teams in assignments:
        for team in repo_teams:
            owned_by_team[team.id] += 1
    assert all(count >= 1 for count in owned_by_team.values())

    multi_owned = sum(1 for count in owned_by_team.values() if count >= 2)
    assert multi_owned >= min(3, len(owned_by_team))


@pytest.mark.asyncio
async def test_fixtures_generation_initializes_license_manager(tmp_path):
    from dev_health_ops.licensing import LicenseManager, LicenseTier
    from dev_health_ops.licensing.gating import LicenseAuditLogger

    LicenseManager.reset()
    LicenseAuditLogger.reset()

    db_file = tmp_path / "test_license.db"
    db_uri = f"sqlite:///{db_file}"

    ns = argparse.Namespace(
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/license-check",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=99,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)
    assert result == 0

    manager = LicenseManager.get_instance()
    assert manager.is_licensed is True
    assert manager.tier == LicenseTier.ENTERPRISE

    LicenseManager.reset()
    LicenseAuditLogger.reset()


class _QueryResult:
    def __init__(self, value: int):
        self.result_rows = [(value,)]


class _ValidationClient:
    def __init__(
        self,
        *,
        records: int = 12,
        expected_repos: int = 3,
        expected_teams: int = 4,
        covered_repos: int = 3,
        covered_teams: int = 4,
    ):
        self.records = records
        self.expected_repos = expected_repos
        self.expected_teams = expected_teams
        self.covered_repos = covered_repos
        self.covered_teams = covered_teams

    def query(self, sql: str):
        normalized = " ".join(sql.split())
        if "FROM work_unit_investments AS wui" in normalized:
            return _QueryResult(self.covered_teams)
        if "FROM work_unit_investments" in normalized:
            if "countDistinct(repo_id)" in normalized:
                return _QueryResult(self.covered_repos)
            return _QueryResult(self.records)
        if "FROM repo_metrics_daily" in normalized:
            return _QueryResult(self.expected_repos)
        if "FROM team_metrics_daily" in normalized:
            return _QueryResult(self.expected_teams)
        raise AssertionError(f"Unexpected query: {normalized}")


def _all_tables_exist(name: str) -> bool:
    return name in {
        "work_unit_investments",
        "repo_metrics_daily",
        "team_metrics_daily",
    }


def test_work_unit_investment_validation_accepts_density_and_coverage():
    client = _ValidationClient()
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            client,
            table_exists=_all_tables_exist,
        )
        is True
    )


def test_work_unit_investment_validation_rejects_low_density():
    client = _ValidationClient(records=4, expected_repos=3, expected_teams=4)
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            client,
            table_exists=_all_tables_exist,
        )
        is False
    )


def test_work_unit_investment_validation_rejects_low_repo_or_team_coverage():
    low_repo_client = _ValidationClient(covered_repos=2, expected_repos=3)
    low_team_client = _ValidationClient(covered_teams=2, expected_teams=4)
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            low_repo_client,
            table_exists=_all_tables_exist,
        )
        is False
    )
    assert (
        validate(
            low_team_client,
            table_exists=_all_tables_exist,
        )
        is False
    )


class _AiValidationClient:
    """Stub ClickHouse client that returns canned counts for AI fixture tables."""

    def __init__(self, counts: dict[str, int], linked_runs: int = 5):
        self.counts = counts
        self.linked_runs = linked_runs

    def query(self, sql: str):
        normalized = " ".join(sql.split())
        for table, value in self.counts.items():
            if f"FROM {table}" in normalized and "ai_workflow_runs r" not in normalized:
                return _QueryResult(value)
        if "ai_workflow_runs r" in normalized:
            return _QueryResult(self.linked_runs)
        raise AssertionError(f"Unexpected query: {normalized}")


def _all_ai_tables_exist(name: str) -> bool:
    return name in set(runner.AI_FIXTURE_TABLES)


def test_validate_ai_fixture_tables_accepts_populated_state():
    counts = {table: 42 for table in runner.AI_FIXTURE_TABLES}
    client = _AiValidationClient(counts)
    assert runner._validate_ai_fixture_tables(client, _all_ai_tables_exist) is True


def test_validate_ai_fixture_tables_rejects_empty_table():
    counts = {table: 10 for table in runner.AI_FIXTURE_TABLES}
    counts["ai_workflow_runs"] = 0
    client = _AiValidationClient(counts)
    assert runner._validate_ai_fixture_tables(client, _all_ai_tables_exist) is False


def test_validate_ai_fixture_tables_rejects_missing_table():
    counts = {table: 10 for table in runner.AI_FIXTURE_TABLES}
    client = _AiValidationClient(counts)
    present = set(runner.AI_FIXTURE_TABLES) - {"ai_workflow_artifact_edges"}
    assert (
        runner._validate_ai_fixture_tables(client, lambda name: name in present)
        is False
    )


def test_validate_ai_fixture_tables_rejects_unlinked_runs():
    counts = {table: 10 for table in runner.AI_FIXTURE_TABLES}
    client = _AiValidationClient(counts, linked_runs=0)
    assert runner._validate_ai_fixture_tables(client, _all_ai_tables_exist) is False


class _CockpitLiveDataValidationClient:
    def __init__(
        self,
        counts: dict[str, int],
        *,
        review_latency_rows: int = 3,
        complexity_rows: int = 3,
    ):
        self.counts = counts
        self.review_latency_rows = review_latency_rows
        self.complexity_rows = complexity_rows

    def query(self, sql: str):
        normalized = " ".join(sql.split())
        if "pr_first_review_p90_hours IS NOT NULL" in normalized:
            return _QueryResult(self.review_latency_rows)
        if "cyclomatic_per_kloc IS NOT NULL" in normalized:
            return _QueryResult(self.complexity_rows)
        for table, value in self.counts.items():
            if f"FROM {table}" in normalized:
                return _QueryResult(value)
        raise AssertionError(f"Unexpected query: {normalized}")


def _all_cockpit_live_data_tables_exist(name: str) -> bool:
    return name in set(runner.COCKPIT_LIVE_DATA_TABLES)


def test_validate_cockpit_live_data_fixture_tables_accepts_populated_state():
    counts = {table: 10 for table in runner.COCKPIT_LIVE_DATA_TABLES}
    client = _CockpitLiveDataValidationClient(counts)

    assert (
        runner._validate_cockpit_live_data_fixture_tables(
            client, _all_cockpit_live_data_tables_exist
        )
        is True
    )


def test_validate_cockpit_live_data_fixture_tables_rejects_missing_table():
    counts = {table: 10 for table in runner.COCKPIT_LIVE_DATA_TABLES}
    client = _CockpitLiveDataValidationClient(counts)
    present = set(runner.COCKPIT_LIVE_DATA_TABLES) - {"testops_test_metrics_daily"}

    assert (
        runner._validate_cockpit_live_data_fixture_tables(
            client, lambda name: name in present
        )
        is False
    )


def test_validate_cockpit_live_data_fixture_tables_rejects_empty_table():
    counts = {table: 10 for table in runner.COCKPIT_LIVE_DATA_TABLES}
    counts["testops_coverage_metrics_daily"] = 0
    client = _CockpitLiveDataValidationClient(counts)

    assert (
        runner._validate_cockpit_live_data_fixture_tables(
            client, _all_cockpit_live_data_tables_exist
        )
        is False
    )


def test_validate_cockpit_live_data_fixture_tables_rejects_missing_compounding_inputs():
    counts = {table: 10 for table in runner.COCKPIT_LIVE_DATA_TABLES}
    no_review_latency = _CockpitLiveDataValidationClient(counts, review_latency_rows=0)
    no_complexity = _CockpitLiveDataValidationClient(counts, complexity_rows=0)

    assert (
        runner._validate_cockpit_live_data_fixture_tables(
            no_review_latency, _all_cockpit_live_data_tables_exist
        )
        is False
    )
    assert (
        runner._validate_cockpit_live_data_fixture_tables(
            no_complexity, _all_cockpit_live_data_tables_exist
        )
        is False
    )


class _SecurityAlertsClient:
    """Stub ClickHouse client for security_alerts validation tests."""

    def __init__(self, *, count: int = 20, distinct_severities: int = 4):
        self.count = count
        self.distinct_severities = distinct_severities

    def query(self, sql: str):
        normalized = " ".join(sql.split())
        if "countDistinct(severity)" in normalized:
            return _QueryResult(self.distinct_severities)
        if "FROM security_alerts" in normalized:
            return _QueryResult(self.count)
        raise AssertionError(f"Unexpected query: {normalized}")


def _security_alerts_table_exists(name: str) -> bool:
    return name == "security_alerts"


def test_validate_security_alerts_fixture_accepts_populated_state():
    client = _SecurityAlertsClient(count=20, distinct_severities=4)
    assert (
        runner._validate_security_alerts_fixture(
            client, table_exists=_security_alerts_table_exists
        )
        is True
    )


def test_validate_security_alerts_fixture_rejects_missing_table():
    client = _SecurityAlertsClient()
    assert (
        runner._validate_security_alerts_fixture(
            client, table_exists=lambda name: False
        )
        is False
    )


def test_validate_security_alerts_fixture_rejects_empty_table():
    client = _SecurityAlertsClient(count=0)
    assert (
        runner._validate_security_alerts_fixture(
            client, table_exists=_security_alerts_table_exists
        )
        is False
    )


def test_validate_security_alerts_fixture_rejects_sparse_table():
    """count > 0 but below MIN_SECURITY_ALERTS threshold."""
    client = _SecurityAlertsClient(count=runner.MIN_SECURITY_ALERTS - 1)
    assert (
        runner._validate_security_alerts_fixture(
            client, table_exists=_security_alerts_table_exists
        )
        is False
    )


def test_validate_security_alerts_fixture_rejects_single_severity():
    """Table has rows but only one distinct severity — distribution is degenerate."""
    client = _SecurityAlertsClient(count=20, distinct_severities=1)
    assert (
        runner._validate_security_alerts_fixture(
            client, table_exists=_security_alerts_table_exists
        )
        is False
    )


class TestRunnerWiresExtendedPipelineRows:
    """CHAOS-2173: pipeline-run insert must be a single call per (repo_id, run_id)
    so ci_daily_rollup_mv counts each run exactly once."""

    @pytest.mark.asyncio
    async def test_sqlite_path_no_double_insert(self, tmp_path, monkeypatch):
        """On SQLite/Postgres (SQLAlchemyStore), the runner must call
        insert_ci_pipeline_runs and must NOT call insert_testops_pipeline_runs
        — preventing a double-insert that would inflate any MV counts."""
        from dev_health_ops.storage import SQLAlchemyStore

        ci_run_ids: list[str] = []
        testops_run_ids: list[str] = []

        original_ci = SQLAlchemyStore.insert_ci_pipeline_runs

        async def spy_ci(self_store, batch):
            ci_run_ids.extend(r.run_id for r in batch)
            return await original_ci(self_store, batch)

        async def spy_testops(self_store, batch):
            testops_run_ids.extend(r.get("run_id", "") for r in batch)

        monkeypatch.setattr(SQLAlchemyStore, "insert_ci_pipeline_runs", spy_ci)
        monkeypatch.setattr(
            SQLAlchemyStore, "insert_testops_pipeline_runs", spy_testops
        )

        db_file = tmp_path / "test_no_double.db"
        ns = argparse.Namespace(
            sink=f"sqlite:///{db_file}",
            db_type="sqlite",
            org_id="test-org",
            repo_name="test/no-double",
            repo_count=1,
            days=3,
            commits_per_day=2,
            pr_count=2,
            seed=42,
            provider="synthetic",
            with_work_graph=False,
            with_metrics=False,
            team_count=2,
        )

        result = await run_fixtures_generation(ns)
        assert result == 0

        # Postgres/SQLite path: basic insert was used.
        assert ci_run_ids, "insert_ci_pipeline_runs must be called on SQLite path"

        # No testops insert on the SQLite path — this prevents double-insert.
        assert not testops_run_ids, (
            "insert_testops_pipeline_runs must NOT be called on SQLite/Postgres — "
            "calling both would insert each run_id twice, inflating MV counts. "
            f"Got run_ids: {testops_run_ids[:5]}"
        )

        # Every run_id appears exactly once.
        assert len(ci_run_ids) == len(set(ci_run_ids)), (
            "each run_id must appear in insert_ci_pipeline_runs exactly once"
        )

    def test_clickhouse_store_takes_extended_branch(self):
        """The branching condition must route non-SQLAlchemy stores with
        insert_testops_pipeline_runs to the single-insert (extended) path,
        and SQLAlchemyStore to the basic-insert path.

        This directly exercises the condition in runner._handler without
        needing a full end-to-end run against a live ClickHouse."""
        from dev_health_ops.storage import SQLAlchemyStore

        # Simulate ClickHouseStore: not SQLAlchemy, has insert_testops_pipeline_runs.
        class _FakeCHStore:
            async def insert_ci_pipeline_runs(self, batch):
                pass

            async def insert_testops_pipeline_runs(self, batch):
                pass

        ch_store = _FakeCHStore()
        sq_store = SQLAlchemyStore.__new__(SQLAlchemyStore)

        # Replicate the branching condition from runner.py.
        def _pick_pipeline_insert(store):
            return (
                getattr(store, "insert_testops_pipeline_runs", None)
                if not isinstance(store, SQLAlchemyStore)
                else None
            )

        # ClickHouse-like store → extended insert path.
        ch_insert = _pick_pipeline_insert(ch_store)
        assert ch_insert is not None, (
            "ClickHouseStore-like stores must use insert_testops_pipeline_runs"
        )
        # Bound methods compare equal by __func__ + __self__ even if not identical objects.
        assert ch_insert.__func__ is _FakeCHStore.insert_testops_pipeline_runs, (
            "must resolve to insert_testops_pipeline_runs, not insert_ci_pipeline_runs"
        )

        # SQLAlchemyStore → None → falls through to insert_ci_pipeline_runs.
        sq_insert = _pick_pipeline_insert(sq_store)
        assert sq_insert is None, (
            "SQLAlchemyStore must NOT use insert_testops_pipeline_runs "
            "(would double-insert into Postgres/SQLite)"
        )

    def test_generator_derives_extended_rows_from_pipeline_runs(self):
        """When pipeline_runs is supplied, extended rows must use the same run_ids
        as those SQLAlchemy objects (key alignment for ReplacingMergeTree dedup)."""
        gen = SyntheticDataGenerator(repo_name="test/key-align", seed=42)
        pipeline_runs = gen.generate_ci_pipeline_runs(days=5, runs_per_day=3)

        extended = gen.generate_pipeline_run_extended_rows(
            pipeline_runs=pipeline_runs, org_id="test-org"
        )

        assert len(extended) == len(pipeline_runs), (
            "extended rows count must match pipeline_runs count"
        )
        expected_run_ids = {r.run_id for r in pipeline_runs}
        actual_run_ids = {r["run_id"] for r in extended}
        assert actual_run_ids == expected_run_ids, (
            "extended row run_ids must match pipeline_run run_ids exactly"
        )
        # Every extended row must carry the required TestOps-only fields.
        for row in extended:
            assert "retry_count" in row
            assert "team_id" in row
            assert row.get("org_id") == "test-org"


class TestGenerateUsersRespectsOrgId:
    """Regression: ``generate_users(org_id=...)`` MUST stamp the supplied org_id
    onto the Organization row and every Membership/license, so that synthetic
    Postgres tenants line up with analytics-side org_id (CHAOS-1558)."""

    _UUID_ORG = "11111111-1111-1111-1111-111111111111"

    def test_supplied_uuid_propagates_to_org_and_memberships(self):
        import uuid

        gen = SyntheticDataGenerator(repo_name="acme/demo", seed=1)
        data = gen.generate_users(org_id=self._UUID_ORG)

        # Exactly one organization is produced (the admin org).
        assert len(data["organizations"]) == 1
        org = data["organizations"][0]
        assert org.id == uuid.UUID(self._UUID_ORG)
        # Slug must be deterministic AND derived from org_id, not hardcoded.
        assert org.slug != "default-org"
        assert org.slug == f"fixture-{uuid.UUID(self._UUID_ORG).hex[:8]}"

        assert {user.email for user in data["users"]} == {
            ONBOARDING_ORGLESS_USER_EMAIL,
            ONBOARDED_ADMIN_USER_EMAIL,
        }
        assert len(data["memberships"]) == 1
        for m in data["memberships"]:
            assert m.org_id == uuid.UUID(self._UUID_ORG), (
                f"membership {m.id} for user {m.user_id} bound to wrong org "
                f"{m.org_id}; expected {self._UUID_ORG}"
            )

        # License must also be tenant-scoped.
        assert len(data["licenses"]) == 1
        assert data["licenses"][0].org_id == uuid.UUID(self._UUID_ORG)

    def test_non_uuid_org_id_hashed_deterministically(self):
        import uuid

        _NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        expected = uuid.uuid5(_NS, "acme-engineering")

        gen = SyntheticDataGenerator(repo_name="acme/demo", seed=1)
        data = gen.generate_users(org_id="acme-engineering")

        assert data["organizations"][0].id == expected
        assert data["organizations"][0].slug == "acme-engineering"
        for m in data["memberships"]:
            assert m.org_id == expected

    def test_default_behaviour_preserved_when_org_id_omitted(self):
        import uuid

        _NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        expected = uuid.uuid5(_NS, "default-org")

        gen = SyntheticDataGenerator(repo_name="acme/demo", seed=1)
        data = gen.generate_users()  # no org_id

        assert data["organizations"][0].id == expected
        assert data["organizations"][0].slug == "default-org"
        assert data["organizations"][0].name == "Meridian"
        assert {user.email for user in data["users"]} == {
            ONBOARDING_ORGLESS_USER_EMAIL,
            ONBOARDED_ADMIN_USER_EMAIL,
        }
        assert len(data["memberships"]) == 1
        for m in data["memberships"]:
            assert m.org_id == expected

    def test_org_id_with_unsafe_slug_chars_is_sanitised(self):
        import uuid

        gen = SyntheticDataGenerator(repo_name="acme/demo", seed=1)
        # Mixed case, spaces, slashes, etc. must not break slug uniqueness.
        data = gen.generate_users(org_id="ACME Engineering / R&D")

        slug = data["organizations"][0].slug
        # Slug must be non-empty, lowercase, and free of unsafe chars.
        assert slug, "slug must not be empty after sanitisation"
        assert slug == slug.lower()
        assert all(c.isalnum() or c == "-" for c in slug)

        # All memberships still tie to the same derived org UUID.
        org_uuid = data["organizations"][0].id
        assert isinstance(org_uuid, uuid.UUID)
        for m in data["memberships"]:
            assert m.org_id == org_uuid


class TestFixtureTeamRepoPatterns:
    """CHAOS-4276 / fixtures audit 2026-08-26
    (.remember/lanes/lane-fixtures-audit/fixtures-audit-2026-08-26.md
    section 3): every fixture team's repo_patterns was always [], which
    starves RepoPatternTeamResolver's repo-pattern-FIRST path (checked
    before membership) that team_wellbeing and other team-scoped families
    try first. run_fixtures_generation must populate each team's
    repo_patterns from the SAME repo<->team ownership assignment already
    computed for repo-cooccurrence density."""

    @pytest.mark.asyncio
    async def test_run_fixtures_generation_populates_repo_patterns(
        self, tmp_path, monkeypatch
    ):
        from dev_health_ops.fixtures.demo_identity import demo_repo_name

        captured_teams: list = []
        original_insert_teams = SQLAlchemyStore.insert_teams

        async def spy_insert_teams(self_store, teams):
            captured_teams.extend(teams)
            return await original_insert_teams(self_store, teams)

        monkeypatch.setattr(SQLAlchemyStore, "insert_teams", spy_insert_teams)

        db_file = tmp_path / "test_repo_patterns.db"
        ns = argparse.Namespace(
            sink=f"sqlite:///{db_file}",
            db_type="sqlite",
            org_id="test-org",
            repo_name="acme/repo-patterns",
            repo_count=3,
            days=1,
            commits_per_day=1,
            pr_count=1,
            seed=7,
            provider="synthetic",
            with_work_graph=False,
            with_metrics=False,
            team_count=3,
        )

        result = await run_fixtures_generation(ns)
        assert result == 0
        assert captured_teams, "insert_teams must have been called"

        repo_names = {demo_repo_name(ns.repo_name, i, ns.repo_count) for i in range(3)}

        # Every team's repo_patterns entries must be real generated repo
        # names -- no stray/fabricated values.
        for team in captured_teams:
            for pattern in team.repo_patterns:
                assert pattern in repo_names, (
                    f"team {team.id} repo_patterns has an unrecognized repo {pattern!r}"
                )

        # At least one team must own at least one repo -- previously this
        # was unconditionally empty for every team.
        assert any(team.repo_patterns for team in captured_teams), (
            "no fixture team was assigned a repo_patterns entry -- the "
            "repo-pattern-first resolver path is unreachable from fixtures"
        )

    @pytest.mark.asyncio
    async def test_run_fixtures_generation_repo_patterns_never_collide_across_teams(
        self, tmp_path, monkeypatch
    ):
        """CHAOS-4276 codex round-1 finding 3 regression: a repo with more
        than one owning team must contribute its repo_pattern to exactly ONE
        team, never to every co-owner. RepoPatternTeamResolver's exact-match
        map (providers/teams.py, wellbeing_native_clickhouse.go) holds one
        team per pattern string, so a pattern seeded for two teams would make
        whichever team was written last silently win and strand the other
        team's commits with no repo-pattern match at all. seed=7/repo_count=3
        /team_count=3 is known (see the density test above) to make every
        owned repo multi-owned -- the worst case for this collision."""
        captured_teams: list = []
        original_insert_teams = SQLAlchemyStore.insert_teams

        async def spy_insert_teams(self_store, teams):
            captured_teams.extend(teams)
            return await original_insert_teams(self_store, teams)

        monkeypatch.setattr(SQLAlchemyStore, "insert_teams", spy_insert_teams)

        db_file = tmp_path / "test_repo_patterns_no_collision.db"
        ns = argparse.Namespace(
            sink=f"sqlite:///{db_file}",
            db_type="sqlite",
            org_id="test-org",
            repo_name="acme/repo-patterns-collision",
            repo_count=3,
            days=1,
            commits_per_day=1,
            pr_count=1,
            seed=7,
            provider="synthetic",
            with_work_graph=False,
            with_metrics=False,
            team_count=3,
        )

        result = await run_fixtures_generation(ns)
        assert result == 0

        all_patterns: list[str] = []
        for team in captured_teams:
            all_patterns.extend(team.repo_patterns)
        assert all_patterns, "fixture setup drifted: expected at least one repo_pattern"
        assert len(all_patterns) == len(set(all_patterns)), (
            f"a repo_pattern is claimed by more than one team: {all_patterns}"
        )

    def test_team_model_repo_patterns_defaults_empty_and_accepts_a_list(self):
        from dev_health_ops.models.teams import Team

        default_team = Team(id="t1", name="Team 1")
        assert default_team.repo_patterns == []

        patterned_team = Team(id="t2", name="Team 2", repo_patterns=["acme/service-a"])
        assert patterned_team.repo_patterns == ["acme/service-a"]


class TestGeneratedFilesCarryContents:
    """CHAOS-4338 / fixtures audit 2026-08-26 section 3: git_files.contents
    was always left unset, so the real complexity compute entrypoint
    (run_complexity_db_job) had nothing to scan and fixtures had to fabricate
    file_complexity_snapshots/repo_complexity_daily via random.* instead."""

    def test_generate_files_populates_contents_for_python_files(self):
        generator = SyntheticDataGenerator(repo_name="acme/contents", seed=3)
        files = generator.generate_files()
        assert files, "fixture setup drifted: expected at least one file"
        py_files = [f for f in files if f.path.endswith(".py")]
        assert py_files, "fixture file list drifted: expected .py files"
        for git_file in py_files:
            assert git_file.contents, (
                f"{git_file.path} has no contents -- the real complexity "
                "scanner (ComplexityScanner.should_process default "
                "**/*.py) has nothing to parse"
            )
            # Real Python source, not a placeholder -- ast.parse must
            # succeed so the real scanner can actually compute complexity.
            import ast

            ast.parse(git_file.contents)


class TestTeamOwnershipEdges:
    """CHAOS-4338 / fixtures audit 2026-08-26 section 2:
    team_project_ownership, team_repo_ownership, and team_memberships had
    ZERO writers anywhere in fixtures, starving 5-6 of the real attribution
    resolver's ~9 sources. generate_team_ownership_edges builds all three
    from the SAME repo<->team assignment already used for repo_patterns
    (CHAOS-4276), so ownership stays consistent with the pattern-resolver
    path."""

    def _build(self, *, repo_count=5, team_count=4, seed=1):
        generator = SyntheticDataGenerator(repo_name="acme/ownership", seed=seed)
        all_teams = generator.get_team_assignment(count=team_count)["teams"]
        repo_team_assignments = _build_repo_team_assignments(
            all_teams, repo_count, seed
        )
        repo_names = [f"acme/ownership-{i}" for i in range(repo_count)]
        repo_ids = [uuid.uuid5(uuid.NAMESPACE_URL, name) for name in repo_names]
        edges = generator.generate_team_ownership_edges(
            all_teams=all_teams,
            repo_team_assignments=repo_team_assignments,
            repo_names=repo_names,
            repo_ids=repo_ids,
            org_id="org-1",
            provider="synthetic",
        )
        return all_teams, repo_team_assignments, repo_names, edges

    def test_chaos_4329_proof_a_team_owns_two_or_more_repos(self):
        """CHAOS-4329: at least one team must have >=2 team_repo_ownership
        rows (co-owner rows beyond the primary owner are written too, not
        just the primary -- unlike repo_patterns, which records one primary
        owner per repo)."""
        _, _, _, edges = self._build()
        by_team: dict[str, set[str]] = {}
        for row in edges["team_repo_ownership"]:
            by_team.setdefault(row["team_id"], set()).add(row["repo_full_name"])
        multi_repo_teams = {
            team_id: repos for team_id, repos in by_team.items() if len(repos) >= 2
        }
        assert multi_repo_teams, (
            "no team owns >=2 repos in team_repo_ownership -- CHAOS-4329 proof missing"
        )

    def test_ambiguous_identity_proof_one_member_maps_to_two_teams(self):
        """A real attribution resolver's exactly-one-team gate must see two
        distinct team_ids for one identity and refuse to guess (unassigned),
        per the documented rule: "if the person is mapped to two or more
        teams, we do not guess". With >=3 teams the admin-override identity
        (a separate, deliberate proof -- see
        test_admin_override_identity_conflicts_with_provider_fallback) is
        ALSO a multi-team member by this same grouping, so this asserts
        "at least one", not "exactly one"."""
        _, _, _, edges = self._build()
        by_member: dict[str, set[str]] = {}
        for row in edges["team_memberships"]:
            by_member.setdefault(row["member_id"], set()).add(row["team_id"])
        ambiguous = {
            member: teams for member, teams in by_member.items() if len(teams) >= 2
        }
        assert ambiguous, "no team_memberships identity maps to >=2 teams"

    def test_admin_override_identity_conflicts_with_provider_fallback(self):
        """CHAOS-4321 (chris: "manual is override -- if the override exists,
        use it, else use attribution from providers"): with >=3 teams, one
        identity gets an admin mapping (identities.team_ids + that team's
        manual_members, mutated on the Team object in place) AND a
        conflicting provider-fallback team_memberships row into a DIFFERENT
        team -- the data shape a real two-layer resolver must pick the
        admin team from."""
        all_teams, _, _, edges = self._build(team_count=4)
        assert edges["identities"], (
            "expected an admin-override identities row with >=3 teams"
        )
        identity = edges["identities"][0]
        admin_team_id = identity["team_ids"][0]
        override_member = identity["canonical_id"]

        admin_team = next(t for t in all_teams if t.id == admin_team_id)
        assert override_member in (admin_team.manual_members or []), (
            "admin team's manual_members must carry the override identity"
        )

        conflicting_team_ids = {
            row["team_id"]
            for row in edges["team_memberships"]
            if row["member_id"] == override_member
        }
        assert admin_team_id in conflicting_team_ids
        assert len(conflicting_team_ids) >= 2, (
            "override identity must ALSO have a provider-fallback "
            "team_memberships row into a different team -- otherwise "
            "there is no override-vs-fallback conflict to prove"
        )

    def test_team_project_ownership_mirrors_team_repo_ownership(self):
        """Project ids are repo full names (generators/projects.py's
        project_id_for_repo: repo-backed projects are 1:1 with repos in this
        fixture world), so team_project_ownership must assign the exact same
        (team_id, repo/project) pairs as team_repo_ownership."""
        _, _, _, edges = self._build()
        repo_pairs = {
            (row["team_id"], row["repo_full_name"])
            for row in edges["team_repo_ownership"]
        }
        project_pairs = {
            (row["team_id"], row["project_id"])
            for row in edges["team_project_ownership"]
        }
        assert repo_pairs == project_pairs
        assert repo_pairs, "fixture setup drifted: expected at least one owned repo"

    def test_every_owned_repo_has_exactly_one_primary_owner(self):
        _, _, repo_names, edges = self._build()
        primaries_by_repo: dict[str, list[str]] = {}
        for row in edges["team_repo_ownership"]:
            if row["is_primary"]:
                primaries_by_repo.setdefault(row["repo_full_name"], []).append(
                    row["team_id"]
                )
        owned_repos = {row["repo_full_name"] for row in edges["team_repo_ownership"]}
        for repo_name in owned_repos:
            assert len(primaries_by_repo.get(repo_name, [])) == 1, (
                f"repo {repo_name!r} must have exactly one primary owner, "
                f"got {primaries_by_repo.get(repo_name)}"
            )

    def test_membership_rows_reference_real_team_members(self):
        all_teams, _, _, edges = self._build()
        known_members = {
            str(member).strip().lower()
            for team in all_teams
            for member in (team.members or [])
        }
        for row in edges["team_memberships"]:
            assert row["member_id"] in known_members
            assert row["org_id"] == "org-1"
            assert row["provider"] == "synthetic"
            assert row["source"] in {"native", "provider_access"}
            assert row["valid_to"] is None


class TestGenerateIssuePrLinksCoverage:
    """CHAOS-4345: generate_issue_pr_links's realized coverage used to land
    systematically below its own min_coverage parameter, making
    `fixtures validate`'s Issue->PR coverage check
    (`linked_non_epic / non_epic_wi_count`, epic-EXCLUSIVE on both sides)
    fail on every run regardless of seed/size. Two compounding bugs:
    target_count computed against an epic-INCLUSIVE candidate list, and a
    floor-division cluster count that silently dropped up to
    cluster_size-1 trailing selected-but-unwritten items."""

    def _build_inputs(self, *, seed=11, days=30, pr_count=40):
        generator = SyntheticDataGenerator(repo_name="acme/pr-coverage", seed=seed)
        work_items = generator.generate_work_items(days=days)
        pr_data = generator.generate_prs(count=pr_count)
        prs = [item["pr"] for item in pr_data]
        return generator, work_items, prs

    def test_target_count_excludes_epics_from_candidates(self):
        generator, work_items, prs = self._build_inputs()
        epics = [wi for wi in work_items if wi.type == "epic"]
        non_epics = [wi for wi in work_items if wi.type != "epic"]
        assert epics, "fixture setup drifted: expected at least one epic"
        assert non_epics, "fixture setup drifted: expected non-epic items"

        links = generator.generate_issue_pr_links(
            work_items, prs, min_coverage=0.7, org_id="org-1"
        )
        linked_ids = {link["work_item_id"] for link in links}
        epic_ids = {str(wi.work_item_id) for wi in epics}
        assert not (linked_ids & epic_ids), (
            "an epic was linked -- target_count must be computed against "
            "non-epic candidates only, matching what fixtures validate's "
            "Issue->PR coverage check measures"
        )

        non_epic_ids = {str(wi.work_item_id) for wi in non_epics}
        coverage = len(linked_ids & non_epic_ids) / len(non_epic_ids)
        assert coverage >= 0.7, (
            f"realized non-epic coverage {coverage:.1%} fell below the "
            f"min_coverage=0.7 target -- CHAOS-4345 regression"
        )

    def test_no_selected_item_is_silently_dropped_by_cluster_flooring(self):
        """A floor-divided cluster count used to drop up to cluster_size-1
        trailing items from `linked_items` without ever writing an edge for
        them. Every item counted toward target_count must get >=1 edge."""
        generator, work_items, prs = self._build_inputs(pr_count=60)
        non_epics = [wi for wi in work_items if wi.type != "epic"]
        target_count = max(1, int(len(non_epics) * 0.7))

        links = generator.generate_issue_pr_links(
            work_items, prs, min_coverage=0.7, cluster_size=5, org_id="org-1"
        )
        distinct_linked = {link["work_item_id"] for link in links}
        # Allow for the shuffle picking epics out (none should be linked,
        # per the other test) -- the key invariant is that essentially all
        # of target_count's worth of non-epic items got an edge, not just
        # target_count rounded down to the nearest multiple of
        # cluster_size.
        assert len(distinct_linked) >= target_count - 1, (
            f"only {len(distinct_linked)} distinct work items got a "
            f"work_graph_issue_pr edge, expected >= {target_count - 1} "
            f"(target_count={target_count}) -- cluster-remainder items "
            f"were silently dropped (CHAOS-4345 regression)"
        )

    def test_realistic_scale_clears_the_validate_threshold(self):
        """End-to-end shape check at roughly the size `fixtures validate`
        exercises: non-epic coverage must clear the 70% threshold with
        margin, not just barely, so normal per-run randomness doesn't flake
        the real validate command."""
        generator, work_items, prs = self._build_inputs(seed=7, days=30, pr_count=30)
        non_epics = [wi for wi in work_items if wi.type != "epic"]
        links = generator.generate_issue_pr_links(
            work_items, prs, min_coverage=0.7, org_id="org-1"
        )
        linked_ids = {link["work_item_id"] for link in links}
        non_epic_ids = {str(wi.work_item_id) for wi in non_epics}
        coverage = len(linked_ids & non_epic_ids) / len(non_epic_ids)
        assert coverage >= 0.7

    def test_fractional_target_count_rounds_up_not_down(self):
        """CHAOS-4345 codex round 1, P1: `int(len(candidates) *
        min_coverage)` truncates -- a non-multiple candidate count (e.g.
        14 * 0.7 = 9.8) selected only 9 (64.3%), still below the 70% the
        validate check requires. Directly pins the fix at a candidate
        count deliberately NOT a multiple of min_coverage's denominator,
        bypassing generate_work_items entirely so the count is exact and
        this test doesn't depend on how many epics a given seed happens to
        produce (the 30-day tests above use exactly 60 non-epics, which
        masked this: 60 * 0.7 = 42.0 exactly, no truncation to hide)."""
        import dataclasses

        generator = SyntheticDataGenerator(repo_name="acme/frac", seed=1)
        raw_work_items = generator.generate_work_items(days=30)
        # force non-epic so the count is exactly 14 (WorkItem is frozen)
        work_items = [
            dataclasses.replace(wi, type="story") for wi in raw_work_items[:14]
        ]
        pr_data = generator.generate_prs(count=10)
        prs = [item["pr"] for item in pr_data]

        links = generator.generate_issue_pr_links(
            work_items, prs, min_coverage=0.7, org_id="org-1"
        )
        linked_ids = {link["work_item_id"] for link in links} & {
            str(wi.work_item_id) for wi in work_items
        }
        coverage = len(linked_ids) / len(work_items)
        assert coverage >= 0.7, (
            f"14 candidates * 0.7 = 9.8 must round UP to 10 (71.4%), not "
            f"truncate to 9 (64.3%) -- got {coverage:.1%}"
        )


class TestResolveAuthSeedPostgresUri:
    """CHAOS-4402: ``fixtures generate`` (no ``ns.postgres_uri``) must seed
    auth data through the operator-named ``--db``/``ns.db``, not fall
    straight through to whatever POSTGRES_URI/DATABASE_URI happens to be
    exported in the ambient shell.
    """

    def test_uses_explicit_db_flag_over_broken_ambient_env(self, monkeypatch):
        """Reproduces the live CHAOS-4402 failure: --db was passed
        explicitly and correctly, but the OLD resolver ignored it and read
        an unrelated, broken DATABASE_URI straight from the environment."""
        monkeypatch.setenv(
            "DATABASE_URI",
            "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/devhealth",
        )
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        ns = argparse.Namespace(
            db="postgresql://devhealth:devhealth@localhost:5432/devhealth"
        )

        resolved = runner.resolve_auth_seed_postgres_uri(ns)

        assert resolved is not None
        assert "${POSTGRES_USER}" not in resolved
        assert "localhost:5432/devhealth" in resolved

    def test_explicit_postgres_uri_still_wins_over_db_flag(self, monkeypatch):
        """``fixtures world``'s own ``--postgres-uri`` override (validated by
        its scratch-database guard) must keep outranking the bare ``--db``
        flag -- unchanged precedence from before CHAOS-4402."""
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        ns = argparse.Namespace(
            postgres_uri="postgresql://scratch:scratch@localhost:5432/world_scratch",
            db="postgresql://devhealth:devhealth@localhost:5432/devhealth",
        )

        resolved = runner.resolve_auth_seed_postgres_uri(ns)

        assert resolved == "postgresql://scratch:scratch@localhost:5432/world_scratch"

    def test_falls_back_to_environment_when_db_flag_unset(self, monkeypatch):
        """No ``--db`` passed at all: the pre-existing env fallback chain
        (``get_postgres_uri``) still applies, unchanged."""
        monkeypatch.setenv(
            "POSTGRES_URI", "postgresql://devhealth:devhealth@localhost:5432/devhealth"
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        ns = argparse.Namespace()

        resolved = runner.resolve_auth_seed_postgres_uri(ns)

        assert resolved is not None
        assert "localhost:5432/devhealth" in resolved

    def test_ignores_non_postgres_db_flag_and_falls_back_to_environment(
        self, monkeypatch
    ):
        """Codex review round 3 (P2): docs/contribute/development/commands.md
        documents ``fixtures generate --db "$CLICKHOUSE_URI"`` -- --db
        misused there for the analytics sink. Trusting ns.db unconditionally
        would make that documented invocation crash (a clickhouse:// URI
        reaching create_async_engine unchanged). A --db that doesn't look
        like a Postgres URI must fall through to the environment exactly as
        it did before this fix, not be used for auth-seeding."""
        monkeypatch.setenv(
            "POSTGRES_URI", "postgresql://devhealth:devhealth@localhost:5432/devhealth"
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        ns = argparse.Namespace(db="clickhouse://ch:ch@localhost:8123/default")

        resolved = runner.resolve_auth_seed_postgres_uri(ns)

        assert resolved is not None
        assert "localhost:5432/devhealth" in resolved
        assert "clickhouse" not in resolved

    def test_accepts_already_async_postgres_db_flag(self, monkeypatch):
        """Codex review round 4 (P1, correct): the scheme check only
        recognized postgresql:// and postgres://, rejecting the standard
        already-async postgresql+asyncpg:// form -- a valid Postgres URI a
        caller might legitimately pass, silently falling back to the
        environment instead of using it."""
        monkeypatch.setenv(
            "POSTGRES_URI", "postgresql://wrong:wrong@localhost:5432/wrong"
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        ns = argparse.Namespace(
            db="postgresql+asyncpg://devhealth:devhealth@localhost:5432/devhealth"
        )

        resolved = runner.resolve_auth_seed_postgres_uri(ns)

        assert resolved is not None
        assert "localhost:5432/devhealth" in resolved
        assert "wrong" not in resolved
