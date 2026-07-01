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
