"""CHAOS-3219: unit coverage for the new ask-dev-world generator modules
(projects, source_health, conflicts, retention_conversations).

Pure Python / in-memory ORM object construction -- no DB connection.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.fixtures.generators import conflicts as conflicts_gen
from dev_health_ops.fixtures.generators import projects as projects_gen
from dev_health_ops.fixtures.generators import retention_conversations as conv_gen
from dev_health_ops.fixtures.generators import source_health

_NOW = datetime(2026, 8, 5, tzinfo=timezone.utc)


class TestProjects:
    def test_project_id_equals_repo_full_name(self) -> None:
        assert (
            projects_gen.project_id_for_repo("meridian/web-app") == "meridian/web-app"
        )

    def test_build_project_record_defaults_name_to_repo(self) -> None:
        record = projects_gen.build_project_record(
            org_id="org-1", repo_full_name="meridian/web-app", as_of=_NOW
        )
        assert record.name == "meridian/web-app"
        assert record.provider == "synthetic"
        assert record.is_active is True

    def test_build_project_record_display_name_override(self) -> None:
        record = projects_gen.build_project_record(
            org_id="org-1",
            repo_full_name="meridian/web-app",
            display_name="Meridian Web Application (MWA)",
            as_of=_NOW,
        )
        assert record.name == "Meridian Web Application (MWA)"
        assert record.id == "meridian/web-app"

    def test_retired_version_flips_is_active_and_keeps_id(self) -> None:
        active = projects_gen.build_project_record(
            org_id="org-1", repo_full_name="legacy/billing", as_of=_NOW
        )
        retired = projects_gen.build_retired_project_version(
            active, retired_as_of=_NOW + timedelta(hours=1)
        )
        assert retired.id == active.id
        assert retired.org_id == active.org_id
        assert retired.is_active is False
        assert retired.updated_at is not None
        assert active.updated_at is not None
        assert retired.updated_at > active.updated_at

    def test_retired_version_requires_strictly_later_timestamp(self) -> None:
        active = projects_gen.build_project_record(
            org_id="org-1", repo_full_name="legacy/billing", as_of=_NOW
        )
        with pytest.raises(ValueError, match="strictly after"):
            projects_gen.build_retired_project_version(active, retired_as_of=_NOW)

    @pytest.mark.asyncio
    async def test_insert_projects_no_records_is_a_noop(self) -> None:
        class _MustNotInsert:
            def insert(self, *args, **kwargs):
                raise AssertionError("insert must not be called for an empty list")

        await projects_gen.insert_projects(_MustNotInsert(), [])

    @pytest.mark.asyncio
    async def test_insert_projects_calls_client_insert_with_column_names(self) -> None:
        calls = []

        class _StubClient:
            def insert(self, table, matrix, column_names):
                calls.append((table, matrix, column_names))

        record = projects_gen.build_project_record(
            org_id="org-1", repo_full_name="meridian/web-app", as_of=_NOW
        )
        await projects_gen.insert_projects(_StubClient(), [record])
        assert len(calls) == 1
        table, matrix, column_names = calls[0]
        assert table == "projects"
        assert len(matrix) == 1
        assert column_names == list(projects_gen._PROJECT_COLUMNS)


class TestSourceHealth:
    def test_provider_supports_source_incidents(self) -> None:
        assert "pagerduty" in source_health.PROVIDER_SUPPORTS_SOURCE["incidents"]
        assert "github" not in source_health.PROVIDER_SUPPORTS_SOURCE["incidents"]

    def test_provider_supports_source_commits(self) -> None:
        assert "local" in source_health.PROVIDER_SUPPORTS_SOURCE["commits"]
        assert "pagerduty" not in source_health.PROVIDER_SUPPORTS_SOURCE["commits"]

    def test_build_sync_configuration_unconfigured_returns_none(self) -> None:
        spec = source_health.SyncConfigurationSpec(
            org_id="org-1", provider="pagerduty", state="unconfigured"
        )
        assert source_health.build_sync_configuration(spec) is None

    def test_build_sync_configuration_active_success(self) -> None:
        spec = source_health.SyncConfigurationSpec(
            org_id="org-1", provider="local", state="active_success", last_sync_at=_NOW
        )
        config = source_health.build_sync_configuration(spec)
        assert config is not None
        assert config.is_active is True
        assert config.last_sync_success is True
        assert config.last_sync_error is None

    def test_build_sync_configuration_active_failure(self) -> None:
        spec = source_health.SyncConfigurationSpec(
            org_id="org-1", provider="local", state="active_failure", last_sync_at=_NOW
        )
        config = source_health.build_sync_configuration(spec)
        assert config is not None
        assert config.last_sync_success is False
        assert config.last_sync_error is not None

    def test_build_sync_configurations_for_org_skips_unconfigured(self) -> None:
        configs = source_health.build_sync_configurations_for_org(
            "org-1",
            {
                "local": "active_success",
                "jira": "active_success",
                "pagerduty": "unconfigured",
            },
            as_of=_NOW,
        )
        providers = {c.provider for c in configs}
        assert providers == {"local", "jira"}

    def test_sources_configured_for_org(self) -> None:
        configured = source_health.sources_configured_for_org(
            {
                "local": "active_success",
                "jira": "active_success",
                "pagerduty": "unconfigured",
            }
        )
        assert "incidents" not in configured
        assert "commits" in configured
        assert "work_items" in configured

    def test_sources_configured_for_org_empty_when_nothing_configured(self) -> None:
        configured = source_health.sources_configured_for_org(
            {
                "local": "unconfigured",
                "jira": "unconfigured",
                "pagerduty": "unconfigured",
            }
        )
        assert configured == set()


class TestConflicts:
    def test_build_conflicting_ci_runs_distinct_run_ids(self) -> None:
        pair = conflicts_gen.build_conflicting_ci_runs(
            repo_id=uuid.uuid4(), org_id="org-1", as_of=_NOW, seed_label="probe"
        )
        assert pair.run_id_success != pair.run_id_failed

    def test_to_postgres_ci_pipeline_runs_opposite_status(self) -> None:
        pair = conflicts_gen.build_conflicting_ci_runs(
            repo_id=uuid.uuid4(), org_id="org-1", as_of=_NOW, seed_label="probe"
        )
        runs = conflicts_gen.to_postgres_ci_pipeline_runs(pair)
        statuses = {r.status for r in runs}
        assert statuses == {"success", "failed"}
        assert all(r.repo_id == pair.repo_id for r in runs)

    def test_to_clickhouse_extended_rows_shape(self) -> None:
        pair = conflicts_gen.build_conflicting_ci_runs(
            repo_id=uuid.uuid4(), org_id="org-1", as_of=_NOW, seed_label="probe"
        )
        rows = conflicts_gen.to_clickhouse_extended_rows(
            pair, team_id="team-1", service_id="api-gateway"
        )
        assert len(rows) == 2
        assert {r["status"] for r in rows} == {"success", "failed"}
        assert all(r["org_id"] == "org-1" for r in rows)
        assert all(r["repo_id"] == pair.repo_id for r in rows)

    def test_conflicting_runs_share_repo_and_close_queued_window(self) -> None:
        pair = conflicts_gen.build_conflicting_ci_runs(
            repo_id=uuid.uuid4(), org_id="org-1", as_of=_NOW, seed_label="probe"
        )
        runs = conflicts_gen.to_postgres_ci_pipeline_runs(pair)
        queued = sorted(r.queued_at for r in runs)
        assert queued[1] - queued[0] <= timedelta(minutes=10)


class TestRetentionConversations:
    def test_retention_aged_conversation_created_at_matches_age(self) -> None:
        bundle = conv_gen.build_retention_aged_conversation(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="probe",
            retention_days=30,
            age_days=40,
            pinned_now=_NOW,
            title="probe",
        )
        assert bundle.conversation.created_at == _NOW - timedelta(days=40)
        assert (
            bundle.conversation.expires_at
            == bundle.conversation.created_at + timedelta(days=30)
        )

    def test_retention_zero_days_has_no_expiry(self) -> None:
        bundle = conv_gen.build_retention_aged_conversation(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="probe",
            retention_days=0,
            age_days=1,
            pinned_now=_NOW,
            title="probe",
        )
        assert bundle.conversation.expires_at is None
        assert bundle.conversation.retention_days == 0

    def test_deterministic_ids_across_calls(self) -> None:
        org_id = uuid.uuid4()
        user_id = uuid.uuid4()
        first = conv_gen.build_retention_aged_conversation(
            org_id=org_id,
            user_id=user_id,
            id_seed="probe",
            retention_days=30,
            age_days=1,
            pinned_now=_NOW,
            title="probe",
        )
        second = conv_gen.build_retention_aged_conversation(
            org_id=org_id,
            user_id=user_id,
            id_seed="probe",
            retention_days=30,
            age_days=1,
            pinned_now=_NOW,
            title="probe",
        )
        assert first.conversation.id == second.conversation.id

    def test_stale_context_bundle_has_two_runs_and_one_subject_set(self) -> None:
        bundle = conv_gen.build_stale_context_conversation(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="stale-context",
            repo_full_name="rotated/service",
            pinned_now=_NOW,
        )
        assert len(bundle.runs) == 2
        assert len(bundle.subject_sets) == 1
        early_run, late_run = bundle.runs
        assert early_run.started_at < late_run.started_at
        assert bundle.subject_sets[0].run_id == early_run.id
        assert bundle.subject_sets[0].payload["repo_full_name"] == "rotated/service"

    def test_stale_context_early_run_predates_pinned_now_by_default_window(
        self,
    ) -> None:
        bundle = conv_gen.build_stale_context_conversation(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="stale-context",
            repo_full_name="rotated/service",
            pinned_now=_NOW,
        )
        early_run = bundle.runs[0]
        assert (_NOW - early_run.started_at) >= timedelta(days=9)

    def test_validation_packet_covers_every_status(self) -> None:
        bundle = conv_gen.build_validation_packet(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="validation-packet",
            pinned_now=_NOW,
        )
        statuses = {run.grounding_validation_status for run in bundle.runs}
        assert statuses == set(conv_gen.VALIDATION_STATUSES)

    def test_validation_packet_run_ids_are_unique(self) -> None:
        bundle = conv_gen.build_validation_packet(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="validation-packet",
            pinned_now=_NOW,
        )
        run_ids = [run.id for run in bundle.runs]
        assert len(run_ids) == len(set(run_ids))

    def test_insufficient_evidence_run_has_zero_citations(self) -> None:
        bundle = conv_gen.build_validation_packet(
            org_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            id_seed="validation-packet",
            pinned_now=_NOW,
        )
        by_status = {run.grounding_validation_status: run for run in bundle.runs}
        assert by_status["insufficient_evidence"].citation_count == 0
        assert by_status["validated"].citation_count > 0
