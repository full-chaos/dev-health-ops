"""
CIMixin — CI/CD, deploy, incident, testops pipeline/test/coverage,
release confidence, feature flags, telemetry, and release impact write methods.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import (
    CICDMetricsDailyRecord,
    DeployMetricsDailyRecord,
    FeatureFlagEventRecord,
    FeatureFlagLinkRecord,
    FeatureFlagRecord,
    IncidentMetricsDailyRecord,
    ReleaseImpactDailyRecord,
    TelemetrySignalBucketRecord,
)
from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class CIMixin(_ClickHouseSinkBase):
    """Mixin for CI/CD and pipeline-related write methods."""

    def write_cicd_metrics(self, rows: Sequence[CICDMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "cicd_metrics_daily",
            [
                "repo_id",
                "day",
                "pipelines_count",
                "success_rate",
                "avg_duration_minutes",
                "p90_duration_minutes",
                "avg_queue_minutes",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_deploy_metrics(self, rows: Sequence[DeployMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "deploy_metrics_daily",
            [
                "repo_id",
                "day",
                "deployments_count",
                "failed_deployments_count",
                "deploy_time_p50_hours",
                "lead_time_p50_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_incident_metrics(
        self, rows: Sequence[IncidentMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "incident_metrics_daily",
            [
                "repo_id",
                "day",
                "incidents_count",
                "mttr_p50_hours",
                "mttr_p90_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    # CHAOS-5245 deleted write_testops_pipeline_metrics/write_testops_test_metrics/
    # write_testops_coverage_metrics/write_release_confidence -- their Python
    # compute (compute_testops.py/compute_testops_risk.py) is gone entirely,
    # the native Go executors (CHAOS-4284/CHAOS-4294) have no fallback left.

    def write_feature_flags(self, rows: Sequence[FeatureFlagRecord]) -> None:
        if not rows:
            return
        columns = [
            "org_id",
            "provider",
            "flag_key",
            "project_key",
            "repo_id",
            "environment",
            "flag_type",
            "created_at",
            "archived_at",
            "last_synced",
        ]
        _org = getattr(self, "org_id", "") or ""
        matrix = []
        for r in rows:
            matrix.append(
                [
                    r.org_id or _org,
                    r.provider,
                    r.flag_key,
                    r.project_key or "",
                    str(r.repo_id) if r.repo_id else "",
                    r.environment,
                    r.flag_type or "",
                    _dt_to_clickhouse_datetime(r.created_at) if r.created_at else None,
                    _dt_to_clickhouse_datetime(r.archived_at)
                    if r.archived_at
                    else None,
                    _dt_to_clickhouse_datetime(r.last_synced),
                ]
            )
        for chunk in _chunked(matrix, DEFAULT_BATCH_SIZE):
            self.client.insert("feature_flag", chunk, column_names=columns)

    def write_feature_flag_events(self, rows: Sequence[FeatureFlagEventRecord]) -> None:
        if not rows:
            return
        columns = [
            "org_id",
            "event_type",
            "flag_key",
            "environment",
            "repo_id",
            "actor_type",
            "prev_state",
            "next_state",
            "event_ts",
            "ingested_at",
            "source_event_id",
            "dedupe_key",
        ]
        _org = getattr(self, "org_id", "") or ""
        matrix = []
        for r in rows:
            matrix.append(
                [
                    r.org_id or _org,
                    r.event_type,
                    r.flag_key,
                    r.environment,
                    str(r.repo_id) if r.repo_id else "",
                    r.actor_type or "",
                    r.prev_state or "",
                    r.next_state or "",
                    _dt_to_clickhouse_datetime(r.event_ts),
                    _dt_to_clickhouse_datetime(r.ingested_at),
                    r.source_event_id or "",
                    r.dedupe_key,
                ]
            )
        for chunk in _chunked(matrix, DEFAULT_BATCH_SIZE):
            self.client.insert("feature_flag_event", chunk, column_names=columns)

    def write_feature_flag_links(self, rows: Sequence[FeatureFlagLinkRecord]) -> None:
        if not rows:
            return
        columns = [
            "org_id",
            "flag_key",
            "target_type",
            "target_id",
            "provider",
            "link_source",
            "link_type",
            "evidence_type",
            "confidence",
            "valid_from",
            "valid_to",
            "last_synced",
        ]
        _org = getattr(self, "org_id", "") or ""
        matrix = []
        for r in rows:
            matrix.append(
                [
                    r.org_id or _org,
                    r.flag_key,
                    r.target_type,
                    r.target_id,
                    r.provider,
                    r.link_source,
                    r.link_type,
                    r.evidence_type or "",
                    r.confidence,
                    _dt_to_clickhouse_datetime(r.valid_from),
                    _dt_to_clickhouse_datetime(r.valid_to) if r.valid_to else None,
                    _dt_to_clickhouse_datetime(r.last_synced),
                ]
            )
        for chunk in _chunked(matrix, DEFAULT_BATCH_SIZE):
            self.client.insert("feature_flag_link", chunk, column_names=columns)

    def write_telemetry_signal_buckets(
        self, rows: Sequence[TelemetrySignalBucketRecord]
    ) -> None:
        if not rows:
            return
        columns = [
            "org_id",
            "signal_type",
            "signal_count",
            "session_count",
            "unique_pseudonymous_count",
            "endpoint_group",
            "environment",
            "repo_id",
            "release_ref",
            "bucket_start",
            "bucket_end",
            "ingested_at",
            "is_sampled",
            "schema_version",
            "dedupe_key",
        ]
        _org = getattr(self, "org_id", "") or ""
        matrix = []
        for r in rows:
            matrix.append(
                [
                    r.org_id or _org,
                    r.signal_type,
                    r.signal_count,
                    r.session_count,
                    r.unique_pseudonymous_count,
                    r.endpoint_group or "",
                    r.environment,
                    str(r.repo_id) if r.repo_id else "",
                    r.release_ref or "",
                    _dt_to_clickhouse_datetime(r.bucket_start),
                    _dt_to_clickhouse_datetime(r.bucket_end),
                    _dt_to_clickhouse_datetime(r.ingested_at),
                    1 if r.is_sampled else 0,
                    r.schema_version or "",
                    r.dedupe_key,
                ]
            )
        for chunk in _chunked(matrix, DEFAULT_BATCH_SIZE):
            self.client.insert("telemetry_signal_bucket", chunk, column_names=columns)

    def write_release_impact_daily(
        self, rows: Sequence[ReleaseImpactDailyRecord]
    ) -> None:
        if not rows:
            return
        columns = [
            "org_id",
            "day",
            "release_ref",
            "environment",
            "repo_id",
            "release_user_friction_delta",
            "release_post_friction_rate",
            "release_error_rate_delta",
            "release_post_error_rate",
            "time_to_first_user_issue_after_release",
            "release_impact_confidence_score",
            "release_impact_coverage_ratio",
            "flag_exposure_rate",
            "flag_activation_rate",
            "flag_reliability_guardrail",
            "flag_friction_delta",
            "flag_rollout_half_life",
            "flag_churn_rate",
            "issue_to_release_impact_link_rate",
            "rollback_or_disable_after_impact_spike",
            "coverage_ratio",
            "missing_required_fields_count",
            "instrumentation_change_flag",
            "data_completeness",
            "concurrent_deploy_count",
            "computed_at",
        ]
        _org = getattr(self, "org_id", "") or ""
        matrix = []
        for r in rows:
            matrix.append(
                [
                    r.org_id or _org,
                    r.day,
                    r.release_ref,
                    r.environment,
                    str(r.repo_id) if r.repo_id else "",
                    r.release_user_friction_delta,
                    r.release_post_friction_rate,
                    r.release_error_rate_delta,
                    r.release_post_error_rate,
                    r.time_to_first_user_issue_after_release,
                    r.release_impact_confidence_score,
                    r.release_impact_coverage_ratio,
                    r.flag_exposure_rate,
                    r.flag_activation_rate,
                    r.flag_reliability_guardrail,
                    r.flag_friction_delta,
                    r.flag_rollout_half_life,
                    r.flag_churn_rate,
                    r.issue_to_release_impact_link_rate,
                    r.rollback_or_disable_after_impact_spike,
                    r.coverage_ratio,
                    r.missing_required_fields_count,
                    1 if r.instrumentation_change_flag else 0,
                    r.data_completeness,
                    r.concurrent_deploy_count,
                    _dt_to_clickhouse_datetime(r.computed_at)
                    if r.computed_at
                    else None,
                ]
            )
        for chunk in _chunked(matrix, DEFAULT_BATCH_SIZE):
            self.client.insert("release_impact_daily", chunk, column_names=columns)
