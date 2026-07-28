from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    MetricID,
)
from dev_health_ops.api.dev.metrics.definitions import get_metric, list_metrics
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryRequest,
    MetricQueryService,
    MetricSourceRef,
    MetricSourceState,
    RawMetricResult,
    RawMetricRow,
)

UTC = timezone.utc


def _scope(
    *,
    start: datetime = datetime(2026, 7, 1, tzinfo=UTC),
    end: datetime = datetime(2026, 7, 8, tzinfo=UTC),
) -> DevScope:
    duration = end - start
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-1",
        direct_scope=DirectScope.ORGANIZATION,
        time_range=DevTimeRange(start=start, end=end, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=start - duration,
            end=start,
            timezone="UTC",
        ),
    )


class _Source:
    def __init__(self, current: RawMetricResult, prior: RawMetricResult | None = None):
        self.current = current
        self.prior = prior or current
        self.calls = 0

    async def watermark(self, org_id, definition, scope):
        return f"{org_id}:{definition.query_version}:{self.current.watermark}"

    async def query(
        self,
        org_id,
        definition,
        scope,
        *,
        comparison,
        include_series,
        max_series_points,
    ):
        self.calls += 1
        return self.prior if comparison else self.current


def _raw(
    value: float | None,
    *,
    latest_day: date | None = date(2026, 7, 7),
    state: MetricSourceState = MetricSourceState.AVAILABLE,
    metadata: tuple[tuple[str, str], ...] = (),
) -> RawMetricResult:
    watermark = (
        datetime.combine(latest_day, datetime.min.time(), tzinfo=UTC)
        if latest_day
        else None
    )
    return RawMetricResult(
        rows=(RawMetricRow(dimensions=(), value=value),) if value is not None else (),
        watermark=watermark,
        latest_materialized_day=latest_day,
        source_state=state,
        covered_days=7 if latest_day else 0,
        expected_days=7,
        source_refs=(
            MetricSourceRef(
                ref_id="metric-source:test",
                source_table="deploy_metrics_daily",
                source_version="deploy_metrics_daily.v1",
                watermark=watermark,
                query_version="query-change_failure_rate.v1",
            ),
        )
        if watermark
        else (),
        metadata=metadata,
    )


def test_registry_is_exactly_the_eight_authorized_ids() -> None:
    assert tuple(item.metric_id for item in list_metrics()) == tuple(MetricID)
    assert len(list_metrics()) == 8
    with pytest.raises(ValueError, match="Unsupported Ask Dev V1 metric"):
        get_metric("lead_time_p50_hours")


def test_metric_05_registry_uses_deployment_weighted_ratio_only() -> None:
    definition = get_metric(MetricID.CHANGE_FAILURE_RATE)
    assert definition.source_table == "deploy_metrics_daily"
    assert definition.aggregation == (
        "sum_failed_deployments_divided_by_sum_deployments"
    )
    assert "repo_metrics_daily" not in definition.description


@pytest.mark.parametrize(
    ("metric_id", "value", "prior", "unit", "aggregation", "precision"),
    [
        pytest.param(
            MetricID.ITEMS_COMPLETED,
            18.0,
            12.0,
            "items",
            "sum_latest_daily_scope_rows",
            0,
            id="METRIC-01",
        ),
        pytest.param(
            MetricID.CYCLE_TIME_P50_HOURS,
            22.5,
            25.0,
            "hours",
            "average_latest_daily_scope_p50",
            1,
            id="METRIC-02",
        ),
        pytest.param(
            MetricID.AVG_WIP,
            4.25,
            5.0,
            "items",
            "average_latest_daily_status_snapshots",
            1,
            id="METRIC-03",
        ),
        pytest.param(
            MetricID.DEPLOYMENTS_COUNT,
            10.0,
            8.0,
            "deployments",
            "sum_latest_daily_repository_rows",
            0,
            id="METRIC-04",
        ),
        pytest.param(
            MetricID.CHANGE_FAILURE_RATE,
            0.2,
            0.125,
            "ratio",
            "sum_failed_deployments_divided_by_sum_deployments",
            3,
            id="METRIC-05",
        ),
        pytest.param(
            MetricID.INVESTMENT_ALLOCATION_PCT,
            40.0,
            35.0,
            "percent",
            "canonical_theme_completed_work_share",
            1,
            id="METRIC-06",
        ),
        pytest.param(
            MetricID.CYCLOMATIC_PER_KLOC,
            14.25,
            13.5,
            "cyclomatic_per_kloc",
            "average_latest_daily_repository_density",
            2,
            id="METRIC-07",
        ),
        pytest.param(
            MetricID.COMPOUNDING_RISK_SCORE,
            0.62,
            0.55,
            "score_0_to_1",
            "latest_snapshot_mean_across_scope",
            2,
            id="METRIC-08",
        ),
    ],
)
@pytest.mark.asyncio
async def test_metric_corpus_exact_values_windows_and_versions(
    metric_id,
    value,
    prior,
    unit,
    aggregation,
    precision,
) -> None:
    source = _Source(_raw(value), _raw(prior, latest_day=date(2026, 6, 30)))
    result = await MetricQueryService(source).query(
        "org-1",
        "permissions",
        MetricQueryRequest(metric_id, _scope()),
        now=datetime(2026, 7, 8, 12, tzinfo=UTC),
    )
    assert result.values[0].value == pytest.approx(value)
    assert result.values[0].comparison_value == pytest.approx(prior)
    assert result.definition.unit == unit
    assert result.definition.aggregation == aggregation
    assert result.definition.display_precision == precision
    assert result.definition.definition_version == f"{metric_id.value}.v1"
    assert result.definition.query_version == f"query-{metric_id.value}.v1"
    assert result.current_window_start == datetime(2026, 7, 1, tzinfo=UTC)
    assert result.current_window_end == datetime(2026, 7, 8, tzinfo=UTC)
    assert result.comparison_window_start == datetime(2026, 6, 24, tzinfo=UTC)
    assert result.comparison_window_end == datetime(2026, 7, 1, tzinfo=UTC)


@pytest.mark.parametrize("metric_id", list(MetricID))
@pytest.mark.asyncio
async def test_every_metric_distinguishes_zero_from_no_match(metric_id) -> None:
    now = datetime(2026, 7, 8, 12, tzinfo=UTC)
    zero = await MetricQueryService(_Source(_raw(0.0))).query(
        "org-1",
        "permissions",
        MetricQueryRequest(metric_id, _scope(), include_comparison=False),
        now=now,
    )
    missing = await MetricQueryService(_Source(_raw(None))).query(
        "org-1",
        "permissions",
        MetricQueryRequest(metric_id, _scope(), include_comparison=False),
        now=now,
    )
    assert zero.state is MetricDataState.ZERO
    assert zero.values[0].value == 0.0
    assert missing.state is MetricDataState.NO_MATCH
    assert missing.values == ()


@pytest.mark.asyncio
async def test_completed_day_watermark_controls_staleness() -> None:
    service = MetricQueryService(_Source(_raw(3.0, latest_day=date(2026, 7, 6))))
    result = await service.query(
        "org-1",
        "permissions",
        MetricQueryRequest(MetricID.DEPLOYMENTS_COUNT, _scope()),
        now=datetime(2026, 7, 9, 12, tzinfo=UTC),
    )
    assert result.state is MetricDataState.STALE
    assert result.warnings == ("source_stale",)


@pytest.mark.asyncio
async def test_open_utc_day_is_partial_not_stale() -> None:
    now = datetime(2026, 7, 8, 12, tzinfo=UTC)
    scope = _scope(
        start=datetime(2026, 7, 2, tzinfo=UTC),
        end=datetime(2026, 7, 9, tzinfo=UTC),
    )
    service = MetricQueryService(_Source(_raw(3.0, latest_day=date(2026, 7, 7))))
    result = await service.query(
        "org-1",
        "permissions",
        MetricQueryRequest(MetricID.DEPLOYMENTS_COUNT, scope),
        now=now,
    )
    assert result.state is MetricDataState.PARTIAL
    assert result.warnings == ("open_utc_day_partial",)


@pytest.mark.asyncio
async def test_zero_denominator_is_insufficient_not_zero() -> None:
    source = _Source(
        _raw(
            None,
            metadata=(
                ("deployments", "0"),
                ("failed_deployments", "0"),
                ("empty_reason", "zero_denominator"),
            ),
        )
    )
    result = await MetricQueryService(source).query(
        "org-1",
        "permissions",
        MetricQueryRequest(MetricID.CHANGE_FAILURE_RATE, _scope()),
        now=datetime(2026, 7, 8, 12, tzinfo=UTC),
    )
    assert result.state is MetricDataState.INSUFFICIENT_EVIDENCE
    assert result.values == ()


@pytest.mark.parametrize(
    ("source_state", "expected_state"),
    [
        (MetricSourceState.UNCONFIGURED, MetricDataState.UNCONFIGURED),
        (MetricSourceState.UNAVAILABLE, MetricDataState.UNAVAILABLE),
    ],
)
@pytest.mark.asyncio
async def test_source_configuration_states_are_not_collapsed_to_no_match(
    source_state,
    expected_state,
) -> None:
    result = await MetricQueryService(
        _Source(_raw(None, latest_day=None, state=source_state))
    ).query(
        "org-1",
        "permissions",
        MetricQueryRequest(
            MetricID.ITEMS_COMPLETED, _scope(), include_comparison=False
        ),
    )

    assert result.state is expected_state
    assert result.values == ()


@pytest.mark.asyncio
async def test_request_local_cache_includes_source_watermark() -> None:
    source = _Source(_raw(4.0))
    service = MetricQueryService(source)
    request = MetricQueryRequest(MetricID.DEPLOYMENTS_COUNT, _scope())
    await service.query("org-1", "permissions", request)
    await service.query("org-1", "permissions", request)
    assert source.calls == 2  # current + prior, second query is cache hit


@pytest.mark.asyncio
async def test_unsupported_dimension_and_oversized_range_fail_before_query() -> None:
    source = _Source(_raw(1.0))
    service = MetricQueryService(source)
    with pytest.raises(ValueError, match="does not support the requested dimension"):
        await service.query(
            "org-1",
            "permissions",
            MetricQueryRequest(
                MetricID.DEPLOYMENTS_COUNT,
                _scope(),
                dimensions=("repository",),
            ),
        )
    with pytest.raises(ValueError, match="time range is out of bounds"):
        await service.query(
            "org-1",
            "permissions",
            MetricQueryRequest(
                MetricID.DEPLOYMENTS_COUNT,
                _scope(
                    start=datetime(2025, 1, 1, tzinfo=UTC),
                    end=datetime(2026, 7, 8, tzinfo=UTC),
                ),
            ),
        )
    assert source.calls == 0


@pytest.mark.asyncio
async def test_repository_backed_metric_rejects_unresolved_project_repository() -> None:
    source = _Source(_raw(1.0))
    scope = _scope().model_copy(
        update={
            "direct_scope": DirectScope.PROJECT,
            "entity_refs": [
                DevEntityRef(
                    entity_type=EntityType.PROJECT,
                    entity_id="project-1",
                    display_label="Project 1",
                )
            ],
        }
    )

    with pytest.raises(ValueError, match="does not resolve to a repository"):
        await MetricQueryService(source).query(
            "org-1",
            "permissions",
            MetricQueryRequest(MetricID.CYCLOMATIC_PER_KLOC, scope),
        )
    assert source.calls == 0
