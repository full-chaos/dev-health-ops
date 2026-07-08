from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.api.services.sync_coverage import (
    CoverageInterval,
    EffectiveScope,
    UnitWindow,
    _effective_dataset_keys_for_unit,
    _query_dataset_keys_for_scope,
    build_coverage_summary_payload,
    classify_staleness,
    ensure_utc,
    failed_ranges_not_superseded,
    merge_intervals,
    subtract_intervals,
)
from dev_health_ops.models.integrations import IntegrationSource, SyncRunUnit
from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


def _source(source_id: uuid.UUID) -> IntegrationSource:
    return IntegrationSource(
        id=source_id,
        org_id="org-1",
        integration_id=uuid.uuid4(),
        provider="github",
        source_type="repository",
        external_id="acme/repo",
        name="repo",
        full_name="acme/repo",
        metadata_={},
        is_enabled=True,
    )


def _config() -> SyncConfiguration:
    return SyncConfiguration(
        org_id="org-1",
        name="coverage",
        provider="github",
        sync_targets=["git"],
        sync_options={"schedule_cron": "0 * * * *"},
        integration_id=uuid.uuid4(),
        planner_managed=True,
    )


def _window(
    since: datetime,
    before: datetime,
    *,
    source_id: uuid.UUID,
    dataset_key: str = "commits",
    status: str = "success",
    run_time: datetime | None = None,
) -> UnitWindow:
    return UnitWindow(
        since=since,
        before=before,
        source_id=str(source_id),
        dataset_key=dataset_key,
        run_id=str(uuid.uuid4()),
        status=status,
        run_time=run_time or before,
    )


def _summary(
    windows: list[UnitWindow],
    *,
    backfill_requested: list[CoverageInterval] | None = None,
    active_pairs: set[tuple[str, str]] | None = None,
    now: datetime = _dt(2, 1),
    config: SyncConfiguration | None = None,
    scope: EffectiveScope | None = None,
) -> dict:
    source_id = uuid.UUID(windows[0].source_id) if windows else uuid.uuid4()
    config = config or _config()
    scope = scope or EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_id),),
        dataset_keys=("commits",),
    )
    schedule = ScheduledJob(
        org_id="org-1",
        name="sync-coverage",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        sync_config_id=config.id,
    )
    return build_coverage_summary_payload(
        config=config,
        scope=scope,
        windows=windows,
        backfill_requested=backfill_requested or [],
        active_pairs=active_pairs or set(),
        active_schedule=schedule,
        has_schedule_row=True,
        generated_at=now,
    )


_WORK_ITEM_FAMILY_PROVIDERS = ("jira", "gitlab", "github", "linear")


def _composite_unit(
    *,
    provider: str = "github",
    dataset_key: str = "work-items",
    processor_flags: dict[str, bool] | None = None,
    source_id: uuid.UUID | None = None,
    status: str = "success",
) -> SyncRunUnit:
    return SyncRunUnit(
        org_id="org-1",
        sync_run_id=uuid.uuid4(),
        integration_id=uuid.uuid4(),
        source_id=source_id or uuid.uuid4(),
        provider=provider,
        dataset_key=dataset_key,
        cost_class="standard",
        mode="incremental",
        status=status,
        attempts=1,
        processor_flags=processor_flags,
    )


def _expand_unit_to_windows(
    unit: SyncRunUnit,
    *,
    since: datetime,
    before: datetime,
    run_time: datetime,
) -> list[UnitWindow]:
    """Mirror the terminal-window expansion: one ``UnitWindow`` per effective
    dataset key decoded from the unit's work-item-family flags (CHAOS-2721)."""
    return [
        UnitWindow(
            since=since,
            before=before,
            source_id=str(unit.source_id),
            dataset_key=effective_key,
            run_id=str(unit.sync_run_id),
            status=str(unit.status),
            run_time=run_time,
        )
        for effective_key in _effective_dataset_keys_for_unit(unit)
    ]


def test_merge_intervals_collapses_overlap_and_adjacency():
    merged = merge_intervals(
        [
            CoverageInterval(_dt(1), _dt(2), source_ids=("a",)),
            CoverageInterval(_dt(2), _dt(3), source_ids=("b",)),
            CoverageInterval(_dt(5), _dt(6)),
        ]
    )

    assert [(item.since, item.before) for item in merged] == [
        (_dt(1), _dt(3)),
        (_dt(5), _dt(6)),
    ]
    assert merged[0].source_ids == ("a", "b")


def test_merge_intervals_drops_zero_duration_and_same_instant_ranges():
    instant = _dt(1)

    assert merge_intervals([CoverageInterval(instant, instant)]) == []


def test_ensure_utc_marks_naive_datetimes_as_utc():
    naive = datetime(2026, 1, 1, 12, 30)

    assert ensure_utc(naive) == datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)


def test_subtract_intervals_returns_partial_gap():
    gaps = subtract_intervals(
        [CoverageInterval(_dt(1), _dt(4))],
        [CoverageInterval(_dt(1), _dt(2)), CoverageInterval(_dt(3), _dt(4))],
    )

    assert [(gap.since, gap.before) for gap in gaps] == [(_dt(2), _dt(3))]


def test_complete_summary_is_healthy():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(2), source_id=source_id)])

    assert summary["overall"]["health"] == "healthy"
    assert summary["datasets"][0]["status"] == "healthy"
    assert summary["datasets"][0]["gaps"] == []


def test_failed_window_without_later_success_marks_failed():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(2), source_id=source_id, status="failed")])

    assert summary["overall"]["health"] == "failed"
    assert summary["overall"]["failed_range_count"] == 1
    assert summary["datasets"][0]["failed_ranges"][0]["since"] == _dt(1)


def test_backfill_intent_exposes_gap_without_failed_unit():
    source_id = uuid.uuid4()
    summary = _summary(
        [_window(_dt(1), _dt(2), source_id=source_id)],
        backfill_requested=[
            CoverageInterval(_dt(1), _dt(3), source_ids=(str(source_id),))
        ],
    )

    assert summary["overall"]["health"] == "gaps"
    assert [
        (gap["since"], gap["before"]) for gap in summary["datasets"][0]["gaps"]
    ] == [(_dt(2), _dt(3))]
    assert summary["sources"][0]["gap_count"] == 1


def test_dataset_gaps_are_computed_per_source_before_rollup():
    source_a = uuid.uuid4()
    source_b = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_a), _source(source_b)),
        dataset_keys=("commits",),
    )

    summary = _summary(
        [
            _window(_dt(1), _dt(2), source_id=source_a, status="success"),
            _window(_dt(1), _dt(2), source_id=source_b, status="planned"),
        ],
        config=config,
        scope=scope,
    )

    dataset = summary["datasets"][0]
    assert dataset["status"] == "gaps"
    assert [
        (gap["since"], gap["before"], gap["source_ids"]) for gap in dataset["gaps"]
    ] == [(_dt(1), _dt(2), [str(source_b)])]
    assert summary["sources"][0]["gap_count"] == 0
    assert summary["sources"][1]["gap_count"] == 1


def test_planned_units_contribute_requested_intent_only():
    source_id = uuid.uuid4()

    summary = _summary([_window(_dt(1), _dt(2), source_id=source_id, status="planned")])

    dataset = summary["datasets"][0]
    assert dataset["requested_ranges"]
    assert dataset["covered_ranges"] == []
    assert dataset["gaps"]
    assert summary["overall"]["health"] == "gaps"


def test_active_runs_only_mark_touched_pairs_running():
    source_a = uuid.uuid4()
    source_b = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_a), _source(source_b)),
        dataset_keys=("commits",),
    )

    summary = _summary(
        [
            _window(_dt(1), _dt(2), source_id=source_a, status="success"),
            _window(_dt(1), _dt(2), source_id=source_b, status="success"),
        ],
        active_pairs={(str(source_a), "commits")},
        now=_dt(5),
        config=config,
        scope=scope,
    )

    sources = {source["source_id"]: source for source in summary["sources"]}
    assert sources[str(source_a)]["status"] == "running"
    assert sources[str(source_b)]["status"] == "stale"


def test_stale_classification_uses_schedule_grace():
    stale = classify_staleness(_dt(1), now=_dt(3), schedule_interval=timedelta(hours=1))

    assert stale.status == "stale"


def test_empty_legacy_summary_is_insufficient_data():
    config = _config()
    config.integration_id = None
    scope = EffectiveScope(integration_id=None, sources=(), dataset_keys=("commits",))
    summary = _summary([], config=config, scope=scope)

    assert summary["data_basis"] == "legacy"
    assert summary["overall"]["health"] == "insufficient_data"
    assert summary["datasets"][0]["status"] == "insufficient_data"


def test_zero_run_planner_summary_keeps_planner_data_basis():
    summary = _summary([])

    assert summary["data_basis"] == "planner"
    assert summary["overall"]["health"] == "insufficient_data"


def test_failed_retry_superseded_by_later_success_is_healthy():
    source_id = uuid.uuid4()
    failed = _window(
        _dt(1), _dt(2), source_id=source_id, status="failed", run_time=_dt(2)
    )
    success = _window(
        _dt(1), _dt(2), source_id=source_id, status="success", run_time=_dt(3)
    )

    assert failed_ranges_not_superseded([failed], [success]) == []
    assert _summary([failed, success])["overall"]["health"] == "healthy"


def test_full_resync_window_can_cover_entire_requested_range():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(31), source_id=source_id)])

    dataset = summary["datasets"][0]
    assert dataset["covered_ranges"][0]["since"] == _dt(1)
    assert dataset["covered_ranges"][0]["before"] == _dt(31)
    assert summary["overall"]["health"] == "healthy"


def test_backfill_interval_pair_scope_excludes_untouched_pairs():
    # CHAOS-2869 core repro: a backfill's requested interval is scoped to
    # exactly the (source_id, dataset_key) pairs its linked SyncRun planned
    # units for. A pair the run never touched must not inherit a permanent
    # requested gap from that job.
    source_a = uuid.uuid4()
    source_b = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_a), _source(source_b)),
        dataset_keys=("commits",),
    )

    summary = _summary(
        [],
        backfill_requested=[
            CoverageInterval(
                _dt(1),
                _dt(3),
                source_ids=(str(source_a),),
                dataset_keys=("commits",),
            )
        ],
        config=config,
        scope=scope,
    )

    dataset = summary["datasets"][0]
    assert [
        (gap["since"], gap["before"], gap["source_ids"]) for gap in dataset["gaps"]
    ] == [(_dt(1), _dt(3), [str(source_a)])]
    sources = {source["source_id"]: source for source in summary["sources"]}
    assert sources[str(source_a)]["status"] == "gaps"
    assert sources[str(source_b)]["gap_count"] == 0
    assert sources[str(source_b)]["status"] == "insufficient_data"


def test_backfill_interval_without_pair_scope_applies_to_all_scope_pairs():
    # Legacy/unresolved-marker fallback: an interval with no dataset_keys
    # (and no source_ids) still spreads across every pair in scope, matching
    # pre-fix behavior for backfill jobs whose linked SyncRun can't be
    # resolved.
    source_a = uuid.uuid4()
    source_b = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_a), _source(source_b)),
        dataset_keys=("commits",),
    )

    summary = _summary(
        [],
        backfill_requested=[CoverageInterval(_dt(1), _dt(3))],
        config=config,
        scope=scope,
    )

    sources = {source["source_id"]: source for source in summary["sources"]}
    assert sources[str(source_a)]["gap_count"] == 1
    assert sources[str(source_b)]["gap_count"] == 1


@pytest.mark.parametrize("provider", _WORK_ITEM_FAMILY_PROVIDERS)
def test_effective_dataset_keys_expands_true_family_flags_in_canonical_order(
    provider,
):
    # CHAOS-2721/coverage-fix: a collapsed composite unit's true family flags
    # decode to only the enabled child keys, in canonical order -- never every
    # work-item-family key regardless of which flags are set.
    unit = _composite_unit(
        provider=provider,
        processor_flags={
            "family_dataset_work_item_comments": True,
            "family_dataset_work_item_labels": True,
        },
    )

    assert list(_effective_dataset_keys_for_unit(unit)) == [
        "work-item-labels",
        "work-item-comments",
    ]


@pytest.mark.parametrize("provider", _WORK_ITEM_FAMILY_PROVIDERS)
def test_effective_dataset_keys_falls_back_to_raw_key_when_no_family_flag_true(
    provider,
):
    unit = _composite_unit(
        provider=provider,
        processor_flags={"family_dataset_work_item_comments": False},
    )

    assert list(_effective_dataset_keys_for_unit(unit)) == ["work-items"]


def test_effective_dataset_keys_missing_flags_falls_back_to_raw_key():
    unit = _composite_unit(processor_flags=None)

    assert list(_effective_dataset_keys_for_unit(unit)) == ["work-items"]


def test_effective_dataset_keys_ignores_unknown_flags():
    # An unrecognized family_dataset_* flag must never advance coverage for a
    # dataset key that isn't in the canonical work-item-family set.
    unit = _composite_unit(processor_flags={"family_dataset_bogus": True})

    assert list(_effective_dataset_keys_for_unit(unit)) == ["work-items"]


def test_effective_dataset_keys_non_family_dataset_ignores_flags():
    # A raw, non-composite dataset_key must never be expanded even if stray
    # family_dataset_* flags are present on its processor_flags.
    unit = _composite_unit(
        dataset_key="commits",
        processor_flags={"family_dataset_work_item_comments": True},
    )

    assert list(_effective_dataset_keys_for_unit(unit)) == ["commits"]


def test_query_dataset_keys_for_scope_includes_canonical_work_items_for_family_child():
    keys = _query_dataset_keys_for_scope(("work-item-comments",))

    assert "work-items" in keys
    assert "work-item-comments" in keys


def test_query_dataset_keys_for_scope_unchanged_for_non_family_scope():
    keys = _query_dataset_keys_for_scope(("commits", "prs"))

    assert set(keys) == {"commits", "prs"}


@pytest.mark.parametrize("provider", _WORK_ITEM_FAMILY_PROVIDERS)
def test_composite_success_supersedes_old_failed_child_only_when_flag_true(
    provider,
):
    # CHAOS-2721/coverage-fix core repro: a later successful composite
    # work-items unit must supersede an old failed child dataset ONLY for the
    # child whose family_dataset_* flag was true on that composite run. A
    # disabled child dataset must remain failed/gapped.
    source_id = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_id),),
        dataset_keys=("work-item-comments", "work-item-labels"),
    )

    old_failed_comments = _window(
        _dt(1),
        _dt(2),
        source_id=source_id,
        dataset_key="work-item-comments",
        status="failed",
        run_time=_dt(2),
    )
    old_failed_labels = _window(
        _dt(1),
        _dt(2),
        source_id=source_id,
        dataset_key="work-item-labels",
        status="failed",
        run_time=_dt(2),
    )
    composite = _composite_unit(
        provider=provider,
        source_id=source_id,
        processor_flags={"family_dataset_work_item_comments": True},
        status="success",
    )
    later_success_windows = _expand_unit_to_windows(
        composite, since=_dt(1), before=_dt(2), run_time=_dt(3)
    )

    summary = _summary(
        [old_failed_comments, old_failed_labels, *later_success_windows],
        config=config,
        scope=scope,
    )

    datasets = {dataset["dataset_key"]: dataset for dataset in summary["datasets"]}
    assert datasets["work-item-comments"]["failed_ranges"] == []
    assert datasets["work-item-comments"]["status"] == "healthy"
    assert datasets["work-item-labels"]["failed_ranges"] != []
    assert datasets["work-item-labels"]["status"] == "failed"


@pytest.mark.parametrize("provider", _WORK_ITEM_FAMILY_PROVIDERS)
def test_active_composite_run_marks_only_flagged_child_pair_running(provider):
    # An in-flight composite work-items unit must mark ONLY the flagged
    # child dataset pair as running; an unflagged child pair falls back to
    # the ordinary stale/gap/insufficient_data rollup rules.
    source_id = uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_id),),
        dataset_keys=("work-item-comments", "work-item-labels"),
    )
    comments_window = _window(
        _dt(1),
        _dt(2),
        source_id=source_id,
        dataset_key="work-item-comments",
        status="success",
    )
    labels_window = _window(
        _dt(1),
        _dt(2),
        source_id=source_id,
        dataset_key="work-item-labels",
        status="success",
    )
    running_composite = _composite_unit(
        provider=provider,
        source_id=source_id,
        processor_flags={"family_dataset_work_item_comments": True},
        status="running",
    )
    active_pairs = {
        (str(source_id), key)
        for key in _effective_dataset_keys_for_unit(running_composite)
    }

    summary = _summary(
        [comments_window, labels_window],
        active_pairs=active_pairs,
        now=_dt(5),
        config=config,
        scope=scope,
    )

    datasets = {dataset["dataset_key"]: dataset for dataset in summary["datasets"]}
    assert datasets["work-item-comments"]["status"] == "running"
    assert datasets["work-item-labels"]["status"] != "running"
