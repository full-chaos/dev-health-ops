"""Release impact daily metric computation.

Reads from ``telemetry_signal_bucket`` and ``deployments`` tables, computes
per-release/environment impact deltas, and writes ``ReleaseImpactDailyRecord``
rows via the ClickHouse sink.

Recomputation contract (PRD §Late data):
- Always recomputes the last ``recomputation_window_days`` (default 7) days.
- Append-only: new rows with newer ``computed_at`` win via ``argMax``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.schemas import ReleaseImpactDailyRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

logger = logging.getLogger(__name__)

_MIN_SESSIONS_FRICTION = 300
_MIN_EVENTS_ERROR = 1000
_BASELINE_WINDOW_DAYS = 7
_POST_WINDOW_HOURS = 24
_SPIKE_DETECTION_HOURS = 72

_W_COVERAGE = 0.35  # PRD §release_impact_confidence_score weights
_W_SAMPLE = 0.35
_W_CONFOUND = 0.30


def _query_dicts(
    client: Any, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    """Execute a ClickHouse query and return results as list of dicts."""
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


async def compute_release_impact_daily(
    ch_client: Any,
    sink: ClickHouseMetricsSink,
    org_id: str,
    day: date,
    recomputation_window_days: int = 7,
) -> int:
    """Compute release impact metrics for a given day.

    Recomputes the last ``recomputation_window_days`` days (append-only;
    latest ``computed_at`` wins via ``argMax``).

    Returns number of records written.
    """
    computed_at = datetime.now(tz=timezone.utc)
    total_written = 0

    start_day = day - timedelta(days=recomputation_window_days - 1)
    current = start_day
    while current <= day:
        records = _compute_day(ch_client, org_id, current, computed_at)
        if records:
            sink.write_release_impact_daily(records)
            total_written += len(records)
            logger.info(
                "release_impact_daily: wrote %d records for day=%s org=%s",
                len(records),
                current.isoformat(),
                org_id,
            )
        else:
            logger.debug(
                "release_impact_daily: no data for day=%s org=%s",
                current.isoformat(),
                org_id,
            )
        current += timedelta(days=1)

    return total_written


def _compute_day(
    client: Any,
    org_id: str,
    day: date,
    computed_at: datetime,
) -> list[ReleaseImpactDailyRecord]:
    """Compute release impact records for a single day."""

    release_env_pairs = _find_release_env_pairs(client, org_id, day)
    if not release_env_pairs:
        return []

    total_releases_on_day = _count_total_releases(client, org_id, day)
    releases_with_telemetry = len({r for r, _e in release_env_pairs})

    coverage_ratio = (
        releases_with_telemetry / total_releases_on_day
        if total_releases_on_day > 0
        else 0.0
    )

    records: list[ReleaseImpactDailyRecord] = []
    for release_ref, environment in release_env_pairs:
        record = _compute_release_env(
            client=client,
            org_id=org_id,
            day=day,
            release_ref=release_ref,
            environment=environment,
            coverage_ratio=coverage_ratio,
            computed_at=computed_at,
        )
        records.append(record)

    return records


def _find_release_env_pairs(
    client: Any, org_id: str, day: date
) -> list[tuple[str, str]]:
    """Find distinct (release_ref, environment) pairs with telemetry on day."""
    query = """
        SELECT DISTINCT release_ref, environment
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND toDate(bucket_start) = {day:Date}
          AND release_ref != ''
    """
    rows = _query_dicts(client, query, {"org_id": org_id, "day": str(day)})
    return [(r["release_ref"], r["environment"]) for r in rows]


def _count_total_releases(client: Any, org_id: str, day: date) -> int:
    """Count total distinct release_refs deployed on this day."""
    query = """
        SELECT count(DISTINCT release_ref) AS cnt
        FROM deployments
        WHERE release_ref != ''
          AND toDate(coalesce(deployed_at, started_at)) = {day:Date}
    """
    params: dict[str, Any] = {"day": str(day)}
    rows = _query_dicts(client, query, params)
    if rows:
        return int(rows[0].get("cnt", 0))
    return 0


def _get_deploy_timestamp(
    client: Any, org_id: str, release_ref: str, environment: str
) -> datetime | None:
    """Get the deploy timestamp for a release_ref + environment."""
    query = """
        SELECT coalesce(deployed_at, started_at) AS deploy_ts
        FROM deployments
        WHERE release_ref = {release_ref:String}
          AND environment = {environment:String}
        ORDER BY deploy_ts DESC
        LIMIT 1
    """
    rows = _query_dicts(
        client,
        query,
        {"release_ref": release_ref, "environment": environment},
    )
    if rows and rows[0].get("deploy_ts"):
        ts = rows[0]["deploy_ts"]
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts
    return None


def _signal_rate(
    client: Any,
    org_id: str,
    release_ref: str,
    environment: str,
    signal_pattern: str,
    window_start: datetime,
    window_end: datetime,
) -> tuple[float | None, int]:
    """Compute signal_count / session_count rate in a time window.

    Returns (rate, total_sessions).
    """
    query = """
        SELECT
            sum(signal_count) AS total_signals,
            sum(session_count) AS total_sessions
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND environment = {environment:String}
          AND signal_type LIKE {signal_pattern:String}
          AND bucket_start >= {window_start:DateTime64(3)}
          AND bucket_end <= {window_end:DateTime64(3)}
    """
    rows = _query_dicts(
        client,
        query,
        {
            "org_id": org_id,
            "environment": environment,
            "signal_pattern": signal_pattern,
            "window_start": window_start,
            "window_end": window_end,
        },
    )
    if not rows:
        return None, 0
    total_signals = int(rows[0].get("total_signals", 0) or 0)
    total_sessions = int(rows[0].get("total_sessions", 0) or 0)
    if total_sessions == 0:
        return None, 0
    return total_signals / total_sessions, total_sessions


def _compute_delta(
    client: Any,
    org_id: str,
    release_ref: str,
    environment: str,
    deploy_ts: datetime,
    signal_pattern: str,
    min_sessions: int,
) -> tuple[float | None, float | None]:
    """Compute pre/post delta and post rate for a signal type.

    Returns (delta, post_rate).
    Delta = (post_rate - pre_rate) / pre_rate when pre_rate > 0.
    """
    baseline_start = deploy_ts - timedelta(days=_BASELINE_WINDOW_DAYS)
    baseline_end = deploy_ts
    post_start = deploy_ts
    post_end = deploy_ts + timedelta(hours=_POST_WINDOW_HOURS)

    pre_rate, pre_sessions = _signal_rate(
        client,
        org_id,
        release_ref,
        environment,
        signal_pattern,
        baseline_start,
        baseline_end,
    )
    post_rate, post_sessions = _signal_rate(
        client,
        org_id,
        release_ref,
        environment,
        signal_pattern,
        post_start,
        post_end,
    )

    total_sessions = pre_sessions + post_sessions
    if total_sessions < min_sessions:
        return None, post_rate

    if pre_rate is None or pre_rate == 0.0:
        return None, post_rate

    delta = (post_rate - pre_rate) / pre_rate if post_rate is not None else None
    return delta, post_rate


def _time_to_first_friction_spike(
    client: Any,
    org_id: str,
    environment: str,
    deploy_ts: datetime,
) -> float | None:
    """Hours from deploy to first friction signal spike (within 72h)."""
    spike_end = deploy_ts + timedelta(hours=_SPIKE_DETECTION_HOURS)
    query = """
        SELECT min(bucket_start) AS first_friction_ts
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND environment = {environment:String}
          AND signal_type LIKE 'friction.%'
          AND bucket_start >= {deploy_ts:DateTime64(3)}
          AND bucket_start <= {spike_end:DateTime64(3)}
          AND signal_count > 0
    """
    rows = _query_dicts(
        client,
        query,
        {
            "org_id": org_id,
            "environment": environment,
            "deploy_ts": deploy_ts,
            "spike_end": spike_end,
        },
    )
    if not rows:
        return None
    first_ts = rows[0].get("first_friction_ts")
    if first_ts is None:
        return None
    if isinstance(first_ts, datetime):
        if first_ts.tzinfo is None:
            first_ts = first_ts.replace(tzinfo=timezone.utc)
        deploy_utc = (
            deploy_ts if deploy_ts.tzinfo else deploy_ts.replace(tzinfo=timezone.utc)
        )
        delta_hours = (first_ts - deploy_utc).total_seconds() / 3600.0
        return delta_hours if delta_hours >= 0 else None
    return None


def _concurrent_deploy_count(
    client: Any,
    org_id: str,
    release_ref: str,
    environment: str,
    deploy_ts: datetime,
) -> int:
    """Count other releases in same environment within 24h window."""
    window_start = deploy_ts - timedelta(hours=24)
    window_end = deploy_ts + timedelta(hours=24)
    query = """
        SELECT count(DISTINCT release_ref) AS cnt
        FROM deployments
        WHERE environment = {environment:String}
          AND release_ref != {release_ref:String}
          AND release_ref != ''
          AND coalesce(deployed_at, started_at) >= {window_start:DateTime64(3)}
          AND coalesce(deployed_at, started_at) <= {window_end:DateTime64(3)}
    """
    rows = _query_dicts(
        client,
        query,
        {
            "environment": environment,
            "release_ref": release_ref,
            "window_start": window_start,
            "window_end": window_end,
        },
    )
    if rows:
        return int(rows[0].get("cnt", 0) or 0)
    return 0


def _data_completeness(
    client: Any,
    org_id: str,
    release_ref: str,
    environment: str,
    day: date,
) -> float:
    """Compute data completeness: 1.0 if all expected hourly buckets present.

    Expects 24 hourly buckets per day. Scaled proportionally.
    """
    query = """
        SELECT count(DISTINCT toStartOfHour(bucket_start)) AS bucket_hours
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND environment = {environment:String}
          AND toDate(bucket_start) = {day:Date}
    """
    rows = _query_dicts(
        client,
        query,
        {"org_id": org_id, "environment": environment, "day": str(day)},
    )
    if not rows:
        return 0.0
    bucket_hours = int(rows[0].get("bucket_hours", 0) or 0)
    return min(bucket_hours / 24.0, 1.0)


def _compute_confidence(
    coverage_ratio: float,
    total_sessions: int,
    concurrent_deploys: int,
    min_sessions: int = _MIN_SESSIONS_FRICTION,
) -> float:
    """Compute release_impact_confidence_score.

    Factors: coverage, sample sufficiency, concurrent deploy confounding.
    """
    sample_score = min(total_sessions / min_sessions, 1.0) if min_sessions > 0 else 1.0
    confound_score = 1.0 / (1.0 + concurrent_deploys)

    score = (
        _W_COVERAGE * coverage_ratio
        + _W_SAMPLE * sample_score
        + _W_CONFOUND * confound_score
    )
    return max(0.0, min(1.0, score))


def _compute_release_env(
    client: Any,
    org_id: str,
    day: date,
    release_ref: str,
    environment: str,
    coverage_ratio: float,
    computed_at: datetime,
) -> ReleaseImpactDailyRecord:
    deploy_ts = _get_deploy_timestamp(client, org_id, release_ref, environment)
    repo_id = _get_repo_id_for_release(client, release_ref, environment)

    friction_delta: float | None = None
    post_friction_rate: float | None = None
    friction_sessions = 0
    error_delta: float | None = None
    post_error_rate: float | None = None
    error_sessions = 0

    if deploy_ts is not None:
        friction_delta, post_friction_rate = _compute_delta(
            client,
            org_id,
            release_ref,
            environment,
            deploy_ts,
            "friction.%",
            _MIN_SESSIONS_FRICTION,
        )
        error_delta, post_error_rate = _compute_delta(
            client,
            org_id,
            release_ref,
            environment,
            deploy_ts,
            "error.%",
            _MIN_EVENTS_ERROR,
        )

        post_start = deploy_ts
        post_end = deploy_ts + timedelta(hours=_POST_WINDOW_HOURS)
        _, friction_sessions = _signal_rate(
            client,
            org_id,
            release_ref,
            environment,
            "friction.%",
            post_start,
            post_end,
        )
        _, error_sessions = _signal_rate(
            client,
            org_id,
            release_ref,
            environment,
            "error.%",
            post_start,
            post_end,
        )

    total_sessions = friction_sessions + error_sessions
    time_to_first_issue = (
        _time_to_first_friction_spike(client, org_id, environment, deploy_ts)
        if deploy_ts is not None
        else None
    )

    concurrent = (
        _concurrent_deploy_count(client, org_id, release_ref, environment, deploy_ts)
        if deploy_ts is not None
        else 0
    )

    completeness = _data_completeness(client, org_id, release_ref, environment, day)
    confidence = _compute_confidence(
        coverage_ratio,
        total_sessions,
        concurrent,
    )

    missing = sum(
        1
        for v in [friction_delta, post_friction_rate, error_delta, post_error_rate]
        if v is None
    )

    return ReleaseImpactDailyRecord(
        day=day,
        release_ref=release_ref,
        environment=environment,
        repo_id=repo_id,
        release_user_friction_delta=friction_delta,
        release_post_friction_rate=post_friction_rate,
        release_error_rate_delta=error_delta,
        release_post_error_rate=post_error_rate,
        time_to_first_user_issue_after_release=time_to_first_issue,
        release_impact_confidence_score=confidence,
        release_impact_coverage_ratio=coverage_ratio,
        flag_exposure_rate=None,
        flag_activation_rate=None,
        flag_reliability_guardrail=None,
        flag_friction_delta=None,
        flag_rollout_half_life=None,
        flag_churn_rate=None,
        issue_to_release_impact_link_rate=None,
        rollback_or_disable_after_impact_spike=None,
        coverage_ratio=coverage_ratio,
        missing_required_fields_count=missing,
        instrumentation_change_flag=False,
        data_completeness=completeness,
        concurrent_deploy_count=concurrent,
        computed_at=computed_at,
        org_id=org_id,
    )


def _get_repo_id_for_release(
    client: Any, release_ref: str, environment: str
) -> UUID | None:
    """Look up repo_id from deployments for a release_ref."""
    query = """
        SELECT repo_id
        FROM deployments
        WHERE release_ref = {release_ref:String}
          AND environment = {environment:String}
        ORDER BY coalesce(deployed_at, started_at) DESC
        LIMIT 1
    """
    rows = _query_dicts(
        client,
        query,
        {"release_ref": release_ref, "environment": environment},
    )
    if rows and rows[0].get("repo_id"):
        try:
            return UUID(str(rows[0]["repo_id"]))
        except (ValueError, TypeError):
            return None
    return None
