"""Tests for the durable rate-limit observation store (CHAOS-2758).

Covers:
  * the observation write is attempted only after the RETRYING CAS succeeds,
    and is best-effort/non-blocking (its own SAVEPOINT): a DB-level failure
    persisting it must never roll back the deferral or block dispatch.
  * integration_id / route_family / dimension enrichment from the unit row +
    the budget estimate already computed for dispatch (never re-estimated),
    with route-family attribution confidence-gated (never guessed when
    dimension alone cannot disambiguate multiple candidate families).
  * only normalized fields are persisted -- never raw headers, secrets, or
    raw exception text (``reason`` is an allow-listed category).
  * the ws-a exception-unification fix (CHAOS-2753) means
    ``connectors.base.RateLimitException`` (no signal) also produces a row.
  * the beat-scheduled retention prune task.
  * migration 0031 is a guarded, retry-safe (idempotent) upgrade/downgrade.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    ProviderRateLimitObservation,
    SyncDispatchOutbox,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_DISPATCH
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal
from tests.test_sync_units import (
    _mark_dispatching,
    _patch_db_session,
    _patch_finalize_apply,
    _patch_runtime,
    _seed_run,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _clean_env(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)


def _aware(value: datetime) -> datetime:
    """Normalize a datetime read back from SQLite (which drops tzinfo on
    round-trip) to UTC-aware, for comparison against the tz-aware value that
    was written."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Atomic persistence with the RETRYING stamp
# ---------------------------------------------------------------------------


def test_observation_persisted_atomically_with_deferral(db_session, monkeypatch):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)  # provider=github, dataset_key=commits
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    reset_at = datetime(2026, 7, 2, 0, 0, 0, tzinfo=timezone.utc)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub primary rate limit exceeded",
            retry_after_seconds=45.0,
            signal=RateLimitSignal(
                provider="github",
                host="api.github.com",
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=45.0,
                reset_at=reset_at,
                reason="primary",
                request_id="req-123",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "rate_limited_deferred"
    assert unit.status == SyncRunUnitStatus.RETRYING.value

    observations = db_session.query(ProviderRateLimitObservation).all()
    assert len(observations) == 1
    observation = observations[0]
    assert observation.org_id == unit.org_id
    assert observation.provider == "github"
    assert observation.host == "api.github.com"
    assert observation.integration_id == unit.integration_id
    assert observation.sync_run_id == run.id
    assert observation.sync_run_unit_id == unit.id
    assert observation.route_family == "git"  # github "commits" estimator family
    assert observation.route_family_attribution is None  # confidently attributed
    assert observation.dimension == "rest_core"
    assert observation.retry_after_seconds == 45.0
    assert _aware(observation.reset_at) == reset_at
    assert observation.reason == "primary"
    assert observation.request_id == "req-123"
    assert observation.observed_at is not None


def test_observation_not_orphaned_when_deferral_cas_loses_lease(
    db_session, monkeypatch
):
    """A CAS loss on the RETRYING stamp must leave no orphan observation row.

    The observation insert is deliberately placed AFTER the deferral CAS
    rowcount check in ``run_sync_unit`` so the two either commit together or
    neither does. Mirrors the established
    ``test_run_sync_unit_lost_lease_before_work_item_sink_aborts_without_finalize``
    pattern: steal the lease via the SAME shared session mid-flight (rather
    than a second engine/session), so the CAS predicate's ``lease_owner``
    match fails without detaching the ``unit`` ORM object the deferral branch
    still reads (``rate_limit_deferrals`` / ``rate_limit_first_seen_at``).
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        # Simulate another worker stealing the lease while this task is
        # mid-flight, so the deferral CAS below (which matches on the
        # original lease_owner) affects zero rows.
        db_session.refresh(unit)
        unit.lease_owner = "other-worker"
        db_session.flush()
        raise RateLimitException(
            "GitHub primary rate limit exceeded", retry_after_seconds=30.0
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "lease_lost",
    }
    assert db_session.query(ProviderRateLimitObservation).count() == 0


# ---------------------------------------------------------------------------
# Best-effort, non-blocking persistence (HIGH finding, CHAOS-2758 review)
# ---------------------------------------------------------------------------


def test_observation_persist_failure_does_not_block_deferral(db_session, monkeypatch):
    """A DB-level failure writing the observation (e.g. migration 0031 not
    yet applied to every node during a rolling deploy, or schema drift) must
    NOT roll back the RETRYING stamp or the dispatch wakeup -- the
    observation store is diagnostic, not load-bearing.

    Forces the flush the observation-persistence SAVEPOINT performs to raise
    a real ``SQLAlchemyError`` (rather than just making the builder function
    raise), so this exercises the actual ``session.begin_nested()`` /
    rollback path, not merely a Python-level guard.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    real_flush = db_session.flush

    def guarded_flush(*args, **kwargs):
        # Only the observation-persistence SAVEPOINT flushes while a nested
        # transaction is active; every other flush in run_sync_unit (the
        # RETRYING CAS, the outbox wakeup) happens outside one.
        if db_session.in_nested_transaction():
            raise SQLAlchemyError(
                'relation "provider_rate_limit_observations" does not exist'
            )
        return real_flush(*args, **kwargs)

    monkeypatch.setattr(db_session, "flush", guarded_flush)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub primary rate limit exceeded", retry_after_seconds=30.0
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    # The deferral itself must succeed exactly as if the observation write
    # had never been attempted.
    assert result["status"] == "rate_limited_deferred"
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.rate_limit_deferrals == 1
    dispatch_wakeup = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert dispatch_wakeup is not None
    # No observation row -- the SAVEPOINT rolled back the failed insert.
    assert db_session.query(ProviderRateLimitObservation).count() == 0


# ---------------------------------------------------------------------------
# Enrichment: integration_id from the unit row, route_family/dimension from
# the pre-computed budget audit (never re-estimated).
# ---------------------------------------------------------------------------


def test_observation_enriched_with_integration_and_route_family(
    db_session, monkeypatch
):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_units import run_sync_unit

    # "prs" emits three estimates: prs/rest_core, pr_social/graphql_cost,
    # pr_social/secondary_abuse_risk -- a real multi-route-family unit.
    run, unit = _seed_run(db_session, dataset_key="prs")
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    estimate_calls = []
    real_estimate = sync_units.estimate_provider_budget

    def counting_estimate(ctx):
        estimate_calls.append(ctx)
        return real_estimate(ctx)

    monkeypatch.setattr(sync_units, "estimate_provider_budget", counting_estimate)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub GraphQL secondary rate limit",
            retry_after_seconds=60.0,
            signal=RateLimitSignal(
                provider="github",
                dimension=BudgetDimension.GRAPHQL_COST,
                retry_after_seconds=60.0,
                reason="secondary",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    # The estimate was computed exactly once (at dispatch, for the started_at
    # audit) -- the exception branch reuses it, it never re-estimates
    # (estimators require credential decryption).
    assert len(estimate_calls) == 1

    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.integration_id == unit.integration_id
    assert observation.route_family == "pr_social"
    assert observation.route_family_attribution is None  # unique dimension match
    assert observation.dimension == "graphql_cost"


def test_observation_route_family_ambiguous_for_linear_work_items(
    db_session, monkeypatch
):
    """End-to-end regression for the false-precision fix (CHAOS-2758 review).

    Linear's ``work-items`` unit estimates teams/issues/cycles/comments/
    attachments/history ALL under ``graphql_cost`` -- a rate limit tagged
    ``graphql_cost`` cannot disambiguate which one actually failed, so the
    observation must record ``route_family=NULL`` +
    ``route_family_attribution="ambiguous_dimension"`` rather than guessing
    the first-listed family. ``dimension`` itself is still populated so a
    cooldown-gating consumer can fall back to provider+integration+dimension.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import (
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
        run_sync_unit,
    )

    run, unit = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="TEAM",
        name="TEAM",
        full_name="TEAM",
        dataset_key="work-items",
        processor_flags={},
    )
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "Linear rate limited",
            retry_after_seconds=20.0,
            signal=RateLimitSignal(
                provider="linear",
                dimension=BudgetDimension.GRAPHQL_COST,
                retry_after_seconds=20.0,
                reason="primary",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.route_family is None
    assert observation.route_family_attribution == _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION
    assert observation.dimension == "graphql_cost"


def test_observation_sanitizes_non_finite_retry_after_seconds(db_session, monkeypatch):
    """CHAOS-2760 cooldown-gating review finding: a provider-supplied
    ``retry_after_seconds`` that is inf/NaN/negative must never persist
    verbatim. The cooldown-gating reader's ``timedelta(seconds=...)``
    arithmetic raises on a non-finite value; the reader has its own
    fail-open guard for that (``sync/budget_guard.py``), but a corrupt value
    should never be written to the observation store in the first place.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)  # provider=github, dataset_key=commits
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub primary rate limit exceeded",
            retry_after_seconds=float("inf"),
            signal=RateLimitSignal(
                provider="github",
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=float("inf"),
                reason="primary",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.retry_after_seconds is None


def test_observation_clamps_excessive_retry_after_seconds(db_session, monkeypatch):
    """A finite but absurd ``retry_after_seconds`` is clamped to the same
    wall-clock budget the deferral planner enforces
    (``RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS``), not persisted verbatim -- a
    provider asking for a longer wait than the run would ever honor is not
    worth keeping as-is in the durable store.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.rate_limit_defer import (
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    )
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    absurd_delay = float(RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS * 100)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub primary rate limit exceeded",
            retry_after_seconds=absurd_delay,
            signal=RateLimitSignal(
                provider="github",
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=absurd_delay,
                reason="primary",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.retry_after_seconds == float(RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS)


def test_route_family_resolution_keeps_unique_match_and_refuses_to_guess():
    """Unit test of the confidence-gated route-family attribution (CHAOS-2758
    review fix): dimension alone does not disambiguate multi-family units, so
    a match is only trusted when it names exactly one distinct family.
    """
    from dev_health_ops.workers.sync_units import (
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
        _route_family_and_attribution,
    )

    # GitHub "prs": dimension is unique per family here.
    github_prs_audit = [
        {"bucket": {"dimension": "rest_core"}, "route_family": "prs"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "pr_social"},
        {"bucket": {"dimension": "secondary_abuse_risk"}, "route_family": "pr_social"},
    ]
    assert _route_family_and_attribution(github_prs_audit, "secondary_abuse_risk") == (
        "pr_social",
        None,
    )
    assert _route_family_and_attribution(github_prs_audit, "rest_core") == (
        "prs",
        None,
    )
    # A dimension that matches NOTHING in the audit is ambiguous by
    # definition -- the signal names traffic the unit never budgeted, so a
    # family pick would contradict the signal's own dimension.
    assert _route_family_and_attribution(github_prs_audit, "search") == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )
    # Regression (codex re-pass finding): the dimension-miss rule must hold
    # even for SINGLE-family audits. A REST-only GitHub "commits" unit hit by
    # a secondary-abuse signal must not be recorded as a trusted 'git'
    # observation -- CHAOS-2760 falls back to provider+integration+dimension.
    github_commits_audit = [
        {"bucket": {"dimension": "rest_core"}, "route_family": "git"},
        {"bucket": {"dimension": "contents_blob"}, "route_family": "git"},
    ]
    assert _route_family_and_attribution(
        github_commits_audit, "secondary_abuse_risk"
    ) == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )
    # No dimension at all -> same full-candidate-set ambiguity.
    assert _route_family_and_attribution(github_prs_audit, None) == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )

    # GitHub "commit_stats": two dimensions, but they share ONE family --
    # still confidently attributable even without a dimension match.
    github_commit_stats_audit = [
        {"bucket": {"dimension": "rest_core"}, "route_family": "commit_stats"},
        {"bucket": {"dimension": "contents_blob"}, "route_family": "commit_stats"},
    ]
    assert _route_family_and_attribution(github_commit_stats_audit, None) == (
        "commit_stats",
        None,
    )
    assert _route_family_and_attribution(
        github_commit_stats_audit, "contents_blob"
    ) == ("commit_stats", None)

    # Linear "work-items": teams/issues/cycles/comments/attachments/history are
    # ALL graphql_cost -- dimension is genuinely uninformative here. Must NOT
    # guess a family.
    linear_work_items_audit = [
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "teams"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "issues"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "cycles"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "comments"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "attachments"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "history"},
    ]
    assert _route_family_and_attribution(linear_work_items_audit, "graphql_cost") == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )
    assert _route_family_and_attribution(linear_work_items_audit, None) == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )

    # Jira comments dataset: jira_issue_enrichment + jira_comments both
    # rest_core -- ambiguous under that dimension.
    jira_comments_audit = [
        {"bucket": {"dimension": "search"}, "route_family": "jira_jql"},
        {"bucket": {"dimension": "rest_core"}, "route_family": "jira_issue_enrichment"},
        {"bucket": {"dimension": "rest_core"}, "route_family": "jira_comments"},
    ]
    assert _route_family_and_attribution(jira_comments_audit, "search") == (
        "jira_jql",
        None,
    )
    assert _route_family_and_attribution(jira_comments_audit, "rest_core") == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )

    # No budget audit at all -> ambiguous, never a guess.
    assert _route_family_and_attribution(None, "rest_core") == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )
    assert _route_family_and_attribution([], "rest_core") == (
        None,
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION,
    )


# ---------------------------------------------------------------------------
# No raw headers / secrets ever persisted
# ---------------------------------------------------------------------------


def test_observation_contains_no_raw_headers_or_secrets(db_session, monkeypatch):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    class _ExceptionWithRawHeaders(RateLimitException):
        """Simulates a provider client that stashed raw headers on the
        exception for its own in-place retry logic -- the observation writer
        must never read or persist them."""

        def __init__(self) -> None:
            super().__init__(
                "rate limited",
                retry_after_seconds=15.0,
                signal=RateLimitSignal(
                    provider="github",
                    dimension=BudgetDimension.REST_CORE,
                    retry_after_seconds=15.0,
                    reason="primary",
                ),
            )
            self.raw_headers = {
                "Authorization": "Bearer super-secret-token",
                "X-RateLimit-Remaining": "0",
            }

    def rate_limited(ctx, runtime):
        raise _ExceptionWithRawHeaders()

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    getattr(run_sync_unit, "run")(str(unit.id))

    observation = db_session.query(ProviderRateLimitObservation).one()
    mapper = sa.inspect(ProviderRateLimitObservation)
    persisted_columns = {col.key for col in mapper.columns}
    # Only the normalized schema fields exist on the row -- no headers column,
    # no raw dict anywhere in the mapped state.
    assert "raw_headers" not in persisted_columns
    assert "headers" not in persisted_columns
    for column_name in persisted_columns:
        value = getattr(observation, column_name)
        if isinstance(value, str):
            assert "super-secret-token" not in value
            assert "Authorization" not in value


def test_observation_no_signal_reason_never_persists_raw_exception_text(
    db_session, monkeypatch
):
    """Regression for the MEDIUM finding (CHAOS-2758 review): a no-signal
    exception's message can embed a provider's raw response body (legacy
    connectors build their message from ``response.text``). ``reason`` must
    normalize to the fixed ``unknown`` category, never ``str(exc)``.
    """
    from dev_health_ops.connectors.base import (
        RateLimitException as LegacyRateLimitException,
    )
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    secret_bearing_body = (
        'Rate limited: {"message": "API rate limit exceeded", '
        '"token": "ghp_SUPERSECRETTOKEN1234567890", '
        '"Authorization": "Bearer super-secret-token"}'
    )

    def rate_limited(ctx, runtime):
        # No `signal=` -- the message embeds header/body-shaped content, as a
        # real legacy no-signal raise site's `str(response_body)` would.
        raise LegacyRateLimitException(secret_bearing_body, retry_after_seconds=5.0)

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    getattr(run_sync_unit, "run")(str(unit.id))

    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.reason == "unknown"
    mapper = sa.inspect(ProviderRateLimitObservation)
    for column_name in {col.key for col in mapper.columns}:
        value = getattr(observation, column_name)
        if isinstance(value, str):
            assert "SUPERSECRETTOKEN" not in value
            assert "super-secret-token" not in value
            assert "Authorization" not in value
            assert "API rate limit exceeded" not in value


# ---------------------------------------------------------------------------
# Legacy connector exceptions (post CHAOS-2753 unification) also persist
# ---------------------------------------------------------------------------


def test_legacy_connector_exception_also_persists_observation(db_session, monkeypatch):
    from dev_health_ops.connectors.base import (
        RateLimitException as LegacyRateLimitException,
    )
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        # No `signal=` -- matches the real legacy connectors (connectors/
        # github.py, connectors/gitlab.py) at call sites that predate ws-a
        # signal population, and any that still omit one.
        raise LegacyRateLimitException(
            "GitLab rate limited (HTTP 429)", retry_after_seconds=30.0
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.org_id == unit.org_id
    assert observation.provider == "github"  # falls back to ctx.provider
    assert observation.integration_id == unit.integration_id
    assert observation.sync_run_unit_id == unit.id
    assert observation.retry_after_seconds == 30.0
    assert observation.dimension is None  # no signal -> no client-known dimension
    # Single-estimate unit ("commits" -> "git" only) is still confidently
    # attributable even with no dimension signal at all.
    assert observation.route_family == "git"
    assert observation.route_family_attribution is None
    # No signal -> normalized to the "unknown" reason category, NEVER the raw
    # exception message (which for a real legacy connector could embed the
    # provider's raw response body).
    assert observation.reason == "unknown"
    assert observation.host is None
    assert observation.request_id is None


# ---------------------------------------------------------------------------
# Retention prune task
# ---------------------------------------------------------------------------


def _make_observation(**overrides):
    defaults = dict(
        org_id=str(uuid.uuid4()),
        provider="github",
        host="api.github.com",
        integration_id=uuid.uuid4(),
        sync_run_id=uuid.uuid4(),
        sync_run_unit_id=uuid.uuid4(),
        route_family="git",
        dimension="rest_core",
        retry_after_seconds=30.0,
        reason="primary",
        observed_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return ProviderRateLimitObservation(**defaults)


def test_prune_task_deletes_expired_rows_only(db_session, monkeypatch):
    from dev_health_ops.workers.sync_reconciler import prune_rate_limit_observations

    now = datetime.now(timezone.utc)
    expired = _make_observation(observed_at=now - timedelta(days=20))
    borderline_fresh = _make_observation(observed_at=now - timedelta(days=1))
    fresh = _make_observation(observed_at=now - timedelta(hours=1))
    db_session.add_all([expired, borderline_fresh, fresh])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "14")

    result = getattr(prune_rate_limit_observations, "run")()

    assert result["status"] == "completed"
    assert result["deleted"] == 1
    assert result["retention_days"] == 14
    remaining_ids = {
        row.id for row in db_session.query(ProviderRateLimitObservation).all()
    }
    assert remaining_ids == {borderline_fresh.id, fresh.id}


def test_prune_task_honors_explicit_retention_days_override(db_session, monkeypatch):
    from dev_health_ops.workers.sync_reconciler import prune_rate_limit_observations

    now = datetime.now(timezone.utc)
    three_days_old = _make_observation(observed_at=now - timedelta(days=3))
    one_hour_old = _make_observation(observed_at=now - timedelta(hours=1))
    db_session.add_all([three_days_old, one_hour_old])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)
    # Env default (14) would keep both; an explicit override prunes tighter.
    monkeypatch.delenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", raising=False)

    result = getattr(prune_rate_limit_observations, "run")(retention_days=1)

    assert result["deleted"] == 1
    remaining_ids = {
        row.id for row in db_session.query(ProviderRateLimitObservation).all()
    }
    assert remaining_ids == {one_hour_old.id}


# ---------------------------------------------------------------------------
# Migration 0031: guarded, retry-safe (idempotent) upgrade
# ---------------------------------------------------------------------------


def _load_migration_0031():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0031_add_provider_rate_limit_observations"
    )


def test_migration_0031_idempotent_upgrade():
    migration = _load_migration_0031()
    assert migration.revision == "0031"
    assert migration.down_revision == "0030"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            # Operations.context() installs the module-global `alembic.op`
            # proxy (the same `from alembic import op` the migration file
            # imports) for the duration of the block -- the documented way to
            # unit-test an individual migration's upgrade()/downgrade().
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
                column_names = {
                    col["name"]
                    for col in inspector.get_columns("provider_rate_limit_observations")
                }
                assert "route_family_attribution" in column_names
                index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("provider_rate_limit_observations")
                }
                assert "ix_provider_rate_limit_observations_cooldown" in index_names
                assert "ix_provider_rate_limit_observations_org_id" in index_names

                # Re-running upgrade() must be a no-op, not an error (guarded
                # create-if-missing, per the 0020/0025 convention).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert (
                    inspector.get_table_names().count(
                        "provider_rate_limit_observations"
                    )
                    == 1
                )

                migration.downgrade()
                inspector = sa.inspect(conn)
                assert (
                    "provider_rate_limit_observations"
                    not in inspector.get_table_names()
                )

                # downgrade() on an already-absent table is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
    finally:
        engine.dispose()
