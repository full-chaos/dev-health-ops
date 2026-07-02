"""Invariant tests: credentials are not capacity (CHAOS-2756, epic CHAOS-2742).

Product invariant encoded here so it cannot regress silently: **changing or
rotating credentials must never increase sync dispatch capacity, and the planner
supports exactly one credential per integration.** Budget buckets embed a
``credential_fingerprint`` (``sync/budget_types.py``:``BudgetBucketKey``; the
enforcement key is built in ``sync/budget_guard.py``:``_budget_key``), so a
future *unit-level* credential selection, or a credential rotation that spawned a
second fingerprint for one integration, would silently multiply admission
capacity while every existing test stayed green. These tests fail in that world.

Test style follows ``tests/test_chaos_2581_invariants.py`` (in-memory sqlite
sessions) and ``tests/test_budget_estimators.py`` (the real
``BudgetGuard.enforce_run`` gated by tight ``SYNC_BUDGET_BUCKET_LIMITS`` env caps
rather than advisory-lock exclusion, which is a no-op off Postgres — the same
convention ``tests/test_dispatch_guard.py`` relies on).

See ``docs/providers/rate-limit-policy.md`` for the durable policy these tests
guard: the credentials-are-not-capacity contract, and the run-level credential
*freeze* that CHAOS-2755 stamps on ``sync_runs`` (allowed) versus unit-level
credential selection (forbidden).
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.core.encryption import encrypt_value
from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationCredential,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.providers.github.budget import GitHubBudgetEstimator
from dev_health_ops.sync.budget import estimate_provider_budget
from dev_health_ops.sync.budget_guard import BudgetGuard
from dev_health_ops.sync.planner import PlannedUnit, SyncPlanRequest, plan_sync_run
from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap, SyncTaskContext

# --- Credential-field markers -------------------------------------------------
#
# Substrings that mark a model column / dataclass field as carrying credential
# IDENTITY or SELECTION. Any of these on a UNIT-level type (``PlannedUnit`` /
# ``SyncRunUnit``) is the capacity-multiplication vector CHAOS-2742 forbids: a
# per-unit credential lets one integration's run fan capacity out across several
# credentials (budget buckets key on ``credential_fingerprint``, so a second
# credential = a second bucket = doubled admission). Kept deliberately broad so a
# creatively-named future field ("auth_token", "secret_ref", ...) still trips.
_CREDENTIAL_FIELD_MARKERS = (
    "credential",
    "fingerprint",
    "secret",
    "token",
    "api_key",
    "apikey",
    "auth_source",
    "decrypted",
    "private_key",
)

# The ONLY sanctioned place for a credential identity in the sync-run domain is
# the *run-level* stamp CHAOS-2755 (ws-c) adds to ``sync_runs``. Stamping the run
# FREEZES which credential the whole run uses (determinism / mid-run auth
# freeze); it does NOT let individual units pick different credentials. So these
# run-level columns are ALLOWLISTED on ``sync_runs`` and remain FORBIDDEN on
# ``sync_run_units``. Written explicitly so this PR and CHAOS-2755 merge green in
# either order: before ws-c merges, ``sync_runs`` simply has none of them and the
# allowlist check passes vacuously.
_RUN_LEVEL_CREDENTIAL_ALLOWLIST: dict[str, frozenset[str]] = {
    "sync_runs": frozenset({"credential_id", "credential_fingerprint", "auth_source"}),
}

_NARROW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
_NARROW_END = datetime(2026, 1, 3, tzinfo=timezone.utc)

# A zero-span commits window estimates to the REST_CORE floor (2 units); see
# ``GitHubBudgetEstimator``/``_scaled_units``. With 2 in-flight units (=4 budget)
# under an 8-budget bucket limit, exactly 2 of 4 candidates fit and the rest
# defer — the arithmetic the rotation test pins.
_UNIT_BUDGET = 2
_ACTIVE_UNITS = 2
_CANDIDATE_UNITS = 4
_BUCKET_LIMIT = 8
_GITHUB_APP_ENV_VARS = (
    "GITHUB_URL",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "GITHUB_APP_INSTALLATION_ID",
)


def _is_credential_named(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in _CREDENTIAL_FIELD_MARKERS)


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _seed_github_integration(
    session: Session,
    *,
    org_id: str | None = None,
    credential_id: uuid.UUID | None = None,
    dataset_keys: tuple[str, ...] = ("commits",),
) -> tuple[Integration, IntegrationSource]:
    org = org_id or str(uuid.uuid4())
    integration = Integration(
        org_id=org,
        provider="github",
        name="gh-demo",
        config={},
        is_active=True,
    )
    if credential_id is not None:
        integration.credential_id = credential_id
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    session.add(source)
    for dataset_key in dataset_keys:
        session.add(
            IntegrationDataset(
                org_id=org,
                integration_id=integration.id,
                dataset_key=dataset_key,
                is_enabled=True,
                options={},
            )
        )
    session.flush()
    return integration, source


def _seed_run(
    session: Session,
    integration: Integration,
    *,
    mode: str = SyncRunMode.INCREMENTAL.value,
) -> SyncRun:
    run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    return run


def _add_units(
    session: Session,
    run: SyncRun,
    integration: Integration,
    source: IntegrationSource,
    *,
    count: int,
    status: str,
    dataset_key: str = "commits",
    lease_expires_at: datetime | None = None,
) -> list[SyncRunUnit]:
    units: list[SyncRunUnit] = []
    for _ in range(count):
        unit = SyncRunUnit(
            org_id=integration.org_id,
            sync_run_id=run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider="github",
            dataset_key=dataset_key,
            cost_class="medium",
            mode=run.mode,
            since_at=_NARROW_START,
            before_at=_NARROW_START,  # zero span -> REST_CORE floor estimate
            status=status,
            attempts=0,
            processor_flags={},
            lease_expires_at=lease_expires_at,
        )
        session.add(unit)
        units.append(unit)
    session.flush()
    return units


def _backfill_request(integration: Integration) -> SyncPlanRequest:
    return SyncPlanRequest(
        integration_id=str(integration.id),
        org_id=integration.org_id,
        mode=SyncRunMode.BACKFILL.value,
        triggered_by="manual",
        since=_NARROW_START,
        before=_NARROW_END,
    )


def _unit_shapes(session: Session, sync_run_id: str) -> list[tuple[object, ...]]:
    units = (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == uuid.UUID(sync_run_id))
        .all()
    )
    return sorted(
        (
            str(unit.source_id),
            unit.provider,
            unit.dataset_key,
            unit.cost_class,
            unit.mode,
            unit.since_at,
            unit.before_at,
        )
        for unit in units
    )


# --- Shape guards -------------------------------------------------------------


def test_planned_unit_exposes_no_credential_field() -> None:
    """PlannedUnit must stay credential-blind (``sync/planner.py``).

    Introspects the dataclass fields; a future unit-level credential field is
    the capacity-multiplication vector and must fail CI. Contract:
    ``docs/providers/rate-limit-policy.md``.
    """

    offending = [
        field.name
        for field in dataclasses.fields(PlannedUnit)
        if _is_credential_named(field.name)
    ]
    assert offending == [], (
        "PlannedUnit must carry no credential field (CHAOS-2742): a per-unit "
        f"credential is the capacity fan-out vector. Found: {offending}"
    )


def test_sync_run_unit_model_has_no_credential_column() -> None:
    """``sync_run_units`` carries no credential column; ``sync_runs`` may carry
    only the CHAOS-2755 run-level freeze columns.

    Encodes the split from ``docs/providers/rate-limit-policy.md``: run-level
    credential freeze is permitted (determinism), unit-level credential
    selection is forbidden (capacity multiplication).
    """

    unit_credential_columns = [
        column.name
        for column in SyncRunUnit.__table__.columns
        if _is_credential_named(column.name)
    ]
    assert unit_credential_columns == [], (
        "sync_run_units must carry NO credential column (CHAOS-2742): unit-level "
        f"credential selection multiplies budget buckets. Found: "
        f"{unit_credential_columns}"
    )

    # Run-level stamp columns are allowed ONLY on sync_runs and ONLY for the
    # sanctioned freeze set (CHAOS-2755). Any other credential column on
    # sync_runs — or the sanctioned columns landing on a different table — is a
    # violation. Order-independent: empty before ws-c merges, exactly the
    # allowlist after.
    run_credential_columns = {
        column.name
        for column in SyncRun.__table__.columns
        if _is_credential_named(column.name)
    }
    allowed = _RUN_LEVEL_CREDENTIAL_ALLOWLIST["sync_runs"]
    unexpected = run_credential_columns - allowed
    assert unexpected == set(), (
        "Unexpected credential column(s) on sync_runs beyond the CHAOS-2755 "
        f"run-level freeze allowlist {sorted(allowed)}: {sorted(unexpected)}"
    )


# --- Behavior guards ----------------------------------------------------------


def test_plan_sync_run_identical_under_credential_swap(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Planning is credential-blind: swapping the integration's credential
    yields byte-identical units (count, datasets, windows, cost).

    Backfill mode with explicit ``since``/``before`` keeps windows deterministic
    (chunker-derived, wall-clock-independent) so equality is not flaky.

    Both credentials are REAL active rows: since CHAOS-2755 the planner stamps
    the run's auth at plan time and fail-fasts on a missing/inactive credential,
    so a dangling UUID would abort planning — that fail-fast has its own test;
    THIS invariant is about unit shapes being identical across valid swaps.
    """
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-capacity-invariants-secret")
    org = str(uuid.uuid4())

    def _make_credential(name: str, token: str) -> IntegrationCredential:
        credential = IntegrationCredential(
            provider="github",
            name=name,
            org_id=org,
            credentials_encrypted=encrypt_value(json.dumps({"token": token})),
            config={},
            is_active=True,
        )
        db_session.add(credential)
        db_session.flush()
        return credential

    credential_a = _make_credential("primary", "tok-A").id
    credential_b = _make_credential("secondary", "tok-B").id
    integration, _source = _seed_github_integration(
        db_session,
        org_id=org,
        credential_id=credential_a,
        dataset_keys=("commits", "prs"),
    )

    plan_a = plan_sync_run(db_session, _backfill_request(integration))
    shape_a = _unit_shapes(db_session, plan_a.sync_run_id)

    # Rotate the integration's credential; the planner must not notice.
    integration.credential_id = credential_b
    db_session.flush()
    plan_b = plan_sync_run(db_session, _backfill_request(integration))
    shape_b = _unit_shapes(db_session, plan_b.sync_run_id)

    assert credential_a != credential_b  # the swap was real
    assert shape_a, "planner produced no units to compare"
    assert shape_a == shape_b, (
        "plan_sync_run must be identical under a credential swap; a difference "
        "means the planner became credential-aware (CHAOS-2742 violation)"
    )


def test_all_units_of_integration_share_one_budget_fingerprint(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every unit of one integration estimates under exactly one credential
    fingerprint. A second fingerprint per integration = capacity fan-out.
    """

    monkeypatch.setenv("GITHUB_TOKEN", "token-shared")
    for env_var in _GITHUB_APP_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)

    integration, _source = _seed_github_integration(
        db_session, credential_id=None, dataset_keys=("commits", "prs")
    )
    plan = plan_sync_run(db_session, _backfill_request(integration))
    assert plan.unit_ids, "planner produced no units to estimate"

    fingerprints: set[str] = set()
    for unit_id in plan.unit_ids:
        context = SyncTaskBootstrap.load(db_session, unit_id)
        for estimate in estimate_provider_budget(context):
            fingerprints.add(estimate.bucket.credential_fingerprint)

    assert len(fingerprints) == 1, (
        "Every unit of one integration must estimate under exactly one "
        f"credential fingerprint; more than one means capacity multiplication. "
        f"Got: {fingerprints}"
    )
    # Non-vacuous: the fingerprint is credential-derived (a different token
    # yields a different bucket — see the rotation test), so "exactly one" is a
    # real constraint, not a constant.
    assert next(iter(fingerprints))


def _github_fingerprint(token: str) -> str:
    context = SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="source-1",
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode="incremental",
        window_start=_NARROW_START,
        window_end=_NARROW_START,
        processor_flags={},
        credential_id=None,
        decrypted_credentials={"token": token},
        db_url="",
    )
    estimates = GitHubBudgetEstimator().estimate(context)
    return estimates[0].bucket.credential_fingerprint


def _admitted_under_token(token: str, monkeypatch: pytest.MonkeyPatch) -> int:
    for env_var in _GITHUB_APP_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", token)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": _BUCKET_LIMIT})
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            integration, source = _seed_github_integration(session, credential_id=None)
            run = _seed_run(session, integration)
            # In-flight units admitted by a prior dispatch pass; their live
            # consumption must gate this pass in the SAME bucket.
            _add_units(
                session,
                run,
                integration,
                source,
                count=_ACTIVE_UNITS,
                status=SyncRunUnitStatus.RUNNING.value,
                lease_expires_at=None,  # null lease counts as live
            )
            _add_units(
                session,
                run,
                integration,
                source,
                count=_CANDIDATE_UNITS,
                status=SyncRunUnitStatus.PLANNED.value,
            )
            session.commit()
            result = BudgetGuard.enforce_run(session, str(run.id))
            return _CANDIDATE_UNITS - len(result.deferred_unit_ids)
    finally:
        engine.dispose()


def test_credential_rotation_never_increases_admitted_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rotating the credential between dispatch passes admits no more units than
    the un-rotated baseline.

    Pins that ``_active_budget_consumption`` re-estimates in-flight units with
    the CURRENT credential — moving consumption WITH the fingerprint rather than
    stranding it in a frozen/stale bucket while new candidates estimate under the
    rotated one (which would leave the rotated bucket looking empty and admit all
    candidates: capacity doubled). Contract:
    ``docs/providers/rate-limit-policy.md``.
    """

    # Premise: rotation actually changes the credential fingerprint (else the
    # invariant would hold vacuously).
    assert _github_fingerprint("token-A") != _github_fingerprint("token-B")

    baseline = _admitted_under_token("token-A", monkeypatch)
    rotated = _admitted_under_token("token-B", monkeypatch)

    # In-flight consumption leaves room for only (limit - active) / unit budget
    # candidates; if in-flight consumption were ignored, all candidates would fit.
    active_budget = _ACTIVE_UNITS * _UNIT_BUDGET
    expected_admitted = (_BUCKET_LIMIT - active_budget) // _UNIT_BUDGET
    assert baseline == expected_admitted, (
        "expected in-flight consumption to gate candidates to "
        f"{expected_admitted}; got {baseline}"
    )
    # The invariant.
    assert rotated <= baseline, (
        "credential rotation admitted MORE units than baseline "
        f"({rotated} > {baseline}): rotation multiplied capacity"
    )
    assert rotated < _CANDIDATE_UNITS, (
        "rotation admitted every candidate — in-flight consumption stopped "
        "gating after the fingerprint changed"
    )
