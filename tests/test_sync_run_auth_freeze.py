"""Run-auth freeze: a run's credential is resolved once at plan time and frozen
for every later phase (CHAOS-2755).

These tests exercise the full path: ``plan_sync_run`` stamps the credential onto
the ``SyncRun``; ``SyncTaskBootstrap.load`` and
``reference_discovery._load_discovery_context`` prefer the run-stamped credential;
mid-run edits to ``Integration.credential_id`` (repoint) or to the credential's
secret bytes (in-place rotation) can no longer change a stamped run's auth.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.core.encryption import encrypt_value
from dev_health_ops.credentials.fingerprint import (
    AUTH_SOURCE_ENVIRONMENT,
    AUTH_SOURCE_INTEGRATION_CREDENTIAL,
)
from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    Organization,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.watermarks import set_watermark
from dev_health_ops.workers import reference_discovery
from dev_health_ops.workers.sync_bootstrap import (
    RunAuthFingerprintMismatchError,
    SyncTaskBootstrap,
)

ORG_UUID = uuid.UUID("00000000-0000-0000-0000-000000002755")
ORG_ID = str(ORG_UUID)


@pytest.fixture(autouse=True)
def _encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Deterministic key so encrypt_value / decrypt_value round-trip regardless of
    # the ambient (direnv-scrubbed) environment.
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-run-auth-freeze-secret")
    monkeypatch.delenv("SYNC_RUN_AUTH_STRICT", raising=False)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        feature = FeatureFlag(
            key="canonical_incident_ingestion",
            name="Canonical Incident Ingestion",
            category="integrations",
            min_tier="community",
            is_enabled=True,
        )
        session.add_all(
            (
                Organization(
                    id=ORG_UUID,
                    slug="auth-freeze-org",
                    name="Auth Freeze Org",
                    tier="team",
                ),
                feature,
            )
        )
        session.flush()
        session.add(
            OrgFeatureOverride(
                org_id=ORG_UUID,
                feature_id=feature.id,
                is_enabled=True,
            )
        )
        session.commit()
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session: Session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _make_credential(
    session: Session,
    *,
    token: str,
    provider: str = "github",
    name: str = "default",
    is_active: bool = True,
) -> IntegrationCredential:
    config = (
        {"account_id": "acme", "subdomain": "acme"} if provider == "pagerduty" else {}
    )
    credential = IntegrationCredential(
        provider=provider,
        name=name,
        org_id=ORG_ID,
        credentials_encrypted=encrypt_value(json.dumps({"token": token})),
        config=config,
        is_active=is_active,
    )
    session.add(credential)
    session.flush()
    return credential


def _make_integration(
    session: Session,
    *,
    provider: str = "github",
    credential_id: uuid.UUID | None = None,
) -> Integration:
    integration = Integration(
        org_id=ORG_ID,
        provider=provider,
        name=f"{provider} integration",
        config={},
        is_active=True,
        credential_id=credential_id,
    )
    session.add(integration)
    session.flush()
    return integration


def _make_source(
    session: Session,
    integration: Integration,
    *,
    external_id: str = "full-chaos/dev-health",
) -> IntegrationSource:
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider=integration.provider,
        source_type="repo",
        external_id=external_id,
        name=external_id.rsplit("/", 1)[-1],
        full_name=external_id,
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.flush()
    return source


def _make_dataset(
    session: Session,
    integration: Integration,
    *,
    dataset_key: str = "commits",
    options: dict[str, object] | None = None,
) -> IntegrationDataset:
    dataset = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=True,
        options=options or {},
    )
    session.add(dataset)
    session.flush()
    return dataset


def _plan(
    session: Session,
    integration: Integration,
    *,
    mode: str = SyncRunMode.INCREMENTAL.value,
    dataset_key: str = "commits",
    dataset_options: dict[str, object] | None = None,
    source_external_id: str = "full-chaos/dev-health",
) -> tuple[SyncRun, SyncRunUnit]:
    _make_source(session, integration, external_id=source_external_id)
    _make_dataset(
        session,
        integration,
        dataset_key=dataset_key,
        options=dataset_options,
    )
    plan = plan_sync_run(
        session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=mode,
            triggered_by="manual",
        ),
    )
    run = session.get(SyncRun, plan.sync_run_id)
    assert run is not None
    unit = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == plan.sync_run_id,
            SyncRunUnit.provider == integration.provider,
            SyncRunUnit.dataset_key == dataset_key,
        )
        .one()
    )
    return run, unit


def test_full_resync_bootstrap_ignores_persisted_watermark(db_session: Session) -> None:
    credential = _make_credential(db_session, token="tok-A", provider="pagerduty")
    integration = _make_integration(
        db_session,
        provider="pagerduty",
        credential_id=credential.id,
    )
    _run, unit = _plan(
        db_session,
        integration,
        mode=SyncRunMode.FULL_RESYNC.value,
        dataset_key="incidents",
        source_external_id="acme",
    )
    historical_watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    set_watermark(
        db_session,
        ORG_ID,
        "acme",
        "incidents",
        historical_watermark,
    )

    context = SyncTaskBootstrap.load(db_session, str(unit.id))

    assert context.resume_cursor is None


def test_pagerduty_executable_plan_rejects_missing_credential(
    db_session: Session,
) -> None:
    integration = _make_integration(db_session, provider="pagerduty")
    _make_source(db_session, integration, external_id="acme")
    _make_dataset(db_session, integration, dataset_key="incidents")

    with pytest.raises(ValueError, match="organization-scoped credential"):
        plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=ORG_ID,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="manual",
            ),
        )


def test_bootstrap_loads_persisted_dataset_options(db_session: Session) -> None:
    credential = _make_credential(db_session, token="tok-A", provider="pagerduty")
    integration = _make_integration(
        db_session,
        provider="pagerduty",
        credential_id=credential.id,
    )
    _run, unit = _plan(
        db_session,
        integration,
        dataset_key="incident-alerts",
        dataset_options={"enrichment_cap": 2, "enabled": False},
    )

    context = SyncTaskBootstrap.load(db_session, str(unit.id))

    assert context.dataset_options == {
        "enrichment_cap": 2,
        "enabled": False,
        "legacy_targets": ["operational"],
    }


def test_midrun_credential_repoint_does_not_change_stamped_run_auth(db_session):
    """The core invariant: repointing Integration.credential_id AFTER planning
    leaves every later resolution on the stamped credential."""
    cred_a = _make_credential(db_session, token="tok-A", name="primary")
    cred_b = _make_credential(db_session, token="tok-B", name="secondary")
    integration = _make_integration(db_session, credential_id=cred_a.id)

    run, unit = _plan(db_session, integration)
    assert run.auth_source == AUTH_SOURCE_INTEGRATION_CREDENTIAL
    assert run.credential_id == cred_a.id

    # Mid-run repoint of the mutable integration pointer.
    integration.credential_id = cred_b.id
    db_session.flush()

    ctx = SyncTaskBootstrap.load(db_session, str(unit.id))
    # Frozen on the stamped credential, NOT the repointed one.
    assert ctx.credential_id == str(cred_a.id)
    assert ctx.decrypted_credentials.get("token") == "tok-A"


def test_null_stamp_legacy_run_falls_back_to_integration(db_session):
    """Pre-migration runs (auth_source NULL) keep working via the mutable
    integration.credential_id path, and remain mutable (not frozen)."""
    cred_a = _make_credential(db_session, token="tok-A", name="primary")
    cred_b = _make_credential(db_session, token="tok-B", name="secondary")
    integration = _make_integration(db_session, credential_id=cred_a.id)
    source = _make_source(db_session, integration)

    # A run persisted BEFORE this feature: no stamp columns set.
    run = SyncRun(
        org_id=ORG_ID,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(run)
    db_session.flush()
    unit = SyncRunUnit(
        org_id=ORG_ID,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider=integration.provider,
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
    )
    db_session.add(unit)
    db_session.flush()

    assert run.auth_source is None

    ctx = SyncTaskBootstrap.load(db_session, str(unit.id))
    assert ctx.credential_id == str(cred_a.id)
    assert ctx.decrypted_credentials.get("token") == "tok-A"

    # Legacy runs stay on the mutable path: repointing changes resolution.
    integration.credential_id = cred_b.id
    db_session.flush()
    ctx2 = SyncTaskBootstrap.load(db_session, str(unit.id))
    assert ctx2.credential_id == str(cred_b.id)
    assert ctx2.decrypted_credentials.get("token") == "tok-B"


def test_env_auth_run_stamped_as_environment(db_session, monkeypatch):
    """A NULL integration credential yields auth_source='environment', which is
    distinguishable from a legacy NULL stamp (auth_source column itself NULL)."""
    for env_var in ("GITHUB_TOKEN", "GITHUB_URL", "GITHUB_APP_ID"):
        monkeypatch.delenv(env_var, raising=False)
    integration = _make_integration(db_session, credential_id=None)

    run, _unit = _plan(db_session, integration)

    assert run.auth_source == AUTH_SOURCE_ENVIRONMENT
    assert run.auth_source is not None  # distinguishable from legacy NULL stamp
    assert run.credential_id is None
    assert isinstance(run.credential_fingerprint, str)
    assert len(run.credential_fingerprint) == 64


def test_stamp_contains_no_raw_secret_material(db_session):
    """The stamped fingerprint is a safe-scope digest — no token bytes and NOT
    the full-payload runtime-cache hash."""
    from dev_health_ops.workers.sync_bootstrap import (
        _credential_fingerprint as full_payload_fingerprint,
    )

    secret = "super-secret-token-value-1234567890"
    cred = _make_credential(db_session, token=secret)
    integration = _make_integration(db_session, credential_id=cred.id)

    run, _unit = _plan(db_session, integration)

    fingerprint = run.credential_fingerprint
    assert isinstance(fingerprint, str)
    assert len(fingerprint) == 64
    # Raw secret never appears in the persisted witness.
    assert secret not in fingerprint
    # Must NOT be the full-payload hash of the decrypted secret mapping.
    assert fingerprint != full_payload_fingerprint({"token": secret})


def test_reference_discovery_uses_run_stamped_credential(db_session, monkeypatch):
    """Discovery resolves the SAME credential as the units of the same run, even
    after the integration pointer is repointed mid-run."""
    import dev_health_ops.db as db

    cred_a = _make_credential(db_session, token="tok-A", name="primary")
    cred_b = _make_credential(db_session, token="tok-B", name="secondary")
    integration = _make_integration(db_session, credential_id=cred_a.id)
    run, _unit = _plan(db_session, integration)

    integration.credential_id = cred_b.id
    db_session.flush()

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )
    context = reference_discovery._load_discovery_context(run.id)
    assert context["credentials"].get("token") == "tok-A"


def test_fingerprint_mismatch_warns_by_default_fails_when_strict(
    db_session, monkeypatch, caplog
):
    """An in-place secret edit (same credential id, rotated token): warn and
    continue with the new secret by default; hard-fail under SYNC_RUN_AUTH_STRICT."""
    cred = _make_credential(db_session, token="tok-A")
    integration = _make_integration(db_session, credential_id=cred.id)
    run, unit = _plan(db_session, integration)
    stamped_fingerprint = run.credential_fingerprint

    # Rotate the secret in place (same credential id) — the stamped fingerprint
    # no longer matches the credential's content.
    cred.credentials_encrypted = encrypt_value(json.dumps({"token": "tok-A-rotated"}))
    db_session.flush()

    # Default: warn-and-continue with the new secret.
    with caplog.at_level("WARNING"):
        ctx = SyncTaskBootstrap.load(db_session, str(unit.id))
    assert ctx.decrypted_credentials.get("token") == "tok-A-rotated"
    assert ctx.credential_id == str(cred.id)
    assert any(
        "sync_run_auth.fingerprint_mismatch" in record.message
        or "fingerprint_mismatch" in record.getMessage()
        for record in caplog.records
    )
    # The stamp itself is untouched (freeze is about reads, not rewrites).
    assert run.credential_fingerprint == stamped_fingerprint

    # Strict: the same edit hard-fails the unit non-retryably.
    monkeypatch.setenv("SYNC_RUN_AUTH_STRICT", "1")
    with pytest.raises(RunAuthFingerprintMismatchError):
        SyncTaskBootstrap.load(db_session, str(unit.id))
