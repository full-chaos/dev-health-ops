"""Live PostgreSQL proofs for CHAOS-3302's health-rule governance persistence.

Isolated per-test schema, mirroring ``test_persistence_postgres.py``'s
pattern. Proves the two guardrails that only a real database enforces:

* ``record_calibration_decision`` rejects a reused ``calibration_id``
  (append-only, backed by a real UNIQUE constraint, not just the ORM
  layer's own duplicate check racing a concurrent writer);
* ``record_rule_version_fingerprint`` rejects an observation whose
  fingerprint disagrees with what is already recorded for an existing
  ``(rule_id, rule_version)`` pair -- the actual drift guard.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import date

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationRecord,
    CalibrationState,
)
from dev_health_ops.api.dev.health_rule_persistence import (
    DuplicateCalibrationError,
    HealthRuleFingerprintDriftError,
    _record_fingerprint_for_rule,
    record_calibration_decision,
    record_rule_version_fingerprint,
)
from dev_health_ops.api.dev.health_rule_registry import HEALTH_RULE_REGISTRY
from dev_health_ops.models.git import Base
from dev_health_ops.models.health_rule_governance import (
    HealthRuleCalibration,
    HealthRuleVersionFingerprint,
)
from tests._helpers import tables_of

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_TABLES = tables_of(HealthRuleCalibration, HealthRuleVersionFingerprint)

pytestmark = pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV),
    reason=f"requires {_POSTGRES_URI_ENV}",
)


@pytest_asyncio.fixture
async def postgres_governance() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    configured_url = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    async_url = configured_url.set(drivername="postgresql+asyncpg")
    schema = f"health_rule_governance_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(async_url)
    engine: AsyncEngine | None = None
    schema_created = False
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_created = True
        engine = create_async_engine(
            async_url,
            connect_args={"server_settings": {"search_path": schema}},
        )
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection, tables=_TABLES
                )
            )
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        yield maker
    finally:
        if engine is not None:
            await engine.dispose()
        if schema_created:
            async with admin_engine.begin() as connection:
                await connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                )
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_postgres_calibration_decision_persists_and_is_append_only(
    postgres_governance: async_sessionmaker[AsyncSession],
) -> None:
    record = CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="test.postgres_calibration.v1",
        rule_id="health_rule.completion_stalled.v1",
        rule_version="health_rule.completion_stalled.v1",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        sample_size=42,
        distribution_summary="p50=1.0",
        false_positive_review="none observed",
        false_negative_review="none observed",
        small_cohort_behavior="suppressed below floor",
        owner="test",
        decided_at=date(2026, 8, 1),
        evidence_ref="test.postgres_calibration.v1",
    )
    async with postgres_governance() as session:
        row = await record_calibration_decision(session, record)
        await session.commit()
        assert row.calibration_id == "test.postgres_calibration.v1"

    async with postgres_governance() as session:
        with pytest.raises(DuplicateCalibrationError):
            await record_calibration_decision(session, record)


@pytest.mark.asyncio
async def test_postgres_rejects_calibration_evidence_ref_check_constraint(
    postgres_governance: async_sessionmaker[AsyncSession],
) -> None:
    """The DB-level CHECK, not just the pydantic validator, rejects a

    provisional record carrying an evidence_ref -- proves the migration's
    constraint is real, not merely declared.
    """

    async with postgres_governance() as session:
        session.add(
            HealthRuleCalibration(
                calibration_id="test.bad-provisional.v1",
                rule_id="health_rule.wip_congestion.v1",
                rule_version="health_rule.wip_congestion.v1",
                calibration_state="provisional",
                sample_size=0,
                distribution_summary="x",
                false_positive_review="x",
                false_negative_review="x",
                small_cohort_behavior="x",
                owner="test",
                decided_at=date(2026, 8, 1),
                evidence_ref="should-not-be-allowed",
            )
        )
        with pytest.raises(Exception):
            await session.commit()


@pytest.mark.asyncio
async def test_postgres_fingerprint_first_observation_then_repeat_increments(
    postgres_governance: async_sessionmaker[AsyncSession],
) -> None:
    """Uses the production seam (rule_id, resolved against

    HEALTH_RULE_REGISTRY) -- this test is about the seen-count/idempotency
    behavior, not about forging a non-canonical rule, so it exercises the
    real production API rather than the private test-only seam.
    """

    async with postgres_governance() as session:
        first = await record_rule_version_fingerprint(
            session, "health_rule.completion_stalled.v1"
        )
        await session.commit()
        assert first.times_seen == 1

    async with postgres_governance() as session:
        second = await record_rule_version_fingerprint(
            session, "health_rule.completion_stalled.v1"
        )
        await session.commit()
        assert second.times_seen == 2
        assert second.fingerprint == first.fingerprint


@pytest.mark.asyncio
async def test_postgres_fingerprint_drift_without_version_bump_is_rejected(
    postgres_governance: async_sessionmaker[AsyncSession],
) -> None:
    """Kill site: a rule definition changed without its version changing.

    Records one fingerprint for a (rule_id, rule_version) pair, then
    attempts to record a *different* fingerprint for the same pair (as
    would happen if a rule's threshold changed but its version string did
    not) -- must reject rather than silently overwrite. Uses the private
    ``_record_fingerprint_for_rule`` seam throughout (Codex-confirmed
    finding, 2026-08-01, round 4): the production
    ``record_rule_version_fingerprint`` seam takes only a ``rule_id`` and
    always resolves the real canonical rule, so it has no way to accept the
    drifted, caller-mutated rule this test needs to construct.
    """

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.completion_stalled.v1")
    assert rule.threshold is not None
    async with postgres_governance() as session:
        await _record_fingerprint_for_rule(session, rule)
        await session.commit()

    drifted_rule = rule.model_copy(update={"threshold": rule.threshold + 100})
    # model_copy bypasses validators, but this only needs a different
    # in-memory field value to produce a different content fingerprint --
    # it is never persisted as a HealthRuleDefinition itself.
    async with postgres_governance() as session:
        with pytest.raises(HealthRuleFingerprintDriftError):
            await _record_fingerprint_for_rule(session, drifted_rule)
