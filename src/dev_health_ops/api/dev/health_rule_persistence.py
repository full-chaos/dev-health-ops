"""Persistence for CHAOS-3302's calibration records and rule-version fingerprints.

Two writes, both against ``models.health_rule_governance``:

* ``record_calibration_decision`` -- append-only insert of one
  ``CalibrationRecord``. Never an update: a recalibration is a new row
  with a new ``calibration_id``, matching the append-only ledger
  discipline already used for ``dev_resolution_ledger.v1``
  (``contracts_v2.subject``).
* ``record_rule_version_fingerprint`` -- upserts the observed
  ``(rule_id, rule_version) -> fingerprint`` mapping and, critically,
  *rejects* an observation whose fingerprint disagrees with what is
  already on file for that exact ``(rule_id, rule_version)`` pair. That
  is the actual teeth behind "persisted rule fingerprints": a rule
  definition that changed without its version changing is a governance
  violation (CHAOS-3302 requires "Version any threshold or applicability
  change"), and this makes that violation a hard failure at write time
  rather than a silent drift only a manifest diff would ever catch.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.health_rule_governance import (
    HealthRuleCalibration,
    HealthRuleVersionFingerprint,
)

from .contracts_v2.health_rules import CalibrationRecord
from .health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    HealthRuleDefinition,
    HealthRuleRegistry,
    rule_version_fingerprint,
)


class HealthRuleFingerprintDriftError(RuntimeError):
    """A rule's content changed without a version bump.

    The rule's ``(rule_id, rule_version)`` pair already has a recorded
    fingerprint that disagrees with the one just computed -- exactly the
    failure mode "persisted rule fingerprints" exists to catch.
    """


class DuplicateCalibrationError(RuntimeError):
    """A calibration_id was already recorded -- calibration is append-only."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def record_calibration_decision(
    session: AsyncSession, record: CalibrationRecord
) -> HealthRuleCalibration:
    existing = await session.scalar(
        select(HealthRuleCalibration).where(
            HealthRuleCalibration.calibration_id == record.calibration_id
        )
    )
    if existing is not None:
        raise DuplicateCalibrationError(
            f"calibration_id {record.calibration_id!r} already recorded -- "
            "calibration decisions are append-only; use a new calibration_id"
        )
    row = HealthRuleCalibration(
        calibration_id=record.calibration_id,
        rule_id=record.rule_id,
        rule_version=record.rule_version,
        calibration_state=record.calibration_state.value,
        sample_size=record.sample_size,
        distribution_summary=record.distribution_summary,
        false_positive_review=record.false_positive_review,
        false_negative_review=record.false_negative_review,
        small_cohort_behavior=record.small_cohort_behavior,
        owner=record.owner,
        decided_at=record.decided_at,
        evidence_ref=record.evidence_ref,
        notes=record.notes,
        created_at=_utc_now(),
    )
    session.add(row)
    await session.flush()
    return row


async def _record_fingerprint_for_rule(
    session: AsyncSession, rule: HealthRuleDefinition
) -> HealthRuleVersionFingerprint:
    """Core fingerprint-persistence logic for an explicitly supplied rule.

    NOT the production seam -- see ``record_rule_version_fingerprint``
    below, which is hard-bound to ``HEALTH_RULE_REGISTRY``. This accepts an
    arbitrary ``HealthRuleDefinition`` (Codex-confirmed finding, 2026-08-01,
    round 4): a caller-supplied definition sharing a canonical
    ``rule_id``/``rule_version`` but differing in some other field (e.g. a
    mutated ``threshold``) could otherwise poison the authoritative
    fingerprint row on first write through the production seam, causing the
    REAL canonical rule to be rejected as drift later. This function exists
    only so tests can prove the drift-rejection mechanism itself against a
    deliberately mutated rule (``test_postgres_fingerprint_drift_without_
    version_bump_is_rejected``) -- never for a caller to persist a
    fingerprint for anything other than a rule already resolved from the
    canonical registry.
    """

    fingerprint = rule_version_fingerprint(rule)
    now = _utc_now()
    existing = await session.scalar(
        select(HealthRuleVersionFingerprint).where(
            HealthRuleVersionFingerprint.rule_id == rule.rule_id,
            HealthRuleVersionFingerprint.rule_version == rule.rule_version,
        )
    )
    if existing is None:
        row = HealthRuleVersionFingerprint(
            rule_id=rule.rule_id,
            rule_version=rule.rule_version,
            fingerprint=fingerprint,
            first_seen_at=now,
            last_seen_at=now,
            times_seen=1,
        )
        session.add(row)
        await session.flush()
        return row
    if existing.fingerprint != fingerprint:
        raise HealthRuleFingerprintDriftError(
            f"rule {rule.rule_id!r} version {rule.rule_version!r} fingerprint "
            f"changed from {existing.fingerprint!r} to {fingerprint!r} without "
            "a version bump"
        )
    existing.last_seen_at = now
    existing.times_seen += 1
    await session.flush()
    return existing


async def record_rule_version_fingerprint(
    session: AsyncSession, rule_id: str
) -> HealthRuleVersionFingerprint:
    """The production persistence seam: hard-bound to ``HEALTH_RULE_REGISTRY``.

    Takes a ``rule_id``, not an arbitrary ``HealthRuleDefinition``
    (Codex-confirmed finding, 2026-08-01, round 4) -- the rule persisted is
    always resolved from the canonical, construction-validated,
    inventory-cross-checked, rebind-resistant module singleton, so a
    caller cannot smuggle a rule sharing a canonical id/version but
    differing in some other field through this seam. Resolution happens
    before ``session`` is touched at all, so an unresolvable ``rule_id``
    (``UnknownRuleError``) never reaches the database.
    """

    rule = HEALTH_RULE_REGISTRY.rule(rule_id)
    return await _record_fingerprint_for_rule(session, rule)


async def record_registry_fingerprints(
    session: AsyncSession, registry: HealthRuleRegistry = HEALTH_RULE_REGISTRY
) -> list[HealthRuleVersionFingerprint]:
    """Convenience: record every canonical rule's fingerprint in one pass."""

    return [
        await record_rule_version_fingerprint(session, rule_id) for rule_id in registry
    ]
