"""Persistence for CHAOS-3302's health-rule governance layer:

"rule-version telemetry and persisted fingerprints" and "calibration
record and owner decision for each launch rule".

Deliberately **not** owned by ``dev_runs`` (unlike the rest of
``models/dev_persistence.py``): a ``HealthRuleDefinition`` is code-owned
and global -- it is not produced by, or scoped to, any one run, user, or
organization, so there is no ``org_id``/``user_id`` ownership tuple to
attach it to. Both tables are governance metadata about the *rules
themselves*, not about any tenant's data.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class HealthRuleCalibration(Base):
    """One append-only calibration decision for one rule version.

    Append-only by construction: ``calibration_id`` is unique, and a
    recalibration is a *new* row with a new ``calibration_id`` (e.g. a
    ``.v2`` suffix), never an update to an existing row -- "Version any
    threshold or applicability change; never rewrite historical findings
    in place."
    """

    __tablename__ = "health_rule_calibrations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    calibration_id: Mapped[str] = mapped_column(String(160), nullable=False)
    rule_id: Mapped[str] = mapped_column(String(160), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(160), nullable=False)
    calibration_state: Mapped[str] = mapped_column(String(32), nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    distribution_summary: Mapped[str] = mapped_column(Text, nullable=False)
    false_positive_review: Mapped[str] = mapped_column(Text, nullable=False)
    false_negative_review: Mapped[str] = mapped_column(Text, nullable=False)
    small_cohort_behavior: Mapped[str] = mapped_column(Text, nullable=False)
    owner: Mapped[str] = mapped_column(String(256), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(Date, nullable=False)
    evidence_ref: Mapped[str | None] = mapped_column(String(160), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        UniqueConstraint("calibration_id", name="uq_health_rule_calibrations_id"),
        CheckConstraint(
            "calibration_state IN "
            "('provisional', 'product_approved', 'data_derived', 'policy_driven')",
            name="ck_health_rule_calibrations_state",
        ),
        CheckConstraint(
            "sample_size >= 0", name="ck_health_rule_calibrations_sample_size"
        ),
        CheckConstraint(
            "(calibration_state = 'provisional' AND evidence_ref IS NULL) OR "
            "(calibration_state != 'provisional' AND evidence_ref IS NOT NULL)",
            name="ck_health_rule_calibrations_evidence_ref",
        ),
    )


class HealthRuleVersionFingerprint(Base):
    """One observed (rule_id, rule_version) -> content fingerprint mapping.

    Upserted, never row-per-observation: ``times_seen``/``last_seen_at``
    advance on every observation of an already-known
    ``(rule_id, rule_version)`` pair. The unique constraint on that pair
    is what makes silent drift detectable -- a rule whose definition
    changed without its version changing would observe a *different*
    fingerprint for an *existing* ``(rule_id, rule_version)`` row, which
    ``health_rule_persistence.record_rule_version_fingerprint`` rejects
    rather than overwrites (see that function's docstring).
    """

    __tablename__ = "health_rule_version_fingerprints"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    rule_id: Mapped[str] = mapped_column(String(160), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(160), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    times_seen: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=1, server_default=text("1")
    )

    __table_args__ = (
        UniqueConstraint(
            "rule_id", "rule_version", name="uq_health_rule_version_fingerprints_rule"
        ),
        CheckConstraint(
            "times_seen >= 1", name="ck_health_rule_version_fingerprints_times_seen"
        ),
    )
