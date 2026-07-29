from __future__ import annotations

import importlib
import uuid
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine

from dev_health_ops.licensing.feature_policy import (
    FeatureDecisionContext,
    FeatureDecisionReason,
    FeatureOverrideSnapshot,
    decide_feature,
)
from dev_health_ops.licensing.registry import (
    ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE,
    ASK_DEV_FEATURE,
    get_features_for_tier,
    is_explicit_purchase_feature,
)
from dev_health_ops.licensing.types import LicenseTier


def test_contextual_entrypoints_default_disabled_for_every_tier() -> None:
    for tier in LicenseTier:
        features = get_features_for_tier(tier)
        assert features[ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE] is False
        assert features[ASK_DEV_FEATURE] is False
    assert is_explicit_purchase_feature(ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE)


def test_contextual_entrypoints_require_an_explicit_override() -> None:
    def context(
        org_override: FeatureOverrideSnapshot | None = None,
    ) -> FeatureDecisionContext:
        return FeatureDecisionContext(
            feature_key=ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE,
            is_registered=True,
            is_storage_valid=True,
            globally_enabled=True,
            min_tier=LicenseTier.COMMUNITY,
            org_tier=LicenseTier.ENTERPRISE,
            org_override=org_override,
            license_override=None,
            evaluated_at=datetime(2026, 7, 29, tzinfo=UTC),
        )

    denied = decide_feature(context())
    enabled = decide_feature(context(FeatureOverrideSnapshot(is_enabled=True)))
    assert (denied.allowed, denied.reason) == (
        False,
        FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED,
    )
    assert (enabled.allowed, enabled.reason) == (
        True,
        FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE,
    )


def test_migration_0070_is_additive_idempotent_and_preserves_base_ask_dev() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0070_seed_ask_dev_contextual_entrypoints_feature_flag"
    )
    assert migration.revision == "0070"
    assert migration.down_revision == "0069"
    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    """
                    CREATE TABLE feature_flags (
                        id TEXT PRIMARY KEY,
                        key TEXT NOT NULL UNIQUE,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        min_tier TEXT NOT NULL,
                        is_enabled BOOLEAN NOT NULL,
                        is_beta BOOLEAN NOT NULL,
                        is_deprecated BOOLEAN NOT NULL,
                        created_at DATETIME NOT NULL,
                        updated_at DATETIME NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                sa.text(
                    """
                    INSERT INTO feature_flags
                        (id, key, name, category, min_tier, is_enabled,
                         is_beta, is_deprecated, created_at, updated_at)
                    VALUES
                        (:id, 'ask_dev', 'Ask Dev', 'analytics', 'community',
                         TRUE, FALSE, FALSE, :now, :now)
                    """
                ),
                {"id": str(uuid.uuid4()), "now": datetime.now(UTC)},
            )
            context = MigrationContext.configure(conn)
            with Operations.context(context):
                migration.upgrade()
                migration.upgrade()
                rows = conn.execute(
                    sa.text("SELECT key, is_enabled FROM feature_flags ORDER BY key")
                ).all()
                assert rows == [
                    ("ask_dev", 1),
                    ("ask_dev_contextual_entrypoints", 1),
                ]
                migration.downgrade()
                remaining = conn.execute(
                    sa.text("SELECT key FROM feature_flags ORDER BY key")
                ).scalars()
                assert list(remaining) == [
                    "ask_dev",
                    "ask_dev_contextual_entrypoints",
                ]
    finally:
        engine.dispose()
