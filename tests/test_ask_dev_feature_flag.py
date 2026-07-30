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
    ASK_DEV_FEATURE,
    get_features_for_tier,
    is_explicit_purchase_feature,
)
from dev_health_ops.licensing.types import LicenseTier


def test_ask_dev_defaults_disabled_without_changing_independent_gates() -> None:
    for tier in LicenseTier:
        features = get_features_for_tier(tier)
        assert features[ASK_DEV_FEATURE] is False
    assert is_explicit_purchase_feature(ASK_DEV_FEATURE) is True
    assert is_explicit_purchase_feature("agent_context_runtime") is True
    assert get_features_for_tier(LicenseTier.TEAM)["byo_llm"] is True


def test_ask_dev_requires_explicit_enable_and_honors_global_kill_switch() -> None:
    def context(
        *,
        globally_enabled: bool = True,
        org_override: FeatureOverrideSnapshot | None = None,
    ) -> FeatureDecisionContext:
        return FeatureDecisionContext(
            feature_key=ASK_DEV_FEATURE,
            is_registered=True,
            is_storage_valid=True,
            globally_enabled=globally_enabled,
            min_tier=LicenseTier.COMMUNITY,
            org_tier=LicenseTier.ENTERPRISE,
            org_override=org_override,
            license_override=None,
            evaluated_at=datetime(2026, 7, 28, tzinfo=UTC),
        )

    default = decide_feature(context())
    enabled = decide_feature(
        context(org_override=FeatureOverrideSnapshot(is_enabled=True))
    )
    killed = decide_feature(
        context(
            globally_enabled=False,
            org_override=FeatureOverrideSnapshot(is_enabled=True),
        )
    )
    assert (default.allowed, default.reason) == (
        False,
        FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED,
    )
    assert (enabled.allowed, enabled.reason) == (
        True,
        FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE,
    )
    assert (killed.allowed, killed.reason) == (
        False,
        FeatureDecisionReason.GLOBAL_DISABLED,
    )


def test_migration_0067_is_additive_idempotent_and_preserves_other_gates() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0067_seed_ask_dev_feature_flag"
    )
    assert migration.revision == "0067"
    assert migration.down_revision == "0065"
    assert migration.branch_labels == ("application_schema",)
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
            for key in ("byo_llm", "agent_context_runtime"):
                conn.execute(
                    sa.text(
                        """
                        INSERT INTO feature_flags
                            (id, key, name, category, min_tier, is_enabled,
                             is_beta, is_deprecated, created_at, updated_at)
                        VALUES
                            (:id, :key, :key, 'analytics', 'community', TRUE,
                             FALSE, FALSE, :now, :now)
                        """
                    ),
                    {"id": str(uuid.uuid4()), "key": key, "now": datetime.now(UTC)},
                )
            context = MigrationContext.configure(conn)
            with Operations.context(context):
                migration.upgrade()
                migration.upgrade()
                rows = conn.execute(
                    sa.text("SELECT key, is_enabled FROM feature_flags ORDER BY key")
                ).all()
                assert rows == [
                    ("agent_context_runtime", 1),
                    ("ask_dev", 1),
                    ("byo_llm", 1),
                ]
                migration.downgrade()
                remaining = conn.execute(
                    sa.text("SELECT key FROM feature_flags ORDER BY key")
                ).scalars()
                assert list(remaining) == [
                    "agent_context_runtime",
                    "ask_dev",
                    "byo_llm",
                ]
    finally:
        engine.dispose()
