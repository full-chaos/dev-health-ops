from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType

import sqlalchemy as sa
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine

_FEATURE_KEY = "canonical_incident_ingestion"
_VERSIONS_DIR = (
    Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic" / "versions"
)


def _canonical_incident_migration_module() -> ModuleType:
    matching_paths = [
        path
        for path in _VERSIONS_DIR.glob("0042_*.py")
        if _FEATURE_KEY in path.read_text(encoding="utf-8")
    ]
    assert len(matching_paths) == 1
    return importlib.import_module(
        f"dev_health_ops.alembic.versions.{matching_paths[0].stem}"
    )


def test_canonical_incident_feature_seed_is_global_only_and_idempotent() -> None:
    migration = _canonical_incident_migration_module()
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
                    CREATE TABLE org_feature_overrides (
                        id TEXT PRIMARY KEY,
                        org_id TEXT NOT NULL,
                        feature_id TEXT NOT NULL,
                        is_enabled BOOLEAN NOT NULL
                    )
                    """
                )
            )
            context = MigrationContext.configure(conn)
            with Operations.context(context):
                migration.upgrade()
                migration.upgrade()

            rows = conn.execute(
                sa.text(
                    """
                    SELECT key, name, category, min_tier, is_enabled
                    FROM feature_flags
                    WHERE key = :key
                    """
                ),
                {"key": _FEATURE_KEY},
            ).all()
            positive_org_rows = conn.execute(
                sa.text(
                    """
                    SELECT COUNT(*)
                    FROM org_feature_overrides
                    WHERE is_enabled = TRUE
                    """
                )
            ).scalar_one()

        assert rows == [
            (
                _FEATURE_KEY,
                "Canonical Incident Ingestion",
                "integrations",
                "community",
                1,
            )
        ]
        assert positive_org_rows == 0
    finally:
        engine.dispose()


def test_canonical_incident_feature_seed_is_in_single_head_graph() -> None:
    migration = _canonical_incident_migration_module()
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)
    revisions = {script.revision for script in scripts.walk_revisions()}

    assert migration.down_revision == "0041"
    assert migration.revision in revisions
    assert scripts.get_heads() == ["0060"]
    assert scripts.get_revision("0043").down_revision == "0042"
    assert scripts.get_revision("0044").down_revision == "0043"
    assert scripts.get_revision("0045").down_revision == "0044"
    assert scripts.get_revision("0046").down_revision == "0045"
    assert scripts.get_revision("0047").down_revision == "0046"
    assert scripts.get_revision("0048").down_revision == "0047"
    assert scripts.get_revision("0049").down_revision == "0048"
    assert scripts.get_revision("0050").down_revision == "0049"
    assert scripts.get_revision("0051").down_revision == "0050"
    assert scripts.get_revision("0052").down_revision == "0051"
    assert scripts.get_revision("0053").down_revision == "0052"
    assert scripts.get_revision("0054").down_revision == "0053"
    assert scripts.get_revision("0055").down_revision == "0054"
    assert scripts.get_revision("0056").down_revision == "0055"
    assert scripts.get_revision("0058").down_revision == "0057"
    assert scripts.get_revision("0059").down_revision == "0058"
    assert scripts.get_revision("0060").down_revision == "0059"
