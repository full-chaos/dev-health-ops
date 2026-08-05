"""CHAOS-3219 Codex adversarial review (CRITICAL, 2026-08-05):

``dev-hops fixtures world --sink``/``--postgres-uri`` accepted ANY
ClickHouse/Postgres URI, including the shared dev ``default``/``devhealth``
databases, with no check at all -- the world flow issues destructive
mutations (``ALTER TABLE ... DELETE``, delete-and-reinsert aging,
``DROP``/``CREATE DATABASE`` in normal operator setup) that must never
reach a real database.

This file proves the fix: :func:`_require_scratch_database` rejects every
known-unsafe target and the missing-scratch-marker case, accepts an
explicit scratch name, and -- the RED test that matters most -- proves
``run_fixtures_world`` refuses BEFORE ever calling ``_generate_world`` /
``_verify_digest`` (i.e. before any connection/migration), not merely
somewhere downstream after damage could already have been done.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from dev_health_ops.fixtures.world import (
    UnsafeSinkError,
    _database_name_from_uri,
    _require_scratch_database,
    run_fixtures_world,
)

_WORLD_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
)


class TestDatabaseNameFromUri:
    def test_extracts_path_component(self) -> None:
        assert (
            _database_name_from_uri("clickhouse://ch:ch@localhost:8123/my_scratch")
            == "my_scratch"
        )

    def test_strips_query_string(self) -> None:
        assert (
            _database_name_from_uri(
                "postgresql+asyncpg://u:p@host:5432/foo_scratch?sslmode=disable"
            )
            == "foo_scratch"
        )

    def test_missing_path_is_empty(self) -> None:
        assert _database_name_from_uri("clickhouse://ch:ch@localhost:8123") == ""


class TestRequireScratchDatabase:
    def test_rejects_none(self) -> None:
        with pytest.raises(UnsafeSinkError, match="no clickhouse URI"):
            _require_scratch_database(None, kind="clickhouse")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(UnsafeSinkError):
            _require_scratch_database("", kind="clickhouse")

    def test_rejects_clickhouse_default(self) -> None:
        with pytest.raises(UnsafeSinkError, match="'default'"):
            _require_scratch_database(
                "clickhouse://ch:ch@localhost:8123/default", kind="clickhouse"
            )

    def test_rejects_clickhouse_missing_path_same_as_default(self) -> None:
        """clickhouse-connect treats an omitted path as the `default`
        database -- the guard must treat the empty name identically."""
        with pytest.raises(UnsafeSinkError):
            _require_scratch_database(
                "clickhouse://ch:ch@localhost:8123", kind="clickhouse"
            )

    def test_rejects_postgres_devhealth(self) -> None:
        with pytest.raises(UnsafeSinkError, match="'devhealth'"):
            _require_scratch_database(
                "postgresql+asyncpg://devhealth:devhealth@localhost:5432/devhealth",
                kind="postgres",
            )

    def test_rejects_postgres_admin_default(self) -> None:
        with pytest.raises(UnsafeSinkError, match="'postgres'"):
            _require_scratch_database(
                "postgresql+asyncpg://u:p@localhost:5432/postgres", kind="postgres"
            )

    def test_rejects_postgres_templates(self) -> None:
        for template in ("template0", "template1"):
            with pytest.raises(UnsafeSinkError, match=template):
                _require_scratch_database(
                    f"postgresql+asyncpg://u:p@localhost:5432/{template}",
                    kind="postgres",
                )

    def test_rejects_database_missing_scratch_marker(self) -> None:
        """A denylist alone is insufficient -- an unenumerated real
        database name (here: a plausible future project db) must ALSO be
        rejected because it lacks the positive scratch marker."""
        with pytest.raises(UnsafeSinkError, match="scratch"):
            _require_scratch_database(
                "clickhouse://ch:ch@localhost:8123/some_other_real_project_db",
                kind="clickhouse",
            )

    def test_accepts_explicit_scratch_name(self) -> None:
        _require_scratch_database(
            "clickhouse://ch:ch@localhost:8123/chaos3219_world_scratch",
            kind="clickhouse",
        )
        _require_scratch_database(
            "postgresql+asyncpg://u:p@localhost:5432/chaos3219_world_scratch_b",
            kind="postgres",
        )

    def test_accepts_ci_local_validate_convention(self) -> None:
        """Matches ci/local_validate.sh's own scratch naming convention."""
        _require_scratch_database(
            "clickhouse://ch:ch@localhost:8123/ci_local_validate_05a00cac4194",
            kind="clickhouse",
        )

    def test_marker_check_is_case_insensitive(self) -> None:
        _require_scratch_database(
            "clickhouse://ch:ch@localhost:8123/MY_SCRATCH_DB", kind="clickhouse"
        )


def _ns_with_defaults(**overrides) -> argparse.Namespace:
    ns = argparse.Namespace(
        manifest=str(_WORLD_DIR / "world.json"),
        sink="clickhouse://ch:ch@localhost:8123/chaos3219_world_scratch",
        postgres_uri="postgresql+asyncpg://u:p@localhost:5432/chaos3219_world_scratch",
        digest_path=None,
        verify_digest=False,
        allow_mixed_org=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


@pytest.mark.asyncio
class TestRunFixturesWorldRefusesBeforeAnyConnection:
    """The RED test that matters: prove the guard fires BEFORE
    `_generate_world`/`_verify_digest` are ever reached, not just that it
    fires eventually. A guard bolted on somewhere downstream, after a
    connection or a migration already ran, would not satisfy the CRITICAL
    finding even if it happened to also return 1."""

    async def test_unsafe_clickhouse_sink_blocks_generation(self, monkeypatch) -> None:
        from dev_health_ops.fixtures import world as world_module

        called = []

        async def _must_not_be_called(*args, **kwargs):
            called.append(True)
            raise AssertionError(
                "_generate_world must never be reached when --sink is unsafe"
            )

        monkeypatch.setattr(world_module, "_generate_world", _must_not_be_called)

        ns = _ns_with_defaults(sink="clickhouse://ch:ch@localhost:8123/default")
        rc = await run_fixtures_world(ns)

        assert rc == 1
        assert called == [], "the guard must short-circuit before generation runs"

    async def test_unsafe_postgres_uri_blocks_generation(self, monkeypatch) -> None:
        from dev_health_ops.fixtures import world as world_module

        called = []

        async def _must_not_be_called(*args, **kwargs):
            called.append(True)
            raise AssertionError(
                "_generate_world must never be reached when --postgres-uri is unsafe"
            )

        monkeypatch.setattr(world_module, "_generate_world", _must_not_be_called)

        ns = _ns_with_defaults(
            postgres_uri="postgresql+asyncpg://devhealth:devhealth@localhost:5432/devhealth"
        )
        rc = await run_fixtures_world(ns)

        assert rc == 1
        assert called == [], "the guard must short-circuit before generation runs"

    async def test_unsafe_sink_also_blocks_verify_digest(self, monkeypatch) -> None:
        from dev_health_ops.fixtures import world as world_module

        called = []

        async def _must_not_be_called(*args, **kwargs):
            called.append(True)
            raise AssertionError(
                "_verify_digest must never be reached when --sink is unsafe"
            )

        monkeypatch.setattr(world_module, "_verify_digest", _must_not_be_called)

        ns = _ns_with_defaults(
            sink="clickhouse://ch:ch@localhost:8123/default", verify_digest=True
        )
        rc = await run_fixtures_world(ns)

        assert rc == 1
        assert called == []

    async def test_safe_uris_reach_generation(self, monkeypatch) -> None:
        """Inverse proof: the guard is not simply always-refusing -- a
        genuinely scratch-named pair of URIs passes through to
        `_generate_world`."""
        from dev_health_ops.fixtures import world as world_module

        called = []

        async def _fake_generate_world(ns, manifest):
            called.append(True)
            return 0

        monkeypatch.setattr(world_module, "_generate_world", _fake_generate_world)

        ns = _ns_with_defaults()  # both URIs are scratch-named despite the helper name
        rc = await run_fixtures_world(ns)

        assert rc == 0
        assert called == [True]
