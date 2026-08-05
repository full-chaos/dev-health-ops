"""CHAOS-3219: `dev-hops fixtures world` CLI wiring + multi-org mixed-org
guard extension.

Mirrors ``tests/test_fixtures_mixed_org_guard.py``'s style: argparse wiring
assertions plus a monkeypatched ``run_fixtures_generation`` to prove the
per-repo loop actually surfaces a guard refusal instead of silently
continuing past it.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from dev_health_ops.fixtures.runner import register_commands
from dev_health_ops.fixtures.world import (
    WorldManifest,
    _generate_world,
    _generation_namespace,
)

_WORLD_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
)


def _world_namespace(**overrides) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    args = [
        "fixtures",
        "world",
        "--manifest",
        str(_WORLD_DIR / "world.json"),
        "--sink",
        "clickhouse://stub:8123/stub",
    ]
    ns = parser.parse_args(args)
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


def test_world_parser_requires_manifest() -> None:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    with pytest.raises(SystemExit):
        parser.parse_args(["fixtures", "world", "--sink", "clickhouse://x/y"])


def test_world_parser_defaults() -> None:
    ns = _world_namespace()
    assert ns.verify_digest is False
    assert ns.allow_mixed_org is False
    assert ns.postgres_uri is None
    assert ns.digest_path is None
    assert callable(ns.func)


def test_world_parser_verify_digest_flag() -> None:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    ns2 = parser.parse_args(
        [
            "fixtures",
            "world",
            "--manifest",
            str(_WORLD_DIR / "world.json"),
            "--verify-digest",
        ]
    )
    assert ns2.verify_digest is True


def test_world_func_is_a_coroutine_function() -> None:
    """cli.py dispatches via `inspect.iscoroutinefunction(func)` ->
    `asyncio.run(func(ns))`; a plain-def wrapper here would silently return
    an un-awaited coroutine and crash with `int(coroutine)` (a real defect
    caught during Lane 1a's own live verification run)."""

    import inspect

    ns = _world_namespace()
    assert inspect.iscoroutinefunction(ns.func)


class TestGenerationNamespace:
    def test_repo_count_one_preserves_exact_repo_name(self) -> None:
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="meridian/web-app",
            sink="clickhouse://stub/stub",
            allow_mixed_org=False,
        )
        assert ns.repo_name == "meridian/web-app"
        assert ns.repo_count == 1

    def test_no_data_probe_repos_use_ordinary_generation_profile(self) -> None:
        """generate_commits/generate_work_items raise on a zero-volume
        profile (`random.randint(1, 0)`) -- the no-data/measured-zero probes
        must generate normally and be zeroed POST-hoc instead (see
        world.NO_DATA_PROBE_REPOS / _run_clickhouse_postprocess)."""
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="probe/source-no-data",
            sink="clickhouse://stub/stub",
            allow_mixed_org=False,
        )
        assert ns.commits_per_day > 0
        assert ns.pr_count > 0

    def test_truncated_probe_gets_elevated_volume(self) -> None:
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        default_ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="meridian/web-app",
            sink="clickhouse://stub/stub",
            allow_mixed_org=False,
        )
        truncated_ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="probe/source-truncated-workgraph",
            sink="clickhouse://stub/stub",
            allow_mixed_org=False,
        )
        assert truncated_ns.pr_count > default_ns.pr_count
        assert truncated_ns.commits_per_day > default_ns.commits_per_day


@pytest.mark.asyncio
async def test_multi_org_world_stops_on_first_mixed_org_refusal(monkeypatch) -> None:
    """Extends the CHAOS-2778 mixed-org guard to the multi-org/multi-repo
    world loop: `run_fixtures_generation` already runs `_ensure_org_
    unpolluted` per call (unchanged, reused verbatim -- see world.py's
    docstring) -- this proves `_generate_world`'s per-repo loop actually
    SURFACES that refusal (stops and returns nonzero) instead of treating a
    guard failure as skippable and moving on to the next repo/org, which
    would silently under-generate the world while still exiting 0."""

    from dev_health_ops.fixtures import world as world_module

    manifest = world_module.load_world_manifest(_WORLD_DIR / "world.json")

    calls: list[str] = []

    async def _fake_run_fixtures_generation(ns: argparse.Namespace) -> int:
        calls.append(ns.repo_name)
        # Simulate MixedOrgError surfaced as exit code 1 by the SECOND repo
        # call (mirrors run_fixtures_generation's own MixedOrgError -> 1
        # contract in runner.py).
        if len(calls) == 2:
            return 1
        return 0

    async def _fake_postgres_phase(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(
        world_module, "run_fixtures_generation", _fake_run_fixtures_generation
    )
    monkeypatch.setattr(world_module, "_run_postgres_phase", _fake_postgres_phase)
    monkeypatch.setenv("POSTGRES_URI", "postgresql+asyncpg://stub/stub")

    ns = _world_namespace()
    rc = await _generate_world(ns, manifest)

    assert rc == 1
    assert len(calls) == 2, (
        "the loop must stop at the first refusal, not silently continue "
        "generating the remaining roster"
    )
