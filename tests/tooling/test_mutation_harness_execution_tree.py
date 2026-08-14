"""Independent contract tests for mutation-harness execution trees."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

import scripts.mutation_harness_execution_tree as execution_tree
from scripts.mutation_harness import HarnessError
from scripts.mutation_harness_execution_tree import (
    OWNERSHIP_MARKER,
    StagedExecutionTree,
    build_source_manifest,
    cleanup_execution_tree,
    create_private_temp_root,
    probe_python_toolchain_independence,
    source_manifest_from_dict,
    source_manifest_to_dict,
    stage_execution_tree,
)


def _git(
    root: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
            *args,
        ],
        cwd=root,
        check=check,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def source_repo(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "tests@example.invalid")
    _git(root, "config", "user.name", "Execution Tree Tests")
    (root / ".gitignore").write_text(".mutation-harness/\nignored-input/\n")
    (root / "tracked.txt").write_bytes(b"committed\n")
    executable = root / "run.sh"
    executable.write_bytes(b"#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    (root / "deleted.txt").write_bytes(b"delete me\n")
    os.symlink("tracked.txt", root / "tracked-link")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "fixture")

    (root / "tracked.txt").write_bytes(b"dirty tracked bytes\n")
    (root / "deleted.txt").unlink()
    executable.chmod(0o744)
    (root / "untracked.txt").write_bytes(b"untracked overlay\n")
    (root / ".mutation-harness").mkdir()
    (root / ".mutation-harness" / "state.json").write_text("must be excluded")
    return root


def test_source_manifest_captures_workspace_without_touching_index(
    source_repo: Path,
) -> None:
    index = source_repo / ".git" / "index"
    index_before = index.read_bytes()

    manifest = build_source_manifest(source_repo)
    entries = {entry.path: entry for entry in manifest.entries}

    assert entries["tracked.txt"].digest == _sha256(b"dirty tracked bytes\n")
    assert entries["deleted.txt"].kind == "deleted"
    assert entries["run.sh"].mode & stat.S_IXUSR
    assert entries["tracked-link"].kind == "symlink"
    assert entries["tracked-link"].link_target == "tracked.txt"
    assert entries["untracked.txt"].tracked is False
    assert ".mutation-harness/state.json" not in entries
    assert index.read_bytes() == index_before
    assert source_manifest_from_dict(source_manifest_to_dict(manifest)) == manifest


def test_manifest_refuses_unignored_harness_state_and_damaged_recovery_record(
    source_repo: Path,
) -> None:
    manifest = build_source_manifest(source_repo)
    damaged = source_manifest_to_dict(manifest)
    damaged["head"] = "different-head"
    with pytest.raises(HarnessError, match="digest mismatch"):
        source_manifest_from_dict(damaged)

    (source_repo / ".gitignore").write_text("ignored-input/\n", encoding="utf-8")
    with pytest.raises(HarnessError, match="not excluded by gitignore"):
        build_source_manifest(source_repo)


def test_manifest_refuses_selective_harness_child_ignore(source_repo: Path) -> None:
    (source_repo / ".gitignore").write_text(
        ".mutation-harness/execution-tree-probe\nignored-input/\n",
        encoding="utf-8",
    )

    with pytest.raises(HarnessError, match="not excluded by gitignore"):
        build_source_manifest(source_repo)


def test_manifest_accepts_directory_ignore_when_state_directory_is_absent(
    source_repo: Path,
) -> None:
    state_directory = source_repo / ".mutation-harness"
    shutil.rmtree(state_directory)

    manifest = build_source_manifest(source_repo)

    assert manifest.entries
    assert not state_directory.exists()


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def test_stage_creates_independent_same_path_overlays(source_repo: Path) -> None:
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "private"
    temporary_root.mkdir(mode=0o700)

    first = stage_execution_tree(
        source_repo,
        temporary_root / "shard-0",
        run_id="run-3807",
        shard_index=0,
        source_manifest=manifest,
        plan_digest="plan-digest",
        workspace_inputs=(),
    )
    second = stage_execution_tree(
        source_repo,
        temporary_root / "shard-1",
        run_id="run-3807",
        shard_index=1,
        source_manifest=manifest,
        plan_digest="plan-digest",
        workspace_inputs=(),
    )

    assert (first.root / "tracked.txt").read_bytes() == b"dirty tracked bytes\n"
    assert not (first.root / "deleted.txt").exists()
    assert stat.S_IMODE((first.root / "run.sh").stat().st_mode) == 0o744
    assert os.readlink(first.root / "tracked-link") == "tracked.txt"
    assert (first.root / "untracked.txt").read_bytes() == b"untracked overlay\n"
    assert not (first.root / ".mutation-harness" / "state.json").exists()
    assert build_source_manifest(first.root) == manifest
    assert _git(first.root, "symbolic-ref", "-q", "HEAD", check=False).returncode != 0

    (first.root / "tracked.txt").write_bytes(b"mutated shard zero\n")
    assert (second.root / "tracked.txt").read_bytes() == b"dirty tracked bytes\n"
    assert (source_repo / "tracked.txt").read_bytes() == b"dirty tracked bytes\n"
    assert build_source_manifest(source_repo) == manifest


def test_private_temp_root_is_mode_0700_and_staging_rejects_wider_mode(
    source_repo: Path,
) -> None:
    private = create_private_temp_root(prefix="chaos-3807-test-")
    try:
        assert stat.S_IMODE(private.stat().st_mode) == 0o700
    finally:
        shutil.rmtree(private)

    wider = source_repo.parent / "wider-root"
    wider.mkdir(mode=0o755)
    wider.chmod(0o755)
    with pytest.raises(HarnessError, match="must have mode 0700"):
        stage_execution_tree(
            source_repo,
            wider / "shard",
            run_id="run-wide",
            shard_index=0,
            source_manifest=build_source_manifest(source_repo),
            plan_digest="plan-digest",
            workspace_inputs=(),
        )


def test_stage_rechecks_invoking_manifest_after_the_overlay(
    source_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "drift-private"
    temporary_root.mkdir(mode=0o700)
    original_copy = execution_tree._copy_manifest_entry

    def copy_then_drift(
        source: Path,
        destination: Path,
        entry: execution_tree.SourceManifestEntry,
    ) -> None:
        original_copy(source, destination, entry)
        if entry == manifest.entries[-1]:
            (source / "tracked.txt").write_bytes(b"drifted during staging\n")

    monkeypatch.setattr(execution_tree, "_copy_manifest_entry", copy_then_drift)
    with pytest.raises(HarnessError, match="workspace changed"):
        stage_execution_tree(
            source_repo,
            temporary_root / "shard",
            run_id="run-drift",
            shard_index=0,
            source_manifest=manifest,
            plan_digest="plan-digest",
            workspace_inputs=(),
        )


def test_workspace_input_copy_has_no_links_and_is_byte_independent(
    source_repo: Path,
) -> None:
    workspace = source_repo / "ignored-input"
    workspace.mkdir()
    original = workspace / "payload.bin"
    original.write_bytes(b"workspace input\n")
    os.link(original, workspace / "source-hardlink.bin")
    os.symlink("payload.bin", workspace / "source-symlink.bin")
    source_digest = _sha256(original.read_bytes())
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "workspace-private"
    temporary_root.mkdir(mode=0o700)

    staged = stage_execution_tree(
        source_repo,
        temporary_root / "shard",
        run_id="run-workspace",
        shard_index=0,
        source_manifest=manifest,
        plan_digest="plan-digest",
        workspace_inputs=("ignored-input",),
    )
    copied = staged.root / "ignored-input"

    copied_files = sorted(path for path in copied.rglob("*") if path.is_file())
    assert copied_files
    assert all(not path.is_symlink() for path in copied.rglob("*"))
    assert len({(path.stat().st_dev, path.stat().st_ino) for path in copied_files}) == 3
    assert all(
        (path.stat().st_dev, path.stat().st_ino)
        != (original.stat().st_dev, original.stat().st_ino)
        for path in copied_files
    )

    (copied / "payload.bin").write_bytes(b"shard-only change\n")
    assert _sha256(original.read_bytes()) == source_digest
    assert build_source_manifest(source_repo) == manifest


def _python_workspace(source_repo: Path) -> Path:
    with (source_repo / ".gitignore").open("a", encoding="utf-8") as handle:
        handle.write(".venv-probe/\n")
    package = source_repo / "src/probe_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("SOURCE = 'fixture'\n", encoding="utf-8")
    environment = source_repo / ".venv-probe"
    subprocess.run(
        [sys.executable, "-m", "venv", str(environment)],
        check=True,
        capture_output=True,
        text=True,
    )
    site_packages = next(environment.glob("lib/python*/site-packages"))
    source_path = str((source_repo / "src").resolve())
    (site_packages / "__editable__.probe.pth").write_text(
        source_path + "\n", encoding="utf-8"
    )
    (site_packages / "__editable___probe_finder.py").write_text(
        f"MAPPING = {{'probe_pkg': {source_path!r}}}\n", encoding="utf-8"
    )
    (environment / "bin/probe-tool").write_text(
        f"#!{environment / 'bin/python'}\nprint('probe')\n",
        encoding="utf-8",
    )
    with (environment / "pyvenv.cfg").open("a", encoding="utf-8") as handle:
        handle.write(f"invoking-root = {source_repo}\n")
    return environment


def test_python_workspace_is_relocated_and_probe_reads_shard_bytes(
    source_repo: Path,
) -> None:
    environment = _python_workspace(source_repo)
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "python-private"
    temporary_root.mkdir(mode=0o700)

    staged = stage_execution_tree(
        source_repo,
        temporary_root / "shard",
        run_id="run-python",
        shard_index=0,
        source_manifest=manifest,
        plan_digest="plan-digest",
        workspace_inputs=(environment.name,),
    )
    copied_environment = staged.root / environment.name
    invoking_bytes = os.fsencode(str(source_repo.resolve()))
    shard_bytes = os.fsencode(str(staged.root.resolve()))

    assert all(not path.is_symlink() for path in copied_environment.rglob("*"))
    assert not any(
        invoking_bytes in path.read_bytes()
        for path in copied_environment.rglob("*")
        if path.is_file()
    )
    relocated = [
        copied_environment / "bin/probe-tool",
        copied_environment / "pyvenv.cfg",
        *copied_environment.rglob("__editable__*.pth"),
        *copied_environment.rglob("__editable__*_finder.py"),
    ]
    assert all(shard_bytes in path.read_bytes() for path in relocated)
    assert build_source_manifest(source_repo) == manifest


def test_editable_direct_url_metadata_is_relocated_to_the_shard(
    source_repo: Path,
) -> None:
    environment = _python_workspace(source_repo)
    site_packages = next(environment.glob("lib/python*/site-packages"))
    distribution = site_packages / "dev_health_ops-1.1.0.post572.dist-info"
    distribution.mkdir()
    direct_url = distribution / "direct_url.json"
    direct_url.write_text(
        json.dumps(
            {
                "dir_info": {"editable": True},
                "url": source_repo.resolve().as_uri(),
            }
        ),
        encoding="utf-8",
    )
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "direct-url-private"
    temporary_root.mkdir(mode=0o700)

    staged = stage_execution_tree(
        source_repo,
        temporary_root / "shard",
        run_id="run-direct-url",
        shard_index=0,
        source_manifest=manifest,
        plan_digest="plan-digest",
        workspace_inputs=(environment.name,),
    )

    relocated = json.loads(
        (
            staged.root / environment.name / direct_url.relative_to(environment)
        ).read_text(encoding="utf-8")
    )
    assert relocated == {
        "dir_info": {"editable": True},
        "url": staged.root.resolve().as_uri(),
    }
    assert build_source_manifest(source_repo) == manifest


def test_toolchain_probe_rejects_a_deliberately_unrelocated_copy(
    source_repo: Path,
) -> None:
    environment = _python_workspace(source_repo)
    bad_shard = source_repo.parent / "bad-shard"
    shutil.copytree(source_repo / "src", bad_shard / "src")
    shutil.copytree(environment, bad_shard / environment.name, symlinks=False)

    with pytest.raises(HarnessError, match="did not read shard-only bytes"):
        probe_python_toolchain_independence(
            shard_root=bad_shard,
            environment=bad_shard / environment.name,
        )


def test_relocation_refuses_unknown_invoking_root_residue(source_repo: Path) -> None:
    environment = _python_workspace(source_repo)
    (environment / "opaque-binding.bin").write_bytes(
        b"binding=" + os.fsencode(str(source_repo.resolve()))
    )
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "residue-private"
    temporary_root.mkdir(mode=0o700)

    with pytest.raises(HarnessError, match="still resolves to the invoking root"):
        stage_execution_tree(
            source_repo,
            temporary_root / "shard",
            run_id="run-residue",
            shard_index=0,
            source_manifest=manifest,
            plan_digest="plan-digest",
            workspace_inputs=(environment.name,),
        )


def test_stage_probe_is_independent_from_the_relocation_pass(
    source_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment = _python_workspace(source_repo)
    manifest = build_source_manifest(source_repo)
    temporary_root = source_repo.parent / "probe-private"
    temporary_root.mkdir(mode=0o700)
    monkeypatch.setattr(
        execution_tree, "relocate_python_environment", lambda **_kwargs: None
    )

    with pytest.raises(HarnessError, match="did not read shard-only bytes"):
        stage_execution_tree(
            source_repo,
            temporary_root / "shard",
            run_id="run-independent-probe",
            shard_index=0,
            source_manifest=manifest,
            plan_digest="plan-digest",
            workspace_inputs=(environment.name,),
        )


def test_staging_failure_after_marker_rolls_back_only_the_owned_partial_tree(
    source_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = source_repo / "ignored-input"
    workspace.mkdir()
    (workspace / "payload.txt").write_text("workspace\n", encoding="utf-8")
    manifest = build_source_manifest(source_repo)
    index = source_repo / ".git/index"
    index_before = index.read_bytes()
    temporary_root = source_repo.parent / "rollback-private"
    temporary_root.mkdir(mode=0o700)
    destination = temporary_root / "shard-0"
    outside_sentinel = temporary_root / "outside-sentinel.txt"
    outside_sentinel.write_text("must survive\n", encoding="utf-8")

    def fail_after_marker(
        _source: Path, staged_root: Path, _workspace_inputs: tuple[str, ...]
    ) -> None:
        assert (staged_root / OWNERSHIP_MARKER).is_file()
        raise HarnessError("deliberate post-marker staging failure")

    monkeypatch.setattr(
        execution_tree, "_materialize_workspace_inputs", fail_after_marker
    )
    with pytest.raises(HarnessError, match="deliberate post-marker staging failure"):
        stage_execution_tree(
            source_repo,
            destination,
            run_id="run-rollback",
            shard_index=0,
            source_manifest=manifest,
            plan_digest="plan-digest",
            workspace_inputs=("ignored-input",),
        )

    assert not destination.exists()
    assert outside_sentinel.read_text(encoding="utf-8") == "must survive\n"
    assert index.read_bytes() == index_before
    assert build_source_manifest(source_repo) == manifest
    assert (
        str(destination)
        not in _git(source_repo, "worktree", "list", "--porcelain").stdout
    )

    preexisting = temporary_root / "preexisting-shard"
    preexisting.mkdir()
    preexisting_sentinel = preexisting / "sentinel.txt"
    preexisting_sentinel.write_text("preexisting\n", encoding="utf-8")
    with pytest.raises(HarnessError, match="destination already exists"):
        stage_execution_tree(
            source_repo,
            preexisting,
            run_id="run-preexisting",
            shard_index=1,
            source_manifest=manifest,
            plan_digest="plan-digest",
            workspace_inputs=("ignored-input",),
        )
    assert preexisting_sentinel.read_text(encoding="utf-8") == "preexisting\n"


def _stage_for_cleanup(source_repo: Path, temporary_root: Path) -> StagedExecutionTree:
    manifest = build_source_manifest(source_repo)
    return stage_execution_tree(
        source_repo,
        temporary_root / "cleanup-shard",
        run_id="run-cleanup",
        shard_index=3,
        source_manifest=manifest,
        plan_digest="cleanup-plan",
        workspace_inputs=(),
    )


def test_cleanup_refuses_a_dangling_shard_state_symlink(source_repo: Path) -> None:
    temporary_root = source_repo.parent / "dangling-state-private"
    temporary_root.mkdir(mode=0o700)
    staged = _stage_for_cleanup(source_repo, temporary_root)
    state = staged.root / ".mutation-harness/state.json"
    os.symlink("missing-state-target.json", state)

    with pytest.raises(HarnessError, match="state is a symlink"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=True
        )
    assert staged.root.exists()


def test_cleanup_refuses_escape_wrong_marker_live_holder_applied_and_dirty_source(
    source_repo: Path,
) -> None:
    temporary_root = source_repo.parent / "cleanup-private"
    temporary_root.mkdir(mode=0o700)
    staged = _stage_for_cleanup(source_repo, temporary_root)

    with pytest.raises(HarnessError, match="death is not proved"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=False
        )
    assert staged.root.exists()

    original_marker = staged.ownership_marker.read_bytes()
    staged.ownership_marker.write_text(json.dumps({"run_id": "foreign"}))
    with pytest.raises(HarnessError, match="ownership marker does not match"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=True
        )
    staged.ownership_marker.write_bytes(original_marker)

    foreign_marker = source_repo.parent / "foreign-owner.json"
    foreign_marker.write_bytes(original_marker)
    redirected_marker = replace(staged, ownership_marker=foreign_marker)
    with pytest.raises(HarnessError, match="outside the owned shard"):
        cleanup_execution_tree(
            redirected_marker,
            temporary_root=temporary_root,
            child_liveness_proven=True,
        )
    assert staged.root.exists()

    state = staged.root / ".mutation-harness/state.json"
    state.write_text("[]", encoding="utf-8")
    with pytest.raises(HarnessError, match="state is not a JSON object"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=True
        )
    state.write_text(json.dumps({"applied": {"id": "M1"}}), encoding="utf-8")
    with pytest.raises(HarnessError, match="has an applied mutation"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=True
        )
    state.unlink()

    target = staged.root / "tracked.txt"
    restored_bytes = target.read_bytes()
    target.write_bytes(b"still mutated\n")
    with pytest.raises(HarnessError, match="source is not restored"):
        cleanup_execution_tree(
            staged, temporary_root=temporary_root, child_liveness_proven=True
        )
    target.write_bytes(restored_bytes)

    outside = source_repo.parent / "outside-sentinel"
    outside.mkdir()
    outside_marker = outside / OWNERSHIP_MARKER
    outside_marker.parent.mkdir(parents=True)
    outside_marker.write_bytes(original_marker)
    escaped = replace(staged, root=outside, ownership_marker=outside_marker)
    with pytest.raises(HarnessError, match="escapes the run root"):
        cleanup_execution_tree(
            escaped, temporary_root=temporary_root, child_liveness_proven=True
        )
    assert outside.exists()

    cleanup_execution_tree(
        staged, temporary_root=temporary_root, child_liveness_proven=True
    )
    assert not staged.root.exists()
    assert build_source_manifest(source_repo) == staged.source_manifest
