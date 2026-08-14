"""Build isolated execution trees for sharded mutation plans.

This module owns only the filesystem boundary.  It snapshots the invoking
workspace, creates a detached disposable worktree, overlays the invoking bytes,
relocates declared workspace inputs, proves Python execution resolves inside the
shard, and removes only a tree whose ownership and restored state are proved.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, cast

from scripts.mutation_harness import HarnessError

OWNERSHIP_MARKER = ".mutation-harness/execution-tree-owner.json"
OWNERSHIP_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SourceManifestEntry:
    """One path in the invoking workspace snapshot."""

    path: str
    kind: Literal["file", "symlink", "deleted"]
    mode: int
    digest: str | None
    link_target: str | None
    tracked: bool


@dataclass(frozen=True)
class SourceManifest:
    """Canonical workspace identity used before and after shard execution."""

    head: str
    entries: tuple[SourceManifestEntry, ...]
    digest: str


@dataclass(frozen=True)
class StagedExecutionTree:
    """One detached, owned worktree prepared for a mutation shard."""

    root: Path
    ownership_marker: Path
    shard_index: int
    run_id: str
    source_root: Path
    source_manifest: SourceManifest
    plan_digest: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(
    root: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if check and completed.returncode != 0:
        detail = completed.stderr.decode(errors="replace").strip()
        raise HarnessError(f"git {' '.join(args)} failed: {detail}")
    return completed


def _nul_paths(output: bytes) -> set[str]:
    return {os.fsdecode(item) for item in output.split(b"\0") if item}


def _validated_relative_path(raw: str) -> str:
    path = PurePosixPath(raw)
    if not raw or path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise HarnessError(f"workspace path is not repository-relative: {raw!r}")
    if path.parts[0] == ".git":
        raise HarnessError(f"workspace manifest must not include Git metadata: {raw}")
    return path.as_posix()


def _assert_repository_root(root: Path) -> Path:
    resolved = root.resolve(strict=True)
    top_level = Path(
        os.fsdecode(_git(resolved, "rev-parse", "--show-toplevel").stdout).strip()
    ).resolve(strict=True)
    if top_level != resolved:
        raise HarnessError(
            f"execution-tree source must be the repository root: {resolved} != {top_level}"
        )
    return resolved


def _assert_harness_state_is_ignored(root: Path) -> None:
    check = _git(
        root,
        "check-ignore",
        "--no-index",
        "--quiet",
        ".mutation-harness",
        check=False,
    )
    if check.returncode == 1:
        raise HarnessError(
            ".mutation-harness is not excluded by gitignore; staging could copy "
            "live mutation state into a shard"
        )
    if check.returncode != 0:
        detail = check.stderr.decode(errors="replace").strip()
        raise HarnessError(f"could not verify .mutation-harness exclusion: {detail}")


def _is_harness_state_path(relative: str) -> bool:
    return PurePosixPath(relative).parts[0] == ".mutation-harness"


def _entry_for_path(root: Path, relative: str, *, tracked: bool) -> SourceManifestEntry:
    target = root / relative
    try:
        metadata = target.lstat()
    except FileNotFoundError:
        if not tracked:
            raise HarnessError(
                f"untracked manifest path disappeared: {relative}"
            ) from None
        return SourceManifestEntry(relative, "deleted", 0, None, None, True)

    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISLNK(metadata.st_mode):
        link_target = os.readlink(target)
        return SourceManifestEntry(
            relative,
            "symlink",
            mode,
            _sha256(os.fsencode(link_target)),
            link_target,
            tracked,
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise HarnessError(
            f"workspace path has unsupported filesystem type: {relative}"
        )
    return SourceManifestEntry(
        relative,
        "file",
        mode,
        _sha256(target.read_bytes()),
        None,
        tracked,
    )


def _manifest_digest(head: str, entries: tuple[SourceManifestEntry, ...]) -> str:
    document = {
        "schema_version": 1,
        "head": head,
        "entries": [
            {
                "path": entry.path,
                "kind": entry.kind,
                "mode": entry.mode,
                "digest": entry.digest,
                "link_target": entry.link_target,
                "tracked": entry.tracked,
            }
            for entry in entries
        ],
    }
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return _sha256(encoded)


def source_manifest_to_dict(manifest: SourceManifest) -> dict[str, object]:
    """Serialize the full recovery authority, not only its digest."""

    return {
        "schema_version": 1,
        "head": manifest.head,
        "entries": [
            {
                "path": entry.path,
                "kind": entry.kind,
                "mode": entry.mode,
                "digest": entry.digest,
                "link_target": entry.link_target,
                "tracked": entry.tracked,
            }
            for entry in manifest.entries
        ],
        "digest": manifest.digest,
    }


def source_manifest_from_dict(raw: Mapping[str, object]) -> SourceManifest:
    """Load a durable source manifest and reject a damaged recovery record."""

    if raw.get("schema_version") != 1:
        raise HarnessError("source manifest schema_version must be 1")
    head = raw.get("head")
    recorded_digest = raw.get("digest")
    entries_raw = raw.get("entries")
    if not isinstance(head, str) or not head:
        raise HarnessError("source manifest head must be a non-empty string")
    if not isinstance(recorded_digest, str) or not recorded_digest:
        raise HarnessError("source manifest digest must be a non-empty string")
    if not isinstance(entries_raw, list):
        raise HarnessError("source manifest entries must be a list")

    entries: list[SourceManifestEntry] = []
    seen: set[str] = set()
    for item in entries_raw:
        if not isinstance(item, dict):
            raise HarnessError("source manifest entry must be a JSON object")
        path_raw = item.get("path")
        kind_raw = item.get("kind")
        mode_raw = item.get("mode")
        digest_raw = item.get("digest")
        link_target_raw = item.get("link_target")
        tracked_raw = item.get("tracked")
        if not isinstance(path_raw, str):
            raise HarnessError("source manifest entry path must be a string")
        path = _validated_relative_path(path_raw)
        if path in seen:
            raise HarnessError(f"source manifest repeats path: {path}")
        seen.add(path)
        if kind_raw not in {"file", "symlink", "deleted"}:
            raise HarnessError(f"source manifest entry has invalid kind: {path}")
        if (
            not isinstance(mode_raw, int)
            or isinstance(mode_raw, bool)
            or mode_raw < 0
            or mode_raw > 0o7777
        ):
            raise HarnessError(f"source manifest entry has invalid mode: {path}")
        if digest_raw is not None and not isinstance(digest_raw, str):
            raise HarnessError(f"source manifest entry has invalid digest: {path}")
        if link_target_raw is not None and not isinstance(link_target_raw, str):
            raise HarnessError(f"source manifest entry has invalid link target: {path}")
        if not isinstance(tracked_raw, bool):
            raise HarnessError(
                f"source manifest entry has invalid tracked flag: {path}"
            )
        kind = cast(Literal["file", "symlink", "deleted"], kind_raw)
        if kind == "deleted" and (
            mode_raw != 0 or digest_raw is not None or link_target_raw is not None
        ):
            raise HarnessError(f"deleted source manifest entry has content: {path}")
        if kind == "file" and (digest_raw is None or link_target_raw is not None):
            raise HarnessError(f"file source manifest entry is incomplete: {path}")
        if kind == "symlink" and (digest_raw is None or link_target_raw is None):
            raise HarnessError(f"symlink source manifest entry is incomplete: {path}")
        entries.append(
            SourceManifestEntry(
                path,
                kind,
                mode_raw,
                digest_raw,
                link_target_raw,
                tracked_raw,
            )
        )
    if [entry.path for entry in entries] != sorted(entry.path for entry in entries):
        raise HarnessError("source manifest entries are not in canonical path order")
    frozen_entries = tuple(entries)
    observed_digest = _manifest_digest(head, frozen_entries)
    if observed_digest != recorded_digest:
        raise HarnessError(
            "source manifest recovery record digest mismatch: "
            f"expected {recorded_digest}, observed {observed_digest}"
        )
    return SourceManifest(head, frozen_entries, recorded_digest)


def build_source_manifest(source_root: Path) -> SourceManifest:
    """Hash all relevant workspace paths without writing the Git index."""

    root = _assert_repository_root(source_root)
    _assert_harness_state_is_ignored(root)
    head = os.fsdecode(_git(root, "rev-parse", "HEAD").stdout).strip()

    head_paths = _nul_paths(
        _git(root, "ls-tree", "-r", "-z", "--name-only", head).stdout
    )
    index_paths = _nul_paths(_git(root, "ls-files", "-z", "--cached").stdout)
    tracked_paths = head_paths | index_paths
    tracked_harness_state = sorted(
        relative for relative in tracked_paths if _is_harness_state_path(relative)
    )
    if tracked_harness_state:
        raise HarnessError(
            ".mutation-harness state is tracked and cannot be staged safely: "
            + ", ".join(tracked_harness_state)
        )
    untracked_paths = (
        _nul_paths(
            _git(root, "ls-files", "-z", "--others", "--exclude-standard").stdout
        )
        - tracked_paths
    )
    untracked_paths = {
        relative for relative in untracked_paths if not _is_harness_state_path(relative)
    }

    entries = tuple(
        _entry_for_path(
            root,
            _validated_relative_path(relative),
            tracked=relative in tracked_paths,
        )
        for relative in sorted(tracked_paths | untracked_paths)
    )
    return SourceManifest(head, entries, _manifest_digest(head, entries))


def verify_source_manifest(source_root: Path, expected: SourceManifest) -> None:
    """Fail if the invoking workspace differs from its frozen manifest."""

    observed = build_source_manifest(source_root)
    if observed != expected:
        raise HarnessError(
            "invoking workspace changed while execution trees were active: "
            f"expected {expected.digest}, observed {observed.digest}"
        )


def create_private_temp_root(*, prefix: str = "mutation-harness-") -> Path:
    """Create the operating-system run root with owner-only permissions."""

    root = Path(tempfile.mkdtemp(prefix=prefix)).resolve()
    root.chmod(0o700)
    return root


def _assert_private_parent(destination: Path) -> Path:
    parent = destination.parent.resolve(strict=True)
    mode = stat.S_IMODE(parent.stat().st_mode)
    if mode != 0o700:
        raise HarnessError(
            f"execution-tree temporary root must have mode 0700: {parent} has {mode:04o}"
        )
    if destination.exists() or destination.is_symlink():
        raise HarnessError(f"execution-tree destination already exists: {destination}")
    return parent


def _marker_document(
    *,
    run_id: str,
    shard_index: int,
    source_manifest_digest: str,
    plan_digest: str,
) -> dict[str, object]:
    return {
        "schema_version": OWNERSHIP_SCHEMA_VERSION,
        "run_id": run_id,
        "shard_index": shard_index,
        "source_manifest_digest": source_manifest_digest,
        "plan_digest": plan_digest,
    }


def _write_marker(
    destination: Path,
    *,
    run_id: str,
    shard_index: int,
    source_manifest_digest: str,
    plan_digest: str,
) -> Path:
    marker = destination / OWNERSHIP_MARKER
    marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            _marker_document(
                run_id=run_id,
                shard_index=shard_index,
                source_manifest_digest=source_manifest_digest,
                plan_digest=plan_digest,
            ),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return marker


def _remove_existing(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink()


def _copy_manifest_entry(
    source_root: Path, destination_root: Path, entry: SourceManifestEntry
) -> None:
    source = source_root / entry.path
    destination = destination_root / entry.path
    if entry.kind == "deleted":
        _remove_existing(destination)
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    _remove_existing(destination)
    if entry.kind == "symlink":
        current_target = os.readlink(source)
        if current_target != entry.link_target:
            raise HarnessError(f"source symlink changed during staging: {entry.path}")
        os.symlink(current_target, destination)
        return

    data = source.read_bytes()
    if _sha256(data) != entry.digest:
        raise HarnessError(f"source file changed during staging: {entry.path}")
    destination.write_bytes(data)
    destination.chmod(entry.mode)


def stage_execution_tree(
    source_root: Path,
    destination: Path,
    *,
    run_id: str,
    shard_index: int,
    source_manifest: SourceManifest,
    plan_digest: str,
    workspace_inputs: tuple[str, ...],
) -> StagedExecutionTree:
    """Create one detached worktree with the exact invoking workspace overlay."""

    source = _assert_repository_root(source_root)
    destination = destination.absolute()
    _assert_private_parent(destination)
    verify_source_manifest(source, source_manifest)

    _git(source, "worktree", "add", "--detach", str(destination), source_manifest.head)
    destination.chmod(0o700)
    marker = _write_marker(
        destination,
        run_id=run_id,
        shard_index=shard_index,
        source_manifest_digest=source_manifest.digest,
        plan_digest=plan_digest,
    )

    detached_head = os.fsdecode(_git(destination, "rev-parse", "HEAD").stdout).strip()
    symbolic = _git(destination, "symbolic-ref", "-q", "HEAD", check=False)
    if detached_head != source_manifest.head or symbolic.returncode == 0:
        raise HarnessError(
            f"execution tree is not detached at validated HEAD {source_manifest.head}"
        )

    for entry in source_manifest.entries:
        _copy_manifest_entry(source, destination, entry)

    if workspace_inputs:
        _materialize_workspace_inputs(source, destination, workspace_inputs)
    verify_source_manifest(source, source_manifest)
    return StagedExecutionTree(
        root=destination,
        ownership_marker=marker,
        shard_index=shard_index,
        run_id=run_id,
        source_root=source,
        source_manifest=source_manifest,
        plan_digest=plan_digest,
    )


def _materialize_workspace_inputs(
    source_root: Path, destination_root: Path, workspace_inputs: tuple[str, ...]
) -> None:
    """Copy ignored inputs without links, relocate them, and probe Python bytes."""

    for raw in workspace_inputs:
        relative = _validated_workspace_input(source_root, raw)
        source = source_root / relative
        destination = destination_root / relative
        if not source.exists() and not source.is_symlink():
            raise HarnessError(f"workspace input does not exist: {relative}")
        _copy_workspace_path(source, destination)
        _assert_independent_copy(source, destination)
        if destination.is_dir() and (destination / "pyvenv.cfg").is_file():
            relocate_python_environment(
                source_root=source_root,
                shard_root=destination_root,
                environment=destination,
            )
            probe_python_toolchain_independence(
                shard_root=destination_root,
                environment=destination,
            )


def _validated_workspace_input(source_root: Path, raw: str) -> str:
    relative = _validated_relative_path(raw)
    first = PurePosixPath(relative).parts[0]
    if first in {".git", ".mutation-harness"}:
        raise HarnessError(f"workspace input is forbidden: {raw}")
    ignored = _git(
        source_root,
        "check-ignore",
        "--no-index",
        "--quiet",
        relative,
        check=False,
    )
    if ignored.returncode == 1:
        raise HarnessError(f"workspace input must be ignored by Git: {relative}")
    if ignored.returncode != 0:
        detail = ignored.stderr.decode(errors="replace").strip()
        raise HarnessError(f"could not validate workspace input {relative}: {detail}")
    return relative


def _copy_workspace_path(source: Path, destination: Path) -> None:
    metadata = source.lstat()
    if stat.S_ISLNK(metadata.st_mode):
        resolved = source.resolve(strict=True)
        if not resolved.is_file():
            raise HarnessError(
                f"workspace input link must resolve to a regular file: {source}"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved, destination, follow_symlinks=True)
        return
    if stat.S_ISREG(metadata.st_mode):
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination, follow_symlinks=True)
        return
    if not stat.S_ISDIR(metadata.st_mode):
        raise HarnessError(f"workspace input has unsupported type: {source}")

    destination.mkdir(parents=True)
    shutil.copystat(source, destination, follow_symlinks=True)
    for child in sorted(source.iterdir(), key=lambda path: path.name):
        _copy_workspace_path(child, destination / child.name)


def _assert_independent_copy(source: Path, destination: Path) -> None:
    source_metadata = source.lstat()
    destination_metadata = destination.lstat()
    if stat.S_ISLNK(destination_metadata.st_mode):
        raise HarnessError(f"workspace input copy contains a symlink: {destination}")
    if stat.S_ISREG(destination_metadata.st_mode):
        source_target = source.resolve(strict=True) if source.is_symlink() else source
        source_identity = (source_target.stat().st_dev, source_target.stat().st_ino)
        destination_identity = (
            destination_metadata.st_dev,
            destination_metadata.st_ino,
        )
        if source_identity == destination_identity:
            raise HarnessError(f"workspace input copy is hard-linked: {destination}")
        return
    if not stat.S_ISDIR(destination_metadata.st_mode):
        raise HarnessError(f"workspace input copy has unsupported type: {destination}")
    if not stat.S_ISDIR(source_metadata.st_mode):
        raise HarnessError(
            f"workspace input copy changed filesystem type: {destination}"
        )
    source_children = {child.name: child for child in source.iterdir()}
    destination_children = {child.name: child for child in destination.iterdir()}
    if source_children.keys() != destination_children.keys():
        raise HarnessError(f"workspace input copy is incomplete: {destination}")
    for name, source_child in source_children.items():
        _assert_independent_copy(source_child, destination_children[name])


def _replace_invoking_root(path: Path, invoking: bytes, shard: bytes) -> None:
    data = path.read_bytes()
    if invoking not in data:
        return
    path.write_bytes(data.replace(invoking, shard))


def relocate_python_environment(
    *, source_root: Path, shard_root: Path, environment: Path
) -> None:
    """Rewrite install-time paths in a copied Python environment."""

    invoking = os.fsencode(str(source_root.resolve(strict=True)))
    shard = os.fsencode(str(shard_root.resolve(strict=True)))
    pyvenv = environment / "pyvenv.cfg"
    if not pyvenv.is_file():
        raise HarnessError(f"Python workspace input has no pyvenv.cfg: {environment}")
    _replace_invoking_root(pyvenv, invoking, shard)

    bin_dir = environment / "bin"
    if not bin_dir.is_dir():
        raise HarnessError(
            f"Python workspace input has no bin directory: {environment}"
        )
    for launcher in sorted(bin_dir.iterdir(), key=lambda path: path.name):
        if not launcher.is_file() or launcher.is_symlink():
            continue
        # Console-script shebangs and activation scripts both record the
        # environment's installation path. Replacing the invoking root covers
        # both forms and leaves system-rooted launchers unchanged.
        _replace_invoking_root(launcher, invoking, shard)

    editable_files = {
        *environment.rglob("__editable__*.pth"),
        *environment.rglob("__editable__*.py"),
        *environment.rglob("__editable__*_finder.py"),
    }
    for editable in sorted(editable_files):
        if editable.is_file() and not editable.is_symlink():
            _replace_invoking_root(editable, invoking, shard)

    # Bytecode embeds source filenames and can retain the invoking environment
    # path. It is a derived cache, so remove it and let the shard interpreter
    # regenerate it with shard-rooted filenames when needed.
    for bytecode in sorted(environment.rglob("*.pyc")):
        bytecode.unlink()
    for cache_directory in sorted(
        environment.rglob("__pycache__"), key=lambda path: len(path.parts), reverse=True
    ):
        if cache_directory.is_dir():
            cache_directory.rmdir()

    residue: list[str] = []
    for path in sorted(environment.rglob("*")):
        if path.is_symlink():
            residue.append(f"{path}: symlink")
            continue
        if path.is_file() and invoking in path.read_bytes():
            residue.append(f"{path}: invoking-root bytes")
    if residue:
        joined = "\n".join(residue[:20])
        raise HarnessError(
            "Python workspace input still resolves to the invoking root after "
            f"relocation:\n{joined}"
        )


def _probe_package(shard_root: Path) -> tuple[Path, str]:
    source = shard_root / "src"
    candidates = sorted(
        path.parent for path in source.rglob("__init__.py") if path.parent.is_dir()
    )
    if not candidates:
        raise HarnessError(
            "toolchain-independence probe requires an importable package under src"
        )
    package = candidates[0]
    relative = package.relative_to(source)
    return package, ".".join(relative.parts)


def probe_python_toolchain_independence(*, shard_root: Path, environment: Path) -> None:
    """Observe the copied interpreter importing bytes that exist only in the shard."""

    package, package_name = _probe_package(shard_root)
    token = secrets.token_hex(16)
    module_name = f"_mutation_harness_probe_{token}"
    sentinel = package / f"{module_name}.py"
    sentinel.write_text(f"VALUE = {token!r}\n", encoding="utf-8")
    interpreter = environment / "bin" / "python"
    if not interpreter.is_file():
        sentinel.unlink(missing_ok=True)
        raise HarnessError(f"Python workspace input has no interpreter: {interpreter}")
    environment_variables = os.environ.copy()
    environment_variables.pop("PYTHONPATH", None)
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-c",
                (
                    f"from {package_name} import {module_name} as probe; "
                    f"print(probe.VALUE, end='')"
                ),
            ],
            cwd=shard_root,
            check=False,
            capture_output=True,
            env=environment_variables,
            text=True,
        )
    finally:
        sentinel.unlink(missing_ok=True)
    if completed.returncode != 0 or completed.stdout != token:
        detail = (completed.stderr or completed.stdout).strip()
        raise HarnessError(
            "toolchain-independence probe did not read shard-only bytes: "
            f"{detail or 'no sentinel value returned'}"
        )


def _read_marker(staged: StagedExecutionTree) -> dict[str, object]:
    try:
        raw = json.loads(staged.ownership_marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise HarnessError(
            f"execution-tree ownership marker is unreadable: {staged.ownership_marker}"
        ) from exc
    if not isinstance(raw, dict):
        raise HarnessError("execution-tree ownership marker is not a JSON object")
    return raw


def cleanup_execution_tree(
    staged: StagedExecutionTree,
    *,
    temporary_root: Path,
    child_liveness_proven: bool,
) -> None:
    """Remove a disposable tree only after every cleanup guard is proved."""

    private_root = temporary_root.resolve(strict=True)
    tree_root = staged.root.resolve(strict=True)
    try:
        tree_root.relative_to(private_root)
    except ValueError as exc:
        raise HarnessError(
            f"execution-tree cleanup path escapes the run root: {tree_root}"
        ) from exc
    if tree_root == private_root:
        raise HarnessError("execution-tree cleanup path is the whole run root")
    if stat.S_IMODE(private_root.stat().st_mode) != 0o700:
        raise HarnessError("execution-tree run root no longer has mode 0700")

    state_directory = tree_root / ".mutation-harness"
    try:
        state_directory_metadata = state_directory.lstat()
    except OSError as exc:
        raise HarnessError(
            f"execution-tree state directory is unreadable: {state_directory}"
        ) from exc
    if stat.S_ISLNK(state_directory_metadata.st_mode) or not stat.S_ISDIR(
        state_directory_metadata.st_mode
    ):
        raise HarnessError(
            f"execution-tree state directory is not a real directory: {state_directory}"
        )

    canonical_marker = tree_root / OWNERSHIP_MARKER
    try:
        recorded_marker = staged.ownership_marker.resolve(strict=True)
    except OSError as exc:
        raise HarnessError(
            f"execution-tree ownership marker is unreadable: {staged.ownership_marker}"
        ) from exc
    if recorded_marker != canonical_marker or canonical_marker.is_symlink():
        raise HarnessError(
            f"execution-tree ownership marker is outside the owned shard: "
            f"{staged.ownership_marker}"
        )

    expected_marker = _marker_document(
        run_id=staged.run_id,
        shard_index=staged.shard_index,
        source_manifest_digest=staged.source_manifest.digest,
        plan_digest=staged.plan_digest,
    )
    if _read_marker(staged) != expected_marker:
        raise HarnessError(
            f"execution-tree ownership marker does not match shard {staged.shard_index}"
        )
    if not child_liveness_proven:
        raise HarnessError(
            f"shard {staged.shard_index} death is not proved; retaining {tree_root}"
        )

    state_path = state_directory / "state.json"
    try:
        state_metadata = state_path.lstat()
    except FileNotFoundError:
        state_metadata = None
    except OSError as exc:
        raise HarnessError(f"shard state is unreadable: {state_path}") from exc
    if state_metadata is not None:
        if stat.S_ISLNK(state_metadata.st_mode):
            raise HarnessError(f"shard state is a symlink: {state_path}")
        if not stat.S_ISREG(state_metadata.st_mode):
            raise HarnessError(f"shard state is not a regular file: {state_path}")
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise HarnessError(f"shard state is unreadable: {state_path}") from exc
        if not isinstance(state, dict):
            raise HarnessError(f"shard state is not a JSON object: {state_path}")
        if state.get("applied") is not None:
            raise HarnessError(
                f"shard {staged.shard_index} has an applied mutation; retaining {tree_root}"
            )

    observed = build_source_manifest(tree_root)
    if observed != staged.source_manifest:
        raise HarnessError(
            f"shard {staged.shard_index} source is not restored: expected "
            f"{staged.source_manifest.digest}, observed {observed.digest}"
        )
    verify_source_manifest(staged.source_root, staged.source_manifest)
    _git(
        staged.source_root,
        "worktree",
        "remove",
        "--force",
        str(tree_root),
    )
