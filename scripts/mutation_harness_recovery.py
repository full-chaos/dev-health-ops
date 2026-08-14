"""Bounded recovery for interrupted mutation-harness coordinator runs."""

from __future__ import annotations

import fcntl
import importlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

STATE_DIRNAME = ".mutation-harness"
STATE_FILENAME = "state.json"
RUNS_DIRNAME = "runs"
MANIFEST_FILENAME = "manifest.json"
SCHEMA_VERSION = 1
_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_PARTIAL_SHARD_NAME = re.compile(r"shard-(0|[1-9][0-9]*)")
_EXECUTION_TREE_MARKER = Path(STATE_DIRNAME) / "execution-tree-owner.json"
_INCOMPLETE_COORDINATOR_LIFECYCLES = frozenset(
    {"staging", "running", "stopping", "aborted", "recovering"}
)
_SHARD_AUTHORITY_KEYS = (
    "shard_index",
    "root",
    "source_root",
    "temporary_root",
    "ownership_marker",
    "liveness_lock",
)

RestoreMutation = Callable[[Path], str]
CleanupOwnedTree = Callable[[Path, Path, int], None]


class RecoveryError(RuntimeError):
    """Recovery could not prove that an intended action was safe."""


def _trusted_root_state_directory(root: Path) -> Path:
    """Return the lexical root state directory without following a link."""

    directory = root / STATE_DIRNAME
    try:
        info = directory.lstat()
    except FileNotFoundError as exc:
        raise RecoveryError(f"root state directory {directory} does not exist") from exc
    except OSError as exc:
        raise RecoveryError(
            f"root state directory {directory} could not be inspected: {exc}"
        ) from exc
    if stat.S_ISLNK(info.st_mode):
        raise RecoveryError(
            f"root state directory {directory} is a symlink; refusing recovery"
        )
    if not stat.S_ISDIR(info.st_mode):
        raise RecoveryError(
            f"root state directory {directory} is not a directory; refusing recovery"
        )
    return directory


def _trusted_authority_directory(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RecoveryError(f"{label} {path} does not exist") from exc
    except OSError as exc:
        raise RecoveryError(f"{label} {path} could not be inspected: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise RecoveryError(f"{label} {path} is a symlink; refusing recovery")
    if not stat.S_ISDIR(info.st_mode):
        raise RecoveryError(f"{label} {path} is not a directory; refusing recovery")


def _trusted_authority_file(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RecoveryError(f"{label} {path} does not exist") from exc
    except OSError as exc:
        raise RecoveryError(f"{label} {path} could not be inspected: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise RecoveryError(f"{label} {path} is a symlink; refusing recovery")
    if not stat.S_ISREG(info.st_mode):
        raise RecoveryError(f"{label} {path} is not a regular file; refusing recovery")


def _trusted_recovery_authority_file(root: Path, path: Path) -> None:
    """Validate every lexical authority component without resolving links."""

    state_directory = _trusted_root_state_directory(root)
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryError(f"recovery authority path is not lexical-safe: {path}")
    try:
        relative = path.relative_to(state_directory)
    except ValueError as exc:
        raise RecoveryError(
            f"recovery authority path {path} escapes {state_directory}"
        ) from exc
    if relative.parts == (STATE_FILENAME,):
        _trusted_authority_file(path, "root state file")
        return
    if (
        len(relative.parts) == 3
        and relative.parts[0] == RUNS_DIRNAME
        and relative.parts[2] == MANIFEST_FILENAME
    ):
        runs_directory = state_directory / RUNS_DIRNAME
        run_directory = runs_directory / relative.parts[1]
        _trusted_authority_directory(runs_directory, "runs directory")
        _trusted_authority_directory(run_directory, "run directory")
        _trusted_authority_file(path, "run manifest")
        return
    raise RecoveryError(f"unrecognized recovery authority path: {path}")


@dataclass(frozen=True)
class ShardRecord:
    """One shard path and its recovery evidence from the run manifest."""

    shard_index: int
    root: Path
    temporary_root: Path
    ownership_marker: Path
    liveness_lock: Path


@dataclass(frozen=True)
class RunRecord:
    """Validated coordinator and manifest state for one interrupted run."""

    run_id: str
    manifest_path: Path
    manifest: dict[str, Any]
    source_root: Path
    source_manifest: dict[str, Any]
    source_manifest_digest: str
    plan_digest: str
    diagnostic_pid: int | None
    temporary_root: Path | None
    staging_temporary_root: Path | None
    staging_shard_limit: int | None
    shards: tuple[ShardRecord, ...]


@dataclass(frozen=True)
class PartialShardRecord:
    """One fully validated pre-child staging tree."""

    shard_index: int
    root: Path
    ownership_marker: Path


@dataclass(frozen=True)
class RecoveryPreflight:
    """The exact manifest-bounded actions and unknown evidence."""

    remove: tuple[Path, ...]
    leave: tuple[Path, ...]
    unknown: tuple[str, ...]

    def render(self, run_id: str) -> str:
        lines = [f"FORCE RECOVERY PREFLIGHT {run_id}", "REMOVE:"]
        lines.extend(f"  {path}" for path in self.remove)
        lines.append("LEAVE:")
        lines.extend(f"  {path}" for path in self.leave)
        lines.append("UNKNOWN:")
        lines.extend(f"  {item}" for item in self.unknown)
        return "\n".join(lines) + "\n"


@contextmanager
def hold_liveness_lock(path: Path) -> Iterator[None]:
    """Hold a process-lifetime advisory lock for one shard child."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _acquire_liveness_lease(path: Path) -> int | None:
    """Hold the shard lock through cleanup, or return None for a live child."""

    if path.is_symlink():
        raise RecoveryError(f"liveness lock {path} is a symlink")
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CLOEXEC)
    except OSError as exc:
        raise RecoveryError(f"liveness lock {path} could not be opened: {exc}") from exc
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return None
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _diagnostic_pid_exists(pid: int | None) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _recovery_blocked(*, liveness_lock_held: bool, diagnostic_pid_exists: bool) -> bool:
    """Decide liveness from the advisory lock; PID status is diagnostic only."""

    _ = diagnostic_pid_exists
    return liveness_lock_held


def _read_json(path: Path, label: str) -> dict[str, Any]:
    if path.is_symlink():
        raise RecoveryError(f"{label} {path} is a symlink")
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RecoveryError(f"{label} {path} could not be read: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RecoveryError(f"{label} {path} must be a JSON object")
    return loaded


def _read_root_recovery_json(root: Path, path: Path, label: str) -> dict[str, Any]:
    """Recheck the root state parent immediately before each recovery read."""

    _trusted_recovery_authority_file(root, path)
    return _read_json(path, label)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RecoveryError(f"state path {path} is a symlink")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    descriptor = -1
    temporary: Path | None = None
    for attempt in range(128):
        candidate = path.with_name(f".{path.name}.recovery-{os.getpid()}-{attempt}")
        try:
            descriptor = os.open(candidate, flags, 0o600)
        except FileExistsError:
            continue
        temporary = candidate
        break
    if temporary is None:
        raise RecoveryError(f"could not allocate an atomic state file beside {path}")
    try:
        data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _write_root_recovery_json(root: Path, path: Path, payload: dict[str, Any]) -> None:
    """Recheck the root state parent immediately before each recovery write."""

    _trusted_recovery_authority_file(root, path)
    _atomic_write_json(path, payload)


def _contained_path(path: Path, root: Path, label: str) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    if resolved == root_resolved or not resolved.is_relative_to(root_resolved):
        raise RecoveryError(f"{label} {path} escapes shard root {root}")
    return resolved


def _private_temporary_root(value: object, source_root: Path) -> Path:
    """Validate a canonical private directory below the operating-system temp root."""

    if not isinstance(value, str) or not value:
        raise RecoveryError(
            "zero-shard run manifest has no durable temporary_root authority"
        )
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryError(f"temporary_root {path} is not an absolute lexical path")
    try:
        temporary_base = Path(tempfile.gettempdir()).resolve(strict=True)
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RecoveryError(
            f"temporary_root {path} could not be resolved: {exc}"
        ) from exc
    if resolved != path:
        raise RecoveryError(
            f"temporary_root {path} is not canonical or contains a symlink"
        )
    if resolved.is_relative_to(source_root) or source_root.is_relative_to(resolved):
        raise RecoveryError(f"temporary_root {path} overlaps source_root {source_root}")
    if (
        resolved == temporary_base
        or not resolved.is_relative_to(temporary_base)
        or resolved in {Path("/").resolve(), source_root / STATE_DIRNAME}
    ):
        raise RecoveryError(
            f"temporary_root {path} is outside the private run boundary"
        )

    relative = resolved.relative_to(temporary_base)
    current = temporary_base
    for component in relative.parts:
        current /= component
        _trusted_authority_directory(current, "temporary-root component")
    try:
        mode = stat.S_IMODE(resolved.lstat().st_mode)
    except OSError as exc:
        raise RecoveryError(
            f"temporary_root {path} could not be inspected: {exc}"
        ) from exc
    if mode != 0o700:
        raise RecoveryError(
            f"temporary_root {path} mode is {mode:#o}, expected private mode 0o700"
        )
    return resolved


def _private_temporary_root_path(value: object, source_root: Path) -> Path:
    """Validate an existing or absent private-root path without following it."""

    if not isinstance(value, str) or not value:
        raise RecoveryError("run manifest has no durable temporary_root authority")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryError(f"temporary_root {path} is not an absolute lexical path")
    try:
        temporary_base = Path(tempfile.gettempdir()).resolve(strict=True)
    except OSError as exc:
        raise RecoveryError(
            f"system temporary root could not be resolved: {exc}"
        ) from exc
    if path == temporary_base or not path.is_relative_to(temporary_base):
        raise RecoveryError(
            f"temporary_root {path} is outside the private run boundary"
        )
    if path.is_relative_to(source_root) or source_root.is_relative_to(path):
        raise RecoveryError(f"temporary_root {path} overlaps source_root {source_root}")

    current = temporary_base
    for component in path.relative_to(temporary_base).parts[:-1]:
        current /= component
        _trusted_authority_directory(current, "temporary-root parent")
    return path


def _missing_private_temporary_root(value: object, source_root: Path) -> Path | None:
    """Return the safe recorded root only when its own lstat reports ENOENT."""

    path = _private_temporary_root_path(value, source_root)
    try:
        path.lstat()
    except FileNotFoundError:
        return path
    except OSError as exc:
        raise RecoveryError(
            f"temporary_root {path} could not be inspected: {exc}"
        ) from exc
    return None


def _shard_authority(raw_shards: object, label: str) -> list[dict[str, object]]:
    if not isinstance(raw_shards, list):
        raise RecoveryError(f"{label} shards must be a list")
    projected: list[dict[str, object]] = []
    for position, raw in enumerate(raw_shards):
        if not isinstance(raw, dict):
            raise RecoveryError(f"{label} shard {position} must be an object")
        projected.append({key: raw.get(key) for key in _SHARD_AUTHORITY_KEYS})
    return projected


def _load_run_record(root: Path, run_id: str) -> tuple[dict[str, Any], RunRecord]:
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise RecoveryError(f"run id {run_id!r} is not a plain name")
    root = root.resolve()
    state_directory = _trusted_root_state_directory(root)
    state_path = state_directory / STATE_FILENAME
    state = _read_root_recovery_json(root, state_path, "root state")
    coordinator = state.get("coordinator_run")
    if not isinstance(coordinator, dict):
        raise RecoveryError(f"root state does not record coordinator run {run_id}")
    recorded_run_id = coordinator.get("run_id")
    if recorded_run_id != run_id:
        raise RecoveryError(
            f"root state records run_id {recorded_run_id!r}, not {run_id!r}"
        )

    expected_manifest = state_directory / RUNS_DIRNAME / run_id / MANIFEST_FILENAME
    manifest_value = coordinator.get("manifest_path")
    if not isinstance(manifest_value, str) or not manifest_value:
        raise RecoveryError("coordinator run has no manifest path")
    manifest_path = Path(manifest_value)
    if not manifest_path.is_absolute() or ".." in manifest_path.parts:
        raise RecoveryError(
            f"manifest path {manifest_path} is not an absolute lexical path"
        )
    if manifest_path != expected_manifest:
        raise RecoveryError(
            f"manifest path {manifest_path} is outside the recorded run boundary "
            f"{expected_manifest}"
        )
    manifest = _read_root_recovery_json(root, manifest_path, "run manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise RecoveryError(
            f"run manifest schema_version must be {SCHEMA_VERSION}, got "
            f"{manifest.get('schema_version')!r}"
        )
    if manifest.get("run_id") != run_id:
        raise RecoveryError(
            f"run manifest records run_id {manifest.get('run_id')!r}, not {run_id!r}"
        )

    source_digest = manifest.get("source_manifest_digest")
    plan_digest = manifest.get("plan_digest")
    if not isinstance(source_digest, str) or not source_digest:
        raise RecoveryError("run manifest has no source_manifest_digest")
    if not isinstance(plan_digest, str) or not plan_digest:
        raise RecoveryError("run manifest has no plan_digest")
    if coordinator.get("source_manifest_digest") != source_digest:
        raise RecoveryError("coordinator and manifest source digests differ")
    if coordinator.get("plan_digest") != plan_digest:
        raise RecoveryError("coordinator and manifest plan digests differ")
    source_root_value = manifest.get("source_root")
    source_manifest = manifest.get("source_manifest")
    if not isinstance(source_root_value, str) or not source_root_value:
        raise RecoveryError("run manifest has no source_root")
    source_root = Path(source_root_value).resolve()
    if coordinator.get("source_root") != source_root_value:
        raise RecoveryError("coordinator and manifest source roots differ")
    if source_root != root:
        raise RecoveryError(
            f"run manifest source_root {source_root} does not match recovery root {root}"
        )
    if not isinstance(source_manifest, dict):
        raise RecoveryError("run manifest has no recovery-loadable source_manifest")
    if source_manifest.get("digest") != source_digest:
        raise RecoveryError(
            "serialized source manifest digest does not match run metadata"
        )
    if coordinator.get("source_manifest") != source_manifest:
        raise RecoveryError("coordinator and manifest source manifests differ")

    raw_shards = manifest.get("shards")
    if not isinstance(raw_shards, list):
        raise RecoveryError("run manifest shards must be a list")
    if _shard_authority(coordinator.get("shards"), "coordinator") != _shard_authority(
        raw_shards, "run manifest"
    ):
        raise RecoveryError("coordinator and manifest shard authorities differ")
    manifest_temporary_root = manifest.get("temporary_root")
    if coordinator.get("temporary_root") != manifest_temporary_root:
        raise RecoveryError("coordinator and manifest temporary_root values differ")
    temporary_root: Path | None = None
    if raw_shards:
        temporary_root = _private_temporary_root_path(
            manifest_temporary_root, source_root
        )
    staging_temporary_root: Path | None = None
    staging_shard_limit: int | None = None
    if not raw_shards:
        if coordinator.get("lifecycle") != "aborted":
            raise RecoveryError(
                "zero-shard recovery requires an aborted coordinator lifecycle"
            )
        if coordinator.get("shards") != []:
            raise RecoveryError(
                "zero-shard manifest differs from the coordinator shard record"
            )
        requested_shards = manifest.get("requested_shards")
        effective_shards = manifest.get("effective_shards")
        if (
            not isinstance(requested_shards, int)
            or isinstance(requested_shards, bool)
            or requested_shards < 1
            or not isinstance(effective_shards, int)
            or isinstance(effective_shards, bool)
            or effective_shards < 1
            or effective_shards > requested_shards
        ):
            raise RecoveryError(
                "zero-shard run manifest has invalid requested/effective shard bounds"
            )
        staging_temporary_root = _private_temporary_root(
            manifest_temporary_root, source_root
        )
        staging_shard_limit = effective_shards
    shards: list[ShardRecord] = []
    seen_indexes: set[int] = set()
    seen_roots: set[Path] = set()
    for position, raw in enumerate(raw_shards):
        if not isinstance(raw, dict):
            raise RecoveryError(f"run manifest shard {position} must be an object")
        index = raw.get("shard_index")
        if not isinstance(index, int) or isinstance(index, bool) or index < 0:
            raise RecoveryError(
                f"run manifest shard {position} has invalid shard_index"
            )
        if index in seen_indexes:
            raise RecoveryError(f"run manifest repeats shard_index {index}")
        seen_indexes.add(index)
        root_value = raw.get("root")
        shard_source_root_value = raw.get("source_root")
        temporary_root_value = raw.get("temporary_root")
        marker_value = raw.get("ownership_marker")
        lock_value = raw.get("liveness_lock")
        if not isinstance(root_value, str) or not root_value:
            raise RecoveryError(f"run manifest shard {index} has incomplete paths")
        if not isinstance(shard_source_root_value, str) or not shard_source_root_value:
            raise RecoveryError(f"run manifest shard {index} has incomplete paths")
        if Path(shard_source_root_value).resolve() != source_root:
            raise RecoveryError(
                f"run manifest shard {index} source_root differs from the run root"
            )
        if not isinstance(temporary_root_value, str) or not temporary_root_value:
            raise RecoveryError(f"run manifest shard {index} has incomplete paths")
        if temporary_root_value != manifest_temporary_root:
            raise RecoveryError(
                f"run manifest shard {index} temporary_root differs from the run root"
            )
        if not isinstance(marker_value, str) or not marker_value:
            raise RecoveryError(f"run manifest shard {index} has incomplete paths")
        if not isinstance(lock_value, str) or not lock_value:
            raise RecoveryError(f"run manifest shard {index} has incomplete paths")
        shard_root = Path(root_value).resolve()
        temporary_root = Path(temporary_root_value).resolve()
        if temporary_root in {
            Path("/").resolve(),
            root,
            (root / STATE_DIRNAME).resolve(),
        }:
            raise RecoveryError(
                f"run manifest shard {index} names unsafe temporary_root "
                f"{temporary_root}"
            )
        if shard_root in {Path("/").resolve(), root, (root / STATE_DIRNAME).resolve()}:
            raise RecoveryError(
                f"run manifest shard {index} names unsafe root {shard_root}"
            )
        if shard_root in seen_roots:
            raise RecoveryError(f"run manifest repeats shard root {shard_root}")
        if shard_root == temporary_root or not shard_root.is_relative_to(
            temporary_root
        ):
            raise RecoveryError(
                f"run manifest shard {index} root {shard_root} escapes temporary_root "
                f"{temporary_root}"
            )
        seen_roots.add(shard_root)
        marker = _contained_path(Path(marker_value), shard_root, "ownership marker")
        lock = _contained_path(Path(lock_value), shard_root, "liveness lock")
        shards.append(
            ShardRecord(
                shard_index=index,
                root=shard_root,
                temporary_root=temporary_root,
                ownership_marker=marker,
                liveness_lock=lock,
            )
        )
    return state, RunRecord(
        run_id=run_id,
        manifest_path=manifest_path,
        manifest=manifest,
        source_root=source_root,
        source_manifest=source_manifest,
        source_manifest_digest=source_digest,
        plan_digest=plan_digest,
        diagnostic_pid=(
            coordinator.get("pid") if isinstance(coordinator.get("pid"), int) else None
        ),
        temporary_root=temporary_root,
        staging_temporary_root=staging_temporary_root,
        staging_shard_limit=staging_shard_limit,
        shards=tuple(shards),
    )


def _marker_error(record: RunRecord, shard: ShardRecord) -> str | None:
    try:
        marker = _read_json(shard.ownership_marker, "ownership marker")
    except RecoveryError as exc:
        return str(exc)
    expected: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": record.run_id,
        "shard_index": shard.shard_index,
        "source_manifest_digest": record.source_manifest_digest,
        "plan_digest": record.plan_digest,
    }
    for key, value in expected.items():
        if marker.get(key) != value:
            return (
                f"{shard.ownership_marker}: ownership marker {key} is "
                f"{marker.get(key)!r}, expected {value!r}"
            )
    return None


def _partial_staging_shards(record: RunRecord) -> tuple[PartialShardRecord, ...]:
    """Validate every entry before authorizing any pre-child staging cleanup."""

    temporary_root = record.staging_temporary_root
    shard_limit = record.staging_shard_limit
    if temporary_root is None or shard_limit is None:
        raise RecoveryError("zero-shard recovery has no private staging authority")
    _private_temporary_root(str(temporary_root), record.source_root)
    try:
        entries = sorted(temporary_root.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise RecoveryError(
            f"temporary_root {temporary_root} could not be enumerated: {exc}"
        ) from exc

    partials: list[PartialShardRecord] = []
    expected_marker_keys = {
        "schema_version",
        "run_id",
        "shard_index",
        "source_manifest_digest",
        "plan_digest",
    }
    for entry in entries:
        match = _PARTIAL_SHARD_NAME.fullmatch(entry.name)
        if match is None:
            raise RecoveryError(f"unexpected private-root entry {entry}")
        shard_index = int(match.group(1))
        if shard_index >= shard_limit:
            raise RecoveryError(
                f"partial shard {entry} exceeds effective shard bound {shard_limit}"
            )
        try:
            info = entry.lstat()
        except OSError as exc:
            raise RecoveryError(
                f"partial shard {entry} could not be inspected: {exc}"
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            raise RecoveryError(f"partial shard {entry} is a symlink")
        if not stat.S_ISDIR(info.st_mode):
            raise RecoveryError(f"partial shard {entry} is not a directory")
        if entry.resolve(strict=True) != entry:
            raise RecoveryError(f"partial shard {entry} is not canonical")

        state_directory = entry / STATE_DIRNAME
        _trusted_authority_directory(state_directory, "partial shard state directory")
        marker_path = entry / _EXECUTION_TREE_MARKER
        _trusted_authority_file(marker_path, "ownership marker")
        try:
            state_entries = {path.name for path in state_directory.iterdir()}
        except OSError as exc:
            raise RecoveryError(
                f"partial shard state directory {state_directory} could not be read: {exc}"
            ) from exc
        if state_entries != {marker_path.name}:
            raise RecoveryError(
                f"partial shard {entry} has child liveness or unexpected state evidence"
            )

        marker = _read_json(marker_path, "ownership marker")
        if set(marker) != expected_marker_keys:
            raise RecoveryError(
                f"{marker_path}: ownership marker does not have the exact field set"
            )
        expected: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": record.run_id,
            "shard_index": shard_index,
            "source_manifest_digest": record.source_manifest_digest,
            "plan_digest": record.plan_digest,
        }
        for key, value in expected.items():
            if marker.get(key) != value:
                raise RecoveryError(
                    f"{marker_path}: ownership marker {key} is "
                    f"{marker.get(key)!r}, expected {value!r}"
                )
        partials.append(
            PartialShardRecord(
                shard_index=shard_index,
                root=entry,
                ownership_marker=marker_path,
            )
        )
    return tuple(partials)


def _build_preflight(record: RunRecord) -> tuple[RecoveryPreflight, tuple[int, ...]]:
    remove: list[Path] = []
    leave: list[Path] = []
    unknown: list[str] = []
    leases: list[int] = []
    for shard in record.shards:
        marker_error = _marker_error(record, shard)
        if marker_error is not None:
            leave.append(shard.root)
            unknown.append(marker_error)
            continue
        try:
            lease = _acquire_liveness_lease(shard.liveness_lock)
        except RecoveryError as exc:
            leave.append(shard.root)
            unknown.append(str(exc))
            continue
        if _recovery_blocked(
            liveness_lock_held=lease is None,
            diagnostic_pid_exists=_diagnostic_pid_exists(record.diagnostic_pid),
        ):
            leave.append(shard.root)
            unknown.append(f"{shard.liveness_lock}: liveness lock is held")
            continue
        if lease is None:
            raise RecoveryError("liveness decision and acquired lease disagree")
        leases.append(lease)
        remove.append(shard.root)
    return (
        RecoveryPreflight(
            remove=tuple(remove), leave=tuple(leave), unknown=tuple(unknown)
        ),
        tuple(leases),
    )


def _shard_has_applied_state(root: Path) -> bool:
    state_path = root / STATE_DIRNAME / STATE_FILENAME
    if state_path.is_symlink():
        raise RecoveryError(f"shard state {state_path} is a symlink")
    try:
        state = _read_json(state_path, "shard state")
    except RecoveryError as exc:
        if not state_path.exists():
            return False
        raise exc
    applied = state.get("applied")
    if applied is None:
        return False
    if not isinstance(applied, dict):
        raise RecoveryError(f"shard state {state_path} has an invalid applied record")
    return True


def _default_restore(root: Path) -> str:
    from scripts.mutation_harness import restore

    return restore(root)


def _default_cleanup_for(record: RunRecord) -> CleanupOwnedTree:
    try:
        module = importlib.import_module("scripts.mutation_harness_execution_tree")
    except ImportError as exc:
        raise RecoveryError(
            "execution-tree cleanup backend is unavailable; retain the owned shard"
        ) from exc
    cleanup = getattr(module, "cleanup_execution_tree", None)
    cleanup_partial = getattr(module, "cleanup_partial_execution_tree", None)
    manifest_from_dict = getattr(module, "source_manifest_from_dict", None)
    staged_type = getattr(module, "StagedExecutionTree", None)
    if not callable(manifest_from_dict):
        raise RecoveryError(
            "execution-tree cleanup backend has no recovery-safe cleanup entry point"
        )
    if record.shards and (not callable(cleanup) or staged_type is None):
        raise RecoveryError(
            "execution-tree cleanup backend has no completed-shard cleanup entry point"
        )
    if not record.shards and not callable(cleanup_partial):
        raise RecoveryError(
            "execution-tree cleanup backend has no partial-staging cleanup entry point"
        )

    try:
        source_manifest = manifest_from_dict(record.source_manifest)
    except Exception as exc:
        raise RecoveryError(f"serialized source manifest is invalid: {exc}") from exc

    def cleanup_one(root: Path, marker: Path, shard_index: int) -> None:
        if not record.shards:
            temporary_root = record.staging_temporary_root
            if temporary_root is None:
                raise RecoveryError("partial cleanup has no temporary_root")
            if marker != root / _EXECUTION_TREE_MARKER:
                raise RecoveryError("partial cleanup marker path changed")
            assert callable(cleanup_partial)
            cleanup_partial(
                record.source_root,
                root,
                temporary_root=temporary_root,
                run_id=record.run_id,
                shard_index=shard_index,
                source_manifest=source_manifest,
                plan_digest=record.plan_digest,
                child_liveness_proven=True,
            )
            return
        assert staged_type is not None
        assert callable(cleanup)
        staged = staged_type(
            root=root,
            ownership_marker=marker,
            shard_index=shard_index,
            run_id=record.run_id,
            source_root=record.source_root,
            source_manifest=source_manifest,
            plan_digest=record.plan_digest,
        )
        cleanup(
            staged,
            temporary_root=next(
                shard.temporary_root
                for shard in record.shards
                if shard.root == root and shard.shard_index == shard_index
            ),
            child_liveness_proven=True,
        )

    return cleanup_one


def _recover_one(
    shard: ShardRecord,
    *,
    restore_mutation: RestoreMutation,
    cleanup_owned_tree: CleanupOwnedTree,
) -> None:
    if _shard_has_applied_state(shard.root):
        try:
            restore_mutation(shard.root)
        except Exception as exc:
            raise RecoveryError(
                f"{shard.root}: applied mutation restore failed: {exc}"
            ) from exc
        if _shard_has_applied_state(shard.root):
            raise RecoveryError(
                f"{shard.root}: restore returned but applied state remains"
            )
    try:
        cleanup_owned_tree(shard.root, shard.ownership_marker, shard.shard_index)
    except Exception as exc:
        raise RecoveryError(f"{shard.root}: verified cleanup failed: {exc}") from exc
    if shard.root.exists():
        raise RecoveryError(
            f"{shard.root}: cleanup returned but the owned shard still exists"
        )


def _recover_partial_staging(
    root: Path,
    state_path: Path,
    state: dict[str, Any],
    record: RunRecord,
    *,
    force: bool,
    cleanup_owned_tree: CleanupOwnedTree | None,
    output: TextIO | None,
) -> str:
    """Recover staging that aborted after ownership marking but before children."""

    import sys

    partials = _partial_staging_shards(record)
    temporary_root = record.staging_temporary_root
    if temporary_root is None:
        raise RecoveryError("partial recovery has no temporary_root")
    preflight = RecoveryPreflight(
        remove=tuple([*(partial.root for partial in partials), temporary_root]),
        leave=(),
        unknown=(),
    )
    if force:
        stream = output or sys.stdout
        stream.write(preflight.render(record.run_id))
        stream.flush()

    coordinator = state.get("coordinator_run")
    if not isinstance(coordinator, dict):
        raise RecoveryError("coordinator state changed during recovery")
    coordinator["lifecycle"] = "recovering"
    _write_root_recovery_json(root, state_path, state)

    cleanup_callback = cleanup_owned_tree
    if partials and cleanup_callback is None:
        cleanup_callback = _default_cleanup_for(record)
    failures: list[str] = []
    removed = 0
    for partial in partials:
        try:
            if cleanup_callback is None:
                raise RecoveryError("partial cleanup callback is unavailable")
            cleanup_callback(
                partial.root,
                partial.ownership_marker,
                partial.shard_index,
            )
            if partial.root.exists():
                raise RecoveryError(
                    f"{partial.root}: cleanup returned but the partial shard still exists"
                )
        except Exception as exc:
            failures.append(f"{partial.root}: verified partial cleanup failed: {exc}")
            if not force:
                break
        else:
            removed += 1

    if not failures:
        try:
            _private_temporary_root(str(temporary_root), record.source_root)
            temporary_root.rmdir()
        except (OSError, RecoveryError) as exc:
            failures.append(f"{temporary_root}: private-root cleanup failed: {exc}")

    if failures:
        coordinator["lifecycle"] = "recovering"
        coordinator["recovery_unknown"] = failures
        _write_root_recovery_json(root, state_path, state)
        raise RecoveryError(
            f"recovery retained pre-child staging; root state remains: {failures[0]}"
        )

    manifest = _read_root_recovery_json(root, record.manifest_path, "run manifest")
    manifest["lifecycle"] = "aborted"
    _write_root_recovery_json(root, record.manifest_path, manifest)
    coordinator["lifecycle"] = "aborted"
    _write_root_recovery_json(root, state_path, state)

    state.pop("coordinator_run", None)
    _write_root_recovery_json(root, state_path, state)
    return (
        f"recovered run {record.run_id} as aborted; removed {removed} "
        "owned staging shard(s)"
    )


def _recover_absent_temporary_root(
    root: Path,
    state_path: Path,
    state: dict[str, Any],
    record: RunRecord,
) -> str:
    """Clear an incomplete run after the whole recorded private root vanished."""

    coordinator = state.get("coordinator_run")
    if not isinstance(coordinator, dict):
        raise RecoveryError("coordinator state changed during recovery")
    lifecycle = coordinator.get("lifecycle")
    if lifecycle not in _INCOMPLETE_COORDINATOR_LIFECYCLES:
        raise RecoveryError(
            f"coordinator lifecycle {lifecycle!r} is not an incomplete lifecycle"
        )

    manifest = _read_root_recovery_json(root, record.manifest_path, "run manifest")
    if manifest != record.manifest:
        raise RecoveryError("run manifest changed during recovery")
    manifest["lifecycle"] = "aborted"
    _write_root_recovery_json(root, record.manifest_path, manifest)
    coordinator["lifecycle"] = "aborted"
    _write_root_recovery_json(root, state_path, state)

    # This is deliberately the final observation before clear-state-last. A path
    # that appeared after the first ENOENT is new, unauthorised filesystem state.
    if (
        _missing_private_temporary_root(
            coordinator.get("temporary_root"), record.source_root
        )
        is None
    ):
        raise RecoveryError(
            f"temporary_root {coordinator.get('temporary_root')} exists or was "
            "replaced during recovery; root state remains"
        )
    state.pop("coordinator_run", None)
    _write_root_recovery_json(root, state_path, state)
    return (
        f"recovered run {record.run_id} as aborted; recorded temporary root was "
        "absent; removed 0 owned shard(s)"
    )


def recover_run(
    root: Path,
    run_id: str,
    *,
    force: bool = False,
    restore_mutation: RestoreMutation | None = None,
    cleanup_owned_tree: CleanupOwnedTree | None = None,
    output: TextIO | None = None,
) -> str:
    """Recover only manifest-owned shards, then clear root state last.

    Recorded PID and process start time are diagnostics. They never decide
    liveness. A held advisory shard lock is the sole live-child refusal.
    """

    import sys

    root = root.resolve()
    state_path = root / STATE_DIRNAME / STATE_FILENAME
    state, record = _load_run_record(root, run_id)
    if not record.shards:
        return _recover_partial_staging(
            root,
            state_path,
            state,
            record,
            force=force,
            cleanup_owned_tree=cleanup_owned_tree,
            output=output,
        )
    missing_temporary_root = _missing_private_temporary_root(
        str(record.temporary_root) if record.temporary_root is not None else None,
        record.source_root,
    )
    if missing_temporary_root is not None:
        return _recover_absent_temporary_root(
            root,
            state_path,
            state,
            record,
        )
    preflight, liveness_leases = _build_preflight(record)
    try:
        if force:
            stream = output or sys.stdout
            stream.write(preflight.render(run_id))
            stream.flush()
        elif preflight.unknown:
            raise RecoveryError(preflight.unknown[0])

        coordinator = state["coordinator_run"]
        if not isinstance(coordinator, dict):
            raise RecoveryError("coordinator state changed during recovery")
        coordinator["lifecycle"] = "recovering"
        _write_root_recovery_json(root, state_path, state)

        restore_callback = restore_mutation or _default_restore
        cleanup_callback = cleanup_owned_tree or _default_cleanup_for(record)
        removable = set(preflight.remove)
        failures: list[str] = []
        removed = 0
        for shard in record.shards:
            if shard.root not in removable:
                continue
            try:
                _recover_one(
                    shard,
                    restore_mutation=restore_callback,
                    cleanup_owned_tree=cleanup_callback,
                )
            except RecoveryError as exc:
                failures.append(str(exc))
                if not force:
                    break
            else:
                removed += 1

        retained = len(record.shards) - removed
        if retained or failures or preflight.unknown:
            coordinator["lifecycle"] = "recovering"
            coordinator["recovery_unknown"] = [*preflight.unknown, *failures]
            _write_root_recovery_json(root, state_path, state)
            detail = failures[0] if failures else preflight.unknown[0]
            raise RecoveryError(
                f"recovery retained {retained} shard(s); root state remains: {detail}"
            )

        # The durable run record is updated before the root gate is cleared. The
        # coordinator state remains present during every restore and cleanup call.
        manifest = _read_root_recovery_json(root, record.manifest_path, "run manifest")
        manifest["lifecycle"] = "aborted"
        _write_root_recovery_json(root, record.manifest_path, manifest)
        coordinator["lifecycle"] = "aborted"
        _write_root_recovery_json(root, state_path, state)

        # Clear the root coordinator record last. Preserve unrelated root state.
        state.pop("coordinator_run", None)
        _write_root_recovery_json(root, state_path, state)
        return f"recovered run {run_id} as aborted; removed {removed} owned shard(s)"
    finally:
        for descriptor in liveness_leases:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
