#!/usr/bin/env python3
"""Fail-closed coordinator for isolated mutation-harness shards.

The coordinator owns the source-root lock, recovery state, aggregate event log,
and final report.  Children receive only shard-local paths.  They stream events
on stdout and durably append results to their shard-local result stream.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Protocol, cast

try:
    from scripts.mutation_harness import (
        REPORT_FILENAME,
        SCHEMA_VERSION,
        VERDICT_BASELINE_FAILED,
        VERDICT_INVALID,
        VERDICT_PROOF_SKIPPED,
        VERDICT_PROOF_VACUOUS,
        VERDICT_STALE_DECLARATION,
        VERDICT_SURVIVED,
        HarnessError,
        Result,
        _atomic_write,
        _read_state,
        _state_dir,
        _write_state,
        acquire_lock,
        release_lock,
    )
except ModuleNotFoundError:  # Direct internal liveness-wrapper execution.
    from mutation_harness import (  # type: ignore[import-not-found,no-redef]
        REPORT_FILENAME,
        SCHEMA_VERSION,
        VERDICT_BASELINE_FAILED,
        VERDICT_INVALID,
        VERDICT_PROOF_SKIPPED,
        VERDICT_PROOF_VACUOUS,
        VERDICT_STALE_DECLARATION,
        VERDICT_SURVIVED,
        HarnessError,
        Result,
        _atomic_write,
        _read_state,
        _state_dir,
        _write_state,
        acquire_lock,
        release_lock,
    )

RUNS_DIRNAME = "runs"
MANIFEST_FILENAME = "manifest.json"
EVENT_LOG_FILENAME = "events.jsonl"
COORDINATOR_SCHEMA_VERSION = 1

AGGREGATE_COMPLETE = "COMPLETE"
AGGREGATE_INVALID = "AGGREGATE_INVALID"
AGGREGATE_CHILD_FAILED = "CHILD_FAILED"
AGGREGATE_SOURCE_DRIFTED = "SOURCE_DRIFTED"
AGGREGATE_CLEANUP_FAILED = "CLEANUP_FAILED"

_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_DURATION_RE = re.compile(r"\((?:\d+(?:\.\d+)?)(?:ns|us|µs|ms|s|m|h)\)")
_POSIX_ABSOLUTE_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_.>:/\-])/(?:[^\s\]\[(){}<>:'\"]+/)*[^\s\]\[(){}<>:'\",;]*"
)
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"\b[A-Za-z]:\\[^\s\]\[(){}<>:'\",;]+")

_CHILD_EVENTS = frozenset({"mutation_started", "mutation_finished"})
_RESULT_FIELDS = frozenset({"id", "verdict", "detail", "failing_proof", "warnings"})
CANONICAL_EXCLUDED_FIELDS = (
    "run_id",
    "shard_index",
    "pid",
    "timestamp",
    "elapsed_ms",
    "event_log_path",
)


class AggregateRefusal(HarnessError):
    """A child result set is not an exact projection of the selected plan."""

    def __init__(
        self,
        reasons: Sequence[str],
        *,
        partial_results: Sequence[Result] = (),
        missing_ids: Sequence[str] = (),
    ) -> None:
        super().__init__("; ".join(reasons))
        self.reasons = tuple(reasons)
        self.partial_results = tuple(partial_results)
        self.missing_ids = tuple(missing_ids)


class DetailNormalizationError(HarnessError):
    """A detail contains run-varying bytes the normalizer does not understand."""


@dataclass(frozen=True)
class SelectedMutation:
    """A plan entry after ``--only`` selection, with both useful ordinals."""

    identifier: str
    plan_ordinal: int
    selected_ordinal: int


@dataclass(frozen=True)
class ShardAssignment:
    """Stable selected-plan ordinals owned by one shard."""

    shard_index: int
    mutations: tuple[SelectedMutation, ...]

    @property
    def assigned_ordinals(self) -> tuple[int, ...]:
        return tuple(item.selected_ordinal for item in self.mutations)


class Cleanup(Protocol):
    """Ownership-checked cleanup supplied by the execution-tree lane."""

    def __call__(self) -> None: ...


@dataclass(frozen=True)
class ChildSpec:
    """Shard-local process contract returned by the staging callback."""

    assignment: ShardAssignment
    root: Path
    source_root: Path
    temporary_root: Path
    argv: tuple[str, ...]
    result_stream: Path
    ownership_marker: Path
    liveness_lock: Path
    environment: Mapping[str, str] = field(default_factory=dict)
    cleanup: Cleanup | None = None


@dataclass(frozen=True)
class ChildResultRecord:
    """One durable child result with coordinator validation metadata."""

    run_id: str
    shard_index: int
    plan_digest: str
    plan_ordinal: int
    mutation_id: str
    result: Result


@dataclass(frozen=True)
class AggregatedResults:
    """Exact, selected-plan-ordered child results."""

    results: tuple[Result, ...]
    records: tuple[ChildResultRecord, ...]


@dataclass
class ChildRuntime:
    """Coordinator-owned live process state."""

    spec: ChildSpec
    process: subprocess.Popen[str]
    stderr_handle: IO[str]
    lifecycle_events: dict[str, str] = field(default_factory=dict)
    finished_events: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class CoordinatorOutcome:
    """Complete aggregate outcome returned to the harness CLI seam."""

    results: tuple[Result, ...]
    exit_code: int
    aggregate_status: str
    report_path: Path
    event_log_path: Path
    unmeasured_ids: tuple[str, ...]
    measured_lost_ids: tuple[str, ...]


def select_and_assign(
    plan_mutation_ids: Sequence[str],
    only: set[str] | None,
    requested_shards: int,
) -> tuple[ShardAssignment, ...]:
    """Apply ``--only`` first, then assign selected ordinal modulo shard count."""

    if requested_shards < 1:
        raise HarnessError("requested shard count must be greater than zero")
    if len(set(plan_mutation_ids)) != len(plan_mutation_ids):
        raise HarnessError("plan mutation identifiers must be unique before assignment")
    known = set(plan_mutation_ids)
    if only is not None:
        unknown = only - known
        if unknown:
            raise HarnessError(f"--only names unknown mutations: {sorted(unknown)}")
    selected = [
        SelectedMutation(identifier, plan_ordinal, selected_ordinal)
        for selected_ordinal, (plan_ordinal, identifier) in enumerate(
            pair
            for pair in enumerate(plan_mutation_ids)
            if only is None or pair[1] in only
        )
    ]
    if not selected:
        raise HarnessError("the selected mutation set is empty")
    effective = min(requested_shards, len(selected))
    buckets: list[list[SelectedMutation]] = [[] for _ in range(effective)]
    for item in selected:
        buckets[item.selected_ordinal % effective].append(item)
    return tuple(
        ShardAssignment(shard_index=index, mutations=tuple(items))
        for index, items in enumerate(buckets)
    )


def append_durable_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    """Append one flushed and fsynced JSON line without following a symlink."""

    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    line = (json.dumps(dict(payload), sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(path, flags, 0o600)
    try:
        written = os.write(descriptor, line)
        if written != len(line):
            raise HarnessError(f"short JSONL write to {path}: {written}/{len(line)}")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Load every complete durable record and report malformed residue."""

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return [], []
    except OSError as exc:
        return [], [f"cannot read child result stream {path}: {exc}"]
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for number, line in enumerate(raw.splitlines(keepends=True), 1):
        if not line.endswith(b"\n"):
            errors.append(f"child result stream {path} ends with a partial JSON line")
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(
                f"child result stream {path} line {number} is not JSON: {exc}"
            )
            continue
        if not isinstance(item, dict):
            errors.append(f"child result stream {path} line {number} is not an object")
            continue
        records.append(item)
    return records, errors


def parse_child_result(raw: Mapping[str, Any]) -> ChildResultRecord:
    """Parse the closed durable child-result protocol."""

    required = {
        "schema_version",
        "run_id",
        "shard_index",
        "plan_digest",
        "plan_ordinal",
        "mutation_id",
        "result",
    }
    unknown = set(raw) - required
    missing = required - set(raw)
    if unknown or missing:
        raise HarnessError(
            f"child result keys differ: missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    if raw["schema_version"] != COORDINATOR_SCHEMA_VERSION:
        raise HarnessError(
            f"unsupported child result schema_version {raw['schema_version']!r}"
        )
    result_raw = raw["result"]
    if not isinstance(result_raw, dict):
        raise HarnessError("child result field 'result' must be an object")
    result_unknown = set(result_raw) - _RESULT_FIELDS
    result_missing = _RESULT_FIELDS - set(result_raw)
    if result_unknown or result_missing:
        raise HarnessError(
            "child mutation result keys differ: "
            f"missing={sorted(result_missing)}, unknown={sorted(result_unknown)}"
        )
    mutation_id = raw["mutation_id"]
    if not isinstance(mutation_id, str) or not mutation_id:
        raise HarnessError("child result mutation_id must be a non-empty string")
    if result_raw["id"] != mutation_id:
        raise HarnessError(
            f"child result id {result_raw['id']!r} does not match {mutation_id!r}"
        )
    warnings = result_raw["warnings"]
    if not isinstance(warnings, list) or not all(isinstance(x, str) for x in warnings):
        raise HarnessError("child result warnings must be a string array")
    failing_proof = result_raw["failing_proof"]
    if failing_proof is not None and not isinstance(failing_proof, str):
        raise HarnessError("child result failing_proof must be null or a string")
    if not isinstance(raw["shard_index"], int) or isinstance(raw["shard_index"], bool):
        raise HarnessError("child result shard_index must be an integer")
    if not isinstance(raw["plan_ordinal"], int) or isinstance(
        raw["plan_ordinal"], bool
    ):
        raise HarnessError("child result plan_ordinal must be an integer")
    for field_name in ("run_id", "plan_digest"):
        if not isinstance(raw[field_name], str) or not raw[field_name]:
            raise HarnessError(f"child result {field_name} must be a non-empty string")
    for field_name in ("verdict", "detail"):
        if not isinstance(result_raw[field_name], str):
            raise HarnessError(f"child mutation result {field_name} must be a string")
    return ChildResultRecord(
        run_id=raw["run_id"],
        shard_index=raw["shard_index"],
        plan_digest=raw["plan_digest"],
        plan_ordinal=raw["plan_ordinal"],
        mutation_id=mutation_id,
        result=Result(
            identifier=mutation_id,
            verdict=result_raw["verdict"],
            detail=result_raw["detail"],
            failing_proof=failing_proof,
            warnings=list(warnings),
        ),
    )


def aggregate_child_results(
    raw_records: Iterable[Mapping[str, Any]],
    assignments: Sequence[ShardAssignment],
    *,
    run_id: str,
    plan_digest: str,
) -> AggregatedResults:
    """Require exactly one correctly bound result for each selected mutation."""

    expected = {
        item.identifier: (assignment.shard_index, item.selected_ordinal)
        for assignment in assignments
        for item in assignment.mutations
    }
    seen: dict[str, ChildResultRecord] = {}
    duplicates: set[str] = set()
    unknown: set[str] = set()
    reasons: list[str] = []
    for raw in raw_records:
        try:
            record = parse_child_result(raw)
        except HarnessError as exc:
            reasons.append(str(exc))
            continue
        if record.run_id != run_id:
            reasons.append(
                f"result {record.mutation_id} run_id mismatch: "
                f"{record.run_id!r} != {run_id!r}"
            )
        if record.plan_digest != plan_digest:
            reasons.append(
                f"result {record.mutation_id} plan digest mismatch: "
                f"{record.plan_digest!r} != {plan_digest!r}"
            )
        binding = expected.get(record.mutation_id)
        if binding is None:
            unknown.add(record.mutation_id)
            continue
        expected_shard, expected_ordinal = binding
        if record.shard_index != expected_shard:
            reasons.append(
                f"result {record.mutation_id} came from shard {record.shard_index}, "
                f"expected {expected_shard}"
            )
        if record.plan_ordinal != expected_ordinal:
            reasons.append(
                f"result {record.mutation_id} ordinal {record.plan_ordinal}, "
                f"expected {expected_ordinal}"
            )
        if record.mutation_id in seen:
            duplicates.add(record.mutation_id)
        else:
            seen[record.mutation_id] = record
    missing = sorted(set(expected) - set(seen))
    if unknown:
        reasons.append(f"unknown mutation results: {sorted(unknown)}")
    if duplicates:
        reasons.append(f"duplicate mutation results: {sorted(duplicates)}")
    if missing:
        reasons.append(f"missing mutation results: {missing}")
    ordered_ids = [
        item.identifier for assignment in assignments for item in assignment.mutations
    ]
    ordered_ids.sort(key=lambda identifier: expected[identifier][1])
    partial = [
        seen[identifier].result for identifier in ordered_ids if identifier in seen
    ]
    if reasons:
        raise AggregateRefusal(reasons, partial_results=partial, missing_ids=missing)
    records = tuple(seen[identifier] for identifier in ordered_ids)
    return AggregatedResults(
        results=tuple(record.result for record in records), records=records
    )


def normalize_detail(
    detail: str,
    *,
    shard_roots: Sequence[Path],
    temporary_roots: Sequence[Path],
) -> str:
    """Normalize only named shard roots, named temporary roots, and durations."""

    normalized = detail
    replacements: list[tuple[str, str]] = []
    for path in shard_roots:
        replacements.append((str(path.resolve()), "<SHARD_ROOT>"))
    for path in temporary_roots:
        replacements.append((str(path.resolve()), "<TMP>"))
    for original, marker in sorted(
        replacements, key=lambda item: len(item[0]), reverse=True
    ):
        normalized = normalized.replace(original, marker)
    normalized = _DURATION_RE.sub("<DURATION>", normalized)
    absolute = _POSIX_ABSOLUTE_PATH_RE.search(normalized)
    windows = _WINDOWS_ABSOLUTE_PATH_RE.search(normalized)
    if absolute is not None or windows is not None:
        match = absolute if absolute is not None else windows
        assert match is not None
        raise DetailNormalizationError(
            f"unrecognised absolute path in result detail: {match.group(0)!r}"
        )
    return normalized


def canonical_result_projection(
    results: Sequence[Result | Mapping[str, Any]],
    *,
    shard_roots: Sequence[Path],
    temporary_roots: Sequence[Path],
) -> tuple[dict[str, Any], ...]:
    """Return the normative serial/sharded differential projection."""

    projected: list[dict[str, Any]] = []
    for item in results:
        if isinstance(item, Result):
            identifier = item.identifier
            verdict = item.verdict
            detail = item.detail
            failing_proof = item.failing_proof
            warnings = item.warnings
        else:
            identifier = item["id"]
            verdict = item["verdict"]
            detail = item["detail"]
            failing_proof = item["failing_proof"]
            warnings = item["warnings"]
        projected.append(
            {
                "id": identifier,
                "verdict": verdict,
                "detail": normalize_detail(
                    str(detail),
                    shard_roots=shard_roots,
                    temporary_roots=temporary_roots,
                ),
                "failing_proof": failing_proof,
                "warnings": list(warnings),
            }
        )
    return tuple(projected)


class CoordinatorLease:
    """Root-lock owner and durable coordinator state lifecycle."""

    def __init__(self, root: Path, lock: Path, state: dict[str, Any]) -> None:
        self.root = root
        self.lock = lock
        self.state = state
        self.closed = False

    def persist(self) -> None:
        _write_state(
            self.root,
            {"schema_version": SCHEMA_VERSION, "coordinator_run": self.state},
        )

    def transition(self, lifecycle: str) -> None:
        self.state["lifecycle"] = lifecycle
        self.persist()

    def clear_and_release(self) -> None:
        if self.closed:
            return
        _write_state(self.root, None)
        release_lock(self.lock)
        self.closed = True

    def retain_and_release(self) -> None:
        if self.closed:
            return
        release_lock(self.lock)
        self.closed = True


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _validate_run_temporary_root(source_root: Path, temporary_root: Path) -> Path:
    """Return one canonical run root that cannot contain or be under the source."""

    resolved_source = source_root.resolve()
    resolved_temporary = temporary_root.resolve()
    if _paths_overlap(resolved_source, resolved_temporary):
        raise HarnessError(
            f"run temporary_root {resolved_temporary} overlaps the invoking "
            f"source root {resolved_source}"
        )
    return resolved_temporary


def _cleanup_pre_authority_startup(
    temporary_root: Path | None,
    run_dir: Path,
    *,
    run_dir_created: bool,
    manifest_written: bool,
) -> None:
    """Remove only empty artifacts created by this startup attempt."""

    if run_dir_created:
        manifest_path = run_dir / MANIFEST_FILENAME
        if (
            manifest_written
            and manifest_path.is_file()
            and not manifest_path.is_symlink()
        ):
            try:
                manifest_path.unlink()
            except OSError:
                pass
        try:
            run_dir.rmdir()
        except OSError:
            pass
    if temporary_root is not None:
        try:
            temporary_root.rmdir()
        except OSError:
            pass


def begin_coordinator_run(
    root: Path,
    *,
    run_id: str,
    source_head: str,
    source_manifest: Mapping[str, Any],
    source_manifest_digest: str,
    plan_path: Path,
    plan_digest: str,
    requested_shards: int,
    effective_shards: int,
    temporary_root_factory: Callable[[str], Path],
) -> CoordinatorLease:
    """Atomically claim the root, then write state before any staging callback."""

    if _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise HarnessError(f"unsafe coordinator run id: {run_id!r}")
    if source_manifest.get("digest") != source_manifest_digest:
        raise HarnessError(
            "source manifest mapping digest does not match source_manifest_digest"
        )
    lock = acquire_lock(root)
    run_dir = (_state_dir(root) / RUNS_DIRNAME / run_id).resolve()
    state_root = _state_dir(root).resolve()
    temporary_root: Path | None = None
    run_dir_created = False
    manifest_written = False
    authority_persisted = False
    try:
        existing = _read_state(root)
        if existing:
            raise HarnessError(
                "root state exists after the coordinator acquired the lock; "
                "recover it before staging"
            )
        if not run_dir.is_relative_to(state_root):
            raise HarnessError("coordinator run directory escapes the state root")
        if run_dir.exists() or run_dir.is_symlink():
            raise FileExistsError(
                f"coordinator run directory already exists: {run_dir}"
            )
        temporary_root = _validate_run_temporary_root(
            root, temporary_root_factory(run_id)
        )
        run_dir.mkdir(parents=True, exist_ok=False)
        run_dir_created = True
        manifest_path = (run_dir / MANIFEST_FILENAME).resolve()
        state: dict[str, Any] = {
            "run_id": run_id,
            "pid": os.getpid(),
            "process_start_time": time.time_ns(),
            "lifecycle": "staging",
            "source_root": str(root.resolve()),
            "temporary_root": str(temporary_root),
            "source_head": source_head,
            "source_manifest": json.loads(json.dumps(dict(source_manifest))),
            "source_manifest_digest": source_manifest_digest,
            "plan_path": str(plan_path.resolve()),
            "plan_digest": plan_digest,
            "requested_shards": requested_shards,
            "effective_shards": effective_shards,
            "manifest_path": str(manifest_path),
            "shards": [],
        }
        lease = CoordinatorLease(root, lock, state)
        _write_manifest(lease)
        manifest_written = True
        lease.persist()
        authority_persisted = True
        return lease
    except BaseException:
        if not authority_persisted:
            _cleanup_pre_authority_startup(
                temporary_root,
                run_dir,
                run_dir_created=run_dir_created,
                manifest_written=manifest_written,
            )
        release_lock(lock)
        raise


class EventWriter:
    """The only writer of the root event log and its monotonic sequence."""

    def __init__(
        self,
        path: Path,
        run_id: str,
        total: int,
        progress: str,
        stream: IO[str],
    ) -> None:
        self.path = path
        self.run_id = run_id
        self.total = total
        self.progress = progress
        self.stream = stream
        self.sequence = 0
        self.active: set[str] = set()
        self.completed: set[str] = set()

    def emit(self, event: str, **fields: Any) -> dict[str, Any]:
        mutation_id = fields.get("mutation_id")
        if event == "mutation_started" and isinstance(mutation_id, str):
            self.active.add(mutation_id)
        if event == "mutation_finished" and isinstance(mutation_id, str):
            self.active.discard(mutation_id)
            self.completed.add(mutation_id)
        self.sequence += 1
        payload = {
            "schema_version": COORDINATOR_SCHEMA_VERSION,
            "sequence": self.sequence,
            "run_id": self.run_id,
            "event": event,
            "completed": len(self.completed),
            "active": len(self.active),
            "total": self.total,
            **fields,
        }
        append_durable_jsonl(self.path, payload)
        if self.progress == "jsonl":
            self.stream.write(json.dumps(payload, sort_keys=True) + "\n")
            self.stream.flush()
        elif self.progress == "human":
            subject = mutation_id or fields.get("shard_index", "")
            self.stream.write(
                f"[{payload['completed']}/{self.total}] {event} {subject}\n"
            )
            self.stream.flush()
        return payload

    def ingest_child(
        self,
        shard: ShardAssignment,
        raw: Mapping[str, Any],
        lifecycle: dict[str, str],
    ) -> None:
        allowed = {
            "schema_version",
            "run_id",
            "event",
            "shard_index",
            "plan_ordinal",
            "mutation_id",
            "phase",
            "verdict",
            "elapsed_ms",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise HarnessError(f"unknown child event keys: {sorted(unknown)}")
        if raw.get("schema_version") != COORDINATOR_SCHEMA_VERSION:
            raise HarnessError("child event schema_version mismatch")
        if raw.get("run_id") != self.run_id:
            raise HarnessError("child event run_id mismatch")
        if raw.get("shard_index") != shard.shard_index:
            raise HarnessError("child event shard_index mismatch")
        event = raw.get("event")
        if event not in _CHILD_EVENTS:
            raise HarnessError(f"child event is not permitted: {event!r}")
        mutation_id = raw.get("mutation_id")
        binding = {item.identifier: item.selected_ordinal for item in shard.mutations}
        if mutation_id not in binding:
            raise HarnessError(f"child event names unassigned mutation {mutation_id!r}")
        if raw.get("plan_ordinal") != binding[mutation_id]:
            raise HarnessError(f"child event ordinal mismatch for {mutation_id}")
        previous = lifecycle.get(mutation_id)
        if event == "mutation_started":
            if previous is not None:
                raise HarnessError(
                    f"shard {shard.shard_index} duplicate or late "
                    f"mutation_started event for {mutation_id}"
                )
            lifecycle[mutation_id] = "started"
        elif previous != "started":
            raise HarnessError(
                f"shard {shard.shard_index} mutation_finished event without "
                f"one prior mutation_started event for {mutation_id}"
            )
        else:
            lifecycle[mutation_id] = "finished"
        forwarded = {
            key: value
            for key, value in raw.items()
            if key
            not in {
                "schema_version",
                "run_id",
                "event",
            }
        }
        self.emit(str(event), **forwarded)


def _manifest_payload(lease: CoordinatorLease) -> dict[str, Any]:
    state = lease.state
    source_manifest = state["source_manifest"]
    if (
        not isinstance(source_manifest, Mapping)
        or source_manifest.get("digest") != state["source_manifest_digest"]
    ):
        raise HarnessError(
            "source manifest mapping digest does not match source_manifest_digest"
        )
    recorded_temporary = state.get("temporary_root")
    if not isinstance(recorded_temporary, str):
        raise HarnessError("coordinator state has no temporary_root")
    temporary_root = Path(recorded_temporary)
    if not temporary_root.is_absolute() or temporary_root.resolve() != temporary_root:
        raise HarnessError("coordinator temporary_root is not canonical and absolute")
    _validate_run_temporary_root(Path(state["source_root"]), temporary_root)
    for shard in state["shards"]:
        if shard.get("temporary_root") != recorded_temporary:
            raise HarnessError(
                "recorded shard temporary_root does not match the coordinator run"
            )
    return {
        "schema_version": COORDINATOR_SCHEMA_VERSION,
        "run_id": state["run_id"],
        "source_root": state["source_root"],
        "temporary_root": recorded_temporary,
        "source_head": state["source_head"],
        "source_manifest": state["source_manifest"],
        "source_manifest_digest": state["source_manifest_digest"],
        "plan_path": state["plan_path"],
        "plan_digest": state["plan_digest"],
        "requested_shards": state["requested_shards"],
        "effective_shards": state["effective_shards"],
        "shards": state["shards"],
    }


def _write_manifest(lease: CoordinatorLease) -> None:
    target = Path(lease.state["manifest_path"])
    _atomic_write(
        target,
        (json.dumps(_manifest_payload(lease), indent=2) + "\n").encode("utf-8"),
    )


def _record_child(lease: CoordinatorLease, spec: ChildSpec) -> None:
    shard = {
        "shard_index": spec.assignment.shard_index,
        "root": str(spec.root.resolve()),
        "source_root": str(spec.source_root.resolve()),
        "temporary_root": str(spec.temporary_root.resolve()),
        "ownership_marker": str(spec.ownership_marker.resolve()),
        "liveness_lock": str(spec.liveness_lock.resolve()),
        "assigned_ordinals": list(spec.assignment.assigned_ordinals),
        "lifecycle": "staged",
        "pid": None,
        "process_start_time": None,
    }
    lease.state["shards"].append(shard)
    _write_manifest(lease)
    lease.persist()


def _validate_child_spec(
    source_root: Path,
    temporary_root: Path,
    spec: ChildSpec,
    prior_roots: set[Path],
) -> None:
    """Keep every child-owned write and liveness token inside one unique shard."""

    resolved_source = source_root.resolve()
    resolved_run_temporary = temporary_root.resolve()
    resolved_root = spec.root.resolve()
    resolved_temporary = spec.temporary_root.resolve()
    if spec.source_root.resolve() != resolved_source:
        raise HarnessError("child spec source_root does not match the coordinator root")
    if resolved_temporary != resolved_run_temporary:
        raise HarnessError(
            "child temporary_root does not match the coordinator run temporary_root"
        )
    if _paths_overlap(resolved_source, resolved_temporary):
        raise HarnessError(
            f"child temporary_root {resolved_temporary} overlaps the invoking "
            f"source root {resolved_source}"
        )
    if _paths_overlap(resolved_source, resolved_root):
        raise HarnessError(
            f"child shard root {resolved_root} overlaps the invoking source root "
            f"{resolved_source}"
        )
    if resolved_root in prior_roots:
        raise HarnessError(
            f"two mutation shards resolved to the same root: {resolved_root}"
        )
    if not resolved_root.is_relative_to(resolved_temporary):
        raise HarnessError("child shard root escapes its recorded temporary_root")
    for label, path in (
        ("result_stream", spec.result_stream),
        ("ownership_marker", spec.ownership_marker),
        ("liveness_lock", spec.liveness_lock),
    ):
        if not path.resolve().is_relative_to(resolved_root):
            raise HarnessError(f"child {label} escapes shard root {resolved_root}")
    if not spec.argv or not all(spec.argv):
        raise HarnessError("child argv must contain non-empty strings")
    prior_roots.add(resolved_root)


def _launch_child(spec: ChildSpec, run_id: str, plan_digest: str) -> ChildRuntime:
    stderr_path = spec.root / ".mutation-harness" / "child.stderr.log"
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    argv = (
        sys.executable,
        str(Path(__file__).resolve()),
        "_hold-liveness",
        str(spec.liveness_lock),
        "--",
        *spec.argv,
    )
    environment = os.environ.copy()
    environment.update(spec.environment)
    environment.update(
        {
            "MUTATION_HARNESS_RUN_ID": run_id,
            "MUTATION_HARNESS_SHARD_INDEX": str(spec.assignment.shard_index),
            "MUTATION_HARNESS_PLAN_DIGEST": plan_digest,
            "MUTATION_HARNESS_RESULT_STREAM": str(spec.result_stream),
        }
    )
    try:
        process = subprocess.Popen(  # noqa: S603 - reviewed argv, shell=False
            list(argv),
            cwd=spec.root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=stderr_handle,
            text=True,
            bufsize=1,
        )
    except BaseException:
        stderr_handle.close()
        raise
    if process.stdout is None:
        process.terminate()
        process.wait()
        stderr_handle.close()
        raise HarnessError("child stdout pipe was not created")
    return ChildRuntime(spec=spec, process=process, stderr_handle=stderr_handle)


def _parse_child_event_line(runtime: ChildRuntime, line: str) -> dict[str, Any]:
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        raise HarnessError(
            f"shard {runtime.spec.assignment.shard_index} emitted invalid JSON: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise HarnessError("child event must be a JSON object")
    return raw


def _append_missing_lifecycle_errors(runtime: ChildRuntime, errors: list[str]) -> None:
    for item in runtime.spec.assignment.mutations:
        lifecycle = runtime.lifecycle_events.get(item.identifier)
        if lifecycle is None:
            errors.append(
                f"shard {runtime.spec.assignment.shard_index} missing "
                f"mutation_started event for {item.identifier}"
            )
        if lifecycle != "finished":
            errors.append(
                f"shard {runtime.spec.assignment.shard_index} missing "
                f"mutation_finished event for {item.identifier}"
            )


def _tail_children(
    runtimes: Sequence[ChildRuntime], writer: EventWriter
) -> tuple[dict[int, int], list[str]]:
    selector = selectors.DefaultSelector()
    by_stream: dict[IO[str], ChildRuntime] = {}
    errors: list[str] = []
    for runtime in runtimes:
        assert runtime.process.stdout is not None
        selector.register(runtime.process.stdout, selectors.EVENT_READ)
        by_stream[runtime.process.stdout] = runtime
    try:
        while by_stream:
            for key, _mask in selector.select():
                stream = cast(IO[str], key.fileobj)
                assert hasattr(stream, "readline")
                line = stream.readline()
                runtime = by_stream[stream]
                if line == "":
                    selector.unregister(stream)
                    del by_stream[stream]
                    continue
                try:
                    raw = _parse_child_event_line(runtime, line)
                    writer.ingest_child(
                        runtime.spec.assignment, raw, runtime.lifecycle_events
                    )
                    if raw.get("event") == "mutation_finished":
                        mutation_id = raw.get("mutation_id")
                        if isinstance(mutation_id, str):
                            runtime.finished_events.add(mutation_id)
                except HarnessError as exc:
                    errors.append(str(exc))
    finally:
        selector.close()
    exits: dict[int, int] = {}
    for runtime in runtimes:
        exits[runtime.spec.assignment.shard_index] = runtime.process.wait()
        runtime.stderr_handle.close()
        _append_missing_lifecycle_errors(runtime, errors)
        writer.emit(
            "shard_finished",
            shard_index=runtime.spec.assignment.shard_index,
            phase="complete" if runtime.process.returncode == 0 else "aborted",
        )
    return exits, errors


def _result_payload(result: Result) -> dict[str, Any]:
    return {
        "id": result.identifier,
        "verdict": result.verdict,
        "detail": result.detail,
        "failing_proof": result.failing_proof,
        "warnings": result.warnings,
    }


def _result_exit_policy(results: Sequence[Result], assert_all_killed: bool) -> int:
    unmeasured_verdicts = {
        VERDICT_BASELINE_FAILED,
        VERDICT_INVALID,
        VERDICT_STALE_DECLARATION,
        VERDICT_PROOF_VACUOUS,
        VERDICT_PROOF_SKIPPED,
    }
    if any(result.verdict in unmeasured_verdicts for result in results):
        return 1
    if assert_all_killed and any(
        result.verdict == VERDICT_SURVIVED for result in results
    ):
        return 1
    return 0


def write_aggregate_report(
    report_path: Path,
    report: Mapping[str, Any],
    *,
    all_children_complete: bool,
) -> None:
    """Write the final report only after every launched child has terminated."""

    if not all_children_complete:
        raise HarnessError(
            "refusing to write an aggregate before all children complete"
        )
    _atomic_write(
        report_path,
        (json.dumps(dict(report), indent=2) + "\n").encode("utf-8"),
    )


def coordinator_run(
    root: Path,
    plan_path: Path,
    plan_name: str,
    plan_mutation_ids: Sequence[str],
    only: set[str] | None,
    assert_all_killed: bool,
    *,
    requested_shards: int,
    progress: str,
    source_head: str,
    source_manifest: Mapping[str, Any],
    source_manifest_digest: str,
    plan_digest: str,
    source_manifest_reader: Callable[[], str],
    temporary_root_factory: Callable[[str], Path],
    child_factory: Callable[[ShardAssignment, str], ChildSpec],
    run_id: str | None = None,
    progress_stream: IO[str] | None = None,
    before_report: Callable[[], None] | None = None,
) -> CoordinatorOutcome:
    """Run isolated children while the coordinator owns every root artifact."""

    if progress not in {"human", "jsonl", "none"}:
        raise HarnessError(f"unknown progress mode: {progress!r}")
    assignments = select_and_assign(plan_mutation_ids, only, requested_shards)
    effective_shards = len(assignments)
    actual_run_id = run_id or f"run-{uuid.uuid4().hex}"
    lease = begin_coordinator_run(
        root,
        run_id=actual_run_id,
        source_head=source_head,
        source_manifest=source_manifest,
        source_manifest_digest=source_manifest_digest,
        plan_path=plan_path,
        plan_digest=plan_digest,
        requested_shards=requested_shards,
        effective_shards=effective_shards,
        temporary_root_factory=temporary_root_factory,
    )
    run_dir = Path(lease.state["manifest_path"]).parent
    temporary_root = Path(lease.state["temporary_root"])
    event_log = run_dir / EVENT_LOG_FILENAME
    report_path = _state_dir(root) / REPORT_FILENAME
    writer = EventWriter(
        event_log,
        actual_run_id,
        sum(len(item.mutations) for item in assignments),
        progress,
        progress_stream or sys.stderr,
    )
    children: list[ChildSpec] = []
    runtimes: list[ChildRuntime] = []
    aggregate_status = AGGREGATE_COMPLETE
    results: tuple[Result, ...] = ()
    unmeasured: tuple[str, ...] = ()
    measured_lost: tuple[str, ...] = ()
    protocol_errors: list[str] = []
    cleanup_errors: list[str] = []
    state_retained = False
    try:
        _write_manifest(lease)
        writer.emit("run_started")
        if source_manifest_reader() != source_manifest_digest:
            aggregate_status = AGGREGATE_SOURCE_DRIFTED
            raise AggregateRefusal(["source manifest changed before staging"])
        resolved_child_roots: set[Path] = set()
        for assignment in assignments:
            spec = child_factory(assignment, actual_run_id)
            if spec.assignment != assignment:
                raise HarnessError(
                    f"child factory returned the wrong assignment for shard "
                    f"{assignment.shard_index}"
                )
            _validate_child_spec(root, temporary_root, spec, resolved_child_roots)
            children.append(spec)
            _record_child(lease, spec)
        if source_manifest_reader() != source_manifest_digest:
            aggregate_status = AGGREGATE_SOURCE_DRIFTED
            raise AggregateRefusal(["source manifest changed during staging"])
        lease.transition("running")
        for spec in children:
            runtime = _launch_child(spec, actual_run_id, plan_digest)
            runtimes.append(runtime)
            state_shard = lease.state["shards"][spec.assignment.shard_index]
            state_shard["pid"] = runtime.process.pid
            state_shard["process_start_time"] = time.time_ns()
            state_shard["lifecycle"] = "running"
            lease.persist()
            writer.emit("shard_started", shard_index=spec.assignment.shard_index)
        exits, protocol_errors = _tail_children(runtimes, writer)
        for spec in children:
            state_shard = lease.state["shards"][spec.assignment.shard_index]
            state_shard["lifecycle"] = (
                "complete" if exits[spec.assignment.shard_index] == 0 else "aborted"
            )
        lease.persist()
        raw_records: list[dict[str, Any]] = []
        for spec in children:
            stream_records, stream_errors = _load_jsonl(spec.result_stream)
            raw_records.extend(stream_records)
            protocol_errors.extend(stream_errors)
        try:
            aggregated = aggregate_child_results(
                raw_records,
                assignments,
                run_id=actual_run_id,
                plan_digest=plan_digest,
            )
            results = aggregated.results
        except AggregateRefusal as exc:
            results = exc.partial_results
            unmeasured = exc.missing_ids
            protocol_errors.extend(exc.reasons)
            aggregate_status = AGGREGATE_INVALID
        finished_ids = {
            identifier for runtime in runtimes for identifier in runtime.finished_events
        }
        measured_lost = tuple(sorted(set(unmeasured) & finished_ids))
        unmeasured = tuple(sorted(set(unmeasured) - finished_ids))
        if any(code != 0 for code in exits.values()):
            aggregate_status = AGGREGATE_CHILD_FAILED
            expected_ids = {
                item.identifier
                for assignment in assignments
                for item in assignment.mutations
            }
            present_ids = {result.identifier for result in results}
            measured_lost_set = set(measured_lost)
            unmeasured = tuple(sorted(expected_ids - present_ids - measured_lost_set))
        if protocol_errors and aggregate_status == AGGREGATE_COMPLETE:
            aggregate_status = AGGREGATE_INVALID
        if source_manifest_reader() != source_manifest_digest:
            aggregate_status = AGGREGATE_SOURCE_DRIFTED
            protocol_errors.append("source manifest changed before aggregation")
        for spec in children:
            if spec.cleanup is None:
                continue
            try:
                spec.cleanup()
            except Exception as exc:  # noqa: BLE001 - retain state for operator recovery
                cleanup_errors.append(
                    f"shard {spec.assignment.shard_index} cleanup failed: {exc}"
                )
        if cleanup_errors:
            aggregate_status = AGGREGATE_CLEANUP_FAILED
            state_retained = True
        if before_report is not None:
            before_report()
        report = {
            "schema_version": SCHEMA_VERSION,
            "plan": plan_name,
            "plan_path": str(plan_path),
            "results": [_result_payload(result) for result in results],
            "run_id": actual_run_id,
            "mode": "sharded",
            "aggregate_status": aggregate_status,
            "source_head": source_head,
            "source_manifest_digest": source_manifest_digest,
            "plan_digest": plan_digest,
            "requested_shards": requested_shards,
            "effective_shards": effective_shards,
            "shards": lease.state["shards"],
            "event_log_path": str(event_log),
            "unmeasured_mutation_ids": list(unmeasured),
            "measured_lost_mutation_ids": list(measured_lost),
            "aggregate_errors": protocol_errors + cleanup_errors,
            "canonical_projection": {
                "compared_verbatim": [
                    "id",
                    "verdict",
                    "failing_proof",
                    "warnings",
                    "ordered ids",
                ],
                "normalized": ["detail"],
                "excluded": list(CANONICAL_EXCLUDED_FIELDS),
            },
        }
        write_aggregate_report(
            report_path,
            report,
            all_children_complete=all(
                runtime.process.poll() is not None for runtime in runtimes
            ),
        )
        writer.emit("run_finished", phase=aggregate_status)
        lease.transition(
            "complete" if aggregate_status == AGGREGATE_COMPLETE else "aborted"
        )
        exit_code = _result_exit_policy(results, assert_all_killed)
        if aggregate_status != AGGREGATE_COMPLETE:
            exit_code = 1
        if state_retained:
            lease.retain_and_release()
        else:
            lease.clear_and_release()
        return CoordinatorOutcome(
            results=results,
            exit_code=exit_code,
            aggregate_status=aggregate_status,
            report_path=report_path,
            event_log_path=event_log,
            unmeasured_ids=unmeasured,
            measured_lost_ids=measured_lost,
        )
    except KeyboardInterrupt:
        lease.transition("stopping")
        writer.emit("run_stopping", phase="interrupted")
        for runtime in runtimes:
            if runtime.process.poll() is None:
                runtime.process.terminate()
        for runtime in runtimes:
            runtime.process.wait()
            runtime.stderr_handle.close()
        lease.transition("aborted")
        lease.retain_and_release()
        raise
    except BaseException:
        lease.transition("aborted")
        lease.retain_and_release()
        raise


def _hold_liveness(lock_path: Path, argv: Sequence[str]) -> int:
    """Hold the advisory child-liveness lock for the exact child lifetime."""

    if not argv:
        raise HarnessError("the liveness wrapper needs a child argv")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        child = subprocess.Popen(list(argv))  # noqa: S603
        previous_handlers: dict[signal.Signals, Any] = {}

        def forward(signum: int, _frame: Any) -> None:
            if child.poll() is None:
                child.send_signal(signum)

        for signal_number in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signal_number] = signal.signal(signal_number, forward)
        try:
            return child.wait()
        finally:
            for signal_number, previous in previous_handlers.items():
                signal.signal(signal_number, previous)


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("mode")
    parser.add_argument("lock", nargs="?")
    parser.add_argument("child", nargs=argparse.REMAINDER)
    arguments = parser.parse_args(argv)
    if arguments.mode != "_hold-liveness" or arguments.lock is None:
        raise HarnessError("this module is an internal child-liveness wrapper")
    child = arguments.child
    if child and child[0] == "--":
        child = child[1:]
    return _hold_liveness(Path(arguments.lock), child)


if __name__ == "__main__":
    try:
        raise SystemExit(_main())
    except HarnessError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
