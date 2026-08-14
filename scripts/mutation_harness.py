#!/usr/bin/env python3
"""Shared mutation-testing harness with a verified restore.

Mutation testing here means: deliberately break one line of production source,
prove a specific test notices, then put the line back. The value is entirely in
the "prove a test notices" step -- and the risk is entirely in the "put the line
back" step, because a harness that fails to restore leaves a broken tree behind
a green-looking report.

This harness exists because three independent false results were produced by
ad-hoc per-lane harnesses on 2026-07-26, all of the same shape: the harness
could not detect its own failure. The four concrete failure modes it closes:

  1. A ``trap``-based restore does not fire when the harness is killed or gets
     pushed to the background by a foreground timeout. The source stays mutated
     while the operator believes a monitor is watching. Closed by recording the
     applied mutation in ``.mutation-harness/state.json`` and refusing to do
     anything at all while that record exists -- crash-safety is a file on disk,
     not a signal handler.

  2. A restore "verified" with the wrong tool. One mutation was
     ``if false && (guard)``, which is valid Go: ``go build`` passes, so a build
     can never detect that mutation class. Closed by asserting the restored file
     is byte-identical to a snapshot taken before mutating (``verify_restore``),
     and then by re-running the proof commands and requiring them to pass again.

  3. ``git checkout`` used as the restore, which silently reverted unrelated
     uncommitted edits in the same file and turned a real result into a harness
     artifact. Closed by restoring from the snapshot bytes only. This harness
     never invokes git to modify the tree.

  4. A waiter using ``pgrep -qf "m22.sh"`` matched its own command line and
     waited on itself forever. Closed structurally: this harness is synchronous
     and has no waiters. If you need to run it unattended, redirect its output
     to a file and read the exit code.

It also refuses three anchor mistakes that each produced a false SURVIVED:

  * an anchor matching a different number of times than declared -- one mutation
    landed in a doc comment because the target string appeared twice in the
    file, and a mutation that lands in prose reads exactly like a coverage gap;
  * an anchor whose matched line is a comment, which is never the intended
    target (override with ``allow_comment_anchor``);
  * an anchor that also appears in test sources, which is the signature of a
    test asserting against the very constant being mutated. Reported as a
    warning, because it is a strong signal rather than a certainty.

Protocol per mutation, in order, with the run aborting on any restore fault:

  1. refuse to start unless the tree is verified clean;
  2. snapshot the target file's bytes and digest;
  3. BASELINE -- every proof command must exit 0. A mutation measured against an
     already-red test proves nothing, so this reports BASELINE_FAILED and does
     not mutate;
  4. apply the replacement, asserting the declared occurrence count and that the
     file's content actually changed;
  5. OBSERVE -- re-run the proof commands. Any non-zero exit is KILLED; all zero
     is SURVIVED;
  6. restore the snapshot bytes and assert byte-identity;
  7. POST-RESTORE -- re-run the proof commands and require them to pass again.
     This is what makes the restore claim evidence rather than an assertion.

A SURVIVED result is not automatically a coverage gap: it may be an invalid
mutation, or a change no behavioural test can observe. That judgement is the
operator's, so it must be *declared* in the plan via ``expected_survivor_reason``
rather than explained after the fact in a report. ``--assert-all-killed`` fails
on any undeclared survivor.

Usage:
    python3 scripts/mutation_harness.py verify [--root PATH]
    python3 scripts/mutation_harness.py restore [--root PATH] [--force]
    python3 scripts/mutation_harness.py accept --digest SHA256 [--root PATH]
    python3 scripts/mutation_harness.py run --plan PATH [--only M1,M2]
                                            [--assert-all-killed]
                                            [--progress human|jsonl|none]
    python3 scripts/mutation_harness.py report [--root PATH]

``accept`` is the exit from a safe refusal. Several of ``restore``'s refusals
are terminal -- the snapshot was deleted, or the file was reconciled by hand and
matches neither digest -- and without an acknowledged repair the record could
only be cleared by a successful restore, so the gate stayed red with nothing
left to do. A safe path that dead-ends teaches people to take the unsafe one. It
is not an override: it re-checks the file's identity, requires the caller to pin
the content by digest, refuses while the recorded mutation is still on disk, and
-- the load-bearing part -- requires the mutation's own PROOF COMMANDS to pass on
the repaired tree. No text search can decide whether a mutation is still in
effect, because the effect survives respelling; the proofs are a behavioural
answer to a behavioural question. See ``accept_manual_repair``.

``verify`` is the gate: it belongs in the local validation script so that a
green gate cannot be reported from a tree with a mutation still applied.

WHAT ``verify`` DOES NOT PROVE, stated plainly because the opposite belief is
the same false confidence this tool exists to prevent: it answers only "did
*this* harness leave a mutation applied", by reading its own record. A mutation
applied by a hand edit, or by some other tool, leaves no record and ``verify``
reports the tree clean -- measured, not assumed. Proving a tree is unmutated
would need a pristine digest for every source file, which nothing here has. So
``verify`` passing means "this harness is not mid-run and did not leak", and a
branch that predates this harness and registers no plan passes trivially. That
is the correct scope, but it is a narrower claim than "the source is pristine",
and it must not be quoted as the wider one.

A PLAN IS EXECUTABLE, so review one like code. ``shell=False`` and argv arrays
remove metacharacter parsing; they do not stop a plan from naming ``sh -c`` or
any other executable explicitly, and proof commands run with the invoking
developer's environment and privileges. Treat a contributed plan as a script.

TWO RESIDUAL LIMITATIONS, disclosed rather than discovered later:

  * A proof command that cleans the tree -- ``git clean -fdX`` is the realistic
    case, because ``.mutation-harness/`` is gitignored -- deletes the snapshots
    and the applied record. The in-run restore survives this, because the
    original bytes are held in memory for the whole cycle, but CRASH recovery
    does not: there would be nothing left to recover from. A proof command that
    edits or cleans the tree is a bug in the plan, and the freshness check now
    catches the editing case explicitly.
  * A declared ``build`` command that does not match the proofs' build
    configuration -- their tags, their constraints -- is a check that can pass
    while the proof cannot run. Measured instance: a plain ``go build`` accepted
    a mutation whose orphaned variable only broke the ``-tags integration``
    configuration the proofs actually execute under.
  * A proof command that alternates pass/fail on successive invocations fakes
    the whole green -> red -> green sequence and reports KILLED for a reason
    unrelated to the mutation. Nothing here can distinguish that from a real
    kill; a proof must be deterministic, and a flaky one invalidates its verdict
    the same way it would invalidate any test result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

if TYPE_CHECKING:
    from scripts.mutation_harness_execution_tree import (
        StagedExecutionTree as ExecutionStagedTree,
    )

# Keep ``python scripts/mutation_harness.py`` compatible with package imports.
# Direct script execution otherwise puts only ``scripts/`` on ``sys.path``.
_REPOSITORY_IMPORT_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPOSITORY_IMPORT_ROOT not in sys.path:
    sys.path.insert(0, _REPOSITORY_IMPORT_ROOT)

STATE_DIRNAME = ".mutation-harness"
STATE_FILENAME = "state.json"
REPORT_FILENAME = "report.json"
SNAPSHOT_DIRNAME = "snapshots"
LOCK_DIRNAME = "lock"
RUNS_DIRNAME = "runs"
EVENT_LOG_FILENAME = "events.jsonl"
RESULT_STREAM_FILENAME = "results.jsonl"
SCHEMA_VERSION = 1

# Test sources, for the self-referential-assertion warning. Deliberately broad:
# a false warning costs a moment's thought, a missed one costs a wrong verdict.
TEST_FILE_PATTERNS = ("*_test.go", "test_*.py", "*_test.py")
SKIP_DIRS = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "site",
        "build",
        "dist",
        STATE_DIRNAME,
    }
)
COMMENT_PREFIXES = ("//", "#", "/*", "*")
# Sentinel for "the lock is held but we cannot identify by whom". Not a real pid;
# `mkdir` and the pid write are not atomic, so the gap between them is a real
# state and must not be mistaken for an absent run.
_LOCK_HELD_BY_UNKNOWN = -1
# `path/to/file.ext:123` -- what test runners print at a failure point.
_FILE_LINE_RE = re.compile(r"[\w./-]+\.[A-Za-z]{1,6}:\d+")
# Mutation ids and snapshot filenames must be plain names: they are embedded in
# filesystem paths, and a plan is contributed data, not a trusted path source.
_SAFE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class HarnessError(RuntimeError):
    """A condition that must stop the run rather than be reported as a result."""


@dataclass(frozen=True)
class Mutation:
    """One declared mutation and the commands that must notice it."""

    identifier: str
    path: str
    find: str
    replace: str
    proof: tuple[tuple[str, ...], ...]
    rationale: str
    build: tuple[str, ...] | None = None
    expect_occurrences: int = 1
    allow_comment_anchor: bool = False
    expected_survivor_reason: str | None = None


@dataclass
class Result:
    """The outcome of one mutation, and why."""

    identifier: str
    verdict: str
    detail: str = ""
    warnings: list[str] = field(default_factory=list)
    failing_proof: str | None = None


@dataclass(frozen=True)
class ShardingPlan:
    """Validated plan opt-in consumed by the sharded coordinator."""

    max_shards: int
    workspace_inputs: tuple[str, ...]
    external_resources: str


@dataclass(frozen=True)
class StagedExecutionTree:
    """One owned execution tree prepared for a mutation shard."""

    root: Path
    ownership_marker: Path
    shard_index: int


@dataclass(frozen=True)
class _ChildProtocol:
    """Coordinator-owned metadata for one internal shard child."""

    run_id: str
    shard_index: int
    plan_digest: str
    result_stream: Path
    plan_ordinals: dict[str, int]
    stream: TextIO

    def emit(
        self,
        event: str,
        mutation: Mutation,
        *,
        phase: str | None = None,
        verdict: str | None = None,
        started_ns: int,
    ) -> None:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "event": event,
            "shard_index": self.shard_index,
            "plan_ordinal": self.plan_ordinals[mutation.identifier],
            "mutation_id": mutation.identifier,
            "elapsed_ms": (time.monotonic_ns() - started_ns) // 1_000_000,
        }
        if phase is not None:
            payload["phase"] = phase
        if verdict is not None:
            payload["verdict"] = verdict
        self.stream.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.stream.flush()


class _ProgressEmitter:
    """Write one ordered progress stream to durable storage and stderr."""

    def __init__(
        self,
        root: Path,
        run_id: str,
        progress: str,
        *,
        stream: TextIO | None = None,
    ) -> None:
        if progress not in {"human", "jsonl", "none"}:
            raise HarnessError(f"unknown progress mode: {progress!r}")
        self.run_id = run_id
        self.progress = progress
        self._stream = stream if stream is not None else sys.stderr
        self._sequence = 0
        self._started_ns = time.monotonic_ns()
        self.run_directory = _run_directory(root, run_id)
        self.event_log = self.run_directory / EVENT_LOG_FILENAME

    def emit(
        self,
        event: str,
        *,
        completed: int,
        active: int,
        total: int,
        shard_index: int | None = None,
        plan_ordinal: int | None = None,
        mutation_id: str | None = None,
        phase: str | None = None,
        verdict: str | None = None,
    ) -> None:
        self._sequence += 1
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "sequence": self._sequence,
            "run_id": self.run_id,
            "event": event,
        }
        if shard_index is not None:
            payload["shard_index"] = shard_index
        if plan_ordinal is not None:
            payload["plan_ordinal"] = plan_ordinal
        if mutation_id is not None:
            payload["mutation_id"] = mutation_id
        if phase is not None:
            payload["phase"] = phase
        if verdict is not None:
            payload["verdict"] = verdict
        payload.update(
            {
                "completed": completed,
                "active": active,
                "total": total,
                "elapsed_ms": (time.monotonic_ns() - self._started_ns) // 1_000_000,
            }
        )

        encoded = json.dumps(payload, separators=(",", ":"))
        _append_jsonl(self.event_log, encoded.encode("utf-8") + b"\n")
        if self.progress == "none":
            return
        rendered = encoded if self.progress == "jsonl" else _human_progress(payload)
        self._stream.write(rendered + "\n")
        self._stream.flush()


VERDICT_KILLED = "KILLED"
VERDICT_SURVIVED = "SURVIVED"
VERDICT_SURVIVED_DECLARED = "SURVIVED_DECLARED"
VERDICT_BASELINE_FAILED = "BASELINE_FAILED"
VERDICT_INVALID = "INVALID"
VERDICT_STALE_DECLARATION = "STALE_DECLARATION"
# A proof command that selects no test. Distinct from BASELINE_FAILED (the proof
# ran and was already red) and from INVALID (the mutation never applied): here
# the mutation is fine and the proof is empty, so the entry measured nothing
# while exiting 0.
VERDICT_PROOF_VACUOUS = "PROOF_VACUOUS"
# A proof whose selected tests all SKIPPED. Environment-gated suites (live
# oracles, integration tags) exit 0 while running nothing, so every mutation
# under them reports SURVIVED for a reason unrelated to coverage.
VERDICT_PROOF_SKIPPED = "PROOF_SKIPPED"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_write(target: Path, data: bytes) -> None:
    """Replace a file's contents without ever leaving it truncated.

    A plain write truncates first: an ENOSPC or I/O error partway through leaves
    the source half-written, which is a corrupted tree rather than a mutated one.
    Write a sibling temporary and rename -- rename is atomic within a filesystem,
    so the target only ever holds the old bytes or the new ones.

    The temporary is created with ``O_CREAT | O_EXCL`` under a random name. A
    predictable name is an arbitrary-overwrite primitive: pre-place a symlink at
    it and the write follows the link and truncates whatever it points at, then
    the rename moves the symlink over the source. O_EXCL refuses to open an
    existing path at all, and the random suffix means it cannot be guessed.

    The target's permission bits are copied to the temporary before the rename,
    because a rename replaces the inode: without this an executable would come
    back non-executable while a byte-digest check still reported success.

    KNOWN LIMIT, documented rather than fixed: rename replaces the inode, so a
    hardlink to the target keeps pointing at the ORIGINAL inode and the two are
    permanently split. Nothing short of writing in place -- which reintroduces
    truncation on a failed write -- avoids that, and truncation is the worse
    failure. Extended attributes and ACLs are likewise not carried across.
    """

    if target.is_symlink():
        raise HarnessError(
            f"{target} is a symlink; refusing to write through it. A write that "
            "follows a link truncates whatever it points at, which is an "
            "arbitrary-overwrite primitive rather than a file update."
        )
    mode: int | None = None
    try:
        mode = target.stat().st_mode & 0o7777
    except OSError:
        mode = None

    descriptor, temporary_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".mh-tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(temporary, mode)
        temporary.replace(target)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _run_directory(root: Path, run_id: str) -> Path:
    """Create a private run directory without following predictable links."""

    if _sanitised_identifier(run_id) is None:
        raise HarnessError(f"run id {run_id!r} is not a plain name")
    runs = _state_dir(root) / RUNS_DIRNAME
    if runs.is_symlink():
        raise HarnessError(f"run directory parent {runs} is a symlink")
    runs.mkdir(parents=True, exist_ok=True)
    run_directory = runs / run_id
    if run_directory.is_symlink():
        raise HarnessError(f"run directory {run_directory} is a symlink")
    try:
        run_directory.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise HarnessError(f"run directory already exists: {run_directory}") from exc
    return run_directory


def _append_jsonl(target: Path, line: bytes) -> None:
    """Atomically add one complete JSON line and make it durable before return."""

    if not line.endswith(b"\n") or line.count(b"\n") != 1:
        raise HarnessError("a JSONL record must be exactly one complete line")
    try:
        existing = target.read_bytes()
    except FileNotFoundError:
        existing = b""
    except OSError as exc:
        raise HarnessError(f"could not read durable stream {target}: {exc}") from exc
    if existing and not existing.endswith(b"\n"):
        raise HarnessError(f"durable stream {target} ends with a partial JSON line")
    _atomic_write(target, existing + line)


def _human_progress(payload: dict[str, Any]) -> str:
    """Render stable progress fields without proof output or command tails."""

    subject = str(payload["event"])
    if "mutation_id" in payload:
        subject += f" {payload['mutation_id']}"
    elif "shard_index" in payload:
        subject += f" shard={payload['shard_index']}"
    return (
        f"mutation harness: {subject} "
        f"({payload['completed']}/{payload['total']} complete, "
        f"{payload['active']} active)"
    )


def _result_fields(result: Result) -> dict[str, Any]:
    """The stable result contract shared by reports and durable streams."""

    return {
        "id": result.identifier,
        "verdict": result.verdict,
        "detail": result.detail,
        "failing_proof": result.failing_proof,
        "warnings": result.warnings,
    }


def _write_result_record(
    result_stream: Path, run_id: str, plan_ordinal: int, result: Result
) -> None:
    """Persist one measured result before another mutation may start."""

    durable_result = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "plan_ordinal": plan_ordinal,
        **_result_fields(result),
    }
    _append_jsonl(
        result_stream,
        json.dumps(durable_result, separators=(",", ":")).encode("utf-8") + b"\n",
    )


def _write_child_result_record(
    protocol: _ChildProtocol, mutation: Mutation, result: Result
) -> None:
    """Write the coordinator child schema before the terminal child event."""

    try:
        from scripts.mutation_harness_coordinator import append_durable_jsonl
    except ImportError as exc:
        raise HarnessError("coordinator result-stream backend is unavailable") from exc
    append_durable_jsonl(
        protocol.result_stream,
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": protocol.run_id,
            "shard_index": protocol.shard_index,
            "plan_digest": protocol.plan_digest,
            "plan_ordinal": protocol.plan_ordinals[mutation.identifier],
            "mutation_id": mutation.identifier,
            "result": _result_fields(result),
        },
    )


def _child_protocol_from_environment(root: Path) -> _ChildProtocol:
    """Load the hidden coordinator-child contract and reject direct invocation."""

    required = {
        "run_id": os.environ.get("MUTATION_HARNESS_RUN_ID"),
        "shard_index": os.environ.get("MUTATION_HARNESS_SHARD_INDEX"),
        "plan_digest": os.environ.get("MUTATION_HARNESS_PLAN_DIGEST"),
        "result_stream": os.environ.get("MUTATION_HARNESS_RESULT_STREAM"),
        "plan_ordinals": os.environ.get("MUTATION_HARNESS_PLAN_ORDINALS"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise HarnessError(
            "--internal-child requires coordinator-owned environment fields: "
            + ", ".join(sorted(missing))
        )
    assert all(value is not None for value in required.values())
    try:
        shard_index = int(str(required["shard_index"]))
    except ValueError as exc:
        raise HarnessError("internal child shard index must be an integer") from exc
    if shard_index < 0:
        raise HarnessError("internal child shard index must be non-negative")
    try:
        loaded_ordinals = json.loads(str(required["plan_ordinals"]))
    except json.JSONDecodeError as exc:
        raise HarnessError("internal child plan ordinals are invalid JSON") from exc
    if not isinstance(loaded_ordinals, dict) or not all(
        isinstance(identifier, str)
        and isinstance(ordinal, int)
        and not isinstance(ordinal, bool)
        and ordinal >= 0
        for identifier, ordinal in loaded_ordinals.items()
    ):
        raise HarnessError(
            "internal child plan ordinals must map mutation ids to non-negative integers"
        )
    result_stream = Path(str(required["result_stream"]))
    if not result_stream.is_absolute():
        raise HarnessError("internal child result stream must be an absolute path")
    resolved_root = root.resolve()
    resolved_stream = result_stream.resolve()
    if not resolved_stream.is_relative_to(resolved_root / STATE_DIRNAME):
        raise HarnessError("internal child result stream escapes shard state")
    return _ChildProtocol(
        run_id=str(required["run_id"]),
        shard_index=shard_index,
        plan_digest=str(required["plan_digest"]),
        result_stream=resolved_stream,
        plan_ordinals=dict(loaded_ordinals),
        stream=sys.stdout,
    )


def _sanitised_identifier(candidate: str) -> str | None:
    """Return the value if it is safe to embed in a filename, else None.

    A mutation id reaches the filesystem as part of its snapshot name. Left
    unvalidated, an id of `../../../etc/whatever` writes snapshot bytes outside
    the state directory, and a pre-created symlink at the predictable computed
    name turns that into an arbitrary overwrite. The plan is a data file that
    anyone may contribute, so its strings do not get to name paths.
    """

    if not candidate or len(candidate) > 128:
        return None
    if not _SAFE_NAME_RE.fullmatch(candidate):
        return None
    return candidate


def _state_dir(root: Path) -> Path:
    """The state directory, refusing a symlink.

    Every auxiliary write lands here. A symlinked `.mutation-harness` would
    redirect all of them outside the repository, so the directory itself is part
    of the trust boundary -- not just the files in it.
    """

    directory = root / STATE_DIRNAME
    if directory.is_symlink():
        raise HarnessError(
            f"{directory} is a symlink. Every snapshot, record and report is "
            "written there, so a symlinked state directory redirects all of them. "
            "Remove it and re-run; do not follow it."
        )
    return directory


def _snapshot_dir(root: Path) -> Path:
    """Return the snapshot directory without following a planted child link.

    Checking only ``.mutation-harness`` is insufficient: an attacker can leave
    that trusted directory in place and symlink its predictable ``snapshots``
    child elsewhere. Both the run and recovery paths call this before their
    first snapshot filesystem operation.
    """

    snapshots = _state_dir(root) / SNAPSHOT_DIRNAME
    if snapshots.is_symlink():
        raise HarnessError(
            f"snapshot directory {snapshots} is a symlink. Snapshot bytes are "
            "trusted recovery data, so following that link would read or write "
            "outside the harness state directory. Remove it and re-run."
        )
    return snapshots


def _read_state(root: Path) -> dict[str, Any] | None:
    path = _state_dir(root) / STATE_FILENAME
    # Deliberately NOT `if not path.is_file(): return None`. is_file() answers
    # False for "does not exist" and for "cannot stat" alike -- a directory that
    # lost search permission, an I/O error -- so a recorded mutation would read
    # as no mutation and the gate would report clean. Distinguish the two.
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HarnessError(
            f"{path} could not be read ({exc}). That is not the same as absent: "
            "a mutation may still be applied and recorded here. Fix the "
            "permissions or the device and re-run; do not delete it blind."
        ) from exc
    try:
        loaded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        # An unreadable state file is itself a reason to refuse: it may be
        # recording an applied mutation we can no longer identify.
        raise HarnessError(
            f"{path} exists but could not be parsed ({exc}). A mutation may "
            "still be applied. Inspect the tree by hand before deleting it."
        ) from exc
    if not isinstance(loaded, dict):
        raise HarnessError(f"{path} is not a JSON object")
    return loaded


def _write_state(root: Path, payload: dict[str, Any] | None) -> None:
    directory = _state_dir(root)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / STATE_FILENAME
    if payload is None:
        path.unlink(missing_ok=True)
        return
    # Write-then-rename so a crash cannot leave a half-written record that
    # _read_state would refuse to parse.
    # Through the audited primitive, not a predictable sibling. `state.json.tmp`
    # was guessable, so planting it as a symlink truncated its target -- the same
    # defect as the mutation write, in the file that records the mutation.
    _atomic_write(path, (json.dumps(payload, indent=2) + "\n").encode("utf-8"))


def _update_applied_record(root: Path, key: str, value: Any) -> None:
    """Amend the applied record in place, keeping it the single source of truth."""

    state = _read_state(root) or {}
    applied = state.get("applied")
    if not isinstance(applied, dict):
        raise HarnessError(
            "the applied record vanished mid-run. Something deleted "
            f"{STATE_DIRNAME}/{STATE_FILENAME} -- a proof command running "
            "`git clean -fdX` is the realistic cause, since that directory is "
            "gitignored. Stop and inspect the tree by hand."
        )
    applied[key] = value
    _write_state(root, state)


def _file_identity(target: Path) -> str:
    """A stable identity for the file itself, independent of its path."""

    info = target.stat()
    return f"{info.st_dev}:{info.st_ino}"


def _resolve_target(root: Path, relative: str) -> Path:
    if os.path.isabs(relative):
        raise HarnessError(f"mutation path must be repo-relative, got {relative!r}")
    resolved = (root / relative).resolve()
    root_resolved = root.resolve()
    if not resolved.is_relative_to(root_resolved):
        raise HarnessError(f"mutation path escapes the repository root: {relative!r}")
    if not resolved.is_file():
        raise HarnessError(f"mutation target does not exist: {relative}")
    return resolved


def _run_command(argv: tuple[str, ...], root: Path) -> tuple[int, str]:
    """Run one proof command with no shell.

    Proof commands are argv arrays rather than shell strings on purpose. A plan
    file is data, and running it through a shell would make an ordinary data
    file an execution surface with quoting and interpolation hazards. The cost
    is that pipes and ``&&`` are unavailable -- list the commands separately
    instead, which also makes the failing one identifiable.
    """

    completed = subprocess.run(  # noqa: S603 - argv list, shell=False
        list(argv),
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    tail = (completed.stdout or "") + (completed.stderr or "")
    return completed.returncode, tail[-4000:]


def _format_command(argv: tuple[str, ...]) -> str:
    return " ".join(argv)


def _matched_line(content: str, needle: str) -> str:
    index = content.find(needle)
    if index < 0:
        return ""
    start = content.rfind("\n", 0, index) + 1
    end = content.find("\n", index)
    return content[start:] if end < 0 else content[start:end]


# Cached per root for the lifetime of one process. Without this the tree walk
# below runs once per mutation and reads every test file each time: an 11-mutation
# self-check spent most of its wall clock re-discovering the same file list, and
# the cost scales with the repository rather than with the plan.
_TEST_FILE_CACHE: dict[Path, list[Path]] = {}


def _iter_test_files(root: Path) -> list[Path]:
    cached = _TEST_FILE_CACHE.get(root)
    if cached is not None:
        return cached
    found: list[Path] = []
    for current, directories, files in os.walk(root):
        directories[:] = [name for name in directories if name not in SKIP_DIRS]
        base = Path(current)
        for name in files:
            if any(Path(name).match(pattern) for pattern in TEST_FILE_PATTERNS):
                found.append(base / name)
    _TEST_FILE_CACHE[root] = found
    return found


def _self_referential_warning(root: Path, mutation: Mutation) -> str | None:
    """Warn when the anchor also appears in test sources.

    A test that contains the exact string being mutated is very often asserting
    against the production constant it should be independent of, which makes the
    mutation unkillable for a reason that looks like a coverage gap.
    """

    needle = mutation.find.strip()
    if len(needle) < 12:
        return None
    hits: list[str] = []
    for candidate in _iter_test_files(root):
        try:
            text = candidate.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if needle in text:
            hits.append(str(candidate.relative_to(root)))
        if len(hits) >= 3:
            break
    if not hits:
        return None
    return (
        "the anchor text also appears in test sources "
        f"({', '.join(hits)}) -- check the test is not asserting against the "
        "constant under mutation, which would make this mutation unkillable "
        "for a reason unrelated to coverage"
    )


def _load_plan(path: Path) -> tuple[str, list[Mutation]]:
    try:
        from scripts.mutation_harness_optin import (
            PlanContractError,
            load_plan_contract,
        )

        raw, _sharding = load_plan_contract(path)
    except ImportError as exc:
        raise HarnessError("sharding plan validation backend is unavailable") from exc
    except PlanContractError as exc:
        raise HarnessError(str(exc)) from exc

    return _parse_plan(path, raw)


def _parse_plan(path: Path, raw: dict[str, Any]) -> tuple[str, list[Mutation]]:
    """Parse mutations after the closed plan vocabulary has been validated."""

    if raw.get("schema_version") != SCHEMA_VERSION:
        raise HarnessError(
            f"{path} has schema_version {raw.get('schema_version')!r}, "
            f"expected {SCHEMA_VERSION}"
        )
    entries = raw.get("mutations")
    if not isinstance(entries, list) or not entries:
        raise HarnessError(f"{path} declares no mutations")

    mutations: list[Mutation] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise HarnessError(f"{path}: every mutation must be an object")
        identifier = str(entry.get("id") or "").strip()
        if not identifier:
            raise HarnessError(f"{path}: a mutation is missing its id")
        if _sanitised_identifier(identifier) is None:
            raise HarnessError(
                f"{path}: mutation id {identifier!r} is not a plain name. Ids are\n"
                "                embedded in snapshot filenames, so a value containing a path\n"
                "                separator or .. could write outside the state directory."
            )
        if identifier in seen:
            raise HarnessError(f"{path}: duplicate mutation id {identifier!r}")
        seen.add(identifier)
        find = entry.get("find")
        replace = entry.get("replace")
        if not isinstance(find, str) or not find:
            raise HarnessError(f"{identifier}: 'find' must be a non-empty string")
        if not isinstance(replace, str):
            raise HarnessError(f"{identifier}: 'replace' must be a string")
        if find == replace:
            raise HarnessError(
                f"{identifier}: 'find' and 'replace' are identical, so this "
                "mutation cannot change behaviour"
            )
        proof_raw = entry.get("proof")
        if not isinstance(proof_raw, list) or not proof_raw:
            raise HarnessError(
                f"{identifier}: 'proof' must list at least one command. A "
                "mutation with nothing to observe it is not a measurement."
            )
        proof: list[tuple[str, ...]] = []
        for command in proof_raw:
            if not isinstance(command, list) or not command:
                raise HarnessError(
                    f"{identifier}: every proof command must be a non-empty argv "
                    'array, e.g. ["go", "test", "-run", "^TestX$", "./pkg/"]. '
                    "Shell strings are rejected because the plan is data, not "
                    "an execution surface."
                )
            if not all(isinstance(word, str) and word for word in command):
                raise HarnessError(
                    f"{identifier}: argv entries must be non-empty strings"
                )
            proof.append(tuple(command))
        rationale = entry.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            raise HarnessError(
                f"{identifier}: 'rationale' is required -- state what property "
                "this mutation is testing for, or the result is uninterpretable"
            )
        expect = entry.get("expect_occurrences", 1)
        if not isinstance(expect, int) or isinstance(expect, bool) or expect < 1:
            raise HarnessError(
                f"{identifier}: 'expect_occurrences' must be a positive integer"
            )
        build_raw = entry.get("build", raw.get("build"))
        build: tuple[str, ...] | None = None
        if build_raw is not None:
            if not isinstance(build_raw, list) or not build_raw:
                raise HarnessError(
                    f"{identifier}: 'build' must be a non-empty argv array"
                )
            if not all(isinstance(word, str) and word for word in build_raw):
                raise HarnessError(
                    f"{identifier}: build argv entries must be non-empty strings"
                )
            build = tuple(build_raw)
        reason = entry.get("expected_survivor_reason")
        if reason is not None and (not isinstance(reason, str) or not reason.strip()):
            raise HarnessError(
                f"{identifier}: 'expected_survivor_reason' must be a non-empty "
                "string when present"
            )
        mutations.append(
            Mutation(
                identifier=identifier,
                path=str(entry.get("file") or ""),
                find=find,
                replace=replace,
                proof=tuple(proof),
                rationale=rationale,
                build=build,
                expect_occurrences=expect,
                allow_comment_anchor=bool(entry.get("allow_comment_anchor", False)),
                expected_survivor_reason=reason,
            )
        )
    return str(raw.get("name") or path.name), mutations


def _load_sharding_plan(path: Path, raw: dict[str, Any]) -> ShardingPlan | None:
    """Load and validate the optional sharding plan contract."""

    del raw  # The authoritative loader reads and validates the complete file.
    try:
        from scripts.mutation_harness_optin import (
            PlanContractError,
            load_plan_contract,
        )

        _loaded, contract = load_plan_contract(path)
    except ImportError as exc:
        raise HarnessError("sharding plan validation backend is unavailable") from exc
    except PlanContractError as exc:
        raise HarnessError(str(exc)) from exc
    if contract is None:
        return None
    return ShardingPlan(
        max_shards=contract.max_shards,
        workspace_inputs=contract.workspace_inputs,
        external_resources=contract.external_resources,
    )


def stage_execution_tree(
    source_root: Path,
    destination: Path,
    *,
    run_id: str,
    shard_index: int,
    source_manifest_digest: str,
    plan_digest: str,
    workspace_inputs: tuple[str, ...],
    source_manifest: Any | None = None,
) -> ExecutionStagedTree:
    """Create one isolated, owned execution tree for a mutation shard."""

    try:
        from scripts import mutation_harness_execution_tree as execution_tree
    except ImportError as exc:
        raise HarnessError("execution-tree backend is unavailable") from exc
    frozen = source_manifest or execution_tree.build_source_manifest(source_root)
    if frozen.digest != source_manifest_digest:
        raise HarnessError(
            "source manifest digest does not match the frozen execution-tree manifest"
        )
    return execution_tree.stage_execution_tree(
        source_root,
        destination,
        run_id=run_id,
        shard_index=shard_index,
        source_manifest=frozen,
        plan_digest=plan_digest,
        workspace_inputs=workspace_inputs,
    )


def coordinator_run(
    root: Path,
    plan_path: Path,
    only: set[str] | None,
    assert_all_killed: bool,
    *,
    requested_shards: int,
    progress: str,
) -> tuple[list[Result], int]:
    """Coordinate an opted-in sharded mutation run."""

    try:
        from scripts import mutation_harness_coordinator as coordinator
        from scripts import mutation_harness_execution_tree as execution_tree
        from scripts.mutation_harness_optin import (
            PlanContractError,
            external_resource_environment,
            load_plan_contract,
            validate_requested_shards,
        )
    except ImportError as exc:
        raise HarnessError("sharded execution backend is unavailable") from exc

    root = root.resolve()
    plan_path = plan_path.resolve()
    try:
        raw, sharding = load_plan_contract(plan_path)
        validate_requested_shards(sharding, requested_shards)
    except PlanContractError as exc:
        raise HarnessError(str(exc)) from exc
    if sharding is None:
        raise HarnessError("the plan does not opt in to sharding")
    try:
        relative_plan = plan_path.relative_to(root)
    except ValueError as exc:
        raise HarnessError(
            "sharded plan must be inside the repository so every frozen execution "
            "tree runs the same plan bytes"
        ) from exc

    plan_name, mutations = _parse_plan(plan_path, raw)
    source_manifest = execution_tree.build_source_manifest(root)
    source_manifest_mapping = execution_tree.source_manifest_to_dict(source_manifest)
    plan_digest = _digest(plan_path.read_bytes())
    temporary_root: Path | None = None
    owned_roots: set[Path] = set()

    def source_manifest_reader() -> str:
        return execution_tree.build_source_manifest(root).digest

    def child_factory(assignment: Any, run_id: str) -> Any:
        nonlocal temporary_root
        if temporary_root is None:
            temporary_root = execution_tree.create_private_temp_root(
                prefix=f"mutation-harness-{run_id}-"
            )
        staged = stage_execution_tree(
            root,
            temporary_root / f"shard-{assignment.shard_index}",
            run_id=run_id,
            shard_index=assignment.shard_index,
            source_manifest_digest=source_manifest.digest,
            plan_digest=plan_digest,
            workspace_inputs=sharding.workspace_inputs,
            source_manifest=source_manifest,
        )
        owned_roots.add(staged.root.resolve())
        staged_plan = staged.root / relative_plan
        if _digest(staged_plan.read_bytes()) != plan_digest:
            raise HarnessError("staged plan bytes do not match the frozen plan digest")
        selected_ids = [item.identifier for item in assignment.mutations]
        plan_ordinals = {
            item.identifier: item.selected_ordinal for item in assignment.mutations
        }
        environment = external_resource_environment(
            sharding, run_id, assignment.shard_index
        )
        environment["MUTATION_HARNESS_PLAN_ORDINALS"] = json.dumps(
            plan_ordinals, sort_keys=True, separators=(",", ":")
        )
        result_stream = (
            staged.root / STATE_DIRNAME / RUNS_DIRNAME / run_id / RESULT_STREAM_FILENAME
        )

        def cleanup() -> None:
            assert temporary_root is not None
            execution_tree.cleanup_execution_tree(
                staged,
                temporary_root=temporary_root,
                child_liveness_proven=True,
            )
            owned_roots.remove(staged.root.resolve())
            if not owned_roots:
                temporary_root.rmdir()

        argv = [
            sys.executable,
            "-m",
            "scripts.mutation_harness",
            "--root",
            ".",
            "run",
            "--plan",
            str(relative_plan),
            "--only",
            ",".join(selected_ids),
            "--shards",
            "1",
            "--progress",
            "none",
            "--internal-child",
        ]
        if assert_all_killed:
            argv.append("--assert-all-killed")
        return coordinator.ChildSpec(
            assignment=assignment,
            root=staged.root,
            source_root=root,
            temporary_root=temporary_root,
            argv=tuple(argv),
            result_stream=result_stream,
            ownership_marker=staged.ownership_marker,
            liveness_lock=staged.root / STATE_DIRNAME / "child.liveness",
            environment=environment,
            cleanup=cleanup,
        )

    outcome = coordinator.coordinator_run(
        root,
        plan_path,
        plan_name,
        [mutation.identifier for mutation in mutations],
        only,
        assert_all_killed,
        requested_shards=requested_shards,
        progress=progress,
        source_head=source_manifest.head,
        source_manifest=source_manifest_mapping,
        source_manifest_digest=source_manifest.digest,
        plan_digest=plan_digest,
        source_manifest_reader=source_manifest_reader,
        child_factory=child_factory,
    )
    return list(outcome.results), outcome.exit_code


def recover_run(root: Path, run_id: str, *, force: bool = False) -> str:
    """Recover or retain the recorded execution trees for an aborted run."""

    try:
        from scripts.mutation_harness_recovery import RecoveryError
        from scripts.mutation_harness_recovery import recover_run as recover_backend
    except ImportError as exc:
        raise HarnessError("recovery backend is unavailable") from exc
    try:
        return recover_backend(root, run_id, force=force)
    except RecoveryError as exc:
        raise HarnessError(str(exc)) from exc


def acquire_lock(root: Path) -> Path:
    """Take an exclusive lock, or explain precisely how to break a stale one."""

    lock = _state_dir(root) / LOCK_DIRNAME
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock.mkdir()
    except FileExistsError as exc:
        owner = "unknown"
        pid_file = lock / "pid"
        if pid_file.is_file():
            owner = pid_file.read_text(encoding="utf-8").strip() or owner
        raise HarnessError(
            f"another mutation run holds {lock} (pid {owner}). If that process "
            f"is gone this is a stale lock. Remove it with:\n"
            f"        rm -rf {shlex.quote(str(lock))}\n"
            "    Then "
            "run 'verify' BEFORE anything else -- a killed run can leave both a "
            "stale lock and an applied mutation, and the lock is the harmless "
            "half."
        ) from exc
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
    return lock


def release_lock(lock: Path) -> None:
    shutil.rmtree(lock, ignore_errors=True)


def _live_run(root: Path) -> int | None:
    """Return the pid of a mutation run that is still alive, if any.

    A mutation on disk has two very different explanations -- a run is mid-cycle,
    or a dead run leaked it -- and they call for opposite responses: wait, or
    repair. Conflating them wasted real time and produced a false accusation
    against a lane whose harness was working correctly, so `verify` distinguishes
    them rather than leaving the reader to guess.
    """

    lock = _state_dir(root) / LOCK_DIRNAME
    if not lock.is_dir():
        return None
    pid_file = lock / "pid"
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        # The lock exists but its pid has not been written yet, or the writer died
        # in that window. `mkdir` then write is not atomic, so "no pid file" does
        # NOT mean "no live run" -- and treating it as dead lets the gate report
        # clean while a run is starting. Unknown is reported as live: a false wait
        # costs patience, a false clean costs a trusted-but-void test result.
        return _LOCK_HELD_BY_UNKNOWN
    except OSError:
        return _LOCK_HELD_BY_UNKNOWN
    try:
        pid = int(raw)
    except ValueError:
        return _LOCK_HELD_BY_UNKNOWN
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        # Owned by another user: it exists, which is what we are asking.
        return pid
    return pid


def _unknown_holder_advice(root: Path) -> str:
    """What to DO about a lock whose holder cannot be named.

    A pid-less lock has no pid to wait for and no pid to check, so every message
    phrased around one is unusable -- and interpolating the sentinel produces
    'wait for pid -1', which is advice no reader can act on. Since `verify` runs
    first in ci/local_validate.sh, a message with no action in it leaves the gate
    red forever. This is the same advice `acquire_lock` gives, kept in one place
    so the two cannot drift.
    """

    lock = _state_dir(root) / LOCK_DIRNAME
    return (
        f"The lock {lock} exists but contains no pid file. `mkdir` and the pid "
        "write are not atomic, so this is either a run in that window right now "
        "or -- far more likely, if nothing is running -- a run that was killed "
        "inside it. There is no pid to wait for. Check for a live "
        "'mutation_harness.py run' process; if there is none, this is a STALE "
        f"lock. Break it with:\n        rm -rf {shlex.quote(str(lock))}"
    )


def verify(root: Path) -> list[str]:
    """Return the reasons the tree is not verified clean. Empty means clean.

    Every outcome below is a failure for gate purposes -- no test result from a
    tree with a mutation on it is trustworthy, whether or not a run is live --
    but the messages must never be confused, because one says "wait", one says
    "repair", and one says "break the stale lock".
    """

    state = _read_state(root)
    coordinator = (state or {}).get("coordinator_run")
    if coordinator is not None:
        if not isinstance(coordinator, dict):
            raise HarnessError("coordinator_run state is not a JSON object")
        run_id = coordinator.get("run_id", "unknown")
        lifecycle = coordinator.get("lifecycle", "unknown")
        return [
            f"coordinator run {run_id} remains in lifecycle {lifecycle}. "
            "No result from this tree is trustworthy until it is recovered. "
            "Recover it with: python3 scripts/mutation_harness.py recover-run "
            f"--run-id {shlex.quote(str(run_id))}"
        ]
    applied = (state or {}).get("applied")
    live = _live_run(root)

    if not applied:
        if live == _LOCK_HELD_BY_UNKNOWN:
            return [
                "a mutation run holds the lock and cannot be identified, and "
                "nothing is recorded as applied yet. A held lock alone is enough "
                "to distrust this tree: the mutation may land underneath a check "
                "that has already read the record.\n    "
                + _unknown_holder_advice(root)
                + "\n    Then re-run this check; if it then reports a leaked "
                "mutation, repair it with: "
                "python3 scripts/mutation_harness.py restore"
            ]
        if live is not None:
            # A run holds the lock but has not applied anything yet -- it is in
            # its baseline. Reporting clean here is a time-of-check hole: the
            # gate would proceed and the mutation would land underneath it. A
            # held lock alone is enough to distrust every result from this tree.
            return [
                f"a mutation run is IN PROGRESS (pid {live}) and has not applied "
                "its mutation yet, so nothing is on disk right now -- but it will "
                "be, possibly before the caller of this check finishes. No result "
                "from this tree is trustworthy until the run completes. Wait for "
                f"pid {live}.\n"
                "    If this does not clear: liveness here is only 'a process with "
                "that pid exists', and pids are reused, so an unrelated long-lived "
                f"process that inherited pid {live} blocks this check forever. "
                f"Check what pid {live} actually is. If it is not the harness, this "
                "is a stale lock and nothing is applied, so it is safe to break: "
                f"rm -rf {shlex.quote(str(_state_dir(root) / LOCK_DIRNAME))}"
            ]
        return []

    identifier = applied.get("mutation_id", "?")
    relative = applied.get("file", "?")
    if live == _LOCK_HELD_BY_UNKNOWN:
        return [
            f"mutation {identifier} is applied to {relative} and the lock's "
            "holder cannot be identified, so this is not the ordinary 'wait for "
            "the run' case and must not be read as one.\n    "
            + _unknown_holder_advice(root)
            + "\n    Then repair the mutation with: "
            "python3 scripts/mutation_harness.py restore"
        ]
    if live is not None:
        return [
            f"a mutation run appears IN PROGRESS (pid {live}), currently applying "
            f"{identifier} to {relative}. If that pid really is the harness this "
            "needs no repair, only patience -- but no test or build result from "
            "this tree is meaningful until it finishes.\n"
            "    Caveat: liveness here is 'a process with that pid exists', and "
            "pids are reused. If the harness died and an unrelated process "
            "inherited its pid, this will say IN PROGRESS forever. Check what "
            f"pid {live} actually is. If it is NOT the harness, break the lock "
            f"yourself and then restore -- in that order:\n"
            f"        rm -rf {shlex.quote(str(_state_dir(root) / LOCK_DIRNAME))}\n"
            "        python3 scripts/mutation_harness.py restore\n"
            "    Not `restore --force`: force is for a lock left by a DEAD pid, "
            "and it refuses precisely while a process with the recorded pid "
            "exists -- so against a reused pid it can only ever refuse. Removing "
            "the lock by hand is the deliberate step that says you checked."
        ]
    return [
        f"mutation {identifier} is still applied to {relative} and NO run is "
        "alive, so it leaked from a dead run. Any result measured on this tree "
        "is void, including a passing build -- a mutation like "
        "'if false && (guard)' compiles cleanly, which is exactly why a build "
        "cannot be used to verify a restore. Repair with: "
        "python3 scripts/mutation_harness.py restore\n"
        "    If `restore` refuses -- the snapshot is gone, or the file holds "
        "neither the original nor the mutation because it was reconciled by "
        f"hand -- undo the mutation in {relative} yourself, then clear the "
        f"record with: {_accept_hint(str(relative))}"
    ]


def _accept_hint(relative: str) -> str:
    """The exact command that clears a record after a by-hand repair.

    Quoted, and preceded by ``--``, because the path comes from the plan: a
    repo-relative path may legitimately contain a space, and one beginning with a
    dash is read as options by every tool that has any. Unquoted, the suggested
    command silently hashes the wrong file or fails -- and a recovery instruction
    that does not run is the dead end this command exists to remove.
    """

    return (
        "python3 scripts/mutation_harness.py accept --digest "
        f"$(shasum -a 256 -- {shlex.quote(relative)} | cut -d' ' -f1)"
    )


def restore(root: Path, force: bool = False) -> str:
    """Put back a mutation left applied by an interrupted run.

    Takes the lock. Without it, a `restore` racing a live run can write the
    snapshot and clear the record in the window before that run applies its
    mutation -- after which the run applies it, dies, and leaves a mutation with
    no record at all, so `verify` reports clean. That is the exact hole this tool
    exists to close, reachable through its own repair path.
    """

    lock: Path | None = None
    try:
        lock = acquire_lock(root)
    except HarnessError:
        live = _live_run(root)
        if live is not None and live != _LOCK_HELD_BY_UNKNOWN and not force:
            raise HarnessError(
                f"a mutation run holds the lock (pid {live}); refusing to restore "
                "underneath it, because clearing the applied record now would let "
                "that run leave a mutation nothing knows about. Wait for it.\n"
                f"    If pid {live} is not really the harness -- pids get reused, "
                "and an unrelated long-lived process that inherited it blocks this "
                "forever -- break the lock yourself and re-run WITHOUT --force:\n"
                f"        rm -rf {shlex.quote(str(_state_dir(root) / LOCK_DIRNAME))}\n"
                "    Not --force: it is for a lock left by a DEAD pid, and it "
                "refuses precisely while a process with the recorded pid exists, "
                "so against a reused pid it can only ever refuse."
            ) from None
        if not force:
            raise
        # --force is for a STALE lock. It must not evict a live one: clearing the
        # record and deleting another run's lock lets that run apply its mutation
        # and die with nothing recording it, after which `verify` reports clean
        # with a mutation on disk -- worse than the state --force was invoked to
        # repair, and reachable through this tool's own escape hatch.
        if live == _LOCK_HELD_BY_UNKNOWN:
            lock_path = _state_dir(root) / LOCK_DIRNAME
            raise HarnessError(
                "--force refused: the lock holder is unknown because the lock "
                "has no readable pid. This may be a live run between mkdir and "
                "its pid write, so force cannot prove the lock stale and must "
                "not evict it. Check that no mutation_harness.py run process is "
                "active, then deliberately break the lock yourself and re-run "
                "restore without --force:\n"
                f"        rm -rf {shlex.quote(str(lock_path))}\n"
                "        python3 scripts/mutation_harness.py restore"
            ) from None
        if live is not None and live != _LOCK_HELD_BY_UNKNOWN:
            raise HarnessError(
                f"--force refused: pid {live} is alive and holds the lock. Force "
                "is for a lock left by a DEAD run. Confirm that pid is not the "
                "harness and that it has exited, then re-run. If you are certain "
                "it is unrelated, remove the lock yourself and re-run without "
                f"--force:\n        rm -rf "
                f"{shlex.quote(str(_state_dir(root) / LOCK_DIRNAME))}"
            ) from None

    try:
        state = _read_state(root)
        applied = (state or {}).get("applied")
        if not applied:
            return "nothing to restore: no mutation is recorded as applied"

        relative = str(applied.get("file") or "")
        snapshot_name = str(applied.get("snapshot") or "")
        expected = str(applied.get("original_sha256") or "")

        # Re-validate the recorded path through the same guard the run used.
        # Recovery previously trusted the record, so a crafted or stale entry
        # could name `../../elsewhere`, or a retargeted in-repo symlink could
        # redirect the write to a different file than the one that was mutated.
        target = _resolve_target(root, relative)
        recorded_identity = applied.get("identity")
        if recorded_identity is not None:
            current_identity = _file_identity(target)
            if current_identity != str(recorded_identity):
                raise HarnessError(
                    f"REFUSING TO RESTORE {relative}: it no longer resolves to the "
                    f"file that was mutated. Recorded {recorded_identity}, now "
                    f"{current_identity}. A symlink was retargeted, or the file was "
                    "replaced. Writing here would repair the wrong file and leave "
                    "the mutated one broken -- and clearing the record would hide "
                    "that. Find the file with the recorded device:inode and undo "
                    f"the mutation by hand; the pre-run content is in "
                    f"{STATE_DIRNAME}/{SNAPSHOT_DIRNAME}/{snapshot_name}."
                )
        # A record whose two digests agree describes nothing: no mutation this
        # harness can produce leaves the file identical to the original, because
        # a no-op replacement is refused before any write. Such a record is
        # corrupt or hand-crafted, and it must be rejected HERE, because the
        # content short-circuit below would otherwise clear the record on a digest
        # that equally names the mutated content -- clearing it with the mutation
        # still on disk.
        recorded_mutated = str(applied.get("mutated_sha256") or "")
        if expected and recorded_mutated and expected == recorded_mutated:
            raise HarnessError(
                f"the record for {relative} carries the same digest as both its "
                f"original and its mutated content ({expected[:12]}). No mutation "
                "produces that, so this record is corrupt and nothing here can be "
                "trusted to decide whether the file is repaired. Inspect the tree "
                f"by hand; the pre-run content, if it survives, is in "
                f"{STATE_DIRNAME}/{SNAPSHOT_DIRNAME}/{snapshot_name}."
            )

        # Content is examined BEFORE the snapshot is required. A user who has
        # already undone the mutation by hand holds a correct tree, and refusing
        # to clear the record because the snapshot is gone leaves the gate red
        # with nothing left to do. A safe refusal that offers no exit is still a
        # dead end, and a dead end is what makes people delete the record blind.
        current = _digest(target.read_bytes())
        if expected and current == expected:
            _write_state(root, None)
            return (
                f"{relative} already matches the recorded original content; "
                "cleared the applied record without writing"
            )

        if _sanitised_identifier(snapshot_name) is None:
            raise HarnessError(
                f"recorded snapshot name {snapshot_name!r} is not a plain "
                "filename; refusing to read it. Recover by hand, then clear the "
                f"record with: {_accept_hint(relative)}"
            )
        snapshot = _snapshot_dir(root) / snapshot_name
        if not snapshot.is_file():
            raise HarnessError(
                f"snapshot {snapshot} is missing, so {relative} cannot be restored "
                "from this harness. Do NOT run `git checkout` on it: the file may "
                "hold pre-run work that was never committed, and discarding that "
                "is the exact loss this tool exists to prevent. Inspect it with "
                "`git diff` first and undo the mutation by hand. "
                "A proof command that runs `git clean -fdX` will delete this "
                f"directory, because {STATE_DIRNAME}/ is gitignored -- if that is "
                "what happened, that proof command is the bug.\n"
                f"    Once {relative} is correct again, clear the record with: "
                f"{_accept_hint(relative)}"
            )
        original = snapshot.read_bytes()
        if expected and _digest(original) != expected:
            raise HarnessError(
                f"snapshot {snapshot} does not match its recorded digest; refusing "
                "to write it over your source. Recover by hand, then clear the "
                f"record with: {_accept_hint(relative)}"
            )
        expected_mutated = applied.get("mutated_sha256")
        if current == _digest(original):
            _write_state(root, None)
            return (
                f"{relative} already matches the snapshot; cleared the applied "
                "record without writing"
            )
        if expected_mutated is None:
            raise HarnessError(
                f"{relative} differs from the snapshot, but the record carries no "
                "mutated digest -- so the run was interrupted before it wrote, and "
                "this content came from somewhere else. Refusing to overwrite it. "
                f"The pre-run content is in {STATE_DIRNAME}/{SNAPSHOT_DIRNAME}/"
                f"{snapshot_name}; reconcile by hand, then clear the record with: "
                f"{_accept_hint(relative)}"
            )
        if current != str(expected_mutated):
            raise HarnessError(
                f"REFUSING TO RESTORE {relative}: it does not hold the mutation "
                f"this record describes. Expected {str(expected_mutated)[:12]}, "
                f"found {current[:12]}. Something edited the file after the "
                "mutation was applied -- an editor, or a proof command -- and "
                "writing the snapshot would destroy that edit. This is the same "
                "refusal the in-run path makes; recovery must not be the weaker "
                f"path. Pre-run content: {STATE_DIRNAME}/{SNAPSHOT_DIRNAME}/"
                f"{snapshot_name}. Reconcile by hand.\n"
                "    If the recorded path is a symlink that has since been "
                "retargeted, the originally-mutated file is NOT the one this "
                "message names -- check that before writing anything.\n"
                f"    Once {relative} is correct again -- your edit preserved and "
                "the mutation gone -- clear the record with: "
                f"{_accept_hint(relative)}"
            )
        _atomic_write(target, original)
        written = target.read_bytes()
        if _digest(written) != _digest(original):
            raise HarnessError(f"wrote {relative} but its content does not match")
        _write_state(root, None)
        return (
            f"restored {relative} from {snapshot_name} and cleared the applied record"
        )
    finally:
        if lock is not None:
            release_lock(lock)
        elif force:
            release_lock(_state_dir(root) / LOCK_DIRNAME)


def _recorded_proof(applied: dict[str, Any], relative: str) -> list[tuple[str, ...]]:
    """The proof commands from the applied record, validated as argv arrays."""

    raw = applied.get("proof")
    if not isinstance(raw, list) or not raw:
        raise HarnessError(
            f"REFUSING TO ACCEPT {relative}: the record carries no proof commands, "
            "so there is nothing that can show this tree behaves like a repaired "
            "one -- and an acceptance backed by nothing is the blanket override "
            "this command must not be. The record predates this field. Verify by "
            f"hand and, only once you are certain, delete {STATE_DIRNAME}/"
            f"{STATE_FILENAME} yourself."
        )
    commands: list[tuple[str, ...]] = []
    for command in raw:
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(word, str) and word for word in command)
        ):
            raise HarnessError(
                f"REFUSING TO ACCEPT {relative}: the record's proof commands are "
                f"malformed ({command!r}). Recover by hand."
            )
        commands.append(tuple(command))
    return commands


def accept_manual_repair(root: Path, claimed_digest: str) -> str:
    """Clear an applied record after the mutation was undone BY HAND.

    Every refusal in `restore` is safe, and several of them are terminal: the
    snapshot was deleted by a proof running `git clean -fdX`, or the file was
    reconciled and now matches neither the original nor the mutation. In those
    states the right thing for the user to do is repair the file themselves --
    and then they were stuck, because the record could only be cleared by a
    successful restore. `verify` stayed red, the gate stayed red, and the only
    way out was to delete the record blind. A tool whose safe path dead-ends
    teaches people to take the unsafe one.

    This is that exit, and it is deliberately NOT "ignore the record". Leak
    detection is the entire point of the record, so clearing it has to be
    EARNED. Three pieces of evidence are required, and each is checked, not
    taken on trust:

      1. the caller states the digest of the file they are accepting. This does
         not prove they read it -- the refusal messages print the digest -- but
         it pins the decision to specific content: if anything writes the file
         between looking and accepting, the digest no longer matches and the
         acceptance fails rather than clearing a record against content nobody
         approved;
      2. the file must not hold the recorded mutation byte-for-byte. That state
         is not a repair, it is the leak itself, and `restore` handles it;
      3. the mutation's own PROOF COMMANDS must pass on the tree as it now
         stands.

    (3) is the whole design, and it is here because three successive attempts to
    answer the question by SEARCHING THE FILE were each wrong in a new way:
    counting anchor occurrences was satisfied by two comments while every code
    site stayed mutated; blanking intact anchors before searching was defeated by
    overlapping occurrences; and any exact-string test at all is defeated by
    reformatting, because the same disabled guard can be spelled a different way.

    Those are not three bugs, they are one: *no test that reads only the current
    text can decide whether a mutation is still in effect*, because the effect
    survives arbitrary respelling. Patching the search a fourth time would have
    been answering the wrong question more carefully.

    So `accept` asks the question this harness already answers everywhere else --
    the same one step 7 of the run protocol asks, for the same reason. A plan
    declares, per mutation, the commands that NOTICE it. Those commands passed on
    the clean tree before the mutation was applied. If they pass now, the tree
    behaves as an unmutated one on exactly the property the mutation was written
    to break -- and that is immune to spelling, formatting, overlap, and every
    other textual accident, because it is not a textual claim.

    The residue, stated rather than left to be discovered: a mutation NO proof
    command can observe -- a survivor -- is accepted on proofs that would have
    passed either way. That is real, and it is not fixable here: such a mutation
    is by construction invisible to the only evidence the plan supplies, so no
    check this tool could run would see it. It is the same limit that makes a
    SURVIVED verdict a matter for the operator's judgement.

    A cheap text tripwire still runs first, because a plainly-still-present
    replacement deserves a better message than a failing test. It can only
    REFUSE; passing it proves nothing and decides nothing.

    Nothing is written to the source. The only effect is removing the record.
    """

    lock = acquire_lock(root)
    try:
        state = _read_state(root)
        applied = (state or {}).get("applied")
        if not applied:
            return "nothing to accept: no mutation is recorded as applied"

        relative = str(applied.get("file") or "")
        target = _resolve_target(root, relative)
        recorded_identity = applied.get("identity")
        if recorded_identity is not None:
            current_identity = _file_identity(target)
            if current_identity != str(recorded_identity):
                raise HarnessError(
                    f"REFUSING TO ACCEPT {relative}: it no longer resolves to the "
                    f"file that was mutated. Recorded {recorded_identity}, now "
                    f"{current_identity}. Accepting would clear the record while "
                    "the file that actually holds the mutation stays broken and "
                    "unrecorded -- which is the leak this record exists to make "
                    "visible. Find the file with the recorded device:inode first."
                )

        current_bytes = target.read_bytes()
        current = _digest(current_bytes)
        claimed = claimed_digest.strip().lower()
        if claimed != current:
            raise HarnessError(
                f"REFUSING TO ACCEPT {relative}: you supplied {claimed[:12] or '(empty)'}"
                f" but the file hashes to {current}. Either the digest names "
                "different content than the file now holds -- something wrote it "
                "since you looked -- or it is for another file. Re-read the file, "
                "confirm it is what you mean to accept, and pass its current "
                f"digest: {_accept_hint(relative)}"
            )

        expected_mutated = applied.get("mutated_sha256")
        if expected_mutated is not None and current == str(expected_mutated):
            raise HarnessError(
                f"REFUSING TO ACCEPT {relative}: it still holds the recorded "
                "mutation byte-for-byte. This is not a repair to acknowledge, it "
                "is the leak itself, and accepting it would hide exactly what the "
                "record exists to report. Put the file back with: "
                "python3 scripts/mutation_harness.py restore"
            )

        # A cheap, one-directional TRIPWIRE, not the evidence. Refusing when the
        # replacement text is plainly still there costs nothing and gives a much
        # better message than a failing proof would. It is skipped where it
        # cannot discriminate, and it is only ever a reason to REFUSE -- passing
        # it means nothing at all, which is why it decides nothing below.
        find = applied.get("find")
        replace = applied.get("replace")
        if isinstance(find, str) and isinstance(replace, str) and replace not in find:
            try:
                text = current_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = ""
            if replace in text:
                raise HarnessError(
                    f"REFUSING TO ACCEPT {relative}: the mutation's replacement "
                    f"text is still in the file -- {replace.strip()[:70]!r}. Undo "
                    "it first; accepting now would clear a record that is telling "
                    "the truth."
                )

        commands = _recorded_proof(applied, relative)
        for command in commands:
            code, tail = _run_command(command, root)
            if code != 0:
                raise HarnessError(
                    f"REFUSING TO ACCEPT {relative}: the mutation's own proof "
                    f"command still fails on this tree.\n    "
                    f"{_format_command(command)}\n{tail.strip()[-1200:]}\n"
                    "    This is the command the plan declared as the thing that "
                    "notices this mutation, and it passed on the clean tree before "
                    "the mutation was applied. Failing now, it is saying the tree "
                    "does not yet behave like a repaired one. Either the mutation "
                    "is still in effect, or something else is broken -- and while "
                    "this cannot pass, no acceptance here would mean anything.\n"
                    "    If the failure is genuinely unrelated and you have "
                    f"verified {relative} by hand, delete {STATE_DIRNAME}/"
                    f"{STATE_FILENAME} yourself; that is a deliberate override, "
                    "and it should look like one."
                )

        identifier = applied.get("mutation_id", "?")
        _write_state(root, None)
        return (
            f"accepted a by-hand repair of {relative} (mutation {identifier}, "
            f"content {current[:12]}): all {len(commands)} recorded proof "
            "command(s) pass on this tree. Cleared the applied record; wrote "
            "nothing."
        )
    finally:
        release_lock(lock)


def _mutated_text(mutation: Mutation, original_text: str) -> str:
    """The exact text the mutation produces. One derivation, used twice.

    Computed before the write so the applied record can carry the digest, and
    again during the write. Deriving it in two places would be a second source of
    truth and would drift.
    """

    mutated = original_text.replace(mutation.find, mutation.replace)
    if mutated == original_text:
        raise HarnessError(
            f"{mutation.identifier}: applying the replacement changed nothing"
        )
    return mutated


def _apply(target: Path, mutation: Mutation, original_text: str) -> None:
    occurrences = original_text.count(mutation.find)
    if occurrences != mutation.expect_occurrences:
        raise HarnessError(
            f"{mutation.identifier}: anchor matches {occurrences} time(s) in "
            f"{mutation.path}, expected {mutation.expect_occurrences}. A "
            "mutation that lands somewhere other than intended -- a doc comment, "
            "a second call site -- reads exactly like a coverage gap. Narrow the "
            "anchor or set expect_occurrences deliberately."
        )
    line = _matched_line(original_text, mutation.find)
    stripped = line.strip()
    if not mutation.allow_comment_anchor and stripped.startswith(COMMENT_PREFIXES):
        raise HarnessError(
            f"{mutation.identifier}: the anchor's matched line is a comment "
            f"({stripped[:70]!r}). Mutating prose cannot change behaviour, so "
            "the result would be a false SURVIVED. Anchor on adjacent code, or "
            "set allow_comment_anchor if this really is the target."
        )
    mutated = _mutated_text(mutation, original_text)
    _atomic_write(target, mutated.encode("utf-8"))


def _require_unchanged(
    target: Path, expected_sha: str, mutation: Mutation, since: str
) -> None:
    """Refuse to write when the file no longer holds what we snapshotted.

    Any long-running step -- the baseline especially -- is a window in which an
    editor save, a formatter, or a proof command with a side effect can change
    the target. A harness that writes a stale snapshot over that change destroys
    a developer's work silently, which is a worse failure than any it detects.
    """

    current = _digest(target.read_bytes())
    if current == expected_sha:
        return
    raise HarnessError(
        f"{mutation.identifier}: {mutation.path} changed while {since}. "
        f"Snapshot digest {expected_sha[:12]}, on disk now {current[:12]}. "
        "Refusing to write, because applying the mutation would overwrite that "
        "change and the restore would overwrite it again. Nothing was modified. "
        "Re-run once the file is settled -- and if a proof command is editing "
        "the tree, that is the bug."
    )


def _restore_after_apply(
    target: Path,
    original_bytes: bytes,
    original_sha: str,
    mutated_sha: str,
    mutation: Mutation,
) -> None:
    """Undo an applied mutation on an early-return path, asserting the result.

    Uses the same three-way check as the main restore. Writing unconditionally
    here would destroy an edit saved during the build -- reintroducing, on the
    new early-return path, exactly the defect the main path was fixed for.
    """

    current = _digest(target.read_bytes())
    if current == original_sha:
        return
    if current != mutated_sha:
        raise HarnessError(
            f"REFUSING TO RESTORE {mutation.path}: it changed while the build ran. "
            f"Expected the mutated content ({mutated_sha[:12]}), found "
            f"{current[:12]}. Writing either version would destroy that change. "
            "The file is left exactly as found and the record is kept."
        )
    _atomic_write(target, original_bytes)
    if _digest(target.read_bytes()) != original_sha:
        raise HarnessError(
            f"FAILED TO RESTORE {mutation.path} after an invalid mutation. The "
            "mutation is still on disk. Repair with: "
            "python3 scripts/mutation_harness.py restore"
        )


# `go test -run <pattern>` whose pattern matches NOTHING exits 0. A proof
# command in that state is not passing -- it is not running, and a mutation
# measured by it is guaranteed to SURVIVE while the report reads as coverage.
#
# Same family as a skipped test reading as `ok`, and not hypothetical: a change
# that deleted a route test left a LIVE-code mutation pointing at
# `-run ^TestNameThatNoLongerExists$`. The pattern matched nothing, the command
# exited 0, and a real fail-closed property lost its guard while the plan still
# listed it as proven.
_VACUOUS_PROOF_MARKERS = (
    "no tests to run",
    "no test files",
)


def _vacuous_proof_reason(tail: str) -> str | None:
    """Return why this proof output proves nothing, or None if it ran something.

    Matches Go's own wording rather than parsing counts: `go test` prints
    `testing: warning: no tests to run` and marks the package
    `ok ... [no tests to run]`, both alongside exit code 0.
    """

    lowered = tail.lower()
    for marker in _VACUOUS_PROOF_MARKERS:
        if marker in lowered:
            return marker
    return None


def _proof_outcome(mutation: Mutation, root: Path) -> tuple[str | None, str]:
    """Run every proof command; return the first failing command and its tail."""

    for command in mutation.proof:
        code, tail = _run_command(command, root)
        if code != 0:
            return _format_command(command), tail
    return None, ""


def _verbose_go_test(command: tuple[str, ...]) -> tuple[str, ...]:
    """Add -v to a `go test` command so per-test outcomes are visible.

    Verbosity is not selection: -v changes what the runner PRINTS, never which
    tests run or how they are judged. Without it `go test` reports a package
    whose every selected test skipped as a bare `ok`, indistinguishable from one
    where they all passed -- which is the whole failure this classification
    exists to catch. Applied only to the harness's own baseline invocation.
    """

    if "test" not in command or not any(
        part == "go" or part.endswith("/go") for part in command
    ):
        return command
    if "-v" in command:
        return command
    return command + ("-v",)


def _baseline_proof_classification(tail: str) -> tuple[str, str] | None:
    """Classify a baseline proof run that EXITED ZERO but proved nothing.

    Two distinct ways that happens, both reported as their own verdict because
    both read as coverage while measuring nothing:

    * ``PROOF_VACUOUS`` -- the -run pattern selected no test at all.
    * ``PROOF_SKIPPED`` -- tests were selected and every one of them skipped,
      typically an env-gated live-oracle or integration suite whose variables
      are unset. A skipped proof passes on the clean tree AND on every mutant,
      so each mutation under it is reported SURVIVED for a reason that has
      nothing to do with coverage -- and a survivor later "declared" with a
      reason would launder an unmeasured entry into documented coverage.

    Baseline-only classification is sufficient: skipping here is decided by the
    environment, which is constant across the baseline and every mutant, so a
    baseline that skips guarantees every verdict under that proof is unmeasured.
    """

    reason = _vacuous_proof_reason(tail)
    if reason is not None:
        return VERDICT_PROOF_VACUOUS, reason
    if "--- SKIP" in tail and "--- PASS" not in tail:
        return VERDICT_PROOF_SKIPPED, "every selected test skipped"
    return None


def _baseline_proofs(
    mutation: Mutation, root: Path
) -> tuple[str | None, str, tuple[str, str, str] | None]:
    """Run every proof ONCE on the clean tree: pass/fail plus classification.

    One execution, not two. An earlier revision of this check ran the proofs a
    second time purely to classify them, which doubled the cost of every
    integration-tagged plan -- 20 minutes a pass here -- for information the
    baseline run already had.
    """

    for command in mutation.proof:
        code, tail = _run_command(_verbose_go_test(command), root)
        if code != 0:
            return _format_command(command), tail, None
        classification = _baseline_proof_classification(tail)
        if classification is not None:
            verdict, reason = classification
            return None, tail, (verdict, reason, _format_command(command))
    return None, "", None


def run_plan(
    root: Path,
    plan_path: Path,
    only: set[str] | None,
    assert_all_killed: bool,
    *,
    progress: str = "none",
    run_id: str | None = None,
    shard_index: int | None = None,
    child_protocol: _ChildProtocol | None = None,
) -> tuple[list[Result], int]:
    blockers = verify(root)
    if blockers:
        raise HarnessError("refusing to run: " + "; ".join(blockers))

    plan_name, mutations = _load_plan(plan_path)
    if only:
        unknown = only - {mutation.identifier for mutation in mutations}
        if unknown:
            raise HarnessError(f"--only names unknown mutations: {sorted(unknown)}")
        mutations = [mutation for mutation in mutations if mutation.identifier in only]

    if child_protocol is not None:
        selected_ids = {mutation.identifier for mutation in mutations}
        if set(child_protocol.plan_ordinals) != selected_ids:
            raise HarnessError(
                "internal child ordinal bindings do not exactly match selected mutations"
            )
        if run_id != child_protocol.run_id or shard_index != child_protocol.shard_index:
            raise HarnessError("internal child run identity is inconsistent")

    snapshot_dir = _snapshot_dir(root)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    selected_run_id = run_id or f"run-{uuid.uuid4().hex}"
    emitter = _ProgressEmitter(root, selected_run_id, progress)
    result_stream = emitter.run_directory / RESULT_STREAM_FILENAME
    results: list[Result] = []
    lock = acquire_lock(root)
    try:
        total = len(mutations)
        emitter.emit(
            "run_started",
            completed=0,
            active=0,
            total=total,
            shard_index=shard_index,
            phase="running",
        )
        if shard_index is not None:
            emitter.emit(
                "shard_started",
                completed=0,
                active=0,
                total=total,
                shard_index=shard_index,
                phase="running",
            )
        try:
            for ordinal, mutation in enumerate(mutations):
                event_ordinal = (
                    child_protocol.plan_ordinals[mutation.identifier]
                    if child_protocol is not None
                    else ordinal
                )
                child_started_ns = time.monotonic_ns()
                emitter.emit(
                    "mutation_started",
                    completed=len(results),
                    active=1,
                    total=total,
                    shard_index=shard_index,
                    plan_ordinal=event_ordinal,
                    mutation_id=mutation.identifier,
                    phase="baseline",
                )
                if child_protocol is not None:
                    child_protocol.emit(
                        "mutation_started",
                        mutation,
                        phase="baseline",
                        started_ns=child_started_ns,
                    )
                result = _run_one(root, mutation, snapshot_dir)
                results.append(result)
                if child_protocol is None:
                    _write_result_record(
                        result_stream, selected_run_id, ordinal, result
                    )
                else:
                    _write_child_result_record(child_protocol, mutation, result)
                emitter.emit(
                    "mutation_finished",
                    completed=len(results),
                    active=0,
                    total=total,
                    shard_index=shard_index,
                    plan_ordinal=event_ordinal,
                    mutation_id=mutation.identifier,
                    verdict=result.verdict,
                )
                if child_protocol is not None:
                    child_protocol.emit(
                        "mutation_finished",
                        mutation,
                        verdict=result.verdict,
                        started_ns=child_started_ns,
                    )
        except BaseException:
            emitter.emit(
                "run_stopping",
                completed=len(results),
                active=0,
                total=total,
                shard_index=shard_index,
                phase="aborted",
            )
            if shard_index is not None:
                emitter.emit(
                    "shard_finished",
                    completed=len(results),
                    active=0,
                    total=total,
                    shard_index=shard_index,
                    phase="aborted",
                )
            emitter.emit(
                "run_finished",
                completed=len(results),
                active=0,
                total=total,
                shard_index=shard_index,
                phase="aborted",
            )
            raise

        if shard_index is not None:
            emitter.emit(
                "shard_finished",
                completed=len(results),
                active=0,
                total=total,
                shard_index=shard_index,
                phase="complete",
            )

        report = {
            "schema_version": SCHEMA_VERSION,
            "plan": plan_name,
            "plan_path": str(plan_path),
            "results": [_result_fields(result) for result in results],
            "run_id": selected_run_id,
            "mode": "serial",
            "event_log": str(emitter.event_log.relative_to(root)),
            "result_stream": str(result_stream.relative_to(root)),
        }
        _state_dir(root).mkdir(parents=True, exist_ok=True)
        _atomic_write(
            _state_dir(root) / REPORT_FILENAME,
            (json.dumps(report, indent=2) + "\n").encode("utf-8"),
        )

        exit_code = 0
        # BASELINE_FAILED, INVALID, STALE_DECLARATION and PROOF_VACUOUS all mean a
        # mutation was never measured. A drifted anchor, a doubled match, a
        # comment-line anchor, or a proof selecting no test silently measures
        # nothing, so exiting 0 would report a plan as verified while part of it did
        # not run -- the same false pass this tool exists to catch.
        if any(
            result.verdict
            in {
                VERDICT_BASELINE_FAILED,
                VERDICT_INVALID,
                VERDICT_STALE_DECLARATION,
                VERDICT_PROOF_VACUOUS,
                VERDICT_PROOF_SKIPPED,
            }
            for result in results
        ):
            exit_code = 1
        if assert_all_killed and any(
            result.verdict == VERDICT_SURVIVED for result in results
        ):
            exit_code = 1
        emitter.emit(
            "run_finished",
            completed=len(results),
            active=0,
            total=total,
            shard_index=shard_index,
            phase="accepted" if exit_code == 0 else "unacceptable",
        )
        return results, exit_code
    finally:
        release_lock(lock)


def _run_one(root: Path, mutation: Mutation, snapshot_dir: Path) -> Result:
    target = _resolve_target(root, mutation.path)
    original_bytes = target.read_bytes()
    original_sha = _digest(original_bytes)
    snapshot_name = f"{mutation.identifier}-{original_sha[:16]}.snapshot"
    _atomic_write(snapshot_dir / snapshot_name, original_bytes)

    warnings: list[str] = []
    self_referential = _self_referential_warning(root, mutation)
    if self_referential:
        warnings.append(self_referential)
    if mutation.build is None:
        warnings.append(
            "no 'build' command is declared, so a mutation that fails to compile "
            "would be recorded as KILLED -- the proof exits non-zero either way. "
            'Declare a build command at plan level (e.g. ["go", "build", '
            '"./..."]) for any compiled language.'
        )

    # Step 3: a mutation measured against an already-red proof proves nothing --
    # and neither does one measured against an already-red BUILD. A build command
    # that cannot pass on the clean tree reports every mutation in the plan as
    # INVALID ("the mutated source does not build"), which reads as a plan full of
    # malformed mutations rather than as one wrong build command. Measured while
    # writing this plan's own build command: a first attempt failed on the
    # unmutated file, and without this check the whole self-plan would have
    # reported INVALID with the mutations blamed.
    if mutation.build is not None:
        baseline_build_code, baseline_build_tail = _run_command(mutation.build, root)
        if baseline_build_code != 0:
            # Before reporting "nothing was mutated", check that is true. A build
            # command that edits the target and THEN fails would otherwise leave
            # changed source, no record, and a clean `verify`, under a message
            # saying the file was never touched. Every early return out of this
            # function makes that claim, so every one of them has to earn it.
            _require_unchanged(target, original_sha, mutation, "the baseline build ran")
            return Result(
                identifier=mutation.identifier,
                verdict=VERDICT_BASELINE_FAILED,
                detail=(
                    "the declared build command fails on the CLEAN tree, so it "
                    "cannot tell a build break from a working one and every "
                    "verdict under it would be meaningless. Fix the build command "
                    "or the tree; nothing was mutated.\n"
                    f"    {_format_command(mutation.build)}\n"
                    f"{baseline_build_tail.strip()[-1200:]}"
                ),
                warnings=warnings,
                failing_proof=_format_command(mutation.build),
            )

    # One baseline pass answers both questions: did the proofs PASS, and did
    # they RUN AT ALL. A -run pattern matching nothing, or a suite whose every
    # selected test skips, exits 0 either way -- so without this the entry sails
    # through and then guarantees a survivor that reads as a coverage gap.
    failing, tail, unmeasured = _baseline_proofs(mutation, root)
    if unmeasured is not None:
        verdict, reason, command = unmeasured
        _require_unchanged(target, original_sha, mutation, "the baseline proofs ran")
        explanation = (
            "Usually a -run pattern naming a test that was renamed or deleted."
            if verdict == VERDICT_PROOF_VACUOUS
            else (
                "Usually an env-gated live-oracle or integration suite whose "
                "variables are unset. A skipped proof passes on the clean tree "
                "AND on every mutant, so every verdict under it is unmeasured."
            )
        )
        return Result(
            identifier=mutation.identifier,
            verdict=verdict,
            detail=(
                f"proof command proves nothing ({reason}): it cannot tell mutated "
                "source from unmutated, so this entry measured NOTHING while "
                f"exiting 0. {explanation} Nothing was mutated.\n"
                f"    {command}"
            ),
            warnings=warnings,
            failing_proof=command,
        )
    if failing is not None:
        # Same reason as the build path above: this return claims the file was
        # never touched, and a proof command with a side effect could have made
        # that false before it failed.
        _require_unchanged(target, original_sha, mutation, "the baseline proofs ran")
        return Result(
            identifier=mutation.identifier,
            verdict=VERDICT_BASELINE_FAILED,
            detail=(
                f"proof command failed on the clean tree: {failing}\n"
                f"{tail.strip()[-1200:]}"
            ),
            warnings=warnings,
            failing_proof=failing,
        )

    # Compute the digest the file WILL hold, before writing it, so the record is
    # complete the moment the mutation can exist on disk. Filled in afterwards
    # instead, a SIGKILL between the write and the update leaves a mutated file
    # whose record cannot prove the content is ours -- and recovery then refuses,
    # correctly but uselessly, for a case this tool created. SIGKILL runs no
    # handler, so only the on-disk record can cover it, and the record has to be
    # right before the window opens rather than after it closes.
    expected_mutated_sha: str | None = None
    try:
        expected_mutated_sha = _digest(
            _mutated_text(mutation, original_bytes.decode("utf-8")).encode("utf-8")
        )
    except (HarnessError, UnicodeDecodeError):
        # The apply below will reject it with the precise reason; leave the record
        # honest about not knowing rather than guessing.
        expected_mutated_sha = None

    # FRESHNESS, immediately before writing. The baseline above may have run for
    # minutes, during which an editor, a formatter, or a proof command with a
    # side effect can have saved an unrelated change. Writing the mutation now
    # from the snapshot taken before all that would destroy that change
    # wholesale, and the restore would destroy it a second time -- so this
    # harness would silently eat work, which is worse than the ad-hoc harnesses
    # it replaces. Never write over content this run has not seen.
    _require_unchanged(target, original_sha, mutation, "the baseline ran")

    # The applied record is written BEFORE the file is touched, not after.
    # Written after, a crash in the window between the write and the record
    # would leave a mutated file that `verify` reports as clean -- the precise
    # hole this harness exists to close, reintroduced one statement later. The
    # cost is that an INVALID mutation, which never writes, must clear the
    # record on its way out.
    _write_state(
        root,
        {
            "schema_version": SCHEMA_VERSION,
            "applied": {
                "mutation_id": mutation.identifier,
                "file": mutation.path,
                "snapshot": snapshot_name,
                "original_sha256": original_sha,
                # Device and inode, so recovery binds to the FILE rather than to
                # the path. Re-resolving `mutation.path` is not identity: an
                # in-repo symlink retargeted after a crash resolves to a
                # different file, and restore would "repair" that one while the
                # originally-mutated file stayed mutated.
                "identity": _file_identity(target),
                # Recorded so RECOVERY can refuse the same way the in-run path
                # does. Without an expected mutated digest, `restore` treats any
                # non-original content as safe to overwrite -- so a crash after a
                # proof or an editor touched the file turns `verify`'s own advice
                # into the data loss this tool exists to prevent. Written as None
                # first and filled in after the apply, because between these two
                # points the file holds the original.
                "mutated_sha256": expected_mutated_sha,
                # The mutation's own text, so a by-hand repair can be CHECKED
                # rather than asserted. `accept` requires the replacement string
                # to be gone and the anchor to be back; without these two fields
                # it would have to take the operator's word, which is the blanket
                # override the acceptance path must not become.
                "find": mutation.find,
                "replace": mutation.replace,
                # The commands that NOTICE this mutation. `accept` re-runs them
                # to decide whether a by-hand repair may clear this record: a
                # behavioural question, because no textual one can be answered
                # soundly. See accept_manual_repair.
                "proof": [list(command) for command in mutation.proof],
                "pid": os.getpid(),
            },
        },
    )

    try:
        _apply(target, mutation, original_bytes.decode("utf-8"))
    except HarnessError as exc:
        # Nothing was written, so the pessimistic record is now a lie in the
        # safe direction. Clear it, but do NOT write the snapshot back: the file
        # is untouched by definition on this path, and writing stale bytes over
        # it is precisely the concurrent-edit destruction guarded against above.
        _write_state(root, None)
        return Result(
            identifier=mutation.identifier,
            verdict=VERDICT_INVALID,
            detail=str(exc),
            warnings=warnings,
        )

    mutated_sha = _digest(target.read_bytes())
    if expected_mutated_sha is not None and mutated_sha != expected_mutated_sha:
        raise HarnessError(
            f"{mutation.identifier}: {mutation.path} does not hold the content this "
            "run computed before writing it. Something wrote the file in the same "
            "instant. Stop and inspect by hand."
        )
    _update_applied_record(root, "mutated_sha256", mutated_sha)

    # A mutation that does not COMPILE is not a kill. The proof command exits
    # non-zero either way, so a build break is indistinguishable from a failing
    # assertion by exit code alone -- and it is worse than a panic-kill, because a
    # panic at least executes the code whereas a build break runs no test at all.
    # Verdict: INVALID, the same as a drifted anchor, because both measured
    # nothing. Real instance: `if evaluated {` mutated to `if true {` orphaned the
    # variable, Go refused to build, and the run recorded KILLED.
    if mutation.build is not None:
        build_code, build_tail = _run_command(mutation.build, root)
        if build_code != 0:
            _restore_after_apply(
                target, original_bytes, original_sha, mutated_sha, mutation
            )
            _write_state(root, None)
            return Result(
                identifier=mutation.identifier,
                verdict=VERDICT_INVALID,
                detail=(
                    "the mutated source does not build, so no test ran and this "
                    "measured nothing. An exit code cannot tell a build break from "
                    "a failing assertion, which is why this is INVALID and not "
                    f"KILLED.\n    {_format_command(mutation.build)}\n"
                    f"{build_tail.strip()[-1200:]}\n"
                    "    Rewrite the mutation so it stays well-formed -- keeping "
                    "the mutated symbol referenced is usually enough."
                ),
                warnings=warnings,
            )
    try:
        failing, tail = _proof_outcome(mutation, root)
    finally:
        # Step 6: restore the bytes this run snapshotted -- never from git, so
        # unrelated uncommitted edits survive -- but only after proving the file
        # still holds exactly what this run wrote. If it holds something else, a
        # third party changed it during OBSERVE and writing either version would
        # lose data, so refuse and hand it to a human with both digests.
        current_sha = _digest(target.read_bytes())
        if current_sha == original_sha:
            # Something already restored it (a concurrent `restore`, or a proof
            # that reverted the file). Nothing to write; the record still has to
            # go, and the post-restore proof below still runs.
            _write_state(root, None)
        elif current_sha != mutated_sha:
            raise HarnessError(
                f"REFUSING TO RESTORE {mutation.path}: it changed during OBSERVE. "
                f"Expected the mutated content ({mutated_sha[:12]}) but found "
                f"{current_sha[:12]}. Writing either the snapshot or the mutation "
                "would destroy whatever was saved. The file is left exactly as "
                f"found and the applied record is kept in {STATE_DIRNAME}/"
                f"{STATE_FILENAME} so nothing else runs. Reconcile by hand; the "
                f"pre-run content is in {STATE_DIRNAME}/{SNAPSHOT_DIRNAME}/"
                f"{snapshot_name}."
            )
        else:
            _atomic_write(target, original_bytes)
            if _digest(target.read_bytes()) != original_sha:
                raise HarnessError(
                    f"FAILED TO RESTORE {mutation.path}. The mutation is still on "
                    "disk and every result in this run is void. The applied record "
                    f"in {STATE_DIRNAME}/{STATE_FILENAME} is intentionally left in "
                    "place so the next invocation refuses to run. Repair with: "
                    "python3 scripts/mutation_harness.py restore"
                )
            _write_state(root, None)

    if failing is None:
        verdict = (
            VERDICT_SURVIVED_DECLARED
            if mutation.expected_survivor_reason
            else VERDICT_SURVIVED
        )
        detail = mutation.expected_survivor_reason or (
            "every proof command still passed with the mutation applied. Either "
            "a test is missing, or this mutation is invalid (no-op, wrong "
            "target, or asserted against the constant it mutates). Classify it "
            "by declaring expected_survivor_reason, or add the missing test."
        )
    elif mutation.expected_survivor_reason:
        # Declared to survive, and it did not. The declaration is now stale, and
        # silently accepting the kill loses the reasoning that justified it -- the
        # redundancy or unobservability argument was about code that has since
        # changed. Surfaced as an anomaly rather than a pass.
        verdict = VERDICT_STALE_DECLARATION
        detail = (
            f"declared to survive, but was KILLED by: {failing}\n"
            f"    The declaration is stale: {mutation.expected_survivor_reason}\n"
            "    Re-derive it against the current code. If the mutation is now "
            "genuinely observable, delete expected_survivor_reason -- a stale "
            "declaration is a standing instruction to ignore a real signal."
        )
    else:
        verdict = VERDICT_KILLED
        # The kill SITE, not just the fact of a kill. A mutation killed by a setup
        # precondition, a panic, or an unrelated assertion is materially weaker
        # evidence than one killed by the assertion written for it -- and that
        # difference is invisible in a boolean. Two real misattributions were found
        # only because someone captured this: one mutation was dying in a seeding
        # helper with a count mismatch, another was "killed" by a build failure.
        detail = (
            f"killed by: {failing}\n"
            "    Kill site below. Confirm it is the assertion this mutation "
            "targets: a kill from setup, a panic, or an unrelated test is a weaker "
            "result than the verdict suggests.\n"
            f"{_kill_site(tail)}"
        )

    # Step 7: the bytes match, but prove behaviour came back too. This is what
    # makes the restore evidence instead of a claim -- the exact check that a
    # `go build` cannot perform.
    post_failing, post_tail = _proof_outcome(mutation, root)
    if post_failing is not None:
        raise HarnessError(
            f"{mutation.identifier}: {mutation.path} was restored byte-for-byte, "
            f"but proof command {post_failing!r} now fails on the restored tree. "
            "Something else changed underneath this run, so every result is "
            f"untrustworthy. Stop and investigate.\n{post_tail.strip()[-1200:]}"
        )

    return Result(
        identifier=mutation.identifier,
        verdict=verdict,
        detail=detail,
        warnings=warnings,
        failing_proof=failing,
    )


def _kill_site(tail: str) -> str:
    """Return the most site-like lines of a failing proof's output.

    Heuristic and deliberately generous: showing a few extra lines costs a glance,
    while showing none costs the ability to tell a real kill from an accidental
    one. Prefers lines that name a file and line number, which is what test
    runners emit at the point of failure.
    """

    lines = [line.rstrip() for line in tail.splitlines() if line.strip()]
    if not lines:
        return "    (the proof produced no output, so the kill site is unknown)"
    located = [line for line in lines if _FILE_LINE_RE.search(line)]
    chosen = located[:6] if located else lines[-6:]
    return "\n".join(f"    {line[:200]}" for line in chosen)


def _render(results: list[Result]) -> str:
    lines = ["| mutation | verdict | detail |", "| --- | --- | --- |"]
    for result in results:
        detail = result.detail.splitlines()[0] if result.detail else ""
        lines.append(f"| {result.identifier} | {result.verdict} | {detail[:110]} |")
    body = ["", *lines, ""]
    for result in results:
        for warning in result.warnings:
            body.append(f"WARNING {result.identifier}: {warning}")
    return "\n".join(body)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ and __doc__.splitlines()[0])
    parser.add_argument("--root", default=".", help="repository root")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("verify", help="fail if a mutation is still applied")
    restore_parser = sub.add_parser(
        "restore", help="put back a mutation left by a killed run"
    )
    restore_parser.add_argument(
        "--force",
        action="store_true",
        help="restore when a lock was left by a DEAD run. It deliberately "
        "refuses while a process with the recorded pid exists or the holder "
        "cannot be identified, so for a REUSED or missing pid inspect the "
        "holder, remove the lock directory by hand, and restore without this flag",
    )
    accept_parser = sub.add_parser(
        "accept",
        help="clear the applied record after undoing the mutation by hand "
        "(checked, not taken on trust: the mutation text must be gone)",
    )
    accept_parser.add_argument(
        "--digest",
        required=True,
        help="sha256 of the file as it now stands, which pins the acceptance to "
        "the content you inspected",
    )
    sub.add_parser("report", help="print the last run's report")
    recover_parser = sub.add_parser(
        "recover-run", help="recover an incomplete sharded coordinator run"
    )
    recover_parser.add_argument("--run-id", required=True)
    recover_parser.add_argument(
        "--force",
        action="store_true",
        help="continue safe cleanup of other owned shards after a recovery refusal",
    )
    run_parser = sub.add_parser("run", help="execute a mutation plan")
    run_parser.add_argument("--plan", required=True)
    run_parser.add_argument("--only", default="")
    run_parser.add_argument(
        "--assert-all-killed",
        action="store_true",
        help="exit non-zero on any survivor lacking expected_survivor_reason",
    )
    run_parser.add_argument(
        "--progress",
        choices=("human", "jsonl", "none"),
        default="human",
        help="stream progress to stderr (durable JSONL is always recorded)",
    )
    run_parser.add_argument(
        "--shards",
        type=int,
        default=1,
        help="number of isolated shards (requires plan opt-in above 1)",
    )
    run_parser.add_argument(
        "--internal-child",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args(argv)
    root = Path(args.root).resolve()

    try:
        if args.command == "verify":
            blockers = verify(root)
            if blockers:
                for blocker in blockers:
                    print(f"FAIL: {blocker}", file=sys.stderr)
                return 1
            print("mutation harness: tree is clean, no mutation applied")
            return 0
        if args.command == "restore":
            print(restore(root, force=bool(getattr(args, "force", False))))
            return 0
        if args.command == "accept":
            print(accept_manual_repair(root, str(args.digest)))
            return 0
        if args.command == "report":
            path = _state_dir(root) / REPORT_FILENAME
            if not path.is_file():
                print("no report: nothing has been run in this tree", file=sys.stderr)
                return 1
            print(path.read_text(encoding="utf-8"), end="")
            return 0
        if args.command == "recover-run":
            print(recover_run(root, str(args.run_id), force=bool(args.force)))
            return 0

        only = {item.strip() for item in args.only.split(",") if item.strip()}
        plan_path = Path(args.plan)
        try:
            from scripts.mutation_harness_optin import (
                PlanContractError,
                load_plan_contract,
                validate_requested_shards,
            )

            _raw, sharding = load_plan_contract(plan_path)
            validate_requested_shards(sharding, int(args.shards))
        except ImportError as exc:
            raise HarnessError(
                "sharding plan validation backend is unavailable"
            ) from exc
        except PlanContractError as exc:
            raise HarnessError(str(exc)) from exc

        if args.internal_child:
            if int(args.shards) != 1:
                raise HarnessError("internal children must execute exactly one shard")
            child_protocol = _child_protocol_from_environment(root)
            results, exit_code = run_plan(
                root,
                plan_path,
                only or None,
                args.assert_all_killed,
                progress="none",
                run_id=child_protocol.run_id,
                shard_index=child_protocol.shard_index,
                child_protocol=child_protocol,
            )
            return exit_code
        if int(args.shards) == 1:
            results, exit_code = run_plan(
                root,
                plan_path,
                only or None,
                args.assert_all_killed,
                progress=str(args.progress),
            )
        else:
            results, exit_code = coordinator_run(
                root,
                plan_path,
                only or None,
                args.assert_all_killed,
                requested_shards=int(args.shards),
                progress=str(args.progress),
            )
        print(_render(results))
        for result in results:
            if result.verdict in {
                VERDICT_SURVIVED,
                VERDICT_BASELINE_FAILED,
                VERDICT_INVALID,
                VERDICT_STALE_DECLARATION,
                VERDICT_PROOF_VACUOUS,
                VERDICT_PROOF_SKIPPED,
            }:
                print(f"\n{result.identifier} {result.verdict}:\n{result.detail}")
        return exit_code
    except HarnessError as exc:
        print(f"mutation harness: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
