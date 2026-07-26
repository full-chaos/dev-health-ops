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
    python3 scripts/mutation_harness.py restore [--root PATH]
    python3 scripts/mutation_harness.py run --plan PATH [--only M1,M2]
                                            [--assert-all-killed]
    python3 scripts/mutation_harness.py report [--root PATH]

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
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

STATE_DIRNAME = ".mutation-harness"
STATE_FILENAME = "state.json"
REPORT_FILENAME = "report.json"
SNAPSHOT_DIRNAME = "snapshots"
LOCK_DIRNAME = "lock"
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


VERDICT_KILLED = "KILLED"
VERDICT_SURVIVED = "SURVIVED"
VERDICT_SURVIVED_DECLARED = "SURVIVED_DECLARED"
VERDICT_BASELINE_FAILED = "BASELINE_FAILED"
VERDICT_INVALID = "INVALID"
VERDICT_STALE_DECLARATION = "STALE_DECLARATION"


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
    return root / STATE_DIRNAME


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
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


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
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"could not read plan {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise HarnessError(f"{path} must be a JSON object")
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
            f"is gone this is a stale lock: remove it with 'rm -rf {lock}', then "
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


def _describe_holder(live: int) -> str:
    if live == _LOCK_HELD_BY_UNKNOWN:
        return "pid unknown -- the lock exists but its pid file does not"
    return f"pid {live}"


def verify(root: Path) -> list[str]:
    """Return the reasons the tree is not verified clean. Empty means clean.

    Both outcomes below are failures for gate purposes -- no test result from a
    tree with a mutation on it is trustworthy, whether or not a run is live --
    but the two messages must never be confused, because one says "wait" and the
    other says "repair".
    """

    state = _read_state(root)
    applied = (state or {}).get("applied")
    live = _live_run(root)

    if not applied:
        if live is not None:
            # A run holds the lock but has not applied anything yet -- it is in
            # its baseline. Reporting clean here is a time-of-check hole: the
            # gate would proceed and the mutation would land underneath it. A
            # held lock alone is enough to distrust every result from this tree.
            return [
                f"a mutation run is IN PROGRESS ({_describe_holder(live)}) and has not applied "
                "its mutation yet, so nothing is on disk right now -- but it will "
                "be, possibly before the caller of this check finishes. No result "
                "from this tree is trustworthy until the run completes. Wait for "
                f"pid {live}."
            ]
        return []

    identifier = applied.get("mutation_id", "?")
    relative = applied.get("file", "?")
    if live is not None:
        return [
            f"a mutation run appears IN PROGRESS ({_describe_holder(live)}), currently applying "
            f"{identifier} to {relative}. If that pid really is the harness this "
            "needs no repair, only patience -- but no test or build result from "
            "this tree is meaningful until it finishes.\n"
            "    Caveat: liveness here is 'a process with that pid exists', and "
            "pids are reused. If the harness died and an unrelated process "
            "inherited its pid, this will say IN PROGRESS forever. Check what "
            f"pid {live} actually is; if it is not the harness, repair with: "
            "python3 scripts/mutation_harness.py restore --force"
        ]
    return [
        f"mutation {identifier} is still applied to {relative} and NO run is "
        "alive, so it leaked from a dead run. Any result measured on this tree "
        "is void, including a passing build -- a mutation like "
        "'if false && (guard)' compiles cleanly, which is exactly why a build "
        "cannot be used to verify a restore. Repair with: "
        "python3 scripts/mutation_harness.py restore"
    ]


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
                "that run leave a mutation nothing knows about. Wait for it. If "
                f"pid {live} is not really the harness -- pids get reused -- "
                "re-run with --force."
            ) from None
        if not force:
            raise
        # --force is for a STALE lock. It must not evict a live one: clearing the
        # record and deleting another run's lock lets that run apply its mutation
        # and die with nothing recording it, after which `verify` reports clean
        # with a mutation on disk -- worse than the state --force was invoked to
        # repair, and reachable through this tool's own escape hatch.
        if live is not None and live != _LOCK_HELD_BY_UNKNOWN:
            raise HarnessError(
                f"--force refused: pid {live} is alive and holds the lock. Force "
                "is for a lock left by a DEAD run. Confirm that pid is not the "
                "harness and that it has exited, then re-run. If you are certain "
                f"it is unrelated, remove the lock yourself with 'rm -rf "
                f"{_state_dir(root) / LOCK_DIRNAME}' and re-run without --force."
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
        if _sanitised_identifier(snapshot_name) is None:
            raise HarnessError(
                f"recorded snapshot name {snapshot_name!r} is not a plain "
                "filename; refusing to read it. Recover by hand."
            )
        snapshot = _state_dir(root) / SNAPSHOT_DIRNAME / snapshot_name
        if not snapshot.is_file():
            raise HarnessError(
                f"snapshot {snapshot} is missing, so {relative} cannot be restored "
                "from this harness. Do NOT run `git checkout` on it: the file may "
                "hold pre-run work that was never committed, and discarding that "
                "is the exact loss this tool exists to prevent. Inspect it with "
                "`git diff` first and undo the mutation by hand. "
                "A proof command that runs `git clean -fdX` will delete this "
                f"directory, because {STATE_DIRNAME}/ is gitignored -- if that is "
                "what happened, that proof command is the bug."
            )
        original = snapshot.read_bytes()
        if expected and _digest(original) != expected:
            raise HarnessError(
                f"snapshot {snapshot} does not match its recorded digest; refusing "
                "to write it over your source. Recover by hand."
            )
        expected_mutated = applied.get("mutated_sha256")
        current = _digest(target.read_bytes())
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
                f"{snapshot_name}; reconcile by hand."
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
                "message names -- check that before writing anything."
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
    target: Path, original_bytes: bytes, original_sha: str, mutation: Mutation
) -> None:
    """Undo an applied mutation on an early-return path, asserting the result."""

    _atomic_write(target, original_bytes)
    if _digest(target.read_bytes()) != original_sha:
        raise HarnessError(
            f"FAILED TO RESTORE {mutation.path} after an invalid mutation. The "
            "mutation is still on disk. Repair with: "
            "python3 scripts/mutation_harness.py restore"
        )


def _proof_outcome(mutation: Mutation, root: Path) -> tuple[str | None, str]:
    """Run every proof command; return the first failing command and its tail."""

    for command in mutation.proof:
        code, tail = _run_command(command, root)
        if code != 0:
            return _format_command(command), tail
    return None, ""


def run_plan(
    root: Path,
    plan_path: Path,
    only: set[str] | None,
    assert_all_killed: bool,
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

    snapshot_dir = _state_dir(root) / SNAPSHOT_DIRNAME
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    results: list[Result] = []
    lock = acquire_lock(root)
    try:
        for mutation in mutations:
            results.append(_run_one(root, mutation, snapshot_dir))
    finally:
        release_lock(lock)

    report = {
        "schema_version": SCHEMA_VERSION,
        "plan": plan_name,
        "plan_path": str(plan_path),
        "results": [
            {
                "id": result.identifier,
                "verdict": result.verdict,
                "detail": result.detail,
                "failing_proof": result.failing_proof,
                "warnings": result.warnings,
            }
            for result in results
        ],
    }
    _state_dir(root).mkdir(parents=True, exist_ok=True)
    (_state_dir(root) / REPORT_FILENAME).write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

    exit_code = 0
    # BASELINE_FAILED and INVALID both mean a mutation was never measured. A
    # drifted anchor, a doubled match, or a comment-line anchor silently
    # measures nothing, so exiting 0 would report a plan as verified while part
    # of it did not run -- the same false pass this tool exists to catch.
    if any(
        result.verdict
        in {VERDICT_BASELINE_FAILED, VERDICT_INVALID, VERDICT_STALE_DECLARATION}
        for result in results
    ):
        exit_code = 1
    if assert_all_killed and any(
        result.verdict == VERDICT_SURVIVED for result in results
    ):
        exit_code = 1
    return results, exit_code


def _run_one(root: Path, mutation: Mutation, snapshot_dir: Path) -> Result:
    target = _resolve_target(root, mutation.path)
    original_bytes = target.read_bytes()
    original_sha = _digest(original_bytes)
    snapshot_name = f"{mutation.identifier}-{original_sha[:16]}.snapshot"
    (snapshot_dir / snapshot_name).write_bytes(original_bytes)

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

    # Step 3: a mutation measured against an already-red proof proves nothing.
    failing, tail = _proof_outcome(mutation, root)
    if failing is not None:
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
                # Recorded so RECOVERY can refuse the same way the in-run path
                # does. Without an expected mutated digest, `restore` treats any
                # non-original content as safe to overwrite -- so a crash after a
                # proof or an editor touched the file turns `verify`'s own advice
                # into the data loss this tool exists to prevent. Written as None
                # first and filled in after the apply, because between these two
                # points the file holds the original.
                "mutated_sha256": expected_mutated_sha,
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
            _restore_after_apply(target, original_bytes, original_sha, mutation)
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
        help="restore even when a lock is held (use only after confirming the "
        "recorded pid is not really the harness; pids are reused)",
    )
    sub.add_parser("report", help="print the last run's report")
    run_parser = sub.add_parser("run", help="execute a mutation plan")
    run_parser.add_argument("--plan", required=True)
    run_parser.add_argument("--only", default="")
    run_parser.add_argument(
        "--assert-all-killed",
        action="store_true",
        help="exit non-zero on any survivor lacking expected_survivor_reason",
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
        if args.command == "report":
            path = _state_dir(root) / REPORT_FILENAME
            if not path.is_file():
                print("no report: nothing has been run in this tree", file=sys.stderr)
                return 1
            print(path.read_text(encoding="utf-8"), end="")
            return 0

        only = {item.strip() for item in args.only.split(",") if item.strip()}
        results, exit_code = run_plan(
            root, Path(args.plan), only or None, args.assert_all_killed
        )
        print(_render(results))
        for result in results:
            if result.verdict in {
                VERDICT_SURVIVED,
                VERDICT_BASELINE_FAILED,
                VERDICT_INVALID,
                VERDICT_STALE_DECLARATION,
            }:
                print(f"\n{result.identifier} {result.verdict}:\n{result.detail}")
        return exit_code
    except HarnessError as exc:
        print(f"mutation harness: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
