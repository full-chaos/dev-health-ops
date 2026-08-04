"""Shared execution-artifact machinery for CHAOS-3300 live acceptance smokes.

Codex finding (HIGH, 2026-08-02, against wave31_manifest.py): a
``proven_e2e`` claim required no machine-verifiable execution at all --
every ``proven_e2e`` row had empty ``test_nodeids`` (the field
``execute_manifest`` actually runs), so any row citing an existing evidence
file with ``requires_live_infra=True`` validated clean. The manifest's own
prose ("actually executed 2026-08-02...") was unverifiable narrative.

The response: every ``proven_e2e`` claim points at an execution artifact
the smoke script itself writes as a side effect of running --
``ScenarioRecorder`` below. The artifact records the pass/fail result of
every individual assertion the scenario made (not just "the process exited
0"), the exact bytes of the script that produced it, per-path digests of
the shared fixture surface it ran against, whether the working tree was
clean, and which commit HEAD was at.

BE PRECISE ABOUT WHAT THIS PROVES, because the whole point of this machinery
is that claims match evidence (codex finding, HIGH, 2026-08-03, which
demonstrated a hand-written artifact validating clean):

* It proves CURRENCY. ``script_sha256`` and ``runtime_digest`` are
  recomputed from the tree being validated, so an artifact describing a
  scenario whose script or fixture surface has since changed is detected
  and reported stale.
* It does NOT prove OCCURRENCE. Nothing here establishes that a live run
  happened. The artifact is unsigned JSON, and the validator recomputes
  exactly the values a forger would compute, so a fabricated artifact with
  correct hashes validates clean. Occurrence rests on the recorded
  per-assertion evidence being written by a real run, and on the policy of
  re-running scenarios -- it is operator-asserted, not validator-proven.
* ``commit_sha`` is METADATA. It is shape-checked but never resolved, so a
  well-formed id naming no commit is accepted. Commit ANCESTRY was removed
  as a validity gate on 2026-08-03: this repository squash-merges, so
  artifact commits leave history on every land, and a rebase orphans them
  just as thoroughly. Binding by content survives both.

A narrower residual gap is CHAOS-3351: the digest is sampled on the host,
while Compose built the containers earlier, so it cannot prove the
containers ran this code. ``ScenarioRecorder`` narrows that window by
digesting at scenario start and refusing to write if the tree moved
mid-run.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceFailure

ARTIFACT_SCHEMA_VERSION = "ask_dev_acceptance_artifact.v1"

__all__ = [
    "ARTIFACT_SCHEMA_VERSION",
    "RUNTIME_DEPENDENCY_PATHS",
    "AcceptanceFailure",
    "AssertionResult",
    "ScenarioRecorder",
    "aggregate_runtime_digest",
    "redact_secrets",
    "runtime_dependency_digest",
    "runtime_dependency_hashes",
]

#: Codex finding (HIGH, 2026-08-02): a smoke script recorded ``str(login)``
#: as an assertion's ``detail`` -- the *entire* login response, including a
#: live access_token and refresh_token JWT, ended up committed in six
#: execution artifacts (the fixture stack's tokens are signed with
#: ``ask-dev-acceptance-jwt-secret-key-v1``, a literal fixture-only secret
#: from ``tests/acceptance/compose.ask-dev.yml``, not a shared dev secret --
#: but a committed artifact must never contain live credentials regardless
#: of blast radius). ``check`` redacts every recorded detail so this cannot
#: recur even if a future script makes the same ``str(response)`` mistake --
#: redaction is a property of the recorder, not something each script author
#: must remember. JWTs always start "eyJ" (base64url of the header's
#: minimum ``{"alg":`` JSON) and are three dot-separated base64url segments.
_JWT_PATTERN = re.compile(
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
)
_REDACTED_JWT_PLACEHOLDER = "[REDACTED_JWT]"


def redact_secrets(detail: str) -> str:
    """Replace any JWT-shaped substring in ``detail`` with a fixed
    placeholder. Exported so ``validate_execution_artifact`` can run the
    same scan against already-written artifacts as a backstop -- redaction
    happening here does not by itself prove an artifact on disk has no
    leaked secret (it could predate this fix, or a future bypass could
    write one directly), so both sides check."""

    return _JWT_PATTERN.sub(_REDACTED_JWT_PLACEHOLDER, detail)


@dataclass(frozen=True, slots=True)
class AssertionResult:
    name: str
    passed: bool
    detail: str


#: The shared fixture surface a live scenario actually executes, beyond its
#: own smoke script. Codex finding (HIGH, 2026-08-03): ``script_sha256``
#: bound the smoke script's bytes and NOTHING else, so changing the scripted
#: provider left all 14 artifacts validating clean while the runs they
#: describe were no longer reproducible -- a false green from an untouched
#: smoke script.
#:
#: What is in this tuple: the recorder that writes artifacts, the API client
#: and fixture preparation, the launcher that decides the fixture shape
#: (seed, repo count, window), the scripted OpenAI-compatible provider that
#: stands in for the model, the Compose definitions that build the stack,
#: and the image build inputs Compose feeds them (``docker/Dockerfile``,
#: ``pyproject.toml``, ``requirements.txt`` -- codex finding, HIGH,
#: 2026-08-03: the API image is BUILT from that Dockerfile, so leaving it
#: out meant a changed base image or dependency pin moved the runtime while
#: the digest sat still). Change any of these and a recorded run is no
#: longer the run you would get today.
#:
#: WHAT THIS DIGEST DOES AND DOES NOT PROVE. It proves CURRENCY: the inputs
#: that determined a recorded run still match this tree, so a stale claim is
#: detectable. It does NOT prove OCCURRENCE -- that the run happened at all
#: -- and it is NOT tamper-resistance. The validator recomputes the digest
#: from the same tree it is checking, so anyone hand-writing an artifact can
#: compute the identical value; the artifact is unsigned JSON. What carries
#: occurrence is the recorded per-assertion results plus the policy of
#: re-running scenarios. A second, narrower gap is recorded in CHAOS-3351:
#: this digest is sampled on the HOST after the scenario finishes, while
#: Compose built the containers earlier, so it cannot prove the containers
#: ran this code. ``ScenarioRecorder`` narrows that window by digesting at
#: scenario start and refusing to write if the tree moved mid-run; closing
#: it properly needs the built image digest.
#:
#: What is deliberately NOT in it, and why. Product code under ``src`` is
#: excluded, apart from the fixture provider, which is a stand-in rather
#: than the system under test. Nothing else pins it -- so for product-code
#: changes these artifacts are HISTORICAL records of a past run, not proof
#: about the current tree, and no row should be read as claiming otherwise.
#: The trade is deliberate: including product code would invalidate every
#: artifact on every product commit, making a live re-mint a precondition
#: for all work, and a gate that expensive gets switched off. Individual
#: smoke scripts are excluded too: each is already bound by its own
#: ``script_sha256``, and folding them in here would make editing one
#: scenario invalidate the other thirteen.
RUNTIME_DEPENDENCY_PATHS: tuple[str, ...] = (
    "compose.yml",
    "docker/Dockerfile",
    "pyproject.toml",
    "requirements.txt",
    "scripts/acceptance/acceptance_artifact.py",
    "scripts/acceptance/prepare_ask_dev_acceptance.py",
    "scripts/acceptance/run_ask_dev_compose.sh",
    "src/dev_health_ops/llm/agent/scripted_openai_service.py",
    "tests/acceptance/compose.ask-dev-provider-profile.yml",
    "tests/acceptance/compose.ask-dev.yml",
)


def runtime_dependency_hashes(root: Path) -> dict[str, str]:
    """Per-path sha256 of the shared fixture surface.

    Recorded per path, not just as an aggregate, because a staleness
    declaration has to name exactly WHICH dependencies drifted -- see
    ``wave31_manifest.DECLARED_STALE_ARTIFACTS``. An aggregate alone
    answers "something moved" but never "what", which would leave the
    declaration unverifiable free text.

    A missing path raises rather than being skipped. Hashing over
    "whatever happened to be present" would let a deleted or renamed
    dependency produce a stable-looking result that silently covers less
    than it claims -- the measurement would not have happened, and the
    result would still look like a pass.
    """

    hashes: dict[str, str] = {}
    for relative in RUNTIME_DEPENDENCY_PATHS:
        path = root / relative
        if not path.is_file():
            raise AcceptanceFailure(
                f"runtime dependency {relative} does not exist under {root} -- "
                "refusing to compute a digest that would silently cover less "
                "than RUNTIME_DEPENDENCY_PATHS claims"
            )
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def runtime_dependency_digest(root: Path) -> str:
    """The aggregate over :func:`runtime_dependency_hashes`, for a cheap
    equality check and a short human-readable value in the artifact."""

    return aggregate_runtime_digest(runtime_dependency_hashes(root))


def aggregate_runtime_digest(hashes: Mapping[str, str]) -> str:
    hasher = hashlib.sha256()
    for relative in RUNTIME_DEPENDENCY_PATHS:
        hasher.update(relative.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(hashes[relative].encode("ascii"))
        hasher.update(b"\0")
    return hasher.hexdigest()


def _repo_root(start: Path) -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=start.parent,
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip())


def _git_head_sha(start: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=start.parent,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


#: Codex finding (HIGH, 2026-08-02): a recorded commit alone says nothing
#: about whether unrelated production code had uncommitted local edits at
#: record time. Recording whether the working tree was clean (ignoring the
#: artifacts directory itself, which this same write necessarily touches)
#: closes that: a dirty tree means what ran might not be what the recorded
#: state says, which validate_execution_artifact rejects as loudly as a
#: missing artifact.
_ARTIFACT_DIR = PurePosixPath("tests/acceptance/artifacts")


def _is_under_artifact_dir(path: str) -> bool:
    return PurePosixPath(path).is_relative_to(_ARTIFACT_DIR)


def _git_status_porcelain(start: Path, *, root: Path) -> str:
    """Serialize the working tree's dirty records, excluding only records
    entirely inside the artifacts directory this write itself touches.

    `--untracked-files=all` is load-bearing, not a nicety. Under git's
    default `normal` mode an entirely-untracked directory collapses to a
    single "?? tests/" record, so the artifacts-directory exclusion never
    matches its own path and a batch mint reports tree_clean=False for
    every scenario. It also pins the behaviour against the local
    `status.showUntrackedFiles` config, which otherwise makes this
    measurement differ between a developer's machine and CI.

    `-z` is equally load-bearing. Codex finding (HIGH, 2026-08-03): the
    exclusion used to be a substring test against the whole status line,
    which silently discarded genuinely dirty PRODUCTION paths that merely
    contained the artifact prefix somewhere -- `?? src/tests/acceptance/
    artifacts/runtime_override.py` reported tree_clean=True -- and
    discarded a rename whose destination escaped the artifacts directory
    entirely. NUL-terminated records remove git's path quoting and make
    a rename's two paths separate fields, so containment can be decided
    per path instead of guessed from the rendered line.
    """

    result = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    fields = result.stdout.split("\0")
    records: list[str] = []
    index = 0
    while index < len(fields):
        entry = fields[index]
        index += 1
        if not entry:
            continue
        # "XY <path>"; git pads to a fixed 3-character prefix.
        status, path = entry[:2], entry[3:]
        paths = [path]
        if "R" in status or "C" in status:
            # A rename/copy's ORIGINAL path is the next NUL field.
            if index < len(fields):
                paths.append(fields[index])
                index += 1
        # Exclude only when EVERY path in the record is genuinely beneath
        # the artifacts directory. A rename out of it still dirties the
        # tree, and so does a production file whose path merely looks
        # similar.
        if all(_is_under_artifact_dir(candidate) for candidate in paths):
            continue
        records.append(f"{status} {' -> '.join(paths)}")
    return "\n".join(records)


@dataclass(slots=True)
class ScenarioRecorder:
    """Collects per-assertion results for one live scenario and writes the
    execution artifact ``validate_execution_artifact`` checks.

    ``check`` both records the result (so a partial run's artifact still
    shows exactly which assertions passed before a failure) and raises
    :class:`AcceptanceFailure` on a false condition, preserving the existing
    scripts' fail-fast control flow -- callers wrap the whole scenario in a
    try/except and call :meth:`write` in both branches (see
    ``smoke_ask_dev_not_found.py``'s ``main`` for the pattern).
    """

    scenario_id: str
    script_path: Path
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    assertions: list[AssertionResult] = field(default_factory=list)
    #: Digest of the fixture surface as it was when this scenario STARTED.
    #: Codex finding (HIGH, 2026-08-03): sampling only at write time meant a
    #: covered file edited mid-run produced an artifact whose digest matched
    #: the tree while the containers had run the older code. Capturing at
    #: start and re-checking at write turns that silent mismatch into a
    #: refusal. It does not close the build-vs-start window (Compose built
    #: the images before this object existed) -- that is CHAOS-3351.
    started_hashes: dict[str, str] | None = field(default=None)

    def check(self, name: str, condition: bool, detail: str) -> None:
        redacted_detail = redact_secrets(detail)
        self.assertions.append(
            AssertionResult(name=name, passed=bool(condition), detail=redacted_detail)
        )
        if not condition:
            raise AcceptanceFailure(f"{name}: {redacted_detail}")

    def capture_runtime_digest(self, root: Path) -> dict[str, str]:
        """Record the fixture surface as of scenario start, once."""

        if self.started_hashes is None:
            self.started_hashes = runtime_dependency_hashes(root)
        return self.started_hashes

    def write(self, artifact_path: Path, *, error: str | None = None) -> dict[str, Any]:
        finished_at = datetime.now(UTC)
        root = _repo_root(self.script_path)
        commit_sha = _git_head_sha(self.script_path)
        script_sha256 = hashlib.sha256(self.script_path.read_bytes()).hexdigest()
        # A scenario that never captured a start digest is treated as having
        # started now: the scripts call check() long before write(), so the
        # realistic drift window is start-to-write, and defaulting here keeps
        # a caller that forgot the explicit capture honest rather than
        # silently unbound.
        started_hashes = self.capture_runtime_digest(root)
        finished_hashes = runtime_dependency_hashes(root)
        if started_hashes != finished_hashes:
            moved = sorted(
                relative
                for relative, digest in finished_hashes.items()
                if started_hashes.get(relative) != digest
            )
            raise AcceptanceFailure(
                "the fixture surface changed while this scenario was running "
                f"({moved}) -- the containers ran one version and the tree "
                "now holds another, so no artifact written here could "
                "describe what actually executed. Re-run against a settled "
                "tree."
            )
        status_porcelain = _git_status_porcelain(self.script_path, root=root)
        tree_clean = status_porcelain == ""
        tree_digest = hashlib.sha256(status_porcelain.encode("utf-8")).hexdigest()
        all_passed = bool(self.assertions) and all(a.passed for a in self.assertions)
        status = "passed" if (error is None and all_passed) else "failed"
        redacted_error = redact_secrets(error) if error is not None else None
        artifact: dict[str, Any] = {
            "schema_version": ARTIFACT_SCHEMA_VERSION,
            "scenario_id": self.scenario_id,
            "tree_clean": tree_clean,
            "tree_digest": tree_digest,
            "script": str(self.script_path.resolve().relative_to(root)),
            "script_sha256": script_sha256,
            "runtime_digest": aggregate_runtime_digest(started_hashes),
            "runtime_dependencies": dict(started_hashes),
            "commit_sha": commit_sha,
            "command": f"{Path(sys.executable).name} {self.script_path.name}",
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "status": status,
            "error": redacted_error,
            "assertions": [
                {"name": a.name, "passed": a.passed, "detail": a.detail}
                for a in self.assertions
            ],
        }
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(artifact, indent=2, sort_keys=False) + "\n", encoding="utf-8"
        )
        return artifact
