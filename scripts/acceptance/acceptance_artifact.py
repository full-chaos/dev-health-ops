"""Shared execution-artifact machinery for CHAOS-3300 live acceptance smokes.

Codex finding (HIGH, 2026-08-02, against wave31_manifest.py): a
``proven_e2e`` claim required no machine-verifiable execution at all -- every
``proven_e2e`` row had empty ``test_nodeids`` (the field ``execute_manifest``
actually runs), so any row citing an existing evidence file with
``requires_live_infra=True`` validated clean, *including a fabricated row
that had never been run*. The manifest's own prose ("actually executed
2026-08-02...") was unverifiable narrative, not a checked fact.

The fix: every ``proven_e2e`` claim now points at an execution artifact the
smoke script itself writes as a side effect of running --
``ScenarioRecorder`` below. The artifact records which git commit the run
executed against, the exact bytes of the script that produced it (so an
edit after the fact is detectable), when it ran, and the pass/fail result of
every individual assertion the scenario made -- not just "the process
exited 0". ``wave31_manifest.validate_execution_artifact`` checks the
artifact exists, parses, records every assertion passing, and was generated
from a commit that is an ancestor of (or equal to) the current HEAD --
catching a fabricated, stale, or partially-failing claim the same way
``execute_manifest`` already catches a fabricated ``test_nodeids`` claim.

Commit-equality is deliberately NOT the check: the artifact necessarily
records the commit HEAD was at *before* the commit that adds the artifact
file itself (you cannot know a commit's own hash before creating it), so
requiring exact equality would make every artifact permanently invalid the
moment it is committed. ``git merge-base --is-ancestor <recorded> HEAD``
is the check this codebase already uses elsewhere for "this state genuinely
led to now, not fabricated or from an unrelated branch" (see the git-state
verification discipline this repo follows). Whether the *code* still matches
what was tested is a separate question, answered by the script_sha256 check.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceFailure

ARTIFACT_SCHEMA_VERSION = "ask_dev_acceptance_artifact.v1"

__all__ = [
    "ARTIFACT_SCHEMA_VERSION",
    "AcceptanceFailure",
    "AssertionResult",
    "ScenarioRecorder",
    "redact_secrets",
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


#: Codex finding (HIGH, 2026-08-02): the ancestor check alone accepts an
#: artifact recorded against ANY ancestor commit, however old, with no way
#: to tell whether unrelated production code had uncommitted local edits at
#: record time that later got silently discarded -- the commit_sha would
#: still be a true ancestor, but would not actually describe what ran.
#: Recording whether the working tree was clean (ignoring the artifacts
#: directory itself, which this same write necessarily touches) closes that
#: gap: a dirty tree means "what ran might not be what commit_sha says",
#: which validate_execution_artifact must reject just as it rejects a
#: missing artifact.
_ARTIFACT_DIR_PREFIX = "tests/acceptance/artifacts/"


def _git_status_porcelain(start: Path, *, root: Path) -> str:
    result = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    relevant_lines = [
        line
        for line in result.stdout.splitlines()
        # porcelain status lines are "XY <path>" (rename lines add
        # "-> <path>"); a plain substring check on the whole line is
        # sufficient since the artifacts directory is the only thing this
        # exclusion needs to cover.
        if _ARTIFACT_DIR_PREFIX not in line
    ]
    return "\n".join(relevant_lines)


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

    def check(self, name: str, condition: bool, detail: str) -> None:
        redacted_detail = redact_secrets(detail)
        self.assertions.append(
            AssertionResult(name=name, passed=bool(condition), detail=redacted_detail)
        )
        if not condition:
            raise AcceptanceFailure(f"{name}: {redacted_detail}")

    def write(self, artifact_path: Path, *, error: str | None = None) -> dict[str, Any]:
        finished_at = datetime.now(UTC)
        root = _repo_root(self.script_path)
        commit_sha = _git_head_sha(self.script_path)
        script_sha256 = hashlib.sha256(self.script_path.read_bytes()).hexdigest()
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
