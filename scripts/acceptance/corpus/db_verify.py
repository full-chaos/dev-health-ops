"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the corpus runner's DB verification
plane -- ``docker compose exec -T``, never a host port, never a new product
API endpoint (team-lead ruling, 2026-08-06).

Exactly three harness concerns get this: :func:`verify_world_digest_via_exec`
(ruling D2), :func:`query_resolution_ledger_via_exec` (the CHAOS-3424
resolution ledger, for ``resolution_path`` derivation), and
:func:`query_transcript_assistant_schema_versions_via_exec` (the CHAOS-3423
transcript row, for ``terminal_persists_assistant_row``). Product-facing
case execution (the SSE HTTP round-trip in
``test_wave4_corpus_runner_live.py``) stays wire-only against the public
API -- this module is not, and must never become, a general-purpose
database client for case assertions.

The exec boundary itself FAILS LOUD on any failure -- ``docker``/``compose``
missing, the ``api`` container not running, a non-zero exit, or output this
module cannot parse -- via :class:`DbVerifyUnavailableError`. This is
distinct from :func:`scripts.acceptance.corpus.resolution_path.
derive_resolution_path`'s own honest ``None`` for a ledger with genuinely
zero rows: that "no data" reading only applies once the exec plane has
actually reached the database and found nothing -- an exec that could not
even be attempted must never be silently treated the same way.
"""

from __future__ import annotations

import json
import re
import subprocess
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from scripts.acceptance.corpus.compose_context import ComposeContext
from scripts.acceptance.corpus.resolution_path import ResolutionLedgerEntry
from scripts.acceptance.corpus.world_digest_guard import WorldDigestVerification

__all__ = [
    "DbVerifyUnavailableError",
    "ExecRunner",
    "exec_in_api",
    "query_resolution_ledger_via_exec",
    "query_transcript_assistant_schema_versions_via_exec",
    "verify_world_digest_via_exec",
]

#: Injectable for tests -- must behave like ``subprocess.run`` (accepting
#: ``capture_output``/``text``/``timeout`` keywords and returning something
#: with ``.returncode``/``.stdout``/``.stderr``).
ExecRunner = Callable[..., Any]


class DbVerifyUnavailableError(Exception):
    """The exec verification plane could not be reached at all.

    Never caught internally and downgraded to "no data" -- see the module
    docstring. A caller that wants "the corpus run cannot proceed without
    this measurement" behavior should let this propagate; the CHAOS-3219
    Phase 2 mandate ("a measurement that did not happen must FAIL, loudly")
    applies directly here.
    """


def exec_in_api(
    context: ComposeContext,
    command: Sequence[str],
    *,
    runner: ExecRunner = subprocess.run,
    timeout: float = 60.0,
) -> str:
    """Run ``command`` inside the acceptance stack's ``api`` container via
    ``docker compose exec -T`` and return its stdout.

    ``-T`` (no pseudo-TTY) is non-negotiable -- allocating one would corrupt
    the machine-parseable stdout contract both inner scripts rely on.
    """

    args = [*context.base_args(), "exec", "-T", context.api_service, *command]
    try:
        result = runner(args, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        raise DbVerifyUnavailableError(
            f"docker compose is not available on this host: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise DbVerifyUnavailableError(
            f"docker compose exec -T {context.api_service} {' '.join(command)} "
            f"timed out after {timeout}s"
        ) from exc
    if result.returncode != 0:
        raise DbVerifyUnavailableError(
            f"docker compose exec -T {context.api_service} {' '.join(command)} "
            f"exited {result.returncode}: {result.stderr.strip()}"
        )
    return result.stdout


def _parse_json_line(stdout: str, *, command: Sequence[str]) -> Mapping[str, Any]:
    stripped = stdout.strip()
    if not stripped:
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} produced no stdout -- expected one JSON line"
        )
    try:
        payload = json.loads(stripped.splitlines()[-1])
    except json.JSONDecodeError as exc:
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} produced output this module cannot parse "
            f"as JSON: {stripped!r} ({exc})"
        ) from exc
    if not isinstance(payload, Mapping):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} produced a JSON value that is not an object: "
            f"{payload!r}"
        )
    return payload


def _require_str_field(
    payload: Mapping[str, Any], key: str, *, command: Sequence[str]
) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output field {key!r} must be a string, "
            f"got {value!r} ({payload!r})"
        )
    return value


#: `fixtures/world.py`'s `compute_world_digest` always produces
#: `hashlib.sha256(...).hexdigest()` -- exactly 64 lowercase hex characters.
#: Codex round-3 finding (HIGH, confirmed): requiring only `isinstance(str)`
#: let two EMPTY strings ("" == "") report `matched=True` for a
#: WORLD_DIGEST verification that never actually computed a real digest at
#: all -- an empty-string false-green, not merely a formatting nitpick.
_SHA256_HEXDIGEST = re.compile(r"^[0-9a-f]{64}$")


def _require_digest_field(
    payload: Mapping[str, Any], key: str, *, command: Sequence[str]
) -> str:
    value = _require_str_field(payload, key, command=command)
    if not _SHA256_HEXDIGEST.match(value):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output field {key!r} is not a "
            f"64-character lowercase hex sha256 digest: {value!r}"
        )
    return value


def verify_world_digest_via_exec(
    context: ComposeContext,
    *,
    manifest_path_in_container: str,
    sink: str,
    postgres_uri: str,
    digest_path_in_container: str | None = None,
    runner: ExecRunner = subprocess.run,
) -> WorldDigestVerification:
    command = [
        "python",
        "-m",
        "scripts.acceptance.corpus._inner_world_digest",
        "--manifest",
        manifest_path_in_container,
        "--sink",
        sink,
        "--postgres-uri",
        postgres_uri,
    ]
    if digest_path_in_container is not None:
        command += ["--digest-path", digest_path_in_container]
    stdout = exec_in_api(context, command, runner=runner)
    payload = _parse_json_line(stdout, command=command)

    # Codex round-2 finding (HIGH, confirmed): the original version trusted
    # `payload["matched"]` verbatim -- a non-bool truthy value (e.g. the
    # STRING "false", which Python's `bool("false")` evaluates True) could
    # slip a mismatched digest pair past `require_world_digest_match`
    # entirely. Every field is now type-checked, and `matched` is
    # RECOMPUTED from the two digest strings rather than trusted from the
    # inner script's own claim -- the inner script's `matched` value is
    # never consulted at all, closing the class of bug rather than one
    # instance of it.
    pinned_digest = _require_digest_field(payload, "pinned_digest", command=command)
    live_digest = _require_digest_field(payload, "live_digest", command=command)
    drifted_raw = payload.get("drifted_components")
    if not isinstance(drifted_raw, list) or not all(
        isinstance(item, str) for item in drifted_raw
    ):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output field 'drifted_components' must "
            f"be a list of strings, got {drifted_raw!r}"
        )
    return WorldDigestVerification(
        pinned_digest=pinned_digest,
        live_digest=live_digest,
        matched=(pinned_digest == live_digest),
        drifted_components=tuple(drifted_raw),
    )


def _ledger_entry_from_json(
    entry: Any, *, command: Sequence[str]
) -> ResolutionLedgerEntry:
    # Codex round-2 finding (HIGH, confirmed): indexing `entry["outcome"]`
    # directly let a malformed row leak a raw KeyError/TypeError instead of
    # DbVerifyUnavailableError -- every caller of this module expects
    # exactly one exception type for "the verification plane misbehaved".
    if not isinstance(entry, Mapping):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output 'entries' contains a non-object "
            f"entry: {entry!r}"
        )
    outcome = entry.get("outcome")
    mention_id = entry.get("mention_id")
    if not isinstance(outcome, str) or not isinstance(mention_id, str):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output entry is missing a string "
            f"'outcome'/'mention_id': {entry!r}"
        )
    committed_label = entry.get("committed_label")
    committed_canonical_id = entry.get("committed_canonical_id")
    if committed_label is not None and not isinstance(committed_label, str):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output entry 'committed_label' must be "
            f"a string or null: {entry!r}"
        )
    if committed_canonical_id is not None and not isinstance(
        committed_canonical_id, str
    ):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output entry 'committed_canonical_id' "
            f"must be a string or null: {entry!r}"
        )
    return ResolutionLedgerEntry(
        outcome=outcome,
        mention_id=mention_id,
        committed_label=committed_label,
        committed_canonical_id=committed_canonical_id,
    )


def query_resolution_ledger_via_exec(
    context: ComposeContext,
    *,
    run_id: str | uuid.UUID,
    database_uri: str | None = None,
    runner: ExecRunner = subprocess.run,
) -> list[ResolutionLedgerEntry]:
    command = [
        "python",
        "-m",
        "scripts.acceptance.corpus._inner_ledger_query",
        "--run-id",
        str(run_id),
    ]
    if database_uri is not None:
        command += ["--database-uri", database_uri]
    stdout = exec_in_api(context, command, runner=runner)
    payload = _parse_json_line(stdout, command=command)
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output has no 'entries' list: {payload!r}"
        )
    return [_ledger_entry_from_json(entry, command=command) for entry in entries]


def query_transcript_assistant_schema_versions_via_exec(
    context: ComposeContext,
    *,
    conversation_id: str | uuid.UUID,
    database_uri: str | None = None,
    runner: ExecRunner = subprocess.run,
) -> list[str | None]:
    """The ``dev_messages.answer_payload.schema_version`` of every assistant
    row on one conversation -- for ``invariants.terminal_persists_
    assistant_row`` (CHAOS-3423's own guarantee, verified from the corpus
    side, per team-lead direction 2026-08-06 folding this in as the third
    harness concern allowed through the exec verification plane)."""

    command = [
        "python",
        "-m",
        "scripts.acceptance.corpus._inner_transcript_query",
        "--conversation-id",
        str(conversation_id),
    ]
    if database_uri is not None:
        command += ["--database-uri", database_uri]
    stdout = exec_in_api(context, command, runner=runner)
    payload = _parse_json_line(stdout, command=command)
    versions = payload.get("assistant_schema_versions")
    if not isinstance(versions, list) or not all(
        item is None or isinstance(item, str) for item in versions
    ):
        raise DbVerifyUnavailableError(
            f"{' '.join(command)} JSON output 'assistant_schema_versions' "
            f"must be a list of strings/nulls, got {versions!r}"
        )
    return versions
