"""Role- and fault-aware scripted decision-sequence engine (CHAOS-3219 Phase 1
Lane 1b) for :mod:`dev_health_ops.llm.agent.scripted_openai_service`.

Not a product module. This is loaded only by the Ask Dev acceptance scripted
OpenAI-compatible provider, exactly like the module it extends.

Request-fingerprint routing (question-hash, not a tag)
--------------------------------------------------------
CHAOS-3219's corpus runner (Phase 2, a separate lane) must be able to tell
this scripted provider *which frozen registry case* (see
``tests/acceptance/world/ask-dev-world.v1/provider-scripts/registry-ids.v1.json``)
it is driving, so a per-case decision sequence or fault script can be
selected deterministically.

An earlier revision of this module did this by embedding a
``[[case:<id>]]`` marker inside ``DevMessageRequest.question`` itself. Codex
review of that revision (a735495a1) found it HIGH severity: ``question`` is
the literal text persisted verbatim as the user's own conversation message
(``DevPersistenceService.append_user_message_and_run``) -- provider-side
stripping happens far too late to protect that persisted row, any transcript
export, or a replay. Scanning the *provider's own answer* fields
(``scan_public_text``) proved the answer was clean; it never proved the
*user's own message* was.

This revision eliminates the marker entirely. Each frozen registry case
already has fixed, known question text (the corpus runner always asks a
case's *exact* question) -- so routing keys directly off a normalized hash
of the question text itself, computed identically on both sides:

    fingerprint = sha256(casefold(collapse_whitespace(question.strip())))

``role-<role>.json`` stores each case's expected ``question`` string
alongside its decisions/fault script; ``load_role_script`` builds a
``fingerprint -> case`` index at load time and hard-fails (``ValueError``)
if two different case ids ever produce the same fingerprint (a collision
the corpus-authoring lane must resolve by rewording one of the two
questions, never a routing ambiguity this module resolves silently).

The result: nothing acceptance-specific ever enters the persisted
transcript. The user message the corpus runner sends IS the natural
question a real user would ask -- no wrapper, no suffix, no delimiter --
so there is no leak surface to strip, sanitize, or backstop-scan for. A
question that happens to match no scripted case's fingerprint is
indistinguishable, on the wire, from an ordinary/organic question -- and
is treated exactly that way: it falls through to the pre-existing default
heuristic, unchanged, so every existing smoke script, the Wave 3.1 browser
oracle, and the ``legacy_agent`` role-certification probe (none of which
send any of this file's scripted literal questions) keep working exactly
as they did before this module existed. Fail-loud is reserved for
requests this module has affirmative reason to believe were addressed to
a *specific* scripted case (its literal question text matched one
byte-for-byte) but which the resolved script cannot actually serve --
never for "no match found", which is the routine, majority-case outcome
for all non-corpus traffic.

Defensive marker reservation
-----------------------------
The old ``[[case:`` marker is retired, not merely unused: if that literal
substring appears anywhere in a question -- well-formed, malformed,
truncated, or duplicated -- this module fails loud
(``legacy_case_tag_marker_present``) rather than letting it fall through to
the default heuristic. This closes the MEDIUM finding from the same Codex
round: a malformed/truncated tag under the old design silently produced a
generic canned 200 via the untagged fallback, which is exactly the
"unmapped case accidentally passes" failure mode CHAOS-3219 requirement 4
prohibits. The check is a pure string comparison -- no file I/O, no script
loading -- so it can never be skipped due to a missing/misconfigured
scripts directory the way script-based routing itself can (see below).

Why script/registry infrastructure failures do not fail loud
---------------------------------------------------------------
Unlike the marker check, a failure to *load* the scripts directory/role
file (missing directory, malformed JSON, a role with no script at all) is
treated as "no scripted match" -- falls through to the default heuristic --
never a 422. This is deliberate: with the tag eliminated, there is no
per-request syntactic signal left to distinguish "this corpus/script
pipeline is broken" from "this was never meant to be a scripted case" (both
look identical: an ordinary-shaped question, no match found). Given that
distinction cannot be made honestly at the wire level, and given breaking
every pre-existing non-corpus acceptance smoke/probe/oracle the moment the
scripts directory is unavailable would be a severe, silent regression in a
shared lane other work depends on, infra failures degrade to "act as if
untagged" rather than fail the request. The infra-is-broken case is instead
caught where it belongs: the static conformance suite
(``tests/acceptance/test_ask_dev_provider_roles.py``) loads the registry and
every enabled role's script unconditionally and asserts they parse
correctly -- the same "static wiring guards run in the unit tier, before
anyone trusts a green acceptance run" pattern already used elsewhere in this
lane (``test_ask_dev_compose.py``, the wave31 manifest's own integrity
checks).

A *matched* case (question text equal, byte-for-byte after normalization,
to one this role explicitly scripted) that then cannot actually be served
-- its decision list is exhausted for this round, its fault's
pre-fault-decisions ran out, or it asks for a tool the client never offered
-- is a different, much stronger signal: an exact sentence match is not
plausible by accident, so treating it as "this really was meant to invoke a
specific scripted case, and something about that script is broken" and
failing loud is correct, and is exactly what
``UnmappedCaseError``/``ScriptEngine.resolve`` still raises for those paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_SCHEMA_VERSION = "ask_dev_provider_script.v1"

#: Env var that overrides where role script JSON files are read from. Highest
#: precedence -- set by whatever launches the scripted provider (a Compose
#: service, a unit test) when the repo checkout is not reachable at its usual
#: relative path (e.g. a container image that copied only the scripts
#: directory in, not the full source tree). A missing/misconfigured
#: directory degrades to "no scripted match" for ordinary requests -- see
#: module docstring -- so this never needs to exist for untagged/default
#: traffic to keep working.
SCRIPTS_DIR_ENV = "ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR"

#: Env var selecting which role this provider process is scripted as. Only
#: ``legacy_agent`` has a working, production-representative probe today
#: (``llm.agent.role_readiness.RoleReadinessService.certify_role`` raises
#: ``NotImplementedError`` for every other ``AgentRole`` member -- see that
#: module) -- so this is the only value any real Compose service sets. The
#: mechanism is written generically so a future role only needs a new
#: ``role-<role>.json`` file plus this env var on its own service, once
#: CHAOS-3285 PR4 lands a probe for it.
ROLE_ENV = "ASK_DEV_SCRIPTED_PROVIDER_ROLE"
DEFAULT_ROLE = "legacy_agent"

_WORLD_RELATIVE_SCRIPTS_DIR = Path(
    "tests/acceptance/world/ask-dev-world.v1/provider-scripts"
)

#: The retired routing marker. Reserved, never accepted, in ANY form -- see
#: module docstring "Defensive marker reservation".
LEGACY_CASE_TAG_MARKER = "[[case:"

_WHITESPACE_RUN = re.compile(r"\s+")


def normalize_question_text(question: str) -> str:
    """Pinned normalization for question-fingerprint matching: collapse
    every run of whitespace to a single space, trim the ends, casefold.

    Pinned precisely, not "reasonable-effort" -- two questions that
    normalize identically are treated as the exact same scripted case, and
    changing this function silently reclassifies every existing script
    entry's fingerprint. If this ever needs to change, every
    ``role-*.json`` file's effective routing changes with it -- bump
    ``SCRIPT_SCHEMA_VERSION`` and re-verify every checked-in script's
    ``question`` still resolves as intended.
    """

    return _WHITESPACE_RUN.sub(" ", question.strip()).casefold()


def question_fingerprint(question: str) -> str:
    """Full SHA-256 hex digest of the normalized question text. Used as a
    dict key only -- not truncated, so there is no meaningful collision risk
    beyond SHA-256 itself (a same-role *different-question* collision is
    astronomically unlikely; a same-role *same-question* "collision" is not
    a collision at all, it is two case ids trying to claim the same
    question, which ``load_role_script`` rejects at load time instead)."""

    return hashlib.sha256(normalize_question_text(question).encode("utf-8")).hexdigest()


class UnmappedCaseError(Exception):
    """A question matched a scripted case but could not actually be served,
    or a script/registry file itself is malformed. Never raised for "no
    scripted case matched this question" -- that is the routine outcome for
    non-corpus traffic and returns ``None`` instead (see module docstring).
    """

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class ToolCallDecision:
    tool: str
    arguments: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class FinalAnswerDecision:
    value: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DisambiguationDecision:
    prompt: str
    candidates: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RefusalDecision:
    code: str
    message: str


ScriptedDecision = (
    ToolCallDecision | FinalAnswerDecision | DisambiguationDecision | RefusalDecision
)

#: Sentinel decision meaning "this case is intentionally scripted to behave
#: exactly like the default heuristic would for this exact question" -- used
#: so a corpus case can be addressed by its exact question text (making it
#: individually identifiable in the fault matrix / conformance suite)
#: without duplicating the existing grounded tool-call sequence in JSON. See
#: ``status.single-project.positive-control-v1`` in ``role-legacy_agent.json``.
DELEGATE_DEFAULT = "delegate_default"


@dataclass(frozen=True, slots=True)
class HttpFaultError:
    """An HTTP-error-shaped fault: the scripted server must respond with a
    non-2xx status and this JSON body, never a 200 canned answer."""

    status: int
    code: str
    message: str
    retry_after_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class ScriptTurn:
    """One resolved turn: either a decision to encode as a normal chat
    completion, or an HTTP error to send instead. Exactly one of
    ``decision``/``http_error`` is set. ``delay_ms`` applies before either is
    sent (the ``slow-response`` fault mechanism; 0 for every other case)."""

    decision: ScriptedDecision | None
    http_error: HttpFaultError | None
    delay_ms: int = 0

    def __post_init__(self) -> None:
        if (self.decision is None) == (self.http_error is None):
            raise ValueError("ScriptTurn must set exactly one of decision/http_error")


#: The 6 fault types CHAOS-3219 §1 bullet 2 names, pinned so a typo in a
#: script JSON file's "type" field fails loudly (unknown fault type) instead
#: of silently no-op'ing.
FAULT_TYPES = frozenset(
    {
        "fail-before-frame",
        "fail-after-frame",
        "unsafe-error-text",
        "oversized-output",
        "slow-response",
        "retry-storm-trigger",
    }
)

#: Floor for the ``oversized-output`` fault -- comfortably larger than any
#: real ``direct_summary``/fact text a grounded answer produces (the
#: existing acceptance oracle's longest rendered sentence is well under 200
#: bytes), so a script author cannot accidentally author a "big-ish" string
#: that fails to actually exercise an oversized-payload code path.
MIN_OVERSIZED_BYTES = 65_536


def _decision_from_payload(payload: Mapping[str, Any]) -> ScriptedDecision:
    kind = payload.get("type")
    if kind == "tool_call":
        tool = payload.get("tool")
        arguments = payload.get("arguments") or {}
        if not isinstance(tool, str) or not tool:
            raise ValueError("tool_call decision requires a non-empty 'tool'")
        if not isinstance(arguments, Mapping):
            raise ValueError("tool_call decision 'arguments' must be an object")
        return ToolCallDecision(tool=tool, arguments=dict(arguments))
    if kind == "final_answer":
        value = payload.get("value")
        if not isinstance(value, Mapping):
            raise ValueError("final_answer decision requires an object 'value'")
        return FinalAnswerDecision(value=dict(value))
    if kind == "disambiguation":
        prompt = payload.get("prompt")
        candidates = payload.get("candidates")
        if not isinstance(prompt, str) or not prompt:
            raise ValueError("disambiguation decision requires a non-empty 'prompt'")
        if not isinstance(candidates, Sequence) or not all(
            isinstance(item, str) for item in candidates
        ):
            raise ValueError("disambiguation decision requires string 'candidates'")
        return DisambiguationDecision(
            prompt=prompt, candidates=tuple(str(item) for item in candidates)
        )
    if kind == "refusal":
        code = payload.get("code")
        message = payload.get("message")
        if not isinstance(code, str) or not code:
            raise ValueError("refusal decision requires a non-empty 'code'")
        if not isinstance(message, str) or not message:
            raise ValueError("refusal decision requires a non-empty 'message'")
        return RefusalDecision(code=code, message=message)
    raise ValueError(f"unknown scripted decision type {kind!r}")


def _oversized_final_answer(min_bytes: int) -> FinalAnswerDecision:
    # Deliberately not realistic prose -- the point is a payload whose
    # encoded size crosses the configured floor, not plausible content. A
    # single repeated sentence keeps this human-legible in a debugger while
    # still being trivially checkable by byte length.
    filler = ("Scripted acceptance oversized-output fault payload segment. ") * (
        min_bytes // 56 + 1
    )
    return FinalAnswerDecision(value={"status": "degraded", "direct_summary": filler})


@dataclass(frozen=True, slots=True)
class _CaseScript:
    """One case id's parsed script entry."""

    case_id: str
    question: str
    kind: str  # "decisions" | "fault"
    decisions: tuple[ScriptedDecision, ...] = ()
    fault_type: str | None = None
    fires_from_round: int = 0
    pre_fault_decisions: tuple[ScriptedDecision, ...] = ()
    http_error: HttpFaultError | None = None
    oversized_min_bytes: int | None = None
    delay_ms: int = 0
    fault_decision: ScriptedDecision | None = None

    def resolve(self, *, round_index: int) -> ScriptTurn:
        if self.kind == "decisions":
            if round_index >= len(self.decisions):
                raise UnmappedCaseError(
                    "script_exhausted",
                    f"case {self.case_id!r} has no scripted decision for round "
                    f"{round_index} (only {len(self.decisions)} scripted)",
                )
            return ScriptTurn(decision=self.decisions[round_index], http_error=None)

        assert self.kind == "fault"  # noqa: S101 - internal invariant, not user input
        if round_index < self.fires_from_round:
            if round_index >= len(self.pre_fault_decisions):
                raise UnmappedCaseError(
                    "fault_pre_decisions_exhausted",
                    f"case {self.case_id!r} fault {self.fault_type!r} has no "
                    f"pre-fault decision for round {round_index}",
                )
            return ScriptTurn(
                decision=self.pre_fault_decisions[round_index], http_error=None
            )

        if self.fault_type == "oversized-output":
            assert self.oversized_min_bytes is not None  # noqa: S101
            return ScriptTurn(
                decision=_oversized_final_answer(self.oversized_min_bytes),
                http_error=None,
                delay_ms=self.delay_ms,
            )
        if self.fault_type == "slow-response":
            decision = self.fault_decision or FinalAnswerDecision(
                value={
                    "status": "degraded",
                    "direct_summary": "Scripted slow-response fault final answer.",
                }
            )
            return ScriptTurn(
                decision=decision, http_error=None, delay_ms=self.delay_ms
            )

        assert self.http_error is not None  # noqa: S101 - fail-*/unsafe/retry-storm
        return ScriptTurn(
            decision=None, http_error=self.http_error, delay_ms=self.delay_ms
        )


def _parse_case_entry(case_id: str, payload: Mapping[str, Any]) -> _CaseScript | str:
    """Return the parsed entry, or the literal string ``"delegate_default"``
    for that sentinel kind (kept out of ``_CaseScript`` itself so callers can
    special-case it without constructing a dataclass for a no-op)."""

    question = payload.get("question")
    if not isinstance(question, str) or not question.strip():
        raise ValueError(f"case {case_id!r} requires a non-empty 'question'")

    kind = payload.get("kind")
    if kind == DELEGATE_DEFAULT:
        return DELEGATE_DEFAULT
    if kind == "decisions":
        raw_decisions = payload.get("decisions")
        if not isinstance(raw_decisions, Sequence) or not raw_decisions:
            raise ValueError(f"case {case_id!r} 'decisions' must be a non-empty list")
        return _CaseScript(
            case_id=case_id,
            question=question,
            kind="decisions",
            decisions=tuple(_decision_from_payload(item) for item in raw_decisions),
        )
    if kind == "fault":
        fault = payload.get("fault")
        if not isinstance(fault, Mapping):
            raise ValueError(f"case {case_id!r} 'fault' must be an object")
        fault_type = fault.get("type")
        if fault_type not in FAULT_TYPES:
            raise ValueError(
                f"case {case_id!r} has unknown fault type {fault_type!r}; "
                f"must be one of {sorted(FAULT_TYPES)}"
            )
        fires_from_round = int(fault.get("fires_from_round", 0))
        pre_fault_raw = fault.get("pre_fault_decisions") or []
        if not isinstance(pre_fault_raw, Sequence):
            raise ValueError(f"case {case_id!r} 'pre_fault_decisions' must be a list")
        pre_fault_decisions = tuple(
            _decision_from_payload(item) for item in pre_fault_raw
        )
        http_error_payload = fault.get("http_error")
        http_error: HttpFaultError | None = None
        if http_error_payload is not None:
            if not isinstance(http_error_payload, Mapping):
                raise ValueError(f"case {case_id!r} 'http_error' must be an object")
            http_error = HttpFaultError(
                status=int(http_error_payload["status"]),
                code=str(http_error_payload["code"]),
                message=str(http_error_payload["message"]),
                retry_after_seconds=(
                    float(http_error_payload["retry_after_seconds"])
                    if "retry_after_seconds" in http_error_payload
                    else None
                ),
            )
        oversized_min_bytes = None
        if fault_type == "oversized-output":
            oversized_min_bytes = int(fault.get("min_bytes", MIN_OVERSIZED_BYTES))
            if oversized_min_bytes < MIN_OVERSIZED_BYTES:
                raise ValueError(
                    f"case {case_id!r} oversized-output min_bytes "
                    f"({oversized_min_bytes}) is below the floor "
                    f"({MIN_OVERSIZED_BYTES}) -- would not provably exercise "
                    "an oversized-payload code path"
                )
        elif (
            fault_type
            in {
                "fail-before-frame",
                "fail-after-frame",
                "unsafe-error-text",
                "retry-storm-trigger",
            }
            and http_error is None
        ):
            raise ValueError(
                f"case {case_id!r} fault type {fault_type!r} requires 'http_error'"
            )
        fault_decision_payload = fault.get("decision")
        fault_decision = (
            _decision_from_payload(fault_decision_payload)
            if fault_decision_payload is not None
            else None
        )
        return _CaseScript(
            case_id=case_id,
            question=question,
            kind="fault",
            fault_type=fault_type,
            fires_from_round=fires_from_round,
            pre_fault_decisions=pre_fault_decisions,
            http_error=http_error,
            oversized_min_bytes=oversized_min_bytes,
            delay_ms=int(fault.get("delay_ms", 0)),
            fault_decision=fault_decision,
        )
    raise ValueError(f"case {case_id!r} has unknown script kind {kind!r}")


@dataclass(frozen=True, slots=True)
class RoleScript:
    role: str
    #: case id -> parsed entry, kept for referential-integrity / documentation
    #: conformance checks (``registry-ids.v1.json`` cross-checks). NOT used
    #: for request-time routing -- see ``by_fingerprint``.
    cases: Mapping[str, _CaseScript | str]
    #: normalized-question-fingerprint -> (case id, parsed entry). This is
    #: what ``ScriptEngine.resolve`` actually looks up.
    by_fingerprint: Mapping[str, tuple[str, _CaseScript | str]]


def _entry_question(entry: _CaseScript | str, raw: Mapping[str, Any]) -> str:
    if isinstance(entry, _CaseScript):
        return entry.question
    return str(raw["question"])


def _scripts_dir() -> Path:
    override = os.getenv(SCRIPTS_DIR_ENV)
    if override:
        return Path(override)
    # Walk up from this file looking for the repo-relative conventional
    # location. Works for a full source checkout (local dev, unit tests, a
    # container image that COPYs the whole tree) without requiring the
    # directory to exist at import time -- infra failures degrade to "no
    # scripted match" for ordinary requests (see module docstring).
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / _WORLD_RELATIVE_SCRIPTS_DIR
        if candidate.is_dir():
            return candidate
    # Last resort: relative to CWD, so an explicit `cd` into the repo root
    # still works even if the package was installed outside of a checkout
    # (e.g. site-packages) and no override was set.
    return _WORLD_RELATIVE_SCRIPTS_DIR


def load_registry_ids(*, scripts_dir: Path | None = None) -> frozenset[str]:
    directory = scripts_dir or _scripts_dir()
    path = directory / "registry-ids.v1.json"
    if not path.is_file():
        raise UnmappedCaseError(
            "scripts_directory_unavailable",
            f"case-id registry not found at {path} (set {SCRIPTS_DIR_ENV} if "
            "this scripted provider does not run from a full repo checkout)",
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    ids: set[str] = set()
    for group in payload.get("groups", {}).values():
        ids.update(group.get("ids", []))
    return frozenset(ids)


def load_role_script(role: str, *, scripts_dir: Path | None = None) -> RoleScript:
    directory = scripts_dir or _scripts_dir()
    path = directory / f"role-{role}.json"
    if not path.is_file():
        raise UnmappedCaseError(
            "role_not_scripted",
            f"no scripted implementation for provider role {role!r} at {path}",
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCRIPT_SCHEMA_VERSION:
        raise UnmappedCaseError(
            "role_script_schema_mismatch",
            f"role-{role}.json schema_version {payload.get('schema_version')!r} "
            f"!= {SCRIPT_SCHEMA_VERSION!r}",
        )
    if payload.get("role") != role:
        raise UnmappedCaseError(
            "role_script_schema_mismatch",
            f"role-{role}.json declares role {payload.get('role')!r}, expected {role!r}",
        )
    raw_cases = payload.get("cases")
    if not isinstance(raw_cases, Mapping):
        raise UnmappedCaseError(
            "role_script_schema_mismatch", f"role-{role}.json 'cases' must be an object"
        )
    cases: dict[str, _CaseScript | str] = {}
    by_fingerprint: dict[str, tuple[str, _CaseScript | str]] = {}
    for case_id, raw_entry in raw_cases.items():
        entry = _parse_case_entry(case_id, raw_entry)
        cases[case_id] = entry
        question = _entry_question(entry, raw_entry)
        fingerprint = question_fingerprint(question)
        if fingerprint in by_fingerprint:
            other_case_id, _ = by_fingerprint[fingerprint]
            raise ValueError(
                f"role-{role}.json: cases {other_case_id!r} and {case_id!r} "
                "normalize to the identical question fingerprint -- two "
                "different cases cannot share one question; reword one"
            )
        by_fingerprint[fingerprint] = (case_id, entry)
    return RoleScript(role=role, cases=cases, by_fingerprint=by_fingerprint)


@dataclass(frozen=True, slots=True)
class ScriptEngine:
    """Loaded-once view over the registry + one role's script file. Construct
    fresh per-process (the HTTP handler caches an instance on the server
    object) -- this is cheap, deterministic JSON parsing, not a network call.
    """

    role: str
    registry_ids: frozenset[str]
    role_script: RoleScript

    @classmethod
    def load(cls, role: str, *, scripts_dir: Path | None = None) -> ScriptEngine:
        directory = scripts_dir or _scripts_dir()
        return cls(
            role=role,
            registry_ids=load_registry_ids(scripts_dir=directory),
            role_script=load_role_script(role, scripts_dir=directory),
        )

    def resolve(self, question: str, *, round_index: int) -> ScriptTurn | str | None:
        """Return a ``ScriptTurn`` to serve, the ``DELEGATE_DEFAULT``
        sentinel string, or ``None`` if no scripted case's question matches
        ``question`` at all -- the routine, majority-case outcome for
        non-corpus traffic, and NOT an error (see module docstring). Raises
        ``UnmappedCaseError`` only once a case has affirmatively matched but
        cannot actually be served for this round.
        """

        fingerprint = question_fingerprint(question)
        entry = self.role_script.by_fingerprint.get(fingerprint)
        if entry is None:
            return None
        _case_id, script_entry = entry
        if script_entry == DELEGATE_DEFAULT:
            return DELEGATE_DEFAULT
        assert isinstance(script_entry, _CaseScript)  # noqa: S101
        return script_entry.resolve(round_index=round_index)


def try_load_engine(
    role: str, *, scripts_dir: Path | None = None
) -> ScriptEngine | None:
    """``ScriptEngine.load`` wrapped so any infra failure (missing
    directory, malformed JSON, a role/registry file that fails to parse or
    collides) degrades to "no engine available" rather than propagating --
    see module docstring "Why script/registry infrastructure failures do not
    fail loud". Callers that want the raw failure (the conformance suite,
    proving the checked-in files themselves are valid) should call
    ``load_role_script``/``load_registry_ids``/``ScriptEngine.load`` directly
    instead.
    """

    try:
        return ScriptEngine.load(role, scripts_dir=scripts_dir)
    except (UnmappedCaseError, ValueError, OSError, json.JSONDecodeError):
        return None


def current_role() -> str:
    return os.getenv(ROLE_ENV, DEFAULT_ROLE) or DEFAULT_ROLE


def sleep_for_fault(delay_ms: int) -> None:
    if delay_ms > 0:
        time.sleep(delay_ms / 1000)


__all__ = [
    "DEFAULT_ROLE",
    "DELEGATE_DEFAULT",
    "FAULT_TYPES",
    "LEGACY_CASE_TAG_MARKER",
    "MIN_OVERSIZED_BYTES",
    "ROLE_ENV",
    "SCRIPTS_DIR_ENV",
    "SCRIPT_SCHEMA_VERSION",
    "DisambiguationDecision",
    "FinalAnswerDecision",
    "HttpFaultError",
    "RefusalDecision",
    "RoleScript",
    "ScriptEngine",
    "ScriptTurn",
    "ScriptedDecision",
    "ToolCallDecision",
    "UnmappedCaseError",
    "current_role",
    "load_registry_ids",
    "load_role_script",
    "normalize_question_text",
    "question_fingerprint",
    "sleep_for_fault",
    "try_load_engine",
]
