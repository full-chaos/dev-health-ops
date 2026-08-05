"""Role- and fault-aware scripted decision-sequence engine (CHAOS-3219 Phase 1
Lane 1b) for :mod:`dev_health_ops.llm.agent.scripted_openai_service`.

Not a product module. This is loaded only by the Ask Dev acceptance scripted
OpenAI-compatible provider, exactly like the module it extends.

Request-fingerprint routing
----------------------------
CHAOS-3219's corpus runner (Phase 2, a separate lane) must be able to tell
this scripted provider *which frozen registry case* (see
``tests/acceptance/world/ask-dev-world.v1/provider-scripts/registry-ids.v1.json``)
it is driving, so a per-case decision sequence or fault script can be
selected deterministically. The only channel that survives, byte-for-byte,
from an HTTP call against the real ``/api/v1/dev/**`` surface all the way to
this provider's wire request is ``DevMessageRequest.question`` --
``PromptComposer.compose()`` embeds it verbatim as ``user_payload["question"]``
(``api/dev/prompts/composer.py``), and this module's sibling helper
``_question_from_messages`` already relies on that same channel for the
pre-existing ``LIST_METRICS_QUESTION`` literal-match and
``_evidence_query_from_question`` behaviors. No other client-controlled field
reaches this far unmodified: ``conversation_id``/``client_message_id``/
``request_id`` are ``OpaqueID`` values consumed for internal storage keys,
never re-serialized into the provider request.

So the case id is embedded as a tag inside the question text itself:

    "<real question text> [[case:<case-id>]]"

``extract_case_id`` parses it back out. The corpus runner is responsible for
appending this tag when it authors a case's question (documented here, not
implemented here -- Phase 2 Lane 2a/2b owns the runner and case authoring).

Why the tag cannot leak into a rendered answer
-----------------------------------------------
Two independent reasons, not one:

1. **By construction**: every scripted decision this module produces is
   built from the script JSON's own field values (or the pre-existing
   default heuristic's own fabricated copy) -- the raw ``question`` string
   (tag included) is read only to resolve *which* script entry applies. It
   is never copied into a ``direct_summary``, section, fact, or any other
   answer field. ``extract_case_id`` returns the captured case id alone,
   never the surrounding text.
2. **Defense in depth, in case (1) is ever violated by a future edit**: the
   tag's own shape cannot trip either backstop production already runs over
   every public copy field (``contracts_v2/validators.scan_public_text`` via
   ``validate_no_internal_leakage``, and ``no_match_terminal.INTERNAL_TOKEN_DENYLIST``):
   - ``INTERNAL_TOKEN_DENYLIST`` / ``PUBLIC_TEXT_FORBIDDEN_TOKENS`` are built
     from enum member ``.value``s and are, without exception, underscore-
     joined snake_case tokens (``_underscore_members`` explicitly filters to
     only members containing ``"_"``; the hand-written
     ``PUBLIC_TEXT_FORBIDDEN_TOKENS`` set is the same shape:
     ``"forbidden_or_not_found"``, ``"not_measured"``, ...). Every registry
     case id and this module's own tag alphabet use ``-`` (kebab) inside
     dot-separated segments, never ``_`` -- see ``_CASE_ID_PATTERN`` below,
     which the frozen registry ids in ``registry-ids.v1.json`` were checked
     against. A substring match against an underscore token can never fire
     on a hyphenated one.
   - ``_VERSIONED_ID_PATTERN`` in the same module flags a dotted token whose
     *last* segment matches ``v\\d+`` (``status.entity.v2`` shaped). No
     registry case id ends in a bare version segment (the one exception,
     ``status.single-project.positive-control-v1``, ends in
     ``...-v1`` fused onto the qualifier word, not a standalone
     ``.v1`` segment) -- so the pattern does not match any tag this module
     emits either.

The tag delimiters themselves (``[[case:`` / ``]]``) are also not
plausible-looking natural-language prose a human reviewer of a leaked string
would mistake for anything else, which is a second, independent reason a
leak (were one to occur) would be caught immediately rather than blend in.
"""

from __future__ import annotations

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
#: directory in, not the full source tree). Resolution is lazy (only
#: attempted once a request actually carries a case tag), so the untagged /
#: pre-CHAOS-3219 default-heuristic path never depends on this directory
#: existing at all -- see module docstring and
#: ``scripted_openai_service``'s untagged fallback.
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

#: Case-id tag embedded verbatim in ``DevMessageRequest.question`` by the
#: corpus runner. Segment shape matches the frozen registry convention
#: (``family.subfamily.qualifier``, lowercase, kebab-qualifiers,
#: dot-separated, at least two segments) -- see module docstring for why
#: this shape is denylist-safe.
_CASE_ID_SEGMENT = r"[a-z][a-z0-9-]*"
_CASE_TAG_PATTERN = re.compile(
    rf"\[\[case:({_CASE_ID_SEGMENT}(?:\.{_CASE_ID_SEGMENT})+)\]\]"
)
#: Same tag, plus any adjacent whitespace, for ``strip_case_tag`` -- replacing
#: with a single space (then stripping the ends) avoids leaving a
#: double-space artifact wherever the tag was.
_CASE_TAG_STRIP_PATTERN = re.compile(
    rf"\s*\[\[case:{_CASE_ID_SEGMENT}(?:\.{_CASE_ID_SEGMENT})+\]\]\s*"
)


def extract_case_id(question: str | None) -> str | None:
    """Return the tagged case id from ``question``, or ``None`` if untagged.

    ``None`` is the signal to fall all the way through to the pre-existing,
    untagged default heuristic -- never treated as "case id resolved to
    nothing" (that is ``UnmappedCaseError``, raised only once a tag was
    actually present).
    """

    if not question:
        return None
    match = _CASE_TAG_PATTERN.search(question)
    if match is None:
        return None
    return match.group(1)


def strip_case_tag(question: str | None) -> str | None:
    """Remove a ``[[case:...]]`` tag from ``question``, if present.

    ``delegate_default`` promises a case behaves *exactly* like an untagged
    request; callers apply this before running any legacy heuristic so a
    tagged question is indistinguishable, downstream, from the untagged one
    the tag was appended to. A no-op on an already-untagged question, so
    callers can apply it unconditionally.
    """

    if not question:
        return question
    return _CASE_TAG_STRIP_PATTERN.sub(" ", question).strip()


class UnmappedCaseError(Exception):
    """A case tag was present but could not be resolved to a runnable script.

    Every branch that raises this must be reachable independently of every
    other -- the conformance suite RED-verifies each ``code`` value
    separately so an unmapped case can never silently fall back to a generic
    canned 200 (CHAOS-3219 Phase 1 Lane 1b requirement 4).
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
#: exactly like the untagged default heuristic" -- used so a corpus case can
#: be case-tag-addressable (making it exercisable through per-case fixture
#: routing) without duplicating the existing grounded tool-call sequence in
#: JSON. See ``status.single-project.positive-control-v1`` in
#: ``role-legacy_agent.json``.
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
    kind: str  # "delegate_default" | "decisions" | "fault"
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

    kind = payload.get("kind")
    if kind == DELEGATE_DEFAULT:
        return DELEGATE_DEFAULT
    if kind == "decisions":
        raw_decisions = payload.get("decisions")
        if not isinstance(raw_decisions, Sequence) or not raw_decisions:
            raise ValueError(f"case {case_id!r} 'decisions' must be a non-empty list")
        return _CaseScript(
            case_id=case_id,
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
    cases: Mapping[str, _CaseScript | str]


def _scripts_dir() -> Path:
    override = os.getenv(SCRIPTS_DIR_ENV)
    if override:
        return Path(override)
    # Walk up from this file looking for the repo-relative conventional
    # location. Works for a full source checkout (local dev, unit tests, a
    # container image that COPYs the whole tree) without requiring the
    # directory to exist at import time -- callers only reach this function
    # once a request actually carried a case tag (see module docstring).
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
    cases = {
        case_id: _parse_case_entry(case_id, entry)
        for case_id, entry in raw_cases.items()
    }
    return RoleScript(role=role, cases=cases)


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

    def resolve(self, case_id: str, *, round_index: int) -> ScriptTurn | str:
        """Return a ``ScriptTurn`` to serve, or the ``DELEGATE_DEFAULT``
        sentinel string. Raises ``UnmappedCaseError`` for every case the
        script cannot resolve -- never returns ``None``/falls through
        silently (CHAOS-3219 Phase 1 Lane 1b requirement 4)."""

        if case_id not in self.registry_ids:
            raise UnmappedCaseError(
                "unknown_case_id",
                f"case id {case_id!r} is not in the frozen corpus registry",
            )
        entry = self.role_script.cases.get(case_id)
        if entry is None:
            raise UnmappedCaseError(
                "case_not_scripted",
                f"case id {case_id!r} is a valid registry id but has no "
                f"script entry for role {self.role!r}",
            )
        if entry == DELEGATE_DEFAULT:
            return DELEGATE_DEFAULT
        assert isinstance(entry, _CaseScript)  # noqa: S101
        return entry.resolve(round_index=round_index)


def current_role() -> str:
    return os.getenv(ROLE_ENV, DEFAULT_ROLE) or DEFAULT_ROLE


def sleep_for_fault(delay_ms: int) -> None:
    if delay_ms > 0:
        time.sleep(delay_ms / 1000)


__all__ = [
    "DEFAULT_ROLE",
    "DELEGATE_DEFAULT",
    "FAULT_TYPES",
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
    "extract_case_id",
    "load_registry_ids",
    "load_role_script",
    "sleep_for_fault",
    "strip_case_tag",
]
