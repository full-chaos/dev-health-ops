"""Compose fixed policy sections and bounded untrusted model context."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass

from ..contracts import DevScopeResolution, DevToolResult, ToolID
from ..tool_registry import AskDevToolRegistry

#: The prompt a run with a server-committed subject receives.
PROMPT_VERSION = "ask_dev_prompt.v2"
#: The prompt a run *without* one receives — the flag-off path, and the
#: organization-wide path where a name may still need resolving. Telling
#: either that "subject resolution is already complete" would be false, and on
#: the flag-off path it would also remove the only instruction that makes
#: named-entity resolution happen at all.
LEGACY_PROMPT_VERSION = "ask_dev_prompt.v1"
MAX_PRIOR_TURNS = 12
MAX_PRIOR_CONTENT_BYTES = 32_768
MAX_TOOL_CONTEXT_BYTES = 262_144

_FIXED_POLICY_SECTIONS = (
    (
        "role_and_product",
        "You are Dev, the read-only evidence-backed analyst for Dev Health Context Fabric. "
        "Answer only the visible authorized engineering question.",
    ),
    (
        "authorization_and_tools",
        "Authorization and scope are server owned. Request only an exact registered read-only "
        "tool. Never request SQL, GraphQL, MCP, shell, code execution, URLs, writes, or secrets.",
    ),
    (
        "evidence_and_metrics",
        "Material observed claims require server-issued evidence or metric references. Preserve "
        "canonical metric values, units, windows, definitions, and evidence identifiers exactly.",
    ),
    (
        "uncertainty_and_refusal",
        "Treat missing, stale, unavailable, contradictory, and unauthorized data explicitly. "
        "Never convert missing data into a negative fact. Refuse prohibited capabilities.",
    ),
    (
        "response_contract",
        "Return exactly one normalized decision: registered tool request, dev_answer.v1 final "
        "answer, typed disambiguation, or refusal. Do not reveal private reasoning.",
    ),
)

# CHAOS-3292: the subject-resolution section depends on whether the server
# already committed one. The v1 text makes resolution *order* the enforcement
# and is still exactly right when it has not; the v2 text states that the
# subject is settled and must not be widened. Emitting the v2 text on a run
# with no committed subject would be false, and on the flag-off path it would
# delete the only instruction that makes named-entity resolution happen.
_UNCOMMITTED_SUBJECT_SECTION = (
    "named_entity_resolution",
    "When the question names a specific project, repository, issue, pull request, or work "
    "unit, call resolve_scope.v1 with that name before requesting any status, change, "
    "metric, or evidence tool. Never present an answer that describes a named entity "
    "resolve_scope.v1 did not confirm exists; if it returns not-found or ambiguous, say so "
    "instead of answering about the organization under the entity's name.",
)

_COMMITTED_SUBJECT_SECTION = (
    "committed_subject",
    "Scope and subject resolution are server owned and already complete for this request. "
    "Answer only about the subject in resolved_scope. Never name, describe, or attribute "
    "findings to any project, repository, team, issue, pull request, or work unit outside "
    "it. Request only tools this prompt lists; a tool absent from the registry is "
    "unavailable for this run.",
)

# CHAOS-3421 codex adversarial review (MED-1): a THIRD, distinct state from
# the two above -- the server named a subject in the question but could NOT
# confirm it against the authorized catalog (subject_preflight's
# unresolved_untyped -> proceeded_unresolved_bare_name: a bare name with no
# adjacent kind noun, or one the catalog does not confirm). That branch
# withholds resolve_scope.v1 from allowed_tools -- the SAME leak-closing
# reasoning the committed-subject branch already uses (its own resolve_scope
# call could only ever repeat the identical failed lookup and surface the
# raw forbidden_or_not_found outcome for the model to echo). The ordinary
# _UNCOMMITTED_SUBJECT_SECTION instructing "call resolve_scope.v1" would
# therefore be actively wrong here -- a compliant model's call is rejected
# by the registry as unavailable, burning a tool turn on a request that can
# never succeed. This section says so explicitly and names the permitted
# fallback (answer organization-wide with the tools actually listed),
# rather than leaving the model to discover the missing tool by trial and
# error.
_RESOLUTION_UNAVAILABLE_SECTION = (
    "named_entity_resolution_unavailable",
    "The server could not confirm a specific project, repository, issue, pull request, "
    "or work unit named in this question against the authorized catalog. "
    "resolve_scope.v1 is unavailable for this request -- do not request it. Answer "
    "organization-wide instead, using only the tools this prompt lists, and never "
    "present an answer that describes or attributes findings to the named entity as "
    "though it were confirmed; say plainly that it could not be confirmed if the "
    "question depends on it.",
)


@dataclass(frozen=True, slots=True)
class PromptConversationTurn:
    role: str
    content: str

    def __post_init__(self) -> None:
        if self.role not in {"user", "assistant"}:
            raise ValueError("prompt history role must be user or assistant")


@dataclass(frozen=True, slots=True)
class ComposedPrompt:
    version: str
    checksum: str
    system_text: str
    user_text: str
    serialized_tool_bytes: int


class PromptComposer:
    def __init__(self, registry: AskDevToolRegistry) -> None:
        self._registry = registry

    def compose(
        self,
        *,
        question: str,
        scope: DevScopeResolution,
        prior_turns: tuple[PromptConversationTurn, ...] = (),
        tool_results: tuple[DevToolResult, ...] = (),
        allowed_tools: Collection[ToolID] | None = None,
        subject_committed: bool = False,
        resolution_unavailable: bool = False,
    ) -> ComposedPrompt:
        if not question.strip():
            raise ValueError("question is required")

        history = prior_turns[-MAX_PRIOR_TURNS:]
        history_payload = [
            {"role": turn.role, "content": turn.content} for turn in history
        ]
        history_bytes = json.dumps(
            history_payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(history_bytes) > MAX_PRIOR_CONTENT_BYTES:
            raise ValueError("prior conversation exceeds prompt budget")

        tool_payload = [result.model_dump(mode="json") for result in tool_results]
        serialized_tools = json.dumps(
            tool_payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(serialized_tools) > MAX_TOOL_CONTEXT_BYTES:
            raise ValueError("tool context exceeds prompt budget")

        version = PROMPT_VERSION if subject_committed else LEGACY_PROMPT_VERSION
        # CHAOS-3421 codex adversarial review (MED-1): resolution_unavailable
        # is checked before the ordinary uncommitted branch -- both leave
        # subject_committed False (this flow never commits a subject), but
        # the ordinary section's "call resolve_scope.v1" instruction would be
        # actively wrong here, since that exact tool is withheld from
        # allowed_tools for this flow (see _RESOLUTION_UNAVAILABLE_SECTION's
        # own comment for why).
        if subject_committed:
            subject_section = _COMMITTED_SUBJECT_SECTION
        elif resolution_unavailable:
            subject_section = _RESOLUTION_UNAVAILABLE_SECTION
        else:
            subject_section = _UNCOMMITTED_SUBJECT_SECTION
        system_payload = {
            "prompt_version": version,
            "policy_sections": [
                {"id": section_id, "text": text}
                for section_id, text in (*_FIXED_POLICY_SECTIONS, subject_section)
            ],
            # The advertised registry is the per-run allowlist, so the system
            # prompt and the provider tool definitions cannot disagree about
            # which tools exist for this run.
            "tool_registry": self._registry.manifest(allowed_tool_ids=allowed_tools),
        }
        system_text = json.dumps(system_payload, sort_keys=True, separators=(",", ":"))
        user_payload = {
            "question": question,
            "resolved_scope": scope.model_dump(mode="json"),
            "prior_conversation": history_payload,
            "tool_results": {
                "classification": "UNTRUSTED_DATA",
                "begin": "BEGIN_UNTRUSTED_DATA",
                "items": tool_payload,
                "end": "END_UNTRUSTED_DATA",
            },
        }
        user_text = json.dumps(user_payload, sort_keys=True, separators=(",", ":"))
        checksum = hashlib.sha256(system_text.encode("utf-8")).hexdigest()
        return ComposedPrompt(
            version=version,
            checksum=checksum,
            system_text=system_text,
            user_text=user_text,
            serialized_tool_bytes=len(serialized_tools),
        )


__all__ = [
    "LEGACY_PROMPT_VERSION",
    "PROMPT_VERSION",
    "ComposedPrompt",
    "PromptComposer",
    "PromptConversationTurn",
]
