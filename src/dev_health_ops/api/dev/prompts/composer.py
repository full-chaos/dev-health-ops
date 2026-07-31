"""Compose fixed policy sections and bounded untrusted model context."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from ..contracts import DevScopeResolution, DevToolResult
from ..tool_registry import AskDevToolRegistry

PROMPT_VERSION = "ask_dev_prompt.v1"
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
    (
        "named_entity_resolution",
        "When the question names a specific project, repository, issue, pull request, or work "
        "unit, call resolve_scope.v1 with that name before requesting any status, change, "
        "metric, or evidence tool. Never present an answer that describes a named entity "
        "resolve_scope.v1 did not confirm exists; if it returns not-found or ambiguous, say so "
        "instead of answering about the organization under the entity's name.",
    ),
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

        system_payload = {
            "prompt_version": PROMPT_VERSION,
            "policy_sections": [
                {"id": section_id, "text": text}
                for section_id, text in _FIXED_POLICY_SECTIONS
            ],
            "tool_registry": self._registry.manifest(),
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
            version=PROMPT_VERSION,
            checksum=checksum,
            system_text=system_text,
            user_text=user_text,
            serialized_tool_bytes=len(serialized_tools),
        )


__all__ = [
    "PROMPT_VERSION",
    "ComposedPrompt",
    "PromptComposer",
    "PromptConversationTurn",
]
