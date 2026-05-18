"""
AI attribution detection functions for provider normalization.

Detects AI-assisted and agent-created work from provider signals.
Each function is a pure parser — no I/O, no side effects.

Signal precedence (resolved at READ time, persisted raw at WRITE time)::

    MANUAL > PR_LABEL > BOT_AUTHOR > COMMIT_TRAILER > CI_ANNOTATION > BRANCH_NAME > PR_BODY
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import NamedTuple

# Canonical model from CHAOS-1579 (storage-worker)
from dev_health_ops.models.ai_attribution import (
    AIAttributionKind,
    AIAttributionSignal,
    AIAttributionSource,
)

__all__ = [
    # enums
    "AIAttributionSource",
    "AIAttributionKind",
    # signal dataclass
    "AIAttributionSignal",
    # author helper
    "AuthorInfo",
    # registries (read-only views)
    "AI_LABELS",
    "KNOWN_AI_BOTS",
    "CI_BOTS",
    "AI_TRAILER_KEYS",
    # detection functions
    "detect_from_pr_labels",
    "detect_from_author",
    "detect_from_commit_trailers",
    "detect_from_branch_name",
    "detect_from_pr_body",
    "detect_from_ci_annotations",
]

# --------------------------------------------------------------------------
# Default registries
# Extensible in code only — NOT user-configurable per AGENTS.md.
# --------------------------------------------------------------------------

# Known AI attribution PR label names (lowercase for comparison).
AI_LABELS: frozenset[str] = frozenset(
    {
        "ai-assisted",
        "agent-created",
        "ai-review",
        "copilot",
        "claude-code",
        "codex",
        "cursor",
        "windsurf",
    }
)

# Mapping: lowercase label name → attribution kind.
_LABEL_KIND_MAP: dict[str, AIAttributionKind] = {
    "ai-assisted": AIAttributionKind.AI_ASSISTED,
    "agent-created": AIAttributionKind.AGENT_CREATED,
    "ai-review": AIAttributionKind.AI_REVIEW,
    "copilot": AIAttributionKind.AI_ASSISTED,
    "claude-code": AIAttributionKind.AI_ASSISTED,
    "codex": AIAttributionKind.AI_ASSISTED,
    "cursor": AIAttributionKind.AI_ASSISTED,
    "windsurf": AIAttributionKind.AI_ASSISTED,
}

# Known AI bot GitHub logins (exact match, lowercase).
KNOWN_AI_BOTS: frozenset[str] = frozenset(
    {
        "copilot[bot]",
        "claude-code[bot]",
        "cursor-agent[bot]",
        "chatgpt-codex[bot]",
        "sweep-ai[bot]",
        "coderabbit[bot]",
        "devin[bot]",
    }
)

# CI/automation bots that are NOT AI — excluded from attribution.
# Must include github-actions[bot], dependabot[bot], renovate[bot] per plan.
CI_BOTS: frozenset[str] = frozenset(
    {
        "github-actions[bot]",
        "dependabot[bot]",
        "renovate[bot]",
    }
)

# Commit trailer keys that explicitly signal AI attribution (lowercase).
AI_TRAILER_KEYS: frozenset[str] = frozenset(
    {
        "ai-assisted-by",
        "generated-by",
        "x-ai-generated",
    }
)

# Co-authored-by: email/name patterns that indicate AI authorship.
_AI_COAUTHOR_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"copilot@github\.com", re.IGNORECASE),
    re.compile(r"noreply\+copilot@github\.com", re.IGNORECASE),
    re.compile(r"claude.*@anthropic\.com", re.IGNORECASE),
    re.compile(r"<[\w.+\-]*copilot[\w.+\-]*@", re.IGNORECASE),
    re.compile(r"<[\w.+\-]*claude[\w.+\-]*@", re.IGNORECASE),
    re.compile(r"cursor-agent", re.IGNORECASE),
    re.compile(r"chatgpt-codex", re.IGNORECASE),
    re.compile(r"sweep-ai", re.IGNORECASE),
    re.compile(r"devin@", re.IGNORECASE),
]

# Branch-name patterns → (pattern, kind, actor_hint).
_AI_BRANCH_PATTERNS: list[tuple[re.Pattern[str], AIAttributionKind, str]] = [
    (
        re.compile(r"(?:^|[-/])copilot(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "copilot",
    ),
    (
        re.compile(r"(?:^|[-/])claude(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "claude",
    ),
    (
        re.compile(r"(?:^|[-/])cursor(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "cursor",
    ),
    (
        re.compile(r"(?:^|[-/])codex(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "codex",
    ),
    (
        re.compile(r"(?:^|[-/])windsurf(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "windsurf",
    ),
    (
        re.compile(r"(?:^|[-/])devin(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AGENT_CREATED,
        "devin",
    ),
    (
        re.compile(r"(?:^|[-/])agent(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AGENT_CREATED,
        "agent",
    ),
    (
        re.compile(r"(?:^|[-/])ai(?:[-/]|$)", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "ai",
    ),
]

# PR body keyword patterns → (pattern, kind, actor_hint | None).
_PR_BODY_PATTERNS: list[tuple[re.Pattern[str], AIAttributionKind, str | None]] = [
    # Explicit attribution phrases (stronger)
    (
        re.compile(
            r"\b(?:generated|created|authored|written)\s+(?:by|with|using)\s+"
            r"(?:copilot|claude|codex|cursor|windsurf|ai|an\s+ai)\b",
            re.IGNORECASE,
        ),
        AIAttributionKind.AI_ASSISTED,
        None,
    ),
    (
        re.compile(r"\bai[\s\-]assisted\b", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        None,
    ),
    (
        re.compile(r"\bagent[\s\-]created\b", re.IGNORECASE),
        AIAttributionKind.AGENT_CREATED,
        None,
    ),
    # Tool name mentions (weaker — may be incidental discussion)
    (
        re.compile(r"\bcopilot\b", re.IGNORECASE),
        AIAttributionKind.AI_ASSISTED,
        "copilot",
    ),
    (re.compile(r"\bclaude\b", re.IGNORECASE), AIAttributionKind.AI_ASSISTED, "claude"),
    (re.compile(r"\bcodex\b", re.IGNORECASE), AIAttributionKind.AI_ASSISTED, "codex"),
    (re.compile(r"\bcursor\b", re.IGNORECASE), AIAttributionKind.AI_ASSISTED, "cursor"),
]

# CI annotation payload keys that signal AI attribution (lowercase).
_CI_ANNOTATION_KEYS: frozenset[str] = frozenset(
    {
        "ai-generated",
        "ai-assisted",
        "copilot-generated",
        "agent-created",
        "llm-generated",
    }
)


# --------------------------------------------------------------------------
# AuthorInfo — typed input for detect_from_author
# --------------------------------------------------------------------------


class AuthorInfo(NamedTuple):
    """Normalized author metadata passed to ``detect_from_author``.

    Attributes:
        login: GitHub login string (e.g. ``"copilot[bot]"``).
        user_type: GitHub ``type`` field — ``"Bot"``, ``"User"``, or
            ``"Organization"``.  ``None`` when not available.
        app_slug: GitHub App slug when the author is an App installation.
            ``None`` for regular users and bots.
    """

    login: str
    user_type: str | None = None
    app_slug: str | None = None


# --------------------------------------------------------------------------
# Detection functions — pure, no I/O, no side effects
# --------------------------------------------------------------------------


def detect_from_pr_labels(labels: list[str]) -> list[AIAttributionSignal]:
    """Detect AI attribution from PR label names.

    Returns one :class:`AIAttributionSignal` per matching label.
    Source precedence: ``PR_LABEL`` (highest non-manual confidence).

    Args:
        labels: Raw label name strings from the provider.

    Returns:
        List of signals, one per AI-related label found.
        Empty list if no AI labels present.
    """
    signals: list[AIAttributionSignal] = []
    for label in labels:
        normalized = label.strip().lower()
        if normalized in AI_LABELS:
            kind = _LABEL_KIND_MAP.get(normalized, AIAttributionKind.AI_ASSISTED)
            signals.append(
                AIAttributionSignal(
                    source=AIAttributionSource.PR_LABEL,
                    kind=kind,
                    confidence=0.95,
                    actor=None,
                    evidence={"label": label},
                )
            )
    return signals


def detect_from_author(author: AuthorInfo) -> AIAttributionSignal | None:
    """Detect AI attribution from PR/commit author metadata.

    Explicitly excludes CI automation bots:
    ``github-actions[bot]``, ``dependabot[bot]``, ``renovate[bot]``.
    These are automation, not AI.

    Args:
        author: Typed author metadata from the provider.

    Returns:
        An :class:`AIAttributionSignal` if the author is an AI bot,
        ``None`` otherwise (including CI bots).
    """
    login_lower = author.login.strip().lower()

    # CI/automation bots — excluded, NOT AI.
    if login_lower in CI_BOTS:
        return None

    # Known AI bot — high confidence.
    if login_lower in KNOWN_AI_BOTS:
        return AIAttributionSignal(
            source=AIAttributionSource.BOT_AUTHOR,
            kind=AIAttributionKind.AGENT_CREATED,
            confidence=0.90,
            actor=author.login,
            evidence={
                "login": author.login,
                "user_type": author.user_type,
                "app_slug": author.app_slug,
                "known_ai_bot": True,
            },
        )

    # Unknown bot (user_type == "Bot" and login ends with [bot])
    # but NOT in the CI exclusion list — weaker signal.
    if (
        author.user_type
        and author.user_type.lower() == "bot"
        and login_lower.endswith("[bot]")
    ):
        return AIAttributionSignal(
            source=AIAttributionSource.BOT_AUTHOR,
            kind=AIAttributionKind.AGENT_CREATED,
            confidence=0.55,
            actor=author.login,
            evidence={
                "login": author.login,
                "user_type": author.user_type,
                "app_slug": author.app_slug,
                "known_ai_bot": False,
            },
        )

    return None


def detect_from_commit_trailers(message: str) -> list[AIAttributionSignal]:
    """Detect AI attribution from commit message trailer lines.

    Parses:
    - ``AI-Assisted-By: <value>``
    - ``Generated-By: <value>``
    - ``X-AI-Generated: <value>``
    - ``Co-authored-by: <name> <email>`` where email matches a known
      AI bot domain pattern.

    Args:
        message: Full commit message string (may be multi-line).

    Returns:
        List of signals found.  Empty list if no AI trailers detected.
    """
    signals: list[AIAttributionSignal] = []

    for raw_line in message.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        colon_idx = line.find(":")
        if colon_idx <= 0:
            continue

        key = line[:colon_idx].strip().lower()
        value = line[colon_idx + 1 :].strip()

        # AI-specific trailer keys (AI-Assisted-By, Generated-By, etc.)
        if key in AI_TRAILER_KEYS:
            signals.append(
                AIAttributionSignal(
                    source=AIAttributionSource.COMMIT_TRAILER,
                    kind=AIAttributionKind.AI_ASSISTED,
                    confidence=0.85,
                    actor=value or None,
                    evidence={
                        "trailer_key": line[:colon_idx].strip(),
                        "trailer_value": value,
                    },
                )
            )
            continue

        # Co-authored-by: — check for AI bot email patterns.
        if key == "co-authored-by":
            for pattern in _AI_COAUTHOR_PATTERNS:
                if pattern.search(value):
                    signals.append(
                        AIAttributionSignal(
                            source=AIAttributionSource.COMMIT_TRAILER,
                            kind=AIAttributionKind.AI_ASSISTED,
                            confidence=0.80,
                            actor=value or None,
                            evidence={
                                "trailer_key": "Co-authored-by",
                                "trailer_value": value,
                            },
                        )
                    )
                    break  # one signal per Co-authored-by line

    return signals


def detect_from_branch_name(branch: str) -> AIAttributionSignal | None:
    """Detect AI attribution from branch name (weak signal).

    Branch naming conventions like ``copilot/fix-bug``, ``claude/feat``,
    ``codex/refactor``.  Confidence is low (0.35).

    Args:
        branch: The branch name string (e.g. ``"copilot/add-tests"``).

    Returns:
        A single :class:`AIAttributionSignal` on match, ``None``
        otherwise.
    """
    for pattern, kind, actor in _AI_BRANCH_PATTERNS:
        if pattern.search(branch):
            return AIAttributionSignal(
                source=AIAttributionSource.BRANCH_NAME,
                kind=kind,
                confidence=0.35,
                actor=actor,
                evidence={"branch": branch, "matched_pattern": pattern.pattern},
            )
    return None


def detect_from_pr_body(body: str) -> AIAttributionSignal | None:
    """Detect AI attribution from PR description body (weak signal).

    Scans for AI tool mentions and explicit attribution phrases.
    Returns the first (topmost-priority) match.  Confidence is low (0.25).

    Args:
        body: Full PR description body string.  Empty string / ``None``
            is safe to pass.

    Returns:
        A single :class:`AIAttributionSignal` on match, ``None``
        otherwise.
    """
    if not body:
        return None

    for pattern, kind, actor in _PR_BODY_PATTERNS:
        m = pattern.search(body)
        if m:
            return AIAttributionSignal(
                source=AIAttributionSource.PR_BODY,
                kind=kind,
                confidence=0.25,
                actor=actor,
                evidence={
                    "matched_text": m.group(0),
                    "matched_pattern": pattern.pattern,
                },
            )
    return None


def detect_from_ci_annotations(
    annotations: Sequence[Mapping[str, object]],
) -> list[AIAttributionSignal]:
    """Detect AI attribution from CI workflow annotations.

    Inspects annotation ``key``, ``name``, or ``title`` fields for
    known AI attribution markers.

    Args:
        annotations: List of annotation dicts from a CI provider.
            Expected fields: ``key``, ``name``, ``title`` (any subset).

    Returns:
        List of signals found.  Empty list if no AI annotations.
    """
    signals: list[AIAttributionSignal] = []

    for annotation in annotations:
        matched = False
        for field_name in ("key", "name", "title"):
            raw = str(annotation.get(field_name, "") or "").strip().lower()
            if raw in _CI_ANNOTATION_KEYS:
                signals.append(
                    AIAttributionSignal(
                        source=AIAttributionSource.CI_ANNOTATION,
                        kind=AIAttributionKind.AI_ASSISTED,
                        confidence=0.65,
                        actor=None,
                        evidence={"annotation": annotation},
                    )
                )
                matched = True
                break  # one signal per annotation object
        # Also check message/body text for annotation content
        if not matched:
            for field_name in ("message", "body", "text"):
                raw = str(annotation.get(field_name, "") or "").strip().lower()
                for key in _CI_ANNOTATION_KEYS:
                    if key in raw:
                        signals.append(
                            AIAttributionSignal(
                                source=AIAttributionSource.CI_ANNOTATION,
                                kind=AIAttributionKind.AI_ASSISTED,
                                confidence=0.55,
                                actor=None,
                                evidence={"annotation": annotation},
                            )
                        )
                        matched = True
                        break
                if matched:
                    break

    return signals
