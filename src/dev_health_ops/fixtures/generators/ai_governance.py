"""AI governance fixture generators (CHAOS-2209).

Seeds the org-level ``ai_tool_allowlist`` policy table so the governance
views can render a real allowlist verdict instead of a permanent "unknown".
The entries are deterministic and deliberately include every status so the
UI exercises allowed, deprecated, and disallowed renderings.

The ``tool_name``/``model_name`` values intentionally line up with the
attribution evidence written by
:meth:`PrGeneratorMixin.generate_ai_attributions` (``tool_name="claude-code"``,
``model_name="claude"``) so the governance loader's allowlist join produces
non-"unknown" statuses for fixture data.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.audit.ai_governance.models import (
    AIToolAllowlistEntry,
    ToolAllowlistStatus,
)

_ALLOWLIST_SEED: tuple[tuple[str, str | None, ToolAllowlistStatus, str], ...] = (
    (
        "claude-code",
        None,
        ToolAllowlistStatus.ALLOWED,
        "Approved engineering assistant (org policy AI-001).",
    ),
    (
        "claude-code",
        "claude",
        ToolAllowlistStatus.ALLOWED,
        "Approved engineering assistant (org policy AI-001).",
    ),
    (
        "cursor",
        "claude-3.5-sonnet",
        ToolAllowlistStatus.ALLOWED,
        "Approved IDE assistant (org policy AI-001).",
    ),
    (
        "claude-code-agent",
        "claude-sonnet-4",
        ToolAllowlistStatus.ALLOWED,
        "Approved autonomous agent with mandatory human review.",
    ),
    (
        "copilot-legacy",
        None,
        ToolAllowlistStatus.DEPRECATED,
        "Superseded; migrate to an approved assistant by Q3.",
    ),
    (
        "shadow-llm",
        None,
        ToolAllowlistStatus.DISALLOWED,
        "Unvetted external service; blocked by security review.",
    ),
)


def generate_ai_tool_allowlist_entries(
    org_id: str,
    *,
    now: datetime | None = None,
) -> list[AIToolAllowlistEntry]:
    """Build the deterministic org-level allowlist seed rows."""
    stamp = now or datetime.now(timezone.utc)
    entries = [
        AIToolAllowlistEntry(
            org_id=str(org_id),
            tool_name=tool_name,
            model_name=model_name,
            status=status,
            reason=reason,
            updated_at=stamp,
            computed_at=stamp,
        )
        for tool_name, model_name, status, reason in _ALLOWLIST_SEED
    ]
    # '' and NULL share the same ReplacingMergeTree key (ORDER BY
    # ifNull(model_name, '')) — a blank "exact" seed row would replace the
    # wildcard policy on merge. The dataclass normalises, this asserts.
    assert all(entry.model_name != "" for entry in entries)
    return entries
