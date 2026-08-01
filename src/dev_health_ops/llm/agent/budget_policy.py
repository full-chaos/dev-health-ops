"""Per-model-family reasoning/output token budget policy for Ask Dev's agent.

Separate from ``llm/providers/openai_capabilities.py`` on purpose:
``openai_capabilities`` is shared with the non-agent batch/completion
providers (``providers/openai.py``, ``providers/local.py``) and must not
silently change behavior for that path. The agent's *token accounting*
policy is agent-specific and, per CHAOS-3285, role-aware -- it lives here so
it can evolve (per-role visible-output budgets, reasoning headroom) without
touching the batch path's request-shape predicates.

CHAOS-3285 commit 2 wires this module into the adapter with every family's
``reasoning_headroom_tokens`` pinned to ``0``: the mechanism is in place, but
the wire value is byte-identical to pre-commit-2 behavior. The actual
headroom numbers are decided by the live per-role probe (CHAOS-3285 plan
§6.6) and the coupled run-total/cost changes it requires (risk R1), and land
in a later commit.
"""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.llm.providers.openai_capabilities import (
    chat_completion_reasoning_effort,
)

BUDGET_POLICY_VERSION = "ask-dev-budget-v1"


@dataclass(frozen=True, slots=True)
class ModelFamilyBudget:
    family: str
    reasoning_counted_in_output: bool
    reasoning_headroom_tokens: int
    reasoning_effort: str | None

    def request_max_completion_tokens(self, visible_cap: int) -> int:
        """Return the wire ``max_completion_tokens`` value for a visible-output cap.

        For families whose hidden reasoning tokens count against the same
        wire budget as visible output (``reasoning_counted_in_output``), the
        request value is the visible cap plus the family's explicit
        reasoning headroom. For every other family the request value is the
        visible cap unchanged.
        """

        if not self.reasoning_counted_in_output:
            return visible_cap
        return visible_cap + self.reasoning_headroom_tokens


def _family_for(model: str) -> str:
    normalized = model.strip().lower()
    if normalized.startswith("gpt-5"):
        return "gpt-5"
    if normalized.startswith(("o1", "o3", "o4")):
        return "o-series"
    return "default"


def model_family_budget(model: str) -> ModelFamilyBudget:
    """Resolve the token-budget policy for a model, by family.

    Family membership mirrors the reasoning-tier predicates already
    established (and live-verified) in ``openai_capabilities``:
    ``chat_completion_reasoning_effort`` only fires for ``gpt-5*``, and the
    o-series reasoning models (``o1``/``o3``/``o4``) are the family excluded
    from ``supports_temperature`` alongside ``gpt-5*``. Both families count
    hidden reasoning tokens against the same ``max_completion_tokens`` wire
    budget as visible output; every other model does not.
    """

    family = _family_for(model)
    reasoning_counted_in_output = family in ("gpt-5", "o-series")
    return ModelFamilyBudget(
        family=family,
        reasoning_counted_in_output=reasoning_counted_in_output,
        # Pinned at 0 for every family in CHAOS-3285 commit 2 -- see module
        # docstring. Wiring only; no behavior change yet.
        reasoning_headroom_tokens=0,
        reasoning_effort=chat_completion_reasoning_effort(model),
    )


__all__ = [
    "BUDGET_POLICY_VERSION",
    "ModelFamilyBudget",
    "model_family_budget",
]
