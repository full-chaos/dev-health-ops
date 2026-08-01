from __future__ import annotations

import pytest

from dev_health_ops.llm.agent.budget_policy import (
    BUDGET_POLICY_VERSION,
    ModelFamilyBudget,
    model_family_budget,
)


def test_budget_policy_version_is_pinned() -> None:
    assert BUDGET_POLICY_VERSION == "ask-dev-budget-v1"


@pytest.mark.parametrize(
    ("model", "family", "reasoning_counted_in_output", "reasoning_effort"),
    [
        ("gpt-5-nano", "gpt-5", True, "minimal"),
        ("gpt-5-mini", "gpt-5", True, "minimal"),
        ("GPT-5-Nano", "gpt-5", True, "minimal"),
        ("o1-mini", "o-series", True, None),
        ("o3-mini", "o-series", True, None),
        ("o4-mini", "o-series", True, None),
        ("gpt-4o-mini", "default", False, None),
        ("agent-model", "default", False, None),
    ],
)
def test_model_family_budget_resolves_family_and_reasoning_accounting(
    model: str,
    family: str,
    reasoning_counted_in_output: bool,
    reasoning_effort: str | None,
) -> None:
    """Family resolution mirrors the live-verified predicates in
    openai_capabilities (chat_completion_reasoning_effort fires only for
    gpt-5*; o1/o3/o4 are the o-series reasoning family) rather than
    reimplementing them, so the two modules cannot silently drift apart.
    """
    budget = model_family_budget(model)

    assert budget.family == family
    assert budget.reasoning_counted_in_output is reasoning_counted_in_output
    assert budget.reasoning_effort == reasoning_effort


@pytest.mark.parametrize("model", ["gpt-5-nano", "o3-mini", "gpt-4o-mini"])
def test_reasoning_headroom_is_pinned_to_zero_for_every_family(model: str) -> None:
    """CHAOS-3285 commit 2: the mechanism is wired, but no family gets a
    nonzero headroom yet -- the live per-role probe (plan §6.6) decides the
    actual numbers in a later commit. A nonzero headroom here would silently
    change the wire request before that measurement exists.
    """
    assert model_family_budget(model).reasoning_headroom_tokens == 0


@pytest.mark.parametrize(
    ("model", "visible_cap"),
    [
        ("gpt-5-nano", 4_096),
        ("o3-mini", 512),
        ("gpt-4o-mini", 4_096),
        ("local-model", 1),
    ],
)
def test_request_max_completion_tokens_is_unchanged_at_zero_headroom(
    model: str, visible_cap: int
) -> None:
    """Behavior-unchanged proof: with reasoning_headroom_tokens == 0 (the
    only value any family has in this commit), the wire
    max_completion_tokens value must be byte-identical to the pre-commit-2
    behavior of sending max_output_tokens verbatim, for every family --
    reasoning-counted or not.
    """
    budget = model_family_budget(model)

    assert budget.request_max_completion_tokens(visible_cap) == visible_cap


def test_request_max_completion_tokens_adds_headroom_only_for_reasoning_counted_family() -> (
    None
):
    """Proves the formula itself (independent of today's headroom=0 wiring):
    a reasoning-counted family adds its headroom on top of the visible cap;
    a non-reasoning family never does, even if given a nonzero headroom by
    mistake.
    """
    reasoning_family = ModelFamilyBudget(
        family="gpt-5",
        reasoning_counted_in_output=True,
        reasoning_headroom_tokens=8_192,
        reasoning_effort="minimal",
    )
    non_reasoning_family = ModelFamilyBudget(
        family="default",
        reasoning_counted_in_output=False,
        reasoning_headroom_tokens=8_192,
        reasoning_effort=None,
    )

    assert reasoning_family.request_max_completion_tokens(4_096) == 4_096 + 8_192
    assert non_reasoning_family.request_max_completion_tokens(4_096) == 4_096
