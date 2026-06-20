"""Tests for the canonical recommendations registry.

Covers:
- All 5 canonical rule ids are present
- Registry shape (types, required fields non-empty)
- Immutability of RuleDef instances
- get_rule() happy + error paths
- all_rules() returns a stable, non-empty tuple
- Duplicate-id guard raises at import of a tampered registry
"""

from __future__ import annotations

import types

import pytest

from dev_health_ops.recommendations.registry import all_rules, get_rule
from dev_health_ops.recommendations.schema import RuleDef

# ---------------------------------------------------------------------------
# Expected canonical rule ids (per plan §Canonical Rules)
# ---------------------------------------------------------------------------

CANONICAL_IDS = {
    "saturation",
    "review-concentration",
    "thrash",
    "sustainability-risk",
    "compounding-risk",
}

VALID_SEVERITIES: set[str] = {"warning", "critical"}
VALID_THEMES: set[str] = {
    "feature-delivery",
    "operational-support",
    "maintenance-tech-debt",
    "quality-reliability",
    "risk-security",
}


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------


def test_all_five_canonical_ids_present() -> None:
    ids = {rule.id for rule in all_rules()}
    assert ids == CANONICAL_IDS, (
        f"Missing or extra ids: {ids.symmetric_difference(CANONICAL_IDS)}"
    )


def test_all_rules_returns_tuple() -> None:
    result = all_rules()
    assert isinstance(result, tuple)
    assert len(result) == 5


def test_all_rules_stable_order() -> None:
    """all_rules() must return the same order on repeated calls."""
    assert all_rules() == all_rules()


# ---------------------------------------------------------------------------
# Rule shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rule", all_rules())
def test_rule_is_ruledef_instance(rule: RuleDef) -> None:
    assert isinstance(rule, RuleDef)


@pytest.mark.parametrize("rule", all_rules())
def test_rule_id_is_non_empty_string(rule: RuleDef) -> None:
    assert isinstance(rule.id, str)
    assert rule.id.strip() != ""


@pytest.mark.parametrize("rule", all_rules())
def test_rule_title_is_non_empty_string(rule: RuleDef) -> None:
    assert isinstance(rule.title, str)
    assert rule.title.strip() != ""


@pytest.mark.parametrize("rule", all_rules())
def test_rule_description_is_non_empty_string(rule: RuleDef) -> None:
    assert isinstance(rule.description, str)
    assert rule.description.strip() != ""


@pytest.mark.parametrize("rule", all_rules())
def test_rule_success_criterion_is_non_empty_string(rule: RuleDef) -> None:
    assert isinstance(rule.success_criterion, str)
    assert rule.success_criterion.strip() != ""


@pytest.mark.parametrize("rule", all_rules())
def test_rule_severity_is_valid(rule: RuleDef) -> None:
    assert rule.severity in VALID_SEVERITIES


@pytest.mark.parametrize("rule", all_rules())
def test_rule_theme_is_valid(rule: RuleDef) -> None:
    assert rule.theme in VALID_THEMES


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rule", all_rules())
def test_ruledef_is_frozen(rule: RuleDef) -> None:
    with pytest.raises((AttributeError, TypeError)):
        rule.id = "mutated"  # type: ignore[misc]


@pytest.mark.parametrize("rule", all_rules())
def test_ruledef_is_hashable(rule: RuleDef) -> None:
    """Frozen dataclasses must be hashable."""
    assert hash(rule) is not None
    assert rule in {rule}


def test_all_rules_tuple_is_immutable() -> None:
    """The returned tuple cannot be mutated."""
    rules = all_rules()
    with pytest.raises(TypeError):
        rules[0] = rules[1]  # type: ignore[index]


# ---------------------------------------------------------------------------
# get_rule() accessor
# ---------------------------------------------------------------------------


# CANONICAL_IDS is a set; sort it so parametrized test IDs collect in a stable
# order across xdist workers (xdist requires identical collection per worker;
# unordered set iteration triggers "Different tests were collected" — CHAOS-2586).
@pytest.mark.parametrize("rule_id", sorted(CANONICAL_IDS))
def test_get_rule_returns_correct_ruledef(rule_id: str) -> None:
    rule = get_rule(rule_id)
    assert isinstance(rule, RuleDef)
    assert rule.id == rule_id


def test_get_rule_unknown_id_raises_key_error() -> None:
    with pytest.raises(KeyError, match="not-a-real-rule"):
        get_rule("not-a-real-rule")


def test_get_rule_error_message_contains_known_ids() -> None:
    with pytest.raises(KeyError) as exc_info:
        get_rule("bogus")
    message = str(exc_info.value)
    for canonical_id in CANONICAL_IDS:
        assert canonical_id in message


# ---------------------------------------------------------------------------
# Duplicate-ID guard
# ---------------------------------------------------------------------------


def test_duplicate_id_guard_raises_on_import() -> None:
    """Importing a registry with duplicate ids must raise ValueError."""
    # Build a fake registry module with a duplicate rule
    fake_mod_name = "dev_health_ops.recommendations._fake_registry_dup"
    fake_mod = types.ModuleType(fake_mod_name)
    fake_mod.__spec__ = None

    fake_rules_source = """
from dev_health_ops.recommendations.schema import RuleDef

_RULES = (
    RuleDef(
        id="saturation",
        title="T1",
        description="D1",
        success_criterion="S1",
        severity="warning",
        theme="operational-support",
    ),
    RuleDef(
        id="saturation",
        title="T2",
        description="D2",
        success_criterion="S2",
        severity="warning",
        theme="operational-support",
    ),
)

_ids = [rule.id for rule in _RULES]
_seen: set = set()
for _rule_id in _ids:
    if _rule_id in _seen:
        raise ValueError(f"Duplicate rule id {{_rule_id!r}} detected in recommendations registry.")
    _seen.add(_rule_id)
"""
    with pytest.raises(ValueError, match="Duplicate rule id"):
        exec(compile(fake_rules_source, "<fake_registry>", "exec"))  # noqa: S102
