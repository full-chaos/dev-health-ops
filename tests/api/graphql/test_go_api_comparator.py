"""Tests for the Go/Python proof-gate comparator (CHAOS-4366 deliverable 5).

Structured as CHAOS-4381 §6 requires: each planted-defect control asserts
the SPECIFIC terminal state the comparator emits, not a bare pass/fail --
"an unsupported-labeled watermark drift and a real mismatch must never be
collapsible into the same signal an operator reads."
"""

from __future__ import annotations

import math

from dev_health_ops.api.graphql.go_api_comparator import (
    TERMINAL_STATE_MATCH,
    TERMINAL_STATE_MISMATCH,
    TERMINAL_STATE_UNSUPPORTED,
    ResponseSnapshot,
    compare_responses,
)


def _snap(
    data: object, errors: tuple[dict, ...] = (), watermark: str | None = None
) -> ResponseSnapshot:
    return ResponseSnapshot(data=data, errors=errors, watermark=watermark)


# --- Baseline: identical responses match --------------------------------


def test_identical_responses_match() -> None:
    data = {"hotspots": {"edges": [{"id": "a", "count": 3}, {"id": "b", "count": 1}]}}
    result = compare_responses(_snap(data), _snap(dict(data)))
    assert result.terminal_state == TERMINAL_STATE_MATCH
    assert result.findings == ()


# --- §6.1 Removed row ----------------------------------------------------


def test_planted_defect_removed_row_is_mismatch() -> None:
    baseline = {"edges": [{"id": "a"}, {"id": "b"}, {"id": "c"}]}
    candidate = {"edges": [{"id": "a"}, {"id": "b"}]}
    result = compare_responses(_snap(baseline), _snap(candidate))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH
    assert any("length" in f.detail for f in result.findings)


# --- §6.2 Changed nullability ---------------------------------------------


def test_planted_defect_null_where_baseline_has_value_is_mismatch() -> None:
    baseline = {"score": 4.2}
    candidate = {"score": None}
    result = compare_responses(_snap(baseline), _snap(candidate))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_planted_defect_omitted_key_where_baseline_has_null_is_mismatch() -> None:
    """Parity rule 2: present-and-null vs. absent are NOT interchangeable."""
    baseline = {"score": None}
    candidate: dict[str, object] = {}
    result = compare_responses(_snap(baseline), _snap(candidate))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH
    assert any("absent" in f.detail for f in result.findings)


def test_allowlisted_envelope_key_omission_does_not_mismatch() -> None:
    baseline = {"data": {"x": 1}, "extensions": {}}
    candidate = {"data": {"x": 1}}
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        allowlisted_envelope_keys=frozenset({"extensions"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH


# --- §6.3 Changed error path -----------------------------------------------


def test_planted_defect_changed_error_path_is_mismatch() -> None:
    baseline_errors = (
        {"message": "boom", "path": ["a", "b"], "extensions": {"code": "X"}},
    )
    candidate_errors = (
        {"message": "boom", "path": ["a", "c"], "extensions": {"code": "X"}},
    )
    result = compare_responses(
        _snap(None, baseline_errors), _snap(None, candidate_errors)
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_errors_compared_as_set_ignoring_array_order() -> None:
    e1 = {"message": "m1", "path": ["a"], "extensions": {"code": "X"}}
    e2 = {"message": "m2", "path": ["b"], "extensions": {"code": "Y"}}
    result = compare_responses(_snap(None, (e1, e2)), _snap(None, (e2, e1)))
    assert result.terminal_state == TERMINAL_STATE_MATCH


def test_message_only_error_drift_is_not_a_mismatch() -> None:
    baseline_errors = (
        {"message": "original text", "path": ["a"], "extensions": {"code": "X"}},
    )
    candidate_errors = (
        {"message": "reworded text", "path": ["a"], "extensions": {"code": "X"}},
    )
    result = compare_responses(
        _snap(None, baseline_errors), _snap(None, candidate_errors)
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH
    assert any(f.kind == "error_message_drift" for f in result.findings)
    assert not any(f.kind == "mismatch" for f in result.findings)


def test_missing_error_is_mismatch() -> None:
    baseline_errors = ({"message": "m", "path": ["a"], "extensions": {"code": "X"}},)
    result = compare_responses(_snap(None, baseline_errors), _snap(None, ()))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_extra_error_is_mismatch() -> None:
    candidate_errors = ({"message": "m", "path": ["a"], "extensions": {"code": "X"}},)
    result = compare_responses(_snap(None, ()), _snap(None, candidate_errors))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


# --- §6.4 Reordered results (default strict) --------------------------


def test_planted_defect_reordered_results_is_mismatch_by_default() -> None:
    baseline = {"edges": [{"id": "a"}, {"id": "b"}]}
    candidate = {"edges": [{"id": "b"}, {"id": "a"}]}
    result = compare_responses(_snap(baseline), _snap(candidate))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_relaxed_in_tie_block_reorder_still_matches() -> None:
    # Two rows tied on `rank`, one distinct: within the tie the order may
    # differ; the distinct row must stay in its own block position.
    baseline = {
        "edges": [
            {"id": "a", "rank": 1},
            {"id": "b", "rank": 1},
            {"id": "c", "rank": 2},
        ]
    }
    candidate = {
        "edges": [
            {"id": "b", "rank": 1},
            {"id": "a", "rank": 1},
            {"id": "c", "rank": 2},
        ]
    }
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        tie_ordering="relaxed",
        relaxed_list_path="$.data.edges",
        tie_sort_key=lambda item: item["rank"],
        tie_block_id_field="id",
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH


def test_relaxed_cross_tie_block_reorder_still_mismatches() -> None:
    # `c` (rank 2) moved ahead of the rank-1 block -- a cross-block reorder,
    # which `relaxed` must NOT absorb (only in-block reorders are relaxed).
    baseline = {
        "edges": [
            {"id": "a", "rank": 1},
            {"id": "b", "rank": 1},
            {"id": "c", "rank": 2},
        ]
    }
    candidate = {
        "edges": [
            {"id": "c", "rank": 2},
            {"id": "a", "rank": 1},
            {"id": "b", "rank": 1},
        ]
    }
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        tie_ordering="relaxed",
        relaxed_list_path="$.data.edges",
        tie_sort_key=lambda item: item["rank"],
        tie_block_id_field="id",
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_relaxed_only_applies_to_the_declared_list_path() -> None:
    """A second, undeclared list in the same response stays strict even
    when `relaxed` is configured for a different path."""
    baseline = {
        "edges": [{"id": "a", "rank": 1}, {"id": "b", "rank": 1}],
        "other": [{"id": "x"}, {"id": "y"}],
    }
    candidate = {
        "edges": [{"id": "b", "rank": 1}, {"id": "a", "rank": 1}],
        "other": [{"id": "y"}, {"id": "x"}],
    }
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        tie_ordering="relaxed",
        relaxed_list_path="$.data.edges",
        tie_sort_key=lambda item: item["rank"],
        tie_block_id_field="id",
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


# --- Floating point (parity rule 3) ---------------------------------------


def test_tier_a_float_requires_exact_match() -> None:
    result = compare_responses(_snap({"count": 3.0}), _snap({"count": 3.0000001}))
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_tier_b_float_within_tolerance_matches() -> None:
    result = compare_responses(
        _snap({"ratio": 0.123456789}),
        _snap({"ratio": 0.123456789 + 1e-10}),
        float_tier_fields=frozenset({"data.ratio"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH


def test_tier_b_float_outside_tolerance_mismatches() -> None:
    result = compare_responses(
        _snap({"ratio": 1.0}),
        _snap({"ratio": 1.1}),
        float_tier_fields=frozenset({"data.ratio"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_nan_is_always_a_mismatch_even_under_tier_b() -> None:
    result = compare_responses(
        _snap({"ratio": float("nan")}),
        _snap({"ratio": float("nan")}),
        float_tier_fields=frozenset({"data.ratio"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_infinity_is_always_a_mismatch_even_under_tier_b() -> None:
    result = compare_responses(
        _snap({"ratio": math.inf}),
        _snap({"ratio": math.inf}),
        float_tier_fields=frozenset({"data.ratio"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_declared_field_list_indices_are_normalized() -> None:
    """A Tier-B declaration is written without list indices; it must apply
    to every element of a list at that path."""
    baseline = {"edges": [{"score": 1.0}, {"score": 2.0}]}
    candidate = {"edges": [{"score": 1.0 + 1e-10}, {"score": 2.0 + 1e-10}]}
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        float_tier_fields=frozenset({"data.edges.score"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH


# --- Watermark handling (parity rule 4) ------------------------------------


def test_watermark_drift_is_unsupported_never_mismatch() -> None:
    # Data also differs -- watermark drift must still win, not mismatch.
    result = compare_responses(
        _snap({"count": 1}, watermark="2026-08-27T00:00:00Z"),
        _snap({"count": 999}, watermark="2026-08-27T00:00:05Z"),
    )
    assert result.terminal_state == TERMINAL_STATE_UNSUPPORTED
    assert not any(f.kind == "mismatch" for f in result.findings)


def test_matching_watermark_with_real_diff_is_mismatch() -> None:
    result = compare_responses(
        _snap({"count": 1}, watermark="2026-08-27T00:00:00Z"),
        _snap({"count": 2}, watermark="2026-08-27T00:00:00Z"),
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_absent_watermark_on_either_side_skips_watermark_check() -> None:
    result = compare_responses(
        _snap({"count": 1}, watermark=None), _snap({"count": 1}, watermark="anything")
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH


def test_require_watermark_missing_on_one_side_is_unsupported() -> None:
    """Parity rule 4: 'every comparable operation must expose a watermark'
    -- an operation registered as watermark-bearing must not silently MATCH
    when one side never captured one, even if the data happens to agree."""
    result = compare_responses(
        _snap({"count": 1}, watermark=None),
        _snap({"count": 1}, watermark="2026-08-27T00:00:00Z"),
        require_watermark=True,
    )
    assert result.terminal_state == TERMINAL_STATE_UNSUPPORTED
    assert not any(f.kind == "mismatch" for f in result.findings)


def test_require_watermark_missing_on_both_sides_is_unsupported() -> None:
    result = compare_responses(
        _snap({"count": 1}, watermark=None),
        _snap({"count": 1}, watermark=None),
        require_watermark=True,
    )
    assert result.terminal_state == TERMINAL_STATE_UNSUPPORTED


def test_require_watermark_present_and_matching_compares_normally() -> None:
    result = compare_responses(
        _snap({"count": 1}, watermark="2026-08-27T00:00:00Z"),
        _snap({"count": 2}, watermark="2026-08-27T00:00:00Z"),
        require_watermark=True,
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


# --- Top-level data presence (null vs. omission at the envelope root) -----


def test_data_present_on_baseline_only_is_mismatch() -> None:
    """`"data": null` (present) vs. `data` key entirely absent are NOT the
    same thing -- parity rule 2 applies at the top level too."""
    baseline = ResponseSnapshot(data=None, data_present=True)
    candidate = ResponseSnapshot(data=None, data_present=False)
    result = compare_responses(baseline, candidate)
    assert result.terminal_state == TERMINAL_STATE_MISMATCH
    assert any(f.path == "$.data" for f in result.findings)


def test_data_absent_on_both_sides_is_not_itself_a_finding() -> None:
    baseline = ResponseSnapshot(data=None, data_present=False)
    candidate = ResponseSnapshot(data=None, data_present=False)
    result = compare_responses(baseline, candidate)
    assert result.terminal_state == TERMINAL_STATE_MATCH


# --- tie_ordering mode must gate relaxed comparison, not relaxed_list_path alone


def test_relaxed_list_path_without_relaxed_mode_stays_strict() -> None:
    """Passing relaxed_list_path/tie_sort_key while tie_ordering stays the
    default 'strict' must NOT relax that list -- a reorder still mismatches."""
    baseline = {"edges": [{"id": "a", "rank": 1}, {"id": "b", "rank": 1}]}
    candidate = {"edges": [{"id": "b", "rank": 1}, {"id": "a", "rank": 1}]}
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        tie_ordering="strict",
        relaxed_list_path="$.data.edges",
        tie_sort_key=lambda item: item["rank"],
        tie_block_id_field="id",
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


# --- Envelope-key allowlist applies only at the top level of `data` -------


def test_allowlisted_key_omission_inside_nested_data_still_mismatches() -> None:
    """The allowlist exception is for TOP-LEVEL transport-envelope keys
    only. A field happening to be named "extensions" nested inside `data`
    is ordinary business data -- an omission there must still mismatch."""
    baseline: dict[str, object] = {"widget": {"extensions": {"foo": 1}}}
    candidate: dict[str, object] = {"widget": {}}
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        allowlisted_envelope_keys=frozenset({"extensions"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MISMATCH


def test_allowlisted_key_omission_at_top_level_of_data_is_excused() -> None:
    baseline = {"x": 1, "extensions": {}}
    candidate = {"x": 1}
    result = compare_responses(
        _snap(baseline),
        _snap(candidate),
        allowlisted_envelope_keys=frozenset({"extensions"}),
    )
    assert result.terminal_state == TERMINAL_STATE_MATCH
