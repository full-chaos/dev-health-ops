"""F3 (CHAOS-5484 opus-r9): a recorded receipt's per-shape counts.

``_recorded_shape_counts`` used to read ANY JSON object as the writer's own
shape-count provenance: a key that was absent, or present but the wrong
type, both became ``{}`` -- the same value a genuinely computed-and-empty
count renders as. So a row nobody recorded counts for
(``{"note": "manual repair"}``) and a row whose counts are the right shape
but the wrong type (``{"covered_by_shape": {"value": "384"}}``) both
printed the affirmative claim ``covered[] outside[]``, indistinguishable
from a row where the writer genuinely computed zero of each shape.

The fix: recognise the writer's provenance by the PRESENCE of either
``covered_by_shape`` or ``outside_by_shape`` key, not by merely being a
dict, and treat a present-but-malformed counts value as invalidating the
whole object -- unrecorded, not partially trusted.

Both fields carry ``omitempty`` on the Go writer's struct
(``ReceiptProvenance``, internal/goapiproof/run.go), so a real receipt
legitimately omits either key on its own when that shape's map is empty --
that is still "nothing of that kind" (``{}``), not "unrecorded". Only a
payload with NEITHER key, or a malformed value under a key that IS
present, reads as unrecorded.
"""

from __future__ import annotations

from dev_health_ops.api.graphql.go_api_routing_admin import (
    AuthorizingReceipt,
    _recorded_shape_counts,
)


def _receipt(review_evidence: str | None) -> AuthorizingReceipt:
    covered, outside = _recorded_shape_counts(review_evidence)
    return AuthorizingReceipt(
        operation="hotspots",
        receipt_id="11111111-1111-4111-8111-111111111111",
        terminal_state="mismatch",
        baseline_defect=("CHAOS-5447",),
        measurement_route="proof",
        build_binding="per_request",
        observed_at=None,
        covered_by_shape=covered,
        outside_by_shape=outside,
    )


def test_review_evidence_absent_is_unrecorded() -> None:
    assert _recorded_shape_counts(None) == (None, None)
    assert _receipt(None).shape_counts() == "shape counts unrecorded"


def test_operator_free_text_is_unrecorded() -> None:
    text = "enabled by hand after the incident"
    assert _recorded_shape_counts(text) == (None, None)
    assert _receipt(text).shape_counts() == "shape counts unrecorded"


def test_both_keys_present_renders_the_counts() -> None:
    evidence = '{"covered_by_shape":{"value":384},"outside_by_shape":{}}'
    assert _recorded_shape_counts(evidence) == ({"value": 384}, {})
    assert _receipt(evidence).shape_counts() == "covered[value=384] outside[]"


def test_neither_key_present_is_unrecorded_not_empty() -> None:
    """The bug: an object without either key used to render as computed
    and empty (``covered[] outside[]``) instead of unrecorded."""
    assert _recorded_shape_counts("{}") == (None, None)
    assert _receipt("{}").shape_counts() == "shape counts unrecorded"


def test_an_unrelated_object_is_unrecorded_not_empty() -> None:
    evidence = '{"note":"manual repair, counts never computed"}'
    assert _recorded_shape_counts(evidence) == (None, None)
    assert _receipt(evidence).shape_counts() == "shape counts unrecorded"


def test_a_string_count_is_unrecorded_not_empty() -> None:
    """The bug: a key present with the wrong scalar type used to silently
    drop to ``{}`` instead of invalidating the object."""
    evidence = '{"covered_by_shape":{"value":"384"}}'
    assert _recorded_shape_counts(evidence) == (None, None)
    assert _receipt(evidence).shape_counts() == "shape counts unrecorded"


def test_a_bool_count_is_unrecorded_not_empty() -> None:
    """``bool`` is an ``int`` subclass in Python -- excluded explicitly,
    the same rule the Go writer's own JSON decoder applies."""
    evidence = '{"covered_by_shape":{"value":true}}'
    assert _recorded_shape_counts(evidence) == (None, None)
    assert _receipt(evidence).shape_counts() == "shape counts unrecorded"


def test_one_key_legitimately_omitted_by_the_writer_is_not_unrecorded() -> None:
    """``omitempty`` on the Go struct drops a key whose map the writer
    computed as empty -- that key reads as ``{}``, not unrecorded, as long
    as the OTHER key is present so the object is recognisably the
    writer's."""
    evidence = '{"covered_by_shape":{"length":1}}'
    assert _recorded_shape_counts(evidence) == ({"length": 1}, {})
    assert _receipt(evidence).shape_counts() == "covered[length=1] outside[]"

    evidence = '{"outside_by_shape":{"empty_result":2}}'
    assert _recorded_shape_counts(evidence) == ({}, {"empty_result": 2})
    assert _receipt(evidence).shape_counts() == "covered[] outside[empty_result=2]"


def test_malformed_json_is_unrecorded() -> None:
    assert _recorded_shape_counts("{not json") == (None, None)


def test_a_real_match_receipts_provenance_is_recorded_and_empty() -> None:
    """A MATCH receipt has no mismatch findings to shape-classify, so the
    Go writer's own `omitempty` drops BOTH covered_by_shape and
    outside_by_shape -- the provenance is still recognisably the
    writer's, by measurement_route, and reads as computed-and-empty, not
    unrecorded. A version of this fix that recognised the writer's object
    ONLY by the presence of a shape key regressed this exact case: it
    could not tell a genuine match receipt's provenance apart from an
    operator's unrelated JSON, since both omit both shape keys (caught by
    the e2e harness's r8 P3-5 cell, `run_enable_e2e.py`, before landing).
    """
    evidence = '{"measurement_route":"edge","edge_build_binding":"present"}'
    assert _recorded_shape_counts(evidence) == ({}, {})
    assert _receipt(evidence).shape_counts() == "covered[] outside[]"


# Self-review (rule 26, CHAOS-5484): the fix's four decision points,
# exercised on their own rather than only through the reviewer's six
# original inputs -- a mutant dropping any one of them was hand-applied
# against this file and killed before these cases were added.
def test_outside_by_shape_alone_present_with_a_bool_is_unrecorded() -> None:
    """The malformed-value rule is symmetric across both keys, not only
    covered_by_shape (which every other case in this file exercises)."""
    evidence = '{"outside_by_shape":{"value":true}}'
    assert _recorded_shape_counts(evidence) == (None, None)


def test_a_key_present_with_the_wrong_container_type_is_unrecorded() -> None:
    """covered_by_shape present but not itself an object (a list, here)
    invalidates the object the same way a wrong-typed COUNT does."""
    evidence = '{"covered_by_shape":[1,2],"outside_by_shape":{}}'
    assert _recorded_shape_counts(evidence) == (None, None)


def test_both_keys_present_and_empty_is_recorded_and_empty() -> None:
    """Neither key omitted, both genuinely empty -- recorded, not
    unrecorded: distinct from test_neither_key_present_is_unrecorded_not_empty
    (no keys at all) and from the legitimate-single-omission cases."""
    assert _recorded_shape_counts('{"covered_by_shape":{},"outside_by_shape":{}}') == (
        {},
        {},
    )


def test_review_evidence_that_is_not_a_json_object_is_unrecorded() -> None:
    """A JSON array or a bare JSON null both parse successfully but are not
    the writer's object shape."""
    assert _recorded_shape_counts("[]") == (None, None)
    assert _recorded_shape_counts("null") == (None, None)
