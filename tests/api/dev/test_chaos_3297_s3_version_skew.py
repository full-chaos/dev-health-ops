"""CHAOS-3297 stack #3 version-skew read posture (team-lead ruling,
2026-08-02): a frame persisted by pre-s3 ``wrap_legacy_answer_as_frame``
must stay replayable after this branch's F10 metric-evidence-classification
field landed additively (no ``schema_version`` bump).

The fixture at ``fixtures/chaos_3297_s3_pre_s3_frame_metric_no_evidence.json``
is NOT hand-authored. It was captured verbatim from
``origin/main@47bf3c6e8``'s REAL producer chain -- a ``git worktree`` of
that exact commit, ``uv sync``'d fresh, running that commit's own
``dev_health_ops.api.dev.terminal_frames.wrap_legacy_answer_as_frame`` over
a ``DevAnswer`` built from that commit's own ``contract_fixtures.py``
(``_legacy_answer``-style real-evidence substitution, with the metric's
``evidence_ref_ids`` cleared to ``[]`` to represent the "no traceable
source refs" shape ``metrics/service.py``'s ``contract_refs`` genuinely
produces in production -- see ``_wrap_legacy_metric``'s own docstring:
``query_metric.v1`` scrubs evidence on every call, so this is the
UNIVERSAL legacy-metric shape, not a rare edge case), then serialized via
that commit's own ``frame.model_dump(mode="json")`` -- the exact payload
shape ``orchestrator_persistence.record_frame``/
``persistence/service.py.record_frame`` would have written to
``dev_answer_frames.payload``. The one-off capture script is preserved in
the CHAOS-3297 s3 handoff notes for reproducibility.

Reachability note (reported to team-lead alongside this fixture): tracing
every call site of ``get_answer_frame``/``DevAnswerFrame.model_validate``/
``_DevAnswerFrameV2.model_validate`` shows this specific fixture's outcome
(``answered_with_gaps``, always what ``wrap_legacy_answer_as_frame``
produces) replays today via ``router._replayed_result``'s
``answer_payload`` branch (the v1 ``DevAnswer``, which has no
``evidence_classification`` concept at all and is structurally immune) --
the ONE branch that re-validates a stored payload through
``DevAnswerFrame`` (``router.py``'s ``_DevAnswerFrameV2.model_validate``
call) is gated to ``answer_id is None`` rows, which per
``NO_ANSWER_FRAME_FIELD_POLICY``'s ``"metrics": ABSENT`` can never carry a
non-empty ``metrics`` tuple, and that branch is additionally wrapped in
``except ValidationError: _replay_fallback_error(run)`` regardless. The
capability gap this suite closes is real (proved below) even though no
current call site is known to trigger it for an ANSWERED-outcome row --
required defensively, per team-lead's ruling, ahead of s4/s5's cutover
widening what reads a stored frame.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contracts_v2.embedded import MetricEvidenceClassification
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame

_FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "chaos_3297_s3_pre_s3_frame_metric_no_evidence.json"
)


def _pre_s3_payload() -> dict:
    return json.loads(_FIXTURE_PATH.read_text())


def test_fixture_is_genuinely_pre_s3_shaped() -> None:
    """Sanity check on the captured fixture itself, not the code under test."""

    payload = _pre_s3_payload()
    assert payload["schema_version"] == "dev_answer_frame.v1"
    assert payload["metrics"], "fixture must carry at least one metric"
    metric = payload["metrics"][0]
    assert metric["evidence_ref_ids"] == []
    assert "evidence_classification" not in metric
    assert "health_findings" not in payload
    assert "deficiency_findings" not in payload


def test_raw_pre_s3_frame_is_rejected_without_the_read_posture() -> None:
    """Documents the skew bug itself: this must keep failing forever --
    it is the reason ``tolerant_parse_legacy_frame_payload`` exists, not a
    regression to fix away.
    """

    payload = _pre_s3_payload()
    with pytest.raises(ValidationError, match="requires either"):
        DevAnswerFrame.model_validate(payload)


def test_missing_findings_fields_need_no_shim() -> None:
    """The health/deficiency-findings half of the skew needs no read-time
    patch: both fields are ``default_factory``-backed, so a payload that
    predates them just omits the keys and pydantic supplies the (correct,
    empty) default. Proved directly against a variant of the fixture with
    the zero-evidence metric already fixed up, isolating this from the
    metric-XOR failure above (mutate one clause at a time).
    """

    payload = _pre_s3_payload()
    payload["metrics"][0]["evidence_classification"] = (
        MetricEvidenceClassification.LEGACY_V1_UNMINTED.value
    )
    frame = DevAnswerFrame.model_validate(payload)
    assert frame.health_findings == ()
    assert frame.health_findings_truncated is False
    assert frame.deficiency_findings == ()
    assert frame.deficiency_findings_truncated is False


def test_tolerant_parse_makes_the_pre_s3_frame_replayable() -> None:
    payload = _pre_s3_payload()
    patched = tf.tolerant_parse_legacy_frame_payload(payload)
    frame = DevAnswerFrame.model_validate(patched)  # does not raise

    assert frame.metrics[0].evidence_ref_ids == ()
    assert (
        frame.metrics[0].evidence_classification
        is MetricEvidenceClassification.LEGACY_V1_UNMINTED
    )


def test_tolerant_parse_changes_nothing_else() -> None:
    """ "Byte-identical" proof (team-lead's requirement 1): every field the
    pre-s3 payload carried survives untouched -- the only delta introduced
    anywhere in the round trip is the one new key on the one patched
    metric.
    """

    original = _pre_s3_payload()
    patched = tf.tolerant_parse_legacy_frame_payload(original)

    assert patched.keys() == original.keys()
    for key, value in original.items():
        if key != "metrics":
            assert patched[key] == value, key

    original_metric = original["metrics"][0]
    patched_metric = patched["metrics"][0]
    assert patched_metric.keys() == original_metric.keys() | {"evidence_classification"}
    for key, value in original_metric.items():
        assert patched_metric[key] == value, key
    assert (
        patched_metric["evidence_classification"]
        == MetricEvidenceClassification.LEGACY_V1_UNMINTED.value
    )

    # And re-serializing the validated frame reproduces every original
    # field verbatim (the frame constructor introduces no other drift).
    frame = DevAnswerFrame.model_validate(patched)
    reserialized = frame.model_dump(mode="json")
    for key, value in original.items():
        if key != "metrics":
            assert reserialized[key] == value, key


def test_tolerant_parse_does_not_touch_a_metric_with_real_evidence() -> None:
    """A metric that already carries real evidence needs no patch --
    proves the shim is conditioned on the zero-evidence shape, not applied
    unconditionally to every metric it sees.
    """

    payload = _pre_s3_payload()
    real_handle = "ev1_" + ("a1b2c3d4e5" * 4)
    payload["metrics"][0]["evidence_ref_ids"] = [real_handle]
    patched = tf.tolerant_parse_legacy_frame_payload(payload)

    assert "evidence_classification" not in patched["metrics"][0]
    frame = DevAnswerFrame.model_validate(patched)  # already valid, unpatched
    assert frame.metrics[0].evidence_ref_ids == (real_handle,)
    assert frame.metrics[0].evidence_classification is None


def test_tolerant_parse_does_not_mask_an_explicit_null_classification() -> None:
    """Plant defect: a metric with the ``evidence_classification`` key
    PRESENT but ``null``, alongside empty evidence -- a genuinely invalid
    shape (neither disclosure) that must keep failing. If the shim patched
    this too (treating "key present but null" the same as "key absent"),
    it would silently mask a real F10 violation instead of surfacing it --
    exactly the failure mode ``tolerant_parse_legacy_frame_payload``'s own
    docstring rules out.
    """

    payload = _pre_s3_payload()
    payload["metrics"][0]["evidence_classification"] = None
    patched = tf.tolerant_parse_legacy_frame_payload(payload)

    assert patched["metrics"][0]["evidence_classification"] is None
    with pytest.raises(ValidationError, match="requires either"):
        DevAnswerFrame.model_validate(patched)


def test_tolerant_parse_is_idempotent() -> None:
    payload = _pre_s3_payload()
    once = tf.tolerant_parse_legacy_frame_payload(payload)
    twice = tf.tolerant_parse_legacy_frame_payload(once)
    assert once == twice


def test_tolerant_parse_does_not_mutate_its_input() -> None:
    payload = _pre_s3_payload()
    before = json.loads(json.dumps(payload))
    tf.tolerant_parse_legacy_frame_payload(payload)
    assert payload == before
