"""CHAOS-3576 -- the shared harness's ``answer_payload()``/
``grounded_answer_payload()`` metrics fixture must produce a real,
v2-valid ``dev_answer_frame.v1`` for every completed run, and any future
regression must fail LOUDLY rather than silently degrade.

Defect (pre-fix): ``answer_payload()``'s metric carries
``evidence_ref_ids: ["ev_01"]`` and its evidence entry carries
``evidence_ref_id: "ev_01"`` -- both valid v1 ``OpaqueID`` shapes, but far
short of v2's ``EvidenceHandle`` grammar (``ev1_`` + 40 hex,
``contracts_v2.base.EvidenceHandle``, ``min_length=44``).
``terminal_frames.wrap_legacy_answer_as_frame`` re-validates a completed
run's metrics/evidence as their v2 mirrors
(``DevMetricRefV2``/``DevEvidenceRefV2``), so construction raises for
*every* completed run through this fixture -- and ``orchestrator.finish()``
(``orchestrator.py``, the ``except Exception as frame_construction_exc``
block) deliberately swallows that failure and substitutes the always-
registered ``internal_error`` fallback frame, by design, so a frame-layer
bug can never crash or discard an otherwise-successful run. That
production behavior is correct and stays untouched here.

The bug this file guards against is one layer up: nothing told a TEST that
this substitution had happened. A test asserting on
``output.recorder.frames[-1]`` was silently comparing against the generic
``internal_error`` stub instead of the real content frame it believed it
was exercising -- vacuous coverage, the "measurement that did not happen
must FAIL, loudly" class.

Several other suites independently discovered and locally patched around
this exact gap before it was filed here (each rewriting the fixture's
``ev_01`` handles to a v2-valid shape ad hoc): ``test_chaos_3297_frame_e2e
.py``, ``test_chaos_3297_s5_guard_cutover.py`` (filed as "CHAOS-3340" in
its own comment), ``test_chaos_3393_portfolio_orchestrator.py``, and
``test_terminal_frames.py``. This file is the first to fix it at the
fixture's own source rather than working around it per-suite.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev import terminal_frames
from dev_health_ops.api.dev.orchestrator_states import RunState
from tests._chaos_3292_preflight import (
    RUN_ID,
    assert_answered_frame_is_not_construction_fallback,
    case_a1,
)


@pytest.mark.asyncio
async def test_case_a1_frame_construction_does_not_silently_fall_back_to_internal_error() -> (
    None
):
    """N0-equivalent for the fake-recorder harness (CHAOS-3576).

    ``case_a1`` is a known real project: a well-behaved script commits a
    subject, runs one tool, and answers -- the ordinary completed-run shape
    every acceptance case in this module shares. The recorded frame must be
    a genuine ``wrap_legacy_answer_as_frame`` projection of that answer, not
    the orchestrator's own internal_error construction fallback.
    """

    output = await case_a1()

    assert output.result.state is RunState.COMPLETED
    assert output.result.answer is not None

    frame = assert_answered_frame_is_not_construction_fallback(output, run_id=RUN_ID)

    # A positive control alongside the negative one above: the frame that
    # *did* get recorded actually carries this run's content, not merely
    # "some frame that isn't the fallback" (a frame_id typo in the helper
    # itself would otherwise pass this test vacuously).
    assert frame.public_outcome.value == "answered_with_gaps"
    assert frame.metrics, "the answer's metric never reached the frame"
    assert frame.metrics[0].metric_id == "items_completed"


@pytest.mark.asyncio
async def test_construction_fallback_detector_is_not_vacuous() -> None:
    """Anti-vacuity control for the helper itself.

    Proves ``assert_answered_frame_is_not_construction_fallback`` actually
    discriminates: fed the exact fallback frame
    ``orchestrator.finish()`` substitutes on a real construction failure,
    it must raise -- otherwise the positive test above would pass no matter
    what the detector did.
    """

    from datetime import UTC, datetime
    from types import SimpleNamespace

    from tests._chaos_3292_preflight import Recorder

    fallback = terminal_frames.build_error_frame(
        code="internal_error",
        run_id=RUN_ID,
        generated_at=datetime.now(UTC),
    )
    recorder = Recorder()
    recorder.frames.append(fallback)

    # Duck-typed: the helper only reads `.result.answer` and `.recorder`, so
    # a `SimpleNamespace` proves the same contract `RunOutput` does without
    # coupling this control to every other required `OrchestratorResult`
    # field.
    fake_output = SimpleNamespace(
        result=SimpleNamespace(answer=object()), recorder=recorder
    )

    try:
        assert_answered_frame_is_not_construction_fallback(fake_output, run_id=RUN_ID)
    except AssertionError:
        pass
    else:
        raise AssertionError(
            "the detector must reject the orchestrator's own internal_error "
            "fallback frame, and it did not"
        )
