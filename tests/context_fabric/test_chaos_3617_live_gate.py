"""CHAOS-3617: the skip gate itself is verified, not trusted.

Every live test in this suite decides whether to run by calling
``live_gate.require_live_store``. That makes the gate a single point of
failure in the honest direction *and* the dishonest one: if it skipped
unconditionally, the whole live half of the verification would be green and
empty.

So the gate is tested here with no live store involved at all — its two
branches are exercised directly, and one test in this module always runs and
always reports what the environment actually offers.
"""

from __future__ import annotations

import sys

import pytest

from dev_health_ops.context_fabric.graph_arm.flags import REQUIRE_LIVE_FLAG
from tests.context_fabric import live_gate


def test_the_gate_fails_rather_than_skips_when_a_live_run_is_required(
    monkeypatch,
) -> None:
    """The rule the reproduction procedure depends on.

    With the flag set, "no store" must be a failure. If this ever became a
    skip, a recorded reproduction could report a fully green suite in which
    not one live assertion executed.
    """

    monkeypatch.setenv(REQUIRE_LIVE_FLAG, "1")
    monkeypatch.setattr(
        live_gate,
        "live_store_status",
        lambda: live_gate.LiveStoreStatus(config=None, missing="nothing is running"),
    )
    # ``pytest.fail`` raises ``Failed``, which derives from ``BaseException``
    # rather than ``Exception`` -- catching only ``Exception`` here would let
    # the failure escape and this test would report the wrong reason.
    with pytest.raises(BaseException) as excinfo:  # noqa: B017, PT011
        live_gate.require_live_store()
    assert isinstance(excinfo.value, pytest.fail.Exception)
    assert not isinstance(excinfo.value, pytest.skip.Exception)
    assert "did not happen" in str(excinfo.value)


def _hide_graphiti(monkeypatch) -> None:
    """Make ``import graphiti_core`` fail, as it does in the CI unit job.

    Patched at the import machinery rather than by faking a status object, so
    the gate takes the same branch for the same reason CI does — a stubbed
    status would test the branch and not the condition that reaches it.
    """

    import builtins

    real_import = builtins.__import__

    def _blocked(name, *args, **kwargs):
        if name == "graphiti_core" or name.startswith("graphiti_core."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked)
    monkeypatch.delitem(sys.modules, "graphiti_core", raising=False)


def test_the_extra_gate_fails_rather_than_skips_when_a_run_is_required(
    monkeypatch,
) -> None:
    """The same rule, for the half of the suite that needs no server.

    Some measurements need graphiti-core and no FalkorDB — the write path
    builds Graphiti node and edge objects before it talks to anything. One of
    them was reaching the extra directly and dying with
    ``GraphitiUnavailableError`` in CI, which reads as a defect in the arm
    rather than as a measurement that did not happen. It routes through the
    gate now, and the gate must have the same two outcomes as its sibling.
    """

    monkeypatch.setenv(REQUIRE_LIVE_FLAG, "1")
    _hide_graphiti(monkeypatch)
    with pytest.raises(BaseException) as excinfo:  # noqa: B017, PT011
        live_gate.require_graphiti_extra()
    assert isinstance(excinfo.value, pytest.fail.Exception)
    assert not isinstance(excinfo.value, pytest.skip.Exception)
    assert "did not happen" in str(excinfo.value)


def test_the_extra_gate_skips_with_an_actionable_reason_when_not_required(
    monkeypatch,
) -> None:
    """The other outcome, and the one that keeps CI honest rather than red."""

    monkeypatch.delenv(REQUIRE_LIVE_FLAG, raising=False)
    _hide_graphiti(monkeypatch)
    with pytest.raises(BaseException) as excinfo:  # noqa: B017, PT011
        live_gate.require_graphiti_extra()
    assert isinstance(excinfo.value, pytest.skip.Exception)
    assert "context-graph-trial" in str(excinfo.value)
    assert REQUIRE_LIVE_FLAG in str(excinfo.value)


def test_the_extra_gate_is_a_no_op_when_the_extra_is_installed() -> None:
    """The control. A gate that always raised would pass both tests above."""

    pytest.importorskip("graphiti_core")
    live_gate.require_graphiti_extra()


def test_the_gate_skips_with_an_actionable_reason_when_not_required(
    monkeypatch,
) -> None:
    monkeypatch.delenv(REQUIRE_LIVE_FLAG, raising=False)
    monkeypatch.setattr(
        live_gate,
        "live_store_status",
        lambda: live_gate.LiveStoreStatus(config=None, missing="nothing is running"),
    )
    with pytest.raises(pytest.skip.Exception) as excinfo:
        live_gate.require_live_store()
    message = str(excinfo.value)
    assert "nothing is running" in message
    assert REQUIRE_LIVE_FLAG in message, (
        "a skip that does not say how to turn itself into a failure invites "
        "the reader to treat it as coverage"
    )


def test_the_gate_returns_the_config_when_a_store_is_available(monkeypatch) -> None:
    from dev_health_ops.context_fabric.graph_arm.flags import TrialStoreConfig

    config = TrialStoreConfig(uri="falkor://127.0.0.1:6389")
    monkeypatch.setattr(
        live_gate,
        "live_store_status",
        lambda: live_gate.LiveStoreStatus(config=config, missing=None),
    )
    assert live_gate.require_live_store() is config


def test_the_environments_actual_live_store_state_is_reported(record_property) -> None:
    """Always runs; never fails. Records what the environment offered.

    This is the line a reviewer reads to know whether the live half of the
    verification executed in a given run, without having to infer it from an
    absence of skip markers.
    """

    status = live_gate.live_store_status()
    record_property(
        "chaos_3617_live_store",
        "available" if status.available else f"unavailable: {status.missing}",
    )
    # NOT `assert status.available or status.missing` -- `available` is
    # DEFINED as `missing is None`, so that disjunction is a tautology for
    # every well-formed status and could not detect anything. Adversarial
    # review caught it. What is actually worth asserting is the invariant the
    # two fields are supposed to satisfy together: exactly one of them is
    # informative, and an unavailable status must carry a reason a reader can
    # act on rather than an empty string.
    assert status.available == (status.missing is None)
    if not status.available:
        assert status.missing and status.missing.strip(), (
            "an unavailable status must say what is missing; a blank reason "
            "is the skip nobody can act on"
        )


def test_the_store_uri_is_a_conditional_keep_gated_on_the_require_flag() -> None:
    """The suite scrubs the store URI unless the live lane announces itself.

    Two things break if this wiring is ever dropped, and neither is visible
    from a green suite:

    * dropped from ``CONDITIONAL_KEEP_ENV_NAMES`` -> the URI is scrubbed
      unconditionally, the live tests can never run again, and the only
      symptom is a skip line nobody reads;
    * moved to an unconditional keep -> a URI that merely happened to be in
      someone's shell turns the unit tier into an unannounced live run that
      writes real projections.

    Asserted against the checked-in mapping rather than by observing a skip,
    because the skip is what both failure modes look like.
    """

    from tests import _env_isolation

    for name in (
        "CONTEXT_FABRIC_GRAPH_STORE_URI",
        "CONTEXT_FABRIC_GRAPH_STORE_PASSWORD",
    ):
        assert name in _env_isolation.SCRUB_ENV_NAMES, (
            f"{name} must be scrubbed by default"
        )
        assert (
            _env_isolation.CONDITIONAL_KEEP_ENV_NAMES.get(name) == REQUIRE_LIVE_FLAG
        ), f"{name} must be kept only when {REQUIRE_LIVE_FLAG} announces the lane"

    assert "GRAPHITI_TELEMETRY_ENABLED" in _env_isolation.SCRUB_ENV_NAMES
    assert (
        "GRAPHITI_TELEMETRY_ENABLED" not in _env_isolation.CONDITIONAL_KEEP_ENV_NAMES
    ), "telemetry is forced off in code; no lane may re-enable it"


def test_the_harness_summary_pattern_cannot_backtrack_exponentially() -> None:
    """CodeQL py/redos, blocked on #1612 — regression guard.

    The original pattern made the separator optional inside the repeated
    group (``(?:\\d+ \\w+(?:, )?)+``), so ``\\d+ \\w+`` could match with or
    without a trailing ``", "`` and the engine had exponentially many ways to
    split a near-miss before failing. Measured on the exact adversarial shape
    CodeQL named, the old pattern quadrupled per two extra repetitions
    (0.0019s → 0.0077s → 0.0308s → 0.1221s at n=14,16,18,20); the fixed one
    stays at ~3µs. At n=40 the old pattern would not finish in a human
    lifetime.

    The bound below is deliberately enormous relative to the ~microseconds
    this actually takes. A timing assertion with a tight bound would be a
    flake; one with a six-order-of-magnitude margin is a genuine catch for
    the only failure mode that matters here, and cannot fire on a slow
    machine.
    """

    import importlib.util
    import sys
    import time
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "_chaos_3617_guard_injection",
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "chaos_3617_guard_injection.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered BEFORE exec: the harness defines dataclasses, and
    # ``dataclasses`` resolves annotations via ``sys.modules[cls.__module__]``
    # while the class body executes. An unregistered module makes that lookup
    # return None and the import dies inside the stdlib, which reads as a
    # mysterious AttributeError rather than "you skipped a step".
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
        _SUMMARY = module._SUMMARY
    finally:
        sys.modules.pop(spec.name, None)

    probe = "0 " + "000 " * 40
    started = time.perf_counter()
    assert _SUMMARY.search(probe) is None
    assert time.perf_counter() - started < 1.0

    # ...and it still matches what it exists to match.
    assert _SUMMARY.search("13724 passed, 268 skipped, 1 xfailed in 154.82s")
    assert _SUMMARY.search("1 failed, 42 warnings in 1.24s")
    assert _SUMMARY.search("208 passed in 5.40s")


def test_the_documented_guard_count_matches_the_harness() -> None:
    """A hand-maintained number in prose is a claim guaranteed to drift.

    And it did: PR1 shipped with the architecture note claiming 21 guards
    while the harness held 24, because a later round added three mutations
    and updated the code but not the sentence. Nobody noticed, because
    nothing could.

    The number is worth keeping — a reader wants the scale — so it is pinned
    instead of removed. This reads both and compares.
    """

    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    harness = (root / "scripts" / "chaos_3617_guard_injection.py").read_text()
    note = (
        root / "docs" / "contribute" / "architecture" / "graph-investigation-arm.md"
    ).read_text()

    actual = harness.count("mutation_id=")
    assert actual > 20, f"suspiciously few mutations found: {actual}"

    claimed = re.search(r"For each of the \*\*(\d+)\*\* guards", note)
    assert claimed is not None, "the note no longer states a guard count"
    assert int(claimed.group(1)) == actual, (
        f"the architecture note claims {claimed.group(1)} guards; the harness "
        f"defines {actual}"
    )
