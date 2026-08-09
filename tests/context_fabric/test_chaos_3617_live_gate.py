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
    assert status.available or status.missing


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
