"""CHAOS-5025: the API's uvicorn shutdown must always be bounded.

uvicorn.Server.shutdown() drains server_state.connections and then
server_state.tasks, and both loops spin forever unless timeout_graceful_shutdown
is set. Run 33822295135 lost 5h40m to exactly that. These tests pin the bound
and, critically, pin that no ambient env value can switch it back off.
"""

import argparse

import pytest

from dev_health_ops.api import runner


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv(runner.GRACEFUL_SHUTDOWN_SECONDS_ENV, raising=False)


def test_defaults_to_bounded_value():
    assert runner._graceful_shutdown_seconds() == (
        runner.DEFAULT_GRACEFUL_SHUTDOWN_SECONDS
    )
    assert runner.DEFAULT_GRACEFUL_SHUTDOWN_SECONDS > 0


def test_env_override_is_honoured(monkeypatch):
    monkeypatch.setenv(runner.GRACEFUL_SHUTDOWN_SECONDS_ENV, "90")
    assert runner._graceful_shutdown_seconds() == 90


def test_whitespace_is_tolerated(monkeypatch):
    monkeypatch.setenv(runner.GRACEFUL_SHUTDOWN_SECONDS_ENV, "  45  ")
    assert runner._graceful_shutdown_seconds() == 45


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "abc",
        "12.5",
        "0",
        "-1",
        "-3600",
        # codex round 1, P1: rejecting only zero/negative left the hole open --
        # any positive integer was passed straight through, so a 1000-year value
        # restored the unbounded shutdown under a different spelling.
        "3601",
        "31557600000",
        "999999999999999999999",
    ],
)
def test_never_falls_back_to_unbounded(monkeypatch, raw):
    """A junk, zero, or negative override must NOT disable the bound.

    0 and negatives are the dangerous ones: uvicorn treats a falsy
    timeout_graceful_shutdown as "no timeout", which is the pre-CHAOS-5025
    behaviour this fix exists to remove.
    """
    monkeypatch.setenv(runner.GRACEFUL_SHUTDOWN_SECONDS_ENV, raw)
    resolved = runner._graceful_shutdown_seconds()
    assert resolved == runner.DEFAULT_GRACEFUL_SHUTDOWN_SECONDS
    assert resolved > 0


def test_run_api_server_passes_the_bound_to_uvicorn(monkeypatch):
    """The resolved bound must actually reach uvicorn.run()."""
    captured = {}

    def _fake_run(app, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(runner.uvicorn, "run", _fake_run)
    monkeypatch.setenv(runner.GRACEFUL_SHUTDOWN_SECONDS_ENV, "77")

    ns = argparse.Namespace(
        db=None,
        analytics_db=None,
        log_level="INFO",
        reload=False,
        workers=1,
        host="127.0.0.1",
        port=8000,
    )
    assert runner.run_api_server(ns) == 0
    assert captured["timeout_graceful_shutdown"] == 77


def test_upper_bound_is_finite_and_enforced(monkeypatch):
    """codex round 1, P1: the bound needs a ceiling, not just a floor."""
    assert runner.MAX_GRACEFUL_SHUTDOWN_SECONDS > 0
    assert (
        runner.DEFAULT_GRACEFUL_SHUTDOWN_SECONDS <= runner.MAX_GRACEFUL_SHUTDOWN_SECONDS
    )

    monkeypatch.setenv(
        runner.GRACEFUL_SHUTDOWN_SECONDS_ENV,
        str(runner.MAX_GRACEFUL_SHUTDOWN_SECONDS),
    )
    assert runner._graceful_shutdown_seconds() == (runner.MAX_GRACEFUL_SHUTDOWN_SECONDS)

    monkeypatch.setenv(
        runner.GRACEFUL_SHUTDOWN_SECONDS_ENV,
        str(runner.MAX_GRACEFUL_SHUTDOWN_SECONDS + 1),
    )
    assert runner._graceful_shutdown_seconds() == (
        runner.DEFAULT_GRACEFUL_SHUTDOWN_SECONDS
    )


def test_this_variable_is_scrubbed_not_kept():
    """codex round 1, P3 (narrow half).

    The contract test in tests/test_env_isolation_contract.py only asserts that
    the checked-in list MATCHES the derivation -- moving a name into
    KEEP_ENV_NAMES and regenerating keeps that test green while the real src/
    read stays live in the ambient environment. That general weakness belongs to
    the guard's owner, but this PR introduced the variable, so pin THIS one
    directly rather than relying on a contract that can be satisfied both ways.
    """
    from tests import _env_isolation

    name = runner.GRACEFUL_SHUTDOWN_SECONDS_ENV
    assert name in _env_isolation.SCRUB_ENV_NAMES
    assert name not in _env_isolation.KEEP_ENV_NAMES
    assert name in _env_isolation.discover_src_env_names()
