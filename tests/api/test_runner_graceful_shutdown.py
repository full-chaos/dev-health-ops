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


@pytest.mark.parametrize("raw", ["", "   ", "abc", "12.5", "0", "-1", "-3600"])
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
