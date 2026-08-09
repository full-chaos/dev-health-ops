"""CHAOS-3617: the one place a live-store test may decide to skip.

A skipped verification reads as coverage. Every live test in this suite
therefore routes its availability decision through :func:`require_live_store`,
which has exactly two outcomes:

* ``CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`` -> an unavailable store is a
  **failure**, naming precisely what was missing;
* otherwise -> a skip whose reason names the missing piece and the command
  that supplies it.

The reproduction procedure in the architecture note sets the flag, so the
recorded run cannot be a green suite that measured nothing. There is no third
outcome and no per-test bypass: a live test that skipped for its own reasons
is exactly the failure mode this module exists to prevent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pytest

from dev_health_ops.context_fabric.graph_arm.flags import (
    REQUIRE_LIVE_FLAG,
    TRIAL_STORE_URI_VAR,
    TrialStoreConfig,
    live_store_required,
    trial_store_config,
)

__all__ = ["LiveStoreStatus", "live_store_status", "require_live_store"]

_COMPOSE_HINT = (
    "start it with `docker compose --profile graph-trial up -d "
    f"graph-trial-store`, export {TRIAL_STORE_URI_VAR}=falkor://127.0.0.1:6389 "
    f"AND set {REQUIRE_LIVE_FLAG}=1 -- the store URI is a conditional keep in "
    "tests/_env_isolation.py whose lane sentinel is that flag, so the suite "
    "scrubs the URI unless the live lane has announced itself. That is "
    "deliberate: a URI that merely happened to be in your shell must never "
    "turn into an unannounced live run"
)


@dataclass(frozen=True, slots=True)
class LiveStoreStatus:
    """Whether a live trial store is usable, and why not if it is not."""

    config: TrialStoreConfig | None
    missing: str | None

    @property
    def available(self) -> bool:
        return self.missing is None


def live_store_status() -> LiveStoreStatus:
    """Check the extra, the configuration and the connection, in that order."""

    try:
        import graphiti_core  # noqa: F401
    except ModuleNotFoundError:
        return LiveStoreStatus(
            config=None,
            missing=(
                "graphiti-core is not installed (optional extra "
                "'context-graph-trial'); install with "
                "`uv sync --extra context-graph-trial`"
            ),
        )

    config = trial_store_config()
    if config is None:
        return LiveStoreStatus(
            config=None,
            missing=f"{TRIAL_STORE_URI_VAR} is unset; {_COMPOSE_HINT}",
        )

    try:
        import socket

        with socket.create_connection((config.host, config.port), timeout=2):
            pass
    except OSError as exc:
        return LiveStoreStatus(
            config=config,
            missing=(
                f"the trial store at {config.host}:{config.port} is not "
                f"reachable ({exc}); {_COMPOSE_HINT}"
            ),
        )
    return LiveStoreStatus(config=config, missing=None)


def require_live_store() -> TrialStoreConfig:
    """Return the live store's config, or skip/fail per the require flag."""

    status = live_store_status()
    if status.available:
        assert status.config is not None
        return status.config
    message = (
        f"CHAOS-3617 live trial store unavailable: {status.missing}. "
        f"Set {REQUIRE_LIVE_FLAG}=1 to turn this skip into a failure."
    )
    if live_store_required():
        pytest.fail(
            f"{REQUIRE_LIVE_FLAG}=1 was set, so a live-store measurement was "
            f"required and did not happen: {status.missing}"
        )
    pytest.skip(message)


def require_flag_state() -> None:
    """Guard against a live test running with the projection flag off.

    A live write test that silently no-ops because the flag was unset would
    pass while measuring nothing, so the flag is asserted rather than
    assumed.
    """

    assert os.getenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED") == "1"
