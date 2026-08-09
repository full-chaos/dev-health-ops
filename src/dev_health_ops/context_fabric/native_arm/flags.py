"""Operator flags for the native arm (CHAOS-3618 PR 2).

Matches the CHAOS-3617 convention exactly: ``CONTEXT_FABRIC_*`` names, and
``os.getenv(name) == "1"`` so "unset" is off and so is every other value
that merely looks truthy. A half-on state is worse than an off state for a
trial arm, because it produces packets nobody meant to compare.

This module arrives **with the orchestrator wiring it gates**, deliberately.
PR 1 had no flag for the projection because nothing called it, and a flag
gating nothing is dead configuration that reads as a control.

Every name here must also appear in ``tests/_env_isolation.py``'s
``SCRUB_ENV_NAMES``, or CHAOS-3402's drift guard fails the build — an
ambient value from a developer's direnv shell must not decide whether a
test run projects packets.
"""

from __future__ import annotations

import os

__all__ = [
    "NATIVE_PROJECTION_FLAG",
    "native_projection_enabled",
]

#: Gates whether a finished run is projected into an investigation packet
#: at all. Off means the orchestrator holds ``None`` for the producer, so
#: the projection is not merely skipped — it is absent.
NATIVE_PROJECTION_FLAG = "CONTEXT_FABRIC_NATIVE_PROJECTION_ENABLED"


def native_projection_enabled(environ: os._Environ[str] | None = None) -> bool:
    """Whether this process projects native runs into packets."""

    source = os.environ if environ is None else environ
    return source.get(NATIVE_PROJECTION_FLAG) == "1"
