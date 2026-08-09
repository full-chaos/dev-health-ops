"""Operator flags for the native arm (CHAOS-3618 PR 2).

Matches the CHAOS-3617 convention exactly: ``CONTEXT_FABRIC_*`` names, and
``os.getenv(name) == "1"`` so "unset" is off and so is every other value
that merely looks truthy. A half-on state is worse than an off state for a
trial arm, because it produces packets nobody meant to compare.

This module arrives **with the wiring it gates**, deliberately: PR 1 had no
flag for the projection because nothing called it, and a flag gating
nothing is dead configuration that reads as a control.
:func:`~.producer.native_shadow_wiring` is what it gates.

**Stated exactly, because this PR does not close it:**
``native_shadow_wiring`` is not yet called from ``production_runtime.py``,
so no deployed process reads this flag today. It gates a constructor that
is tested and one call site away, not a live path. CHAOS-3619 makes that
call when it needs the trial to run.

Every name here must also appear in ``tests/_env_isolation.py``'s
``SCRUB_ENV_NAMES``, or CHAOS-3402's drift guard fails the build — an
ambient value from a developer's direnv shell must not decide whether a
test run projects packets.

That guard sees a name only through a call whose receiver is spelled
``os``, ``os.environ`` or ``environ`` (``_ENV_TARGETS``). The first version
of this module rebound the mapping to a local named ``source`` and read
``source.get(...)``, which the scan cannot see: the flag was absent from
``SCRUB_ENV_NAMES``, the drift guard was green, and this docstring's claim
was simply untrue. The read below is spelled the way the guard can see,
which is why the name is now in the list.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

__all__ = [
    "NATIVE_PROJECTION_FLAG",
    "native_projection_enabled",
]

#: Gates whether a finished run is projected into an investigation packet
#: at all. Off means the orchestrator holds ``None`` for the producer, so
#: the projection is not merely skipped — it is absent.
NATIVE_PROJECTION_FLAG = "CONTEXT_FABRIC_NATIVE_PROJECTION_ENABLED"


def native_projection_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Whether this process projects native runs into packets."""

    if environ is None:
        environ = os.environ
    return environ.get(NATIVE_PROJECTION_FLAG) == "1"
