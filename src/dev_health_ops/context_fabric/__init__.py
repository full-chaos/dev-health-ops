"""CHAOS-3498 Context Fabric trial code.

Everything under this package is **trial scaffolding for the corrected
CHAOS-3614 discovery** and is not part of any production request path. It is
deliberately a separate top-level package rather than a subpackage of
``api/`` so that the whole experiment is removable with one ``rm -r`` plus
the two registration points that reference it (the ``context-graph-trial``
optional dependency extra and the ``EXTERNAL_DERIVED_STORES`` entry).

Nothing here may be imported by a default production import path. The guard
is ``tests/context_fabric/test_chaos_3617_removability.py``.

Nothing here may become canonical truth for a metric, a declared status,
health, workload, completion or readiness either. Those stay owned by
``dev_health_ops.api.dev``'s deterministic services, which the arms read
from and never replace.

Sub-packages, one per arm of the comparison:

* ``graph_arm`` (CHAOS-3617) -- the real Graphiti-backed shadow arm.
* ``native_arm`` (CHAOS-3618) -- projects the CURRENT Ask Dev investigation
  path into ``ask_dev_investigation_packet.v1``. The honest baseline.

Both emit the same frozen packet contract, and neither may see the other:
that separation is what lets the trial attribute a difference to the arm
rather than to a better-written adapter.
"""

from __future__ import annotations

__all__: list[str] = []
