"""CHAOS-3498 Context Fabric trial code.

Everything under this package is **trial scaffolding for the corrected
CHAOS-3614 discovery** and is not part of any production request path. It is
deliberately a separate top-level package rather than a subpackage of
``api/`` so that the whole experiment is removable with one ``rm -r`` plus
the two registration points that reference it (the ``context-graph-trial``
optional dependency extra and the ``EXTERNAL_DERIVED_STORES`` entry).

Nothing here may be imported by a default production import path. The guard
is ``tests/context_fabric/test_chaos_3617_removability.py``.
"""

from __future__ import annotations

__all__: list[str] = []
