"""Context Fabric trial machinery (CHAOS-3498 corrected discovery).

Everything under this package exists to run the corrected CHAOS-3614
comparison and is flag-off by default. Nothing here is a product surface,
and nothing here may become canonical truth for a metric, a declared
status, health, workload, completion or readiness -- those stay owned by
``dev_health_ops.api.dev``'s deterministic services, which this package
reads from and never replaces.

Sub-packages:

* ``native_arm`` (CHAOS-3618) -- projects the CURRENT Ask Dev investigation
  path into ``ask_dev_investigation_packet.v1``. The honest baseline.
"""

from __future__ import annotations
