"""External-ingest worker-facing package (CHAOS-2690 epic).

Distinct from ``dev_health_ops.api.external_ingest`` (the HTTP layer):
this package holds modules shared between the REST contract and the
CHAOS-2693/2697 worker — starting with ``validate.py`` (CHAOS-2691), the
single source of deep per-record validation. See master-spec.md's module
map (docs/superpowers/plans/2026-07-01-chaos-2690-implementation/) for the
full, cross-ticket ownership list of files that land here over the epic.
"""
