# Ask Dev Wave 1 status/change rule and fixture manifest

The authoritative product and technical requirements remain the Ask Dev PRD,
TRD, and CHAOS-3206 in Linear. This repository note records only the shipped
code/fixture mapping for quick verification.

| Contract | Code owner | Version |
| --- | --- | --- |
| Status snapshot | `api/dev/status_change_service.py` | `status_snapshot.v1` |
| Change summary | `api/dev/status_change_service.py` | `change_summary.v1` |
| Completion assessment | `StatusChangeService._assess` | `actual-completion.v1` |
| Work-graph neighbors | `api/dev/work_graph_neighbors_service.py` | `work_graph_neighbors.v1` |

The machine-readable fixture inventory is
[`tests/fixtures/ask_dev/status_change/manifest.json`](../../tests/fixtures/ask_dev/status_change/manifest.json).
Semantic changes to completion or change classification require a new rule or
contract version and matching fixture-manifest update.

Three source gaps proven by the Wave 1 acceptance audit are recorded on
CHAOS-3208 and remain unavailable rather than inferred:

- required versus optional child-work classification;
- required versus optional CI/acceptance checks when work is skipped;
- a canonical declared project-status projection.

Graph traversal remains depth one, uses the code-owned relationship allowlist,
and returns persisted relationships only. GraphQL fields and agent tool facades
both call the same application services; neither surface owns rule semantics.
