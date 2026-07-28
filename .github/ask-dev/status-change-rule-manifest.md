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

`change_summary.v1` reads five delivery change classes from their canonical
ClickHouse facts: pull requests (`git_pull_requests`), reviews
(`git_pull_request_reviews`), CI runs (`ci_pipeline_runs`), deployments
(`deployments`), and incidents (`operational_incidents`). Incident changes are
included only through persisted deployment-to-incident edges. Every native read
is bounded by organization, authorized repositories, direct scope, the explicit
current window, and the requested item limit. Results use one deterministic
ordering across change classes before the final bound is applied.

A successful bounded query with no rows emits an unknown-freshness source
reference and makes the result partial: without a row watermark, the system does
not claim the source is fresh. A query failure instead emits an unavailable
source reference, degrades the result, and warns that the source is unavailable.
Consumers must not interpret either case as fresh evidence that no change
happened.

Two source gaps proven by the Wave 1 acceptance audit are recorded on
CHAOS-3208 and remain unavailable rather than inferred:

- required versus optional child-work classification;
- required versus optional CI/acceptance checks when work is skipped;

`GAP-ASKDEV-BLOCKER-DIRECTION-01` is remediated by
`canonical-blocks.v2`: every provider persists `source issue --blocks--> target
issue`, the tenant-scoped rebuild removes legacy orientations before rewriting,
and Ask Dev trusts the blocker projection only after its completed-run marker is
fresh for the full authorized repository scope. Linear native issue relations
use the same contract.

`actual-completion.v3` requires release/deployment evidence for issue, project,
and pull-request delivery scopes. Missing evidence is indeterminate and a failed
required deployment is not ready; a merged pull request alone is never proof of
release. Incident rows remain informational unless a persisted canonical
relation or policy marks them blocking. `status_snapshot.v1` explicitly returns
an unavailable result for `WORK_UNIT` until canonical work-unit membership is
projected; it does not issue partial native reads for that scope.

Graph traversal remains depth one, uses the code-owned relationship allowlist,
and returns persisted relationships only. GraphQL fields and agent tool facades
both call the same application services; neither surface owns rule semantics.
