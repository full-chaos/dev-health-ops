---
page_id: con-ask-dev-native-baseline
summary: What the current Ask Dev investigation path can and cannot express in ask_dev_investigation_packet.v1, and the shared shadow seam both trial arms hand their packet to.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/context_fabric/native_arm/
  - src/dev_health_ops/api/dev/investigation_shadow.py
  - src/dev_health_ops/api/dev/investigation_plans/relationship_matrix.py
  - tests/context_fabric/test_chaos_3618_capabilities.py
  - tests/context_fabric/test_chaos_3618_projection.py
  - tests/api/dev/test_chaos_3618_investigation_shadow.py
applicability: current
lifecycle: active
---

# Ask Dev native baseline and the shadow synthesis seam

CHAOS-3618 builds the honest baseline for the corrected CHAOS-3614 trial: it
projects what the **current** Ask Dev investigation path already assembles
into [`ask_dev_investigation_packet.v1`](ask-dev-investigation-packet.md),
the contract CHAOS-3615 froze for both arms, and it adds the single shadow
seam that accepts either arm's packet.
{: .fc-page-lede }

The point of a baseline is not to score well. It is to say precisely what
the product can express today, so that whatever the graph arm adds can be
attributed to the graph rather than to a better-written adapter. Everything
below follows from that.

## The governing constraint

> A native fact that *suggests* a relationship is not a relationship.

The easiest way to write a dishonest baseline is to take a native fact, give
it the contract's nearest relationship type, and let the packet claim lineage
the run never established. Every such judgement therefore lives in one
reviewable table — `context_fabric/native_arm/capabilities.py` — rather than
inline in the projection, and each row is cross-checked at import time and in
tests against contract vocabularies that are owned elsewhere.

## What the native arm can express

| Packet section | Native source |
| --- | --- |
| Analytical job | `DevQuestionIntent` — cardinality, confidence, clarification flag |
| Subject discovery | `DevResolutionLedger` candidates, committed refs, unresolved mentions |
| Comparison cohort | `DevSubjectSet` committed refs and completeness |
| Evidence coverage | Per-observation state, watermark, limitation; real `EvidenceHandle`s |
| Driver analysis | `DeficiencyFinding` (carries both paths and evidence handles) |
| Versions | Plan/query versions plus `TrialMetadata(arm_id="native")` |

The coverage machinery is genuinely the strongest part: source health,
missing sources, watermarks, truncation and authorization filtering are all
first-class native concepts and project cleanly.

## What it cannot — and why

Each gap below is a structural property of landed code, not unfinished work.
`NativeGapMechanism` names which kind each one is, because "the contract
needs a new node kind" and "nobody wired the adapter" have completely
different dispositions.

### Subject kinds: 6 of 10

`contracts_v2.base.EntityKind` has `repository, project, work_unit, issue,
pull_request, team`. The contract's `InvestigationSubjectKind` adds
`portfolio, initiative, service, dependency`. The native path can never name
those four, which removes every relationship that terminates on one.

### Relationships: 4 of 12, and only 3 with evidence

| State | Types |
| --- | --- |
| Available | `implemented_by`, `parent_of`, `contributes_to` |
| Available without evidence | `owned_by_team` |
| Unreachable | `blocked_by`, `references`, `depends_on`, `shares_dependency_with`, `reviews`, `deploys`, `operates`, `belongs_to_portfolio` |

Four distinct mechanisms produce that list:

- **Endpoint kinds discarded.** `WorkGraphNeighborEdge` carries
  `source_type` and `target_type`; `DevGraphEdge` has no field for either,
  and `_wire_work_graph_content` drops both. The types survive only inside an
  evidence *display label*, which is human copy. So no work-graph edge can
  become a `LineageHop`, because `LineageHop.validate_direction_matches_allowlist`
  needs both endpoint kinds. This is filed as an out-of-scope defect; parsing
  the label back out would be exactly the fabrication this arm exists not to
  commit.
- **Sub-kinds flattened.** `status_snapshot` merges declared, child and
  blocker facts into one `status_facts` list, and the executor cannot recover
  which sub-kind a fact came from. A blocker is therefore indistinguishable
  from a status assessment, so `blocked_by` is unreachable.
- **No registered adapter.** `REVIEW`, `CODE_CHANGE`, `TEST_REPORT`,
  `OPERATIONAL_CONTROL`, `COGNITIVE_LOAD` and `INVESTMENT_ALLOCATION` all
  carry `requirement="not_applicable"` and an honest empty relationship
  vocabulary in the relationship matrix.
- **Traversal depth fixed at one.** `work_graph_neighbors_service` rejects
  any depth other than one, so `shares_dependency_with` — inherently a
  two-hop fact — is unreachable by construction.

### Source classes: 10 of 16 observed

Unobserved: `code_change`, `review`, `test_report`, `operational_control`,
`cognitive_load`, `investment_allocation`. The set is *derived* from the
relationship matrix rather than hand-listed, so wiring an adapter widens it
automatically.

`cognitive_load` and `investment_allocation` deserve a note. The team
workload service genuinely computes both, but no plan step declares a source
requirement under either class — they ride under `HEALTH_PROFILE`. So the
measurement exists and the source class is unobserved, and the packet's
`MissingSource.impact` says exactly that rather than implying the number does
not exist.

### The headline: no native run can assert a driver

Three landed contract rules compose:

1. Principal standing requires a supporting relationship path.
2. A supported outcome requires at least one principal or contributing
   driver; a non-supported outcome must carry none.
3. A supported outcome requires every packet section its question family
   demands.

Every family the native interpreter classifies into —
`project_status_drivers`, `pressure_signals`, `struggling_teams`,
`declared_versus_actual` — also requires a populated `related_context`, and
the native arm has no projectable lineage to put there.

So a native packet for a substantive question carries real subjects, a real
cohort, a real evidence index, genuine source-health and missing-source
disclosure, and a measured deficiency finding recorded at `candidate_only` —
with outcome `unsupported` and the reason stated. That is the baseline's
principal measurement, and `test_no_native_run_can_assert_a_driver_today`
pins it against the family registry so a future change fails loudly.

This is not a claim that Ask Dev is unhelpful. The deterministic services
measure real things and the product answers real questions. It is a claim
about one specific axis: the current path cannot fill the shared contract's
driver and lineage sections.

### Question families: 5 native intents have none

`REMAINING_WORK`, `OBSERVED_CHANGE`, `REGISTERED_STATISTICS`,
`METRIC_COMPARISON` and `DATA_TRUST` map to no family at all. Each reduces to
reporting a metric or a source state, which every frozen family lists under
`MANDATORY_PROHIBITED_REDUCTIONS` (`single_dashboard_metric`) as something an
answer may not be. Those runs are reported unprojectable with that reason
rather than forced into the nearest family.

## Anti-corpus-tuning

`classify_question_family(intent_id, shape)` takes **no question text**. A
signature that never receives the question cannot branch on it, which is the
structural form of CHAOS-3618's "do not add bespoke case-specific logic
merely to make the baseline score better". A test asserts the signature keeps
that shape.

`comparison_shape_for` separates the two ways the native path reaches
organization scope: a genuinely org-scoped question becomes `portfolio_wide`,
while org scope reached *with a named reference still unresolved* becomes
`organization_wide` and can only ever reach the clarification family. That is
the producer-side half of `validate_no_unsafe_organization_widening`.

## The shadow seam

`api/dev/investigation_shadow.py` accepts either arm's packet and produces
one comparable record. It sits beside `qua_shadow.py` and copies its posture,
because that posture is already proven in production.

- **Arm-neutral by construction.** The module imports neither arm and
  `evaluate()` has no `arm_id` parameter — identity is read off
  `versions.trial`, so a caller cannot mislabel another arm's packet.
- **Isolated.** Every branch returns a record; nothing raises. The
  orchestrator will additionally wrap the call, exactly as it wraps the QUA
  shadow, because a shadow-mode bug must never fail the run it shadows.
- **No live model call.** No provider is a parameter of any function here.
- **Canonical services keep their authority.** `canonical_bypass_offenders`
  rejects any packet citing an evidence handle no canonical service minted
  for that run. A fabricated measurement cannot arrive as a wrong number —
  the packet contract has nowhere to put one — so it arrives as a citation to
  a handle that never existed, and that is what is checked.
- **Comparable.** Every record carries arm identity, packet schema version,
  projection version and the full evidence-handle lineage.

## Flags

Both are off unless explicitly set to `1`, matching the CHAOS-3617
convention:

| Flag | Effect |
| --- | --- |
| `CONTEXT_FABRIC_NATIVE_PROJECTION_ENABLED` | Native arm projection |
| `CONTEXT_FABRIC_SHADOW_SYNTHESIS_ENABLED` | Shadow seam |

## Historical slice

The native arm emits the `current` slice only. As-of traversal needs
native work-graph edge validity (CHAOS-3569); until it lands, historical rows
are reported `not_comparable_missing_edge_validity` with that capability
named. This does not block the current team/project/status/driver rows.
