---
page_id: con-ask-dev-investigation-packet
summary: The backend-neutral ask_dev_investigation_packet.v1 contract, its question/scoring/fault registries, and the cross-repository ownership map.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/api/dev/investigation_contract/
  - contracts/ask-dev-investigation/v1/
  - scripts/verify_chaos_3615_fault_mode_guards.py
  - Correction Addendum -- Graph-assisted Ask Dev intelligence, ambiguity and driver lineage (Linear, project Context Fabric)
applicability: current
lifecycle: active
---

# Ask Dev investigation packet (`ask_dev_investigation_packet.v1`)

CHAOS-3615 freezes one bounded, backend-neutral investigation contract and
the evaluation framework that goes with it, **before** either arm of the
corrected CHAOS-3614 trial is implemented. The point is narrow and worth
stating plainly: if the contract is written after the arms, whichever arm is
built first defines the questions, the output shape, and the definition of
success — and the comparison is decided before it is run.
{: .fc-page-lede }

This page explains what the packet is, what it deliberately is not, how a
future native arm (CHAOS-3618) and a future graph-assisted arm (CHAOS-3617)
must use it, and which repository owns which part of it.

## Governing rule

> The graph determines what is relevant; canonical services determine what is
> measurable; Ask Dev explains what the combined evidence means.

Every design decision below follows from that split. An investigation may
propose that a dependency stall is the principal driver of a project's
delay. It may not decide that the project is 40% complete, that a team is
understaffed, or that a release is ready — those are canonical measurements,
and the packet carries only references to them, tagged with how they were
obtained.

## What the packet is, and is not

A packet is an **investigation result**: what was looked for, what was
found, how well it is supported, and what could not be established. It is
consumed by the server-owned Ask Dev frame, which synthesizes the answer.

It is explicitly **not** a final user answer, a dashboard response, a
graph-native query response, an LLM reasoning trace, or arbitrary traversal
output. Two of those are prevented structurally rather than by convention:

- there is no prose field anywhere on the contract that an assistant would
  speak — every text field is a bounded rationale, limitation or summary
  attached to a specific structured claim;
- a packet whose outcome is `supported` must assert at least one principal
  or contributing driver, so "here are some links, you work it out" is not a
  representable success.

## Sections

| Section | Contract | What it carries |
| --- | --- | --- |
| Analytical job | `ask_dev_analytical_job.v1` | Normalized job, question family, comparison shape, bounded time context and slice, safe surface/conversation references, interpretation limits |
| Subject discovery | `ask_dev_subject_discovery.v1` | Ranked candidates with match signals and rationale, proposed vs committed state, unresolved mentions, authorization-filtered count |
| Comparison cohort | `ask_dev_comparison_cohort.v1` | Members with inclusion basis and rationale, explicit exclusions with reasons, supported comparison dimensions, completeness and uncertainty |
| Related context | `ask_dev_related_context.v1` | Canonical related entities, bounded lineage paths with per-hop direction and type, the authorized entity set, truncation state |
| Driver analysis | `ask_dev_driver_analysis.v1` | Driver candidates with symptom-vs-driver role, standing, measured/source-asserted/inferred basis, supporting paths and evidence, conflicts, exclusion reasons |
| Evidence and coverage | `ask_dev_evidence_coverage.v1` | The evidence index, source health, missing sources, conflicts, limitations, clarification needs |
| Versions | `ask_dev_investigation_versions.v1` | Schema, query, ranking and projection versions, per-source contract versions, and **optional** trial metadata |

## The five properties that are load-bearing

### 1. Backend neutrality

No graph backend, graph query language, or graph-store concept appears
anywhere on the wire. Arm identity lives in `TrialMetadata`, which is an
*optional* field: a native packet is complete without it, no consumer may
branch on it, and no field anywhere is mandatory for one arm only.

This is checked against the **generated artifacts** — schemas, every
fixture, every registry file — rather than against the Python source, since
the artifacts are what a consumer reads. A neutral source that generated a
leaky schema would pass a source-only scan.

### 2. Commitment is not a prerequisite for discovery

"What teams are currently struggling?" names no subject. An arm that
requires a committed subject before it will investigate cannot answer that
family at all, so the contract permits a packet with zero committed
subjects and a fully populated related-context and driver section. A golden
variant exercises exactly that.

For the same reason the analytical job carries a `job_uncertainty` of
`broad_with_uncertainty` or `ambiguous`, with declared interpretation
limitations. Nothing requires a pre-enumerated intent; what is required is
that the arm say what it decided the question meant.

### 3. No person is ever a subject

Person-level productivity, health, workload and staffing ranking is
prohibited, and the prohibition is structural in two independent places:
`InvestigationSubjectKind` has no person member, and `ComparisonDimension`
has no per-person axis. A validator could be routed around by a future
producer; an absent enum member cannot be.

### 4. Missing staffing data qualifies, it never disqualifies

A capacity or staffing driver must carry a `StaffingQualification`, and when
its denominator is partial or absent the claim may not be presented as
`measured_certain`. It may still be made, at `qualified` or `uncertain`
confidence — a golden variant pins that, because a guard that quietly turned
"qualify" into "refuse" would pass every negative test while making the
capacity families unanswerable.

### 5. No disclosure field has a reassuring default

Every boolean and every `*_count` on the contract is required with no
default. The convenient defaults here are all the flattering ones —
`authorization_filtered_count = 0`, `truncated = False` — so a producer that
forgot the field would be indistinguishable from one that had nothing to
disclose. A test derives this from the models themselves rather than from a
hand-maintained list, so a disclosure field added later cannot dodge it.

## Current versus historical slices

`SLICE_BOUNDARIES` declares what each slice needs. The current slice needs
no as-of instant and consults no edge-validity interval; it is deliberately
the slice CHAOS-3569 does not block, because the corrective plan forbids
blocking current-intelligence discovery on historical-edge modelling.

The historical and current-vs-historical slices need both. CHAOS-3569
(native historical edge validity) is open, so rows whose edges carry no
validity interval cannot have their as-of state reconstructed. The ruling is
that such rows are **NOT COMPARABLE, not blockers**: the packet declares
`not_comparable_missing_edge_validity`, discloses a
`historical_slice_not_comparable` limitation, and stays a valid, supported
investigation. The trial scores that row NOT COMPARABLE rather than failing
it.

## Registries

Four registries ship as machine-readable JSON under
`contracts/ask-dev-investigation/v1/registries/`, so CHAOS-3616 can consume
them without importing Python.

- **Question families** (10). Each declares exact and natural variants,
  required source classes, required packet sections, applicable scoring
  dimensions, a staffing-denominator policy and prohibited reductions. The
  floors — one exact variant, two natural variants, two source classes, two
  sections, three dimensions — are what make "a family is never one metric
  and never one prompt" checkable rather than aspirational.
- **Scoring dimensions** (28). Each names the packet fields it reads and the
  fault modes it can fail. `reported_per_question_family` and
  `aggregate_prohibited` are `Literal[True]`, so "do not collapse these into
  one aggregate score" cannot be flipped by a value change.
- **Fault modes** (11). The corrective plan's own named bad behaviours. Each
  says whether the contract rejects it (`contract_validator`, naming the
  exact validator), whether the field grammar rejects it (`required_field`),
  or whether it is an oracle judgment CHAOS-3616 must score — and names the
  test that proves it.
- **Trial allowlists.** The source classes an arm may draw on with a stated
  reason for each; the twelve closed technical relationship types with their
  canonical orientation; the slice boundaries.

The relationship allowlist bounds traversal and gives "a relationship is
reversed" a definition. It is deliberately **not** a requirement to
pre-model every human question: these are technical edges between canonical
entities (a PR implements an issue; a project depends on a service). "Why is
ACR still not finished?" is answered by composing those edges with canonical
measurements, not by adding a `why_is_it_not_finished` edge type.

## How the fault-mode guards are proved

`tests/api/dev/test_chaos_3615_fault_modes.py` feeds each validator an
arm-shaped bad packet — a payload an implementation could plausibly emit,
differing from the golden only in the behaviour under test — and asserts
both that it is rejected and that the rejection message came from the
*named* guard.

That alone would not prove the guard is load-bearing: a payload can be
caught incidentally by an unrelated field constraint while the real guard is
missing, and the test would still be green.
`scripts/verify_chaos_3615_fault_mode_guards.py` closes that gap by
inverting the experiment. For each of the eleven fault modes it runs a
subprocess that removes exactly one guard — deleting a single
`model_validator` and rebuilding the affected schemas, or giving the
disclosure field the default it does not have — and requires the bad packet
to then be **accepted**. A guard whose removal changes nothing is not the
guard the registry claims it is. The script is wired into the suite and
fails if any registered fault mode has no injection case.

## How a future arm must use this

Both arms are out of scope for CHAOS-3615. When they are authorized:

1. **Emit this packet, unchanged.** An arm that needs a field the contract
   does not have raises a contract change, not a private extension —
   `extra="forbid"` makes the private extension impossible anyway.
2. **Put arm identity in `versions.trial` and nowhere else.** If a consumer
   would behave differently knowing which arm produced a packet, the
   comparison is already compromised.
3. **Draw only on allowlisted sources and relationship types.** Both
   allowlists are closed; widening either is a reviewed contract change.
4. **Let canonical services own every measurement.** The graph proposes;
   `assertion_basis` records which service measured what, and only
   `measured` may be presented as certain.
5. **Regenerate artifacts, never hand-edit them.** `python -m
   dev_health_ops.api.dev.investigation_contract.export write`; the `check`
   mode compares the whole artifact set and is a test.

## Cross-repository ownership map

| Repository | Owns | Consumes | Rationale |
| --- | --- | --- | --- |
| `dev-health-ops` | The contract, its generated artifacts, all four registries, the fixtures and the fault-mode guards. Both future arms and their producers. | Its own `dev_evidence_ref.v1` / `EvidenceHandle` vocabulary, `SourceClass`, `SourceRequirementState`. | The packet is produced by Ask Dev's own investigation path, which is Python and lives here. A graph-assisted arm would use a Python graph library, which is also here. |
| `dev-health-acr` | Nothing in this contract. | Nothing from it. | ACR's `AGENTS.md` bans Python runtime and contract-checking code outright, and separately bans adding a graph database during SVS. Its `evidence_ref.v1` is a *different* contract from ops' `dev_evidence_ref.v1` despite the similar name; ops imports nothing from ACR's contract tree. |
| `dev-health-web` | Nothing. | Nothing. | The packet is an internal trial artifact and is never served to a client. No user-visible Ask Dev behaviour changes in this issue. |

### Why the packet does not live under `contracts/ask-dev/v2`

`contracts/ask-dev/v2` is reserved for wire contracts served to real
clients, and is consumed by web's contract sync. Filing an internal trial
artifact there would misrepresent it and would put CHAOS-3616 iterations on
the critical path of a web contract regeneration. The packet gets its own
root, `contracts/ask-dev-investigation/v1`, with its own exporter instance
and its own manifest. A test asserts the two registries do not overlap.

### If a Go or TypeScript consumer ever needs this

None does today, so no unused generated types are shipped — a generated type
nobody compiles is a maintenance cost with no drift-detection value. The
established vendoring pattern, should one appear, is web's
`scripts/sync-acr-contracts.mjs`: copy from a pinned source commit, keep an
explicit path closure, digest every file, and assert currency in CI. This
packet's exporter already emits the manifest half of that (a sha256 per
artifact), so a future consumer vendors against the manifest rather than
re-deriving the schema.

## Out of scope for CHAOS-3615

No Graphiti or other graph-backend implementation. No native comparison arm.
No trial run, corpus or oracle. No user-visible Ask Dev change. No graph
data exposed through ACR or MCP. No projection, retention or graph-store
migration. No corrected ADR. CHAOS-3616 through CHAOS-3621 remain
unauthorized and must not be self-started from this contract.
