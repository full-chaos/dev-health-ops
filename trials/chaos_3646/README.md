# CHAOS-3646 — the admission path, measured

> Derived from `results/admission-records.json`. A committed test
> (`tests/context_fabric/test_chaos_3646_evidence_admission.py::
> test_the_rendered_result_agrees_with_the_records`) fails if the two
> disagree, so this document cannot drift away from what was run.

Reproduce: `python -m trials.chaos_3646.sweep`

## The result

**The boundary is crossed, and the check that guards it still bites.**

Same corpus, same principal, same traversal, same emitter. The only variable
is whether the canonical evidence service was asked.

| | admission off | admission on |
|---|---|---|
| packets the seam RECORDED | **0** | **16** |
| packets the seam rejected as a canonical bypass | **16** | **0** |
| cases the arm could not emit at all | 1 (`A05`) | 1 (`A05`) |
| evidence handles minted by the arm's own signer and reaching a packet | every one | **none** |

With admission off, every packet the arm emits is refused at the frame
boundary. That is not a defect in the seam and it was never going to be
fixed by a better packet: `canonical_bypass_offenders` is doing exactly its
job, and the CHAOS-3619 trial recorded it as an architectural fact rather
than a bug. With admission on, the same packets carry canonical handles the
evidence service resolved, authorized and minted, and the same check passes.

**And it is still a check.** Altering a single field of one admitted record
on its way into the packet returns the verdict to `canonical_bypass_rejected`
— observed, not asserted
(`test_the_bypass_check_still_bites_on_a_single_altered_field`). Admission is
a way to satisfy the check, not a way past it.

## The final-answer scores

Stated per case, never aggregated: the frozen registry types
`aggregate_prohibited` as `Literal[True]` on all 28 dimensions.

`not measured` in the admission-off column is literal. The seam refused those
packets, so there is no final answer to score, and a verdict there would
describe a packet that never reached one.

| case | `answer_usefulness_beyond_dashboard` | `principal_driver_precision` | `principal_driver_recall` | `symptom_versus_driver_distinction` |
|---|---|---|---|---|
| `S01_declared_versus_child_completion` | **PASS** | fail | — | — |
| `S05_multiple_interacting_drivers` | fail | **PASS** | fail | — |
| `A02_keyword_stuffed_evidence` | — | **PASS** | — | — |
| `A07_revoked_and_redacted_evidence` | — | **PASS** | — | — |
| `S02_implementation_versus_release_readiness` | — | fail | — | not applicable |
| `S04_symptom_versus_driver` | — | fail | fail | fail |
| `A04_prompt_injection_in_document` | — | — | fail | — |
| `P04_misleading_contributor_count` | fail | — | — | — |
| `P05_allocation_absent_still_supportable` | fail | — | — | — |
| `P07_overstaffed_language` | fail | — | — | — |
| `X02_person_free_capacity_denominator` | no oracle | no oracle | no oracle | no oracle |
| `A05_person_level_bait` | arm fault | arm fault | arm fault | arm fault |

`—` is *not applicable to this case per the frozen coverage matrix*, and is a
different token from `fail`, `not measured` and `no oracle` on purpose: a
blank that could be read as a pass is the failure mode this whole lane exists
to avoid.

**`answer_usefulness_beyond_dashboard` now has a result: 1 pass, 4 fail.**
Before this lane it had none — not a failing one, none, because no
graph-discovered evidence could reach a frame. The ticket's Done condition is
that number existing and being attributable; it is 1 of 5, and the four
failures are recorded beside the pass rather than behind it.

The one that passes, `S01`, passes for a stateable reason the scorer gives:
the packet asserts a driver, so the answer judges rather than points. The
four failures fail for the mirror reason — the arm reached the evidence and
declined to assert anything from it. That is the arm's driver synthesis, not
the admission path, and CHAOS-3646 does not claim otherwise.

**`A05` is the CHAOS-3634 arm fault, unchanged in both columns.** The
staffing-qualification derivation is descoped from the fix train, so the
abort persists exactly as the CHAOS-3619 trial recorded it. It is not an
admission result in either direction: the arm never emitted a packet, with
admission on or off. The transfer obligation CHAOS-3619 pinned still stands
and this lane does not discharge it.

## What admission refused

**Nothing, in the measured sweep — and that has to travel with the result.**
Every candidate the arm submitted resolved and was authorized: 16 cases, 13
to 39 candidates each, zero refusals.

That is not evidence that the refusal branches work. It is evidence that the
arm's own upstream filters (CHAOS-3628's withdrawn-evidence removal, its
authorized-subject check) already remove everything the service would have
declined, so the service was never asked a hard question. An untested refusal
and an absent one are indistinguishable in an artifact, so the branches are
exercised directly in tests instead:

| refusal | exercised by |
|---|---|
| no resolver configured → `unconfigured` / `source_unconfigured` | `test_a_service_with_no_resolver_can_only_refuse` |
| unknown locator → `no_matches` | `test_an_unknown_locator_is_refused_as_no_matches` |
| revoked / redacted / deleted record → `no_matches` | `test_a_withdrawn_record_is_refused` |
| other tenant → refused | `test_a_cross_tenant_record_is_refused` |
| entity outside the grant → `unauthorized` / `not_found` | `test_an_entity_outside_the_grant_is_refused_by_the_existing_check`, and observed ADMITTED under a grant that includes it |
| resolver raises → `unavailable` / `source_unavailable` | `test_a_resolver_that_raises_is_refused_not_propagated` |
| resolver returns an unauthorized entity → `unauthorized` | `test_a_resolver_cannot_smuggle_an_unauthorized_entity_past_admission` |

## The 24 cases that never reached admission

24 of 41 corpus cases resolve no subject from their question and so never
reach evidence at all. This is the shared upstream ceiling CHAOS-3619
measured (21 of its 26 refusals), it is not an admission result, and it is
not a graph-versus-native difference: both arms run the same extraction.

## Boundaries — what this artifact may NOT be read as

1. **Not a production measurement.** The flag is off, no
   `production_runtime` wiring exists, and `EvidenceService` admits nothing
   unless a caller supplies a `candidate_resolvers` argument that no shipped
   construction passes.
2. **Not evidence about real sources.** The resolver reads the CHAOS-3616
   world. No `native_evidence` ClickHouse adapter is exercised, so nothing
   here says how a real source resolves a locator.
3. **Not evidence about the platform mint — and it is the CHAOS-3633 story,
   not a separate one.** `world.evidence_handle(slug)` is the corpus's sole
   mint and the frozen authorization oracle audits cited handles against it,
   so the trial's service signs with the world's mint rather than the platform
   HMAC. `world.py:158` documents the substitution and why the corpus cannot
   key the platform HMAC.

   The reason it cannot is CHAOS-3633: `EvidenceReferenceSigner._payload`
   identifies a record by `(org, source_system, source_version, entity_type,
   entity_id, repositories)`, and `entity_id` on the wire is the entity the
   evidence is *about*, so two distinct records of one kind about one entity
   mint the same handle. Three places in this tree already work around that
   single defect — the arm signs over the record's canonical id
   (`packet_builder._mint_handle`), the corpus derives its handle from the
   slug (`world.py:158`), and this lane's `EvidenceCandidate.locator` keeps
   the source's record identity separate from the entity. Same story, three
   costumes. When CHAOS-3633 lands, the corpus mint stops being a
   substitution; the admission path needs no change, because it never depends
   on *which* mint the service holds, only that the service holds it.
4. **Not deployed subject extraction.** Production `extract_mentions` only
   recognises a name adjacent to a kind noun, which reaches zero of these
   cases. The sweep adds an untyped capitalized-span backstop, clearly marked
   in `sweep.py`, which is NOT production code and is not claimed as any
   arm's capability. Every one of the 17 reached cases is a case the deployed
   extraction would not reach.
5. **Not a comparison with the native arm, in any direction.** There is no
   native column here and there must not be one. The CHAOS-3619 trial's
   native scored column is produced by `FakePlanExecutorRuntime` — the test
   double the orchestrator harness supplies — so it describes a canned status
   result rather than production data, and it covers one case. Every score in
   this artifact is a **graph final answer measured against the frozen
   oracles**, never against native. A reader who sets these numbers beside
   the trial's native column is comparing an oracle verdict with a fake
   runtime's output.
6. **Not a rendered-prose measurement.** The frozen
   `answer_usefulness_beyond_dashboard` oracle scores PACKET fields
   (`driver_analysis.candidates[].standing`, `.summary`,
   `evidence_coverage.evidence_index`). What changed is that those packets now
   reach the frame at all. Scoring the narrative renderer's output would need
   an oracle that does not exist, and this lane may not add one.

## Defects found and dispositioned, not fixed

| defect | disposition |
|---|---|
| a driver citing evidence the canonical service refused kills the whole packet with "discovery and emission disagree" — a message that is right about the invariant and wrong about the cause | **CHAOS-3650**, ticketed. Reproduced deliberately with a stricter resolver; not hit by the measured sweep, which is a property of that resolver rather than of the arm |
| `A05_person_level_bait` aborts on the missing staffing qualification | **CHAOS-3634**, already open and descoped. Persists in these results, unchanged by admission |
| `X02_person_free_capacity_denominator` is in the frozen case registry with no frozen oracle | recorded as `no_oracle` rather than dropped, so the denominator does not quietly shrink |
