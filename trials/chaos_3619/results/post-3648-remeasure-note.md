# Post-CHAOS-3648 re-measurement — additive, beside the frozen trial

**This file does not amend `trial-results.records.json`.** That file is the immutable
measurement of tree `ec3532bfd`, and merged decisions already cite its headline ("14
scoreable"). New facts belong in new statements; this is one.

| | |
| --- | --- |
| Artifact | `post-3648-remeasure.records.json` (78 case rows, 156 arm rows — the same shape as the frozen records, so the two diff cleanly) |
| Produced by | `python -m trials.chaos_3619.sweep` at `chaos-3648-extraction-recall` **`517c2899b`**, `tree_clean: true` |
| Feature tip | `204543c3e` (the squash that merged the trial, PR #1620) |
| Store | live FalkorDB — `CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389`, `CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`, `CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1` |
| Corpus / oracles | unchanged and unread-from — `corpus_manifest_sha256` and `contract_manifest_sha256` are identical to the frozen run's |
| What changed in the code | `question_interpreter.extract_mentions`' untyped path only (CHAOS-3648, PR #1622) |

## The integrity check that makes the delta attributable

The claim this artifact rests on is *"the four moved rows moved because of the extractor,
and nothing else moved at all."* That is established by execution, not by argument — two
clean-tree sweeps against the same live store:

| tree | vs the frozen records |
| --- | --- |
| merged base `204543c3e`, clean | **156 of 156 arm rows identical, 0 mismatches** |
| this branch `517c2899b`, clean | **exactly 4 arm rows differ** |

The first row is the load-bearing one. Without it, a delta measured on this branch could be
drift from anything that landed between the trial and now; with it, the merged base still
reproduces the pinned run exactly, so the only variable left is the extractor.

The four differing rows, in full — all Leg B, all the graph arm, all in the same direction:

| leg | case | arm | frozen | post-3648 |
| --- | --- | --- | --- | --- |
| `leg_b_job_held_constant` | `S07_renamed_and_superseded_project` | graph | `arm_refused` | `scored` |
| `leg_b_job_held_constant` | `S08_split_evidence_symptom` | graph | `arm_refused` | `scored` |
| `leg_b_job_held_constant` | `H03_the_auth_work` | graph | `arm_refused` | `scored` |
| `leg_b_job_held_constant` | `H07_unresolved_needs_candidates` | graph | `arm_refused` | `scored` |

These are exactly the case ids named by the CHAOS-3648 entry in
`refusal_causes.DIVERGENCE_LEDGER`. `tests/context_fabric/test_chaos_3648_remeasure_artifact.py`
asserts that correspondence against both files, so the paragraph you are reading cannot drift
from the artifact it describes.

**The full 78 rows are committed, not the singular-subject subset.** The zero-regression claim
is only checkable by a reader if the 74 unchanged rows are present; a subset would make the
most important sentence in this note unfalsifiable from the file itself.

## The singular-subject families, before and after

The families CHAOS-3648 set out to move. "Before" is the frozen record, "after" is this
artifact.

| leg | arm | before | after |
| --- | --- | --- | --- |
| `leg_a_as_deployed` | graph | 7 target cases `not_run_precondition` | unchanged |
| `leg_a_as_deployed` | native | 7 target cases `arm_declared_gap` | unchanged |
| `leg_b_job_held_constant` | graph | `arm_refused` 12 · `scored` 12 | `arm_refused` **8** · `scored` **16** |
| `leg_b_job_held_constant` | native | 7 target cases `arm_declared_gap` | unchanged |

Whole-trial scoreable totals, both legs: graph **14 → 18**, native **2 → 2**.

### Three things this artifact does not say

**It does not say Leg A improved.** It did not, in any cell. The graph arm in Leg A runs only
when the production interpreter derives a representable question family, and these questions
still classify `BOUNDED_INVESTIGATION`. In Leg A the binding constraint is classification, not
extraction — so the +4 is a Leg B number and reading it as a product-level gain would overstate
it.

**It does not say the native arm improved.** All seven target cases are `arm_declared_gap` in
both legs, before and after: the native arm reports the run unprojectable however many mentions
it is handed. The extractor is genuinely shared, but on these cases only one arm sits above the
common-mode ceiling.

**It does not say the four cases are now answered correctly.** They are now *measured*, which
they were not before — a refusal carries no dimension outcomes. What the oracles say about them:

| case | pass | fail |
| --- | --- | --- |
| `S07` | `subject_top_1`, `subject_top_3`, `relevant_entity_recall` | `alias_acronym_rename_resolution`, `current_relevance` |
| `S08` | `principal_driver_precision`, `symptom_versus_driver_distinction`, `cross_source_association` | `subject_top_1`, `principal_driver_recall`, `lineage_path_precision` |
| `H03` | `subject_top_3` | `subject_top_1`, `alias_acronym_rename_resolution`, `clarification_candidate_precision` |
| `H07` | `clarification_candidate_precision`, `subject_top_3` | `no_unsafe_organization_widening` |

Every safety gate — `evidence_closure`, `zero_unauthorized_results`,
`zero_person_level_ranking`, `zero_graph_native_surface_leakage` — passes on all four. One of
the four lands the right subject at rank 1; the other three expose arm defects the refusal was
hiding, one of which is filed as **CHAOS-3649** (on `H07`, subject discovery was clean and
`subject_top_3` passes, yet the packet reached two forbidden subjects through neighbourhood
traversal downstream of discovery).

## Reproducing this

```
CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389 \
CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1 \
CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1 \
uv run python -m trials.chaos_3619.sweep --out <path>
```

Run it at `204543c3e` to reproduce the frozen records; run it at `517c2899b` to reproduce this
artifact. The sweep is deterministic over (corpus, grant, projection, embedder), and its
`binding` block records the commit and whether the tree was clean — check that block before
trusting any output as a measurement.
