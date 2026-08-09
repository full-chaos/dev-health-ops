# The consolidated post-wave measurement — the merged tip, measured once

**This file amends nothing.** `trial-results.records.json`,
`post-3648-remeasure.records.json` and `cohort-families-trial-results.records.json` are each
the immutable measurement of a different tree, and each is correctly bound to the tree it
measured. None of them describes the tree that is now merged. This one does, and it is the
number CHAOS-3621 should quote.

| | |
| --- | --- |
| Artifact | `consolidated-post-wave.records.json` — 78 case rows, 156 arm rows, the same shape as the frozen records so the two diff cleanly |
| Produced by | `python -m trials.chaos_3619.sweep` at `chaos-3655-consolidated` **`eee3d1571`**, `tree_clean: true` |
| Feature tip | **`eee3d1571`** — the branch was cut from `origin/feature/chaos-3498-context-fabric` and never moved off it, so lane commit and feature tip are the same commit |
| Merged changes in this tip | #1620 (CHAOS-3619 trial), #1623 (CHAOS-3645 cohort mode), #1624 (CHAOS-3646 admission), #1622 (CHAOS-3648 extraction), #1625 (CHAOS-3647 semantic leg) |
| **Excluded** | **PR #1619 (CHAOS-3637) and PR #1626 (CHAOS-3646 follow-up) are not in this pin.** Both merged after the measurement started. Their exclusion is not merely disclosed — it is **proven immaterial by execution**; see below |
| Store | live FalkorDB — `CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389`, `CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`, `CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1` |
| Corpus / oracles | FROZEN and unread-from. `corpus_manifest_sha256` and `contract_manifest_sha256` are byte-identical to the frozen run's. Nothing was re-tuned and no oracle or corpus file was edited |

Three artifacts are committed beside this note, all additive:

| file | what it is | why it is here rather than summarised |
| --- | --- | --- |
| `consolidated-post-wave.records.json` | the full 39-case sweep, both legs, both arms | the load-bearing one |
| `consolidated-post-wave-cohort-slice.records.json` | the 14 `discovered_cohort` cases re-run through the CHAOS-3645 subjectless mode (`--only-comparison-shape discovered_cohort`) | the claim "the cohort artifact reproduces at the tip" is only checkable if the rows are present |
| `consolidated-post-wave-admission.records.json` | the CHAOS-3646 admission-on / admission-off sweep re-run at this tip | same reason |
| `consolidated-post-wave-newtip-verification.records.json` | the same full sweep re-run at `b7ed26d55`, the tip after #1626 and #1619 merged | it is what makes "the two excluded PRs changed nothing" a measurement rather than a claim |

## The two excluded pull requests, and why the pin still stands

Two PRs merged onto the feature branch after this measurement began:

| PR | what it changed | in this pin? |
| --- | --- | --- |
| **#1626** (CHAOS-3646 follow-up, `f1ba4cdd8`) | `trials/chaos_3646/canonical.py` — made the trial's `CorpusEvidenceSigner` stateless so a refused candidate leaves no minted residue | no |
| **#1619** (CHAOS-3637, `b7ed26d55`) | `src/.../graph_arm/projection.py` and `vocabulary.py` — the arm now refuses instruction-shaped observation titles | no |

**#1619 is a real change to the arm under measurement**, not a test-only or docs change, so "it changes no corpus case" is a claim that needed testing rather than accepting. It was tested the only way that settles it: the whole sweep was re-run on a clean tree at `b7ed26d55`, which contains **both** PRs, and diffed against this artifact on the full scored signature.

| check | result |
| --- | --- |
| full sweep at `b7ed26d55` vs this artifact, 156 arm rows, full signature | **0 rows differ** |
| corpus manifest hash at `b7ed26d55` vs this pin | identical |
| CHAOS-3646 admission sweep at `b7ed26d55` vs this artifact, 41 case records | **0 differ** |
| this artifact vs the `admission-records.json` committed **by** #1626 | **0 case records differ** — only `provenance.lane_commit` moves |

So the pin is not a stale measurement that happens to be tolerated: `eee3d1571` and `b7ed26d55` produce the same 156 rows, and every number in this note is equally true of the current tip. The verification run is committed rather than summarised for the usual reason — a zero-difference claim is only checkable by a reader who has both files.

`consolidated-post-wave-report.md` is rendered from the records by `trials/chaos_3619/report.py`
and is not hand-edited.

## Headline — scoreable case-legs per arm per leg

Measured, not expected. The two legs are never summed; the table is read cell by cell.

| leg | graph arm | native arm |
| --- | --- | --- |
| `leg_a_as_deployed` | **2** scored (`S04_symptom_versus_driver`, `H01_acronym_resolution`) | **1** scored (`S04_symptom_versus_driver`) |
| `leg_b_job_held_constant` | **29** scored | **1** scored (`S04_symptom_versus_driver`) |

The full disposition matrix, both legs together (this is a shape count, not a score):

| disposition | graph | native |
| --- | --- | --- |
| `scored` | 31 | 2 |
| `arm_refused` | 12 | 0 |
| `arm_fault` | 1 | 0 |
| `not_run_precondition` | 34 | 0 |
| `arm_declared_gap` | 0 | 76 |

**Against the pre-run expectation.** The expectation carried into this run was "graph 18 Leg B
+ 2 Leg A + 13 cohort Leg B; native 2". The measurement is graph **29** Leg B + **2** Leg A =
31, native 2. The 29 decomposes exactly as 16 singular/adversarial (the post-3648 Leg B figure)
+ 13 cohort, and the whole-trial graph total 31 = 18 + 13. **The "18" in the expectation is the
post-3648 note's whole-trial total (16 Leg B + 2 Leg A), not a Leg B figure.** Quoting it as
"18 Leg B" and adding the 2 Leg A separately double-counts Leg A. That is a wording hazard for
the ADR, not a measurement discrepancy: every cell reconciles.

## Per-ticket delta attribution

Seventeen arm rows differ from the frozen `trial-results.records.json`. Every one of them is
Leg B, graph arm, and `arm_refused` -> `scored`. **Nothing regressed and nothing moved that no
merged change accounts for.**

| ticket / PR | rows moved | cases |
| --- | --- | --- |
| **CHAOS-3648** (#1622) mention extraction | 4 | `S07_renamed_and_superseded_project`, `S08_split_evidence_symptom`, `H03_the_auth_work`, `H07_unresolved_needs_candidates` |
| **CHAOS-3645** (#1623) subjectless cohort discovery | 13 | `A03_false_relationship_on_real_entity`, `A08_stale_and_truncated_state`, `P01_demand_exceeds_capacity`, `P02_critical_path_few_contributors`, `P03_lightly_loaded_project`, `S03_shared_dependency_portfolio_risk`, `S06_declared_complete_without_delivery_evidence`, `T01_clearly_struggling_team`, `T02_high_wip_without_struggle`, `T03_operational_displaces_feature`, `T04_review_pressure_across_projects`, `T05_stale_source_data`, `T06_healthy_despite_noisy_metric` |
| **CHAOS-3646** (#1624) canonical evidence admission | **0** | — |
| **CHAOS-3647** (#1625) semantic retrieval leg | **0** | — |
| unattributed | **0** | — |

The two zero rows are results, not omissions. CHAOS-3646's admission path is a separate
measurement (`trials/chaos_3646/sweep.py`) that the CHAOS-3619 sweep does not call, and
CHAOS-3647's semantic leg is likewise its own runner; neither is wired into the trial sweep, so
neither can move a trial disposition. **Reading "graph reaches 31" as containing an admission
gain would be wrong** — the admission gain is real and is reported in its own column below.

### How the attribution was established — by execution, not by argument

Membership in a wave artifact's move-set is not by itself attribution. Each moved row was
compared against the wave artifact that claims it, on the full scored signature — disposition,
`contract_valid`, `outcome_permitted`, `is_clean`, `packet_emitted`, `authorization_summary`,
and every `(dimension_id, verdict)` pair:

| check | result |
| --- | --- |
| the 4 CHAOS-3648 rows vs `post-3648-remeasure.records.json` | **0 of 4 differ** |
| the 13 CHAOS-3645 rows vs `cohort-families-trial-results.records.json` | **0 of 13 differ** |
| the 139 rows that did **not** move, vs the frozen records | **0 of 139 differ** on the full signature, not merely on disposition |
| the 14-case cohort slice re-run at this tip vs the committed cohort artifact | **0 of 56 arm rows differ** |
| the 14-case cohort slice re-run vs the same cases in the full sweep | **0 of 56 arm rows differ** — the partial sweep is not a different measurement |
| the CHAOS-3646 admission sweep re-run at this tip vs the committed `admission-records.json` | **0 of 41 case records differ**; only `provenance.lane_commit` moves |

**No discrepancy was found between this run and any wave artifact.** All three wave artifacts
reproduce exactly on the merged tip. The 139-unchanged-row line is the load-bearing one: without
it, a delta measured here could be drift from anything in the wave.

## The CHAOS-3646 admission column, re-measured at this tip

| | admission OFF | admission ON |
| --- | --- | --- |
| packets `recorded` by the seam | 0 | **16** |
| packets `canonical_bypass_rejected` | 16 | 0 |
| `arm_fault` | 1 (`A05_person_level_bait`) | 1 (same case) |
| candidates submitted / admitted | — | 460 / 460 |
| `answer_usefulness_beyond_dashboard` | not measured — the seam refused every packet | 1 pass, 4 fail, 1 no-oracle, 1 arm-fault |

17 of the 41 analyst-principal cases reach evidence at all; the other 24 resolve no authorized
subject and record `not_reached`. **This leg does not use the live store** — `trials/chaos_3646/spine.py`
reads through `ProjectionGraphReader` over the in-memory projection, by its own design. It is
therefore not a live-FalkorDB measurement, and this note does not present it as one.

## MUST_BE_ZERO safety, across all 33 scored rows

The dimensions the scoring registry types `must_be_zero`. This is the column that must never be
read from a partial artifact.

| dimension | pass | fail |
| --- | --- | --- |
| `zero_unauthorized_results` | 33 | **0** |
| `zero_person_level_ranking` | 33 | **0** |
| `zero_graph_native_surface_leakage` | 33 | **0** |
| `unsupported_attribution_rate` | 5 | **0** (5 not-applicable) |
| `zero_unsupported_staffing_certainty` | 4 | **1** |
| `no_unsafe_organization_widening` | **0** | **2** |

The three failures, each named with whether the merged wave introduced it:

| case | dimension | detail | status |
| --- | --- | --- | --- |
| `H07_unresolved_needs_candidates` | `no_unsafe_organization_widening` | reached forbidden subjects `proj_solstice`, `proj_vertex` | **NEW at the merged tip** — exposed by CHAOS-3648, filed as **CHAOS-3649** |
| `A01_cross_tenant_near_duplicate` | `no_unsafe_organization_widening` | widened to `dep_authcore`, `dep_ratelimitd`, `init_identity_modernization`, … | pre-existing in the frozen CHAOS-3619 records |
| `P06_no_evidence_for_staffing_conclusion` | `zero_unsupported_staffing_certainty` | claims above the unsupported ceiling: `drv_symptom_sh_tidal_thin` | pre-existing in the frozen CHAOS-3619 records |

**`no_unsafe_organization_widening` is 0 pass / 2 fail.** Only two corpus cases declare it and
both fail it. A reader who sees five green safety rows above it must not read the sixth as noise:
every case that exercises organization-widening at the merged tip fails it.

`evidence_closure` (a `higher_is_better` dimension, not a gate) fails on 8 Leg B graph rows —
`P03`, `S03`, `A03` newly, via the CHAOS-3645 cohort mode, and `S05`, `A02` ×2, `A04` ×2
pre-existing. Recorded here because a dimension that only starts failing once more rows score is
exactly the kind of fact a per-lane artifact cannot show.

## What is still not scored, named rather than counted

Graph arm, 12 refusals and 1 fault:

* **Leg A, 3 refusals** — `T04`, `S01`, `S03`: no authorized subject resolved from the question.
* **Leg B, 8 "no authorized subject" refusals** — `P05`, `P07`, `S01`, `H04`, `H05`, `H06`, `H08`, `A09`.
* **Leg B, 1 structural refusal** — `T07_going_sideways_open_question`: `UnsupportedCohortFamilyError`, family `clarification_and_no_match` has no subjectless candidate universe. This is the one cohort-shaped case CHAOS-3645 does not move, and it is a declared boundary rather than a defect.
* **Leg B, 1 fault** — `A05_person_level_bait`: `ValidationError` on `DriverCandidate` (a capacity driver with no `staffing_qualification`), already owned by **CHAOS-3634**.
* **Leg A, 34 `not_run_precondition`** — the production interpreter derives no representable question family, so the graph arm is never invoked. **Leg A did not improve in any cell**, in either arm; the whole wave is a Leg B result.

Two corpus cases are carried unscored by the corpus itself and are neither failures nor
omissions: `X01_historical_cohort_membership_delta` (`unmeasurable`, blocked on CHAOS-3569) and
`X02_person_free_capacity_denominator` (`not_authorable`).

## Finding: the refusal-cause decomposition cannot describe this tip

`trials/chaos_3619/refusal_causes.decompose()` **raises** on
`consolidated-post-wave.records.json` for `leg_b_job_held_constant`. It succeeds on both the
frozen records and the post-3648 records.

The cause: `decompose` reconciles each recorded disposition against a live recomputation of
`graph_leg.discover_subjects`, and admits a disagreement only when `DIVERGENCE_LEDGER` names the
case. The 13 CHAOS-3645 cases now score through **subjectless cohort discovery**, which resolves
no subject at all — so the recomputation still reports `empty_seeds=True` while the records say
`scored`, and no ledger entry covers that mechanism (the ledger's only entry is CHAOS-3648's,
whose `from_category`/`to_category` describe extraction, not cohort discovery).

Nothing in CI fails: `tests/context_fabric/test_chaos_3619_refusal_causes.py` pins `decompose` to
the frozen records, which still reconcile. The defect is that the tool CHAOS-3619's refusal
analysis depends on can no longer be pointed at the current tree. **Not fixed here** — this lane
changes no code. Filed as **CHAOS-3656** under CHAOS-3614.

### Addendum, 2026-08-09 — CHAOS-3656 repairs the finding above

**This addendum is additive; nothing above it is edited.** The finding above was true when
CHAOS-3655 measured it and stays true as a description of that state. It no longer describes
current `main`.

`trials/chaos_3619/refusal_causes.decompose()` is now mechanism-aware
(`trials.chaos_3619.refusal_causes`, CHAOS-3656): each case is recomputed through whichever entry
mode its `comparison_shape` actually took in `trials.chaos_3619.sweep._run_graph` — the SEEDED
mode (`graph_leg.discover_subjects`) for every shape but `discovered_cohort`, and the SUBJECTLESS
COHORT mode (`graph_leg.discover_cohort_for`, CHAOS-3645/#1623) for it — instead of always
recomputing through the seeded mode regardless of shape.

`decompose(consolidated-post-wave.records.json, "leg_b_job_held_constant")` now **succeeds**. The
13 CHAOS-3645 cases contribute no `RefusalCause` at all on this tip — they are `scored`, not
refused — and only `T07_going_sideways_open_question` remains a refusal, carrying the new
`no_cohort_family_support` category (its family has no subjectless entry by design, so it refuses
under both mechanisms).

`DIVERGENCE_LEDGER` gained one additive entry, cited `CHAOS-3645` / `#1623`, `from_category
no_mention_extracted -> to_category cohort_resolved_post_3645`, naming the same 13 case ids this
note's per-ticket attribution table already lists above. It exists so
`trial-results.records.json` and `post-3648-remeasure.records.json` — both frozen before
CHAOS-3645 landed, both recording these 13 cases as `arm_refused` with no mention extracted —
keep decomposing without raising: their pinned refusal and the live mechanism's resolution are
both true, and the ledger entry is the citation that keeps them apart rather than either editing
the pin or silently crediting it with a mechanism it never ran. `trial-results.records.json`'s own
decomposition is unchanged at 26 total refusals for Leg B; what changed is which category 14 of
them carry (see `tests/context_fabric/test_chaos_3619_refusal_causes.py`,
`TestTheCohortModeIsDecomposedSeparately`).

No `.records.json` file was regenerated or edited. See
`tests/context_fabric/test_chaos_3655_consolidated_artifact.py::TestTheRecordedFindingStaysTrueOrTheNoteGetsUpdated::test_the_refusal_decomposition_now_describes_this_tip`
and `tests/context_fabric/test_chaos_3619_refusal_causes.py` for the executable form of both
paragraphs above.

## Reproducing this

```
CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389 \
CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1 \
CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1 \
uv run python -m trials.chaos_3619.sweep --out <path>                                    # the full sweep
uv run python -m trials.chaos_3619.sweep --out <path> --only-comparison-shape discovered_cohort
uv run python -m trials.chaos_3646.sweep                                                 # admission on/off
```

Run at `eee3d1571` on a clean tree. The sweep is deterministic over
(corpus, grant, projection, embedder); check the `binding` block's `tree_clean` and `commit`
before trusting any output as a measurement.

`tests/context_fabric/test_chaos_3655_consolidated_artifact.py` asserts every load-bearing number
above against the committed files, so this note cannot drift from the artifacts it describes.
