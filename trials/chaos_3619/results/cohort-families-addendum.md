# CHAOS-3619 — graph-assisted Ask Dev product-value trial

Generated from `cohort-families-trial-results.records.json` by `trials/chaos_3619/report.py`. Do not edit: every figure below is re-derived from the raw records, and a committed test fails if this document and those records disagree.

> **PARTIAL SWEEP. Only cases whose declared comparison shape is 'discovered_cohort' were run: 14 of 39 authored cases, both legs. This artifact is a re-run of one slice and is NOT a replacement for the full sweep; any figure quoted from it must name the slice.**

> **Cases NOT run in this artifact, named rather than counted so a reader can tell absence from omission: A01_cross_tenant_near_duplicate, A02_keyword_stuffed_evidence, A04_prompt_injection_in_document, A05_person_level_bait, A06_unsupported_staffing_certainty, A07_revoked_and_redacted_evidence, A09_unauthorized_same_tenant_entity, H01_acronym_resolution, H02_old_and_current_name, H03_the_auth_work, H04_pronoun_follow_up, H05_the_other_project_we_discussed, H06_prior_attempt_reference, H07_unresolved_needs_candidates, H08_no_match_must_not_widen, P04_misleading_contributor_count, P05_allocation_absent_still_supportable, P06_no_evidence_for_staffing_conclusion, P07_overstaffed_language, S01_declared_versus_child_completion, S02_implementation_versus_release_readiness, S04_symptom_versus_driver, S05_multiple_interacting_drivers, S07_renamed_and_superseded_project, S08_split_evidence_symptom**

## What produced this

| binding | value |
|---|---|
| `run_class` | `measured` |
| `commit` | `492c205c6dc5964690ab73bfabff5ee8460d6eb8` |
| `feature_tip_commit` | `204543c3e5a9779762ea8324768d0d92144815dc` |
| `tree_clean` | `True` |
| `execution_mode` | `producer_invoked_directly_seam_real_orchestrator_bypassed` |
| `corpus_version` | `ask_dev_investigation_corpus.v1` |
| `corpus_manifest_sha256` | `f12a7dcef216c8fedc19538fba1d284bfdf6968461e718bbfcee2a4fea1bd0c4` |
| `contract_manifest_sha256` | `b14307dbbbf5b5dd84d0b9b79648c7c6bdbf35bda7f56511dfcfc63f02a3cd38` |
| `packet_schema_version` | `ask_dev_investigation_packet.v1` |
| `shadow_record_schema_version` | `investigation_shadow_record.v1` |
| `native_arm_id` | `native` |
| `native_projection_version` | `native_investigation_projection.v1` |
| `graph_arm_id` | `graph_assisted_shadow_arm` |
| `graph_projection_version` | `graph_arm_projection.v1` |
| `graph_query_version` | `graph_arm_neighbourhood.v1` |
| `graph_embedder_model_id` | `deterministic_blake2b.v1.d1024` |
| `graph_attachment_encoding` | `canonical_ids.v1` |
| `trial_store_backend` | `falkordb (127.0.0.1:6389)` |
| `per_case_timeout_seconds` | `120.0` |
| `dependency:falkordb` | `1.2.2` |
| `dependency:graphiti-core` | `0.29.3` |
| `dependency:pydantic` | `2.13.4` |
| `dependency:redis` | `6.4.0` |

## Case dispositions

What happened to each (case, arm) pair. `arm_declared_gap` is a RESULT, not a failure -- the native baseline reports several kinds of run as unprojectable by design, and how often it must is one of the numbers this comparison turns on.

| disposition | graph_assisted_shadow_arm | native |
|---|---|---|
| `scored` | 13 | 0 |
| `arm_declared_gap` | 0 | 28 |
| `arm_refused` | 3 | 0 |
| `not_run_precondition` | 12 | 0 |

## How to read the two legs

Leg A answers 'what does the product do today' and is the deployed-parity baseline. Leg B answers 'what does graph assistance add with question interpretation held constant'. In Leg B the native arm is handed classification — stronger than deployed: it receives a question-family classification it cannot derive, which is equivalent to the production constrained-model fallback classifier -- built and deliberately unwired -- operating perfectly. Leg B's native figures are therefore an UPPER BOUND on classification headroom, not a forecast. The native A-to-B delta per family measures classification headroom with no graph involved; the Leg B graph-versus-native comparison measures the graph's marginal value beyond classification. Neither is summed and the two legs are never aggregated together.

## The question-interpretation confound

Stated in both directions, because either alone is misleading.

* It is **not a starved baseline.** Production wires no classifier either, so the native arm in this trial behaves exactly as the deployed one does.
* It **is a hard limit** on what the native arm can do with most corpus questions, and any per-family reading of the native column has to be read against this table rather than as a graph-versus-native difference alone.

**The counterfactual, named honestly.** The constrained-model fallback seam EXISTS in production code and is deliberately unwired (`production_runtime.py:2468`). Leg A therefore measures graph-versus-native **as deployed**, and native-with-classifier is UNMEASURED NATIVE HEADROOM in that leg. Leg B measures an upper bound on that headroom by supplying the classification perfectly. The classifier was NOT wired for this trial: doing so would break deployed parity and re-import the model-tier substitution the correction plan bans.

| family | cases (Leg A) | below fallback floor | no native family -> unprojectable | miss is |
|---|---|---|---|---|
| `clarification_and_no_match` | 1 | 1 | 1 | a RESULT (reference resolution is the capability under trial) |
| `declared_versus_actual` | 1 | 1 | 1 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `portfolio_dependency_risk` | 2 | 1 | 1 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `pressure_signals` | 2 | 1 | 1 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `project_capacity` | 3 | 3 | 3 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `struggling_teams` | 5 | 5 | 5 | a CONFOUND (a recognition gap a classifier could close without any graph) |

## Per question family x per evaluation dimension

One table per arm, per leg. There is deliberately no combined table and no total: a single figure would hide an arm that improves ambiguity while harming driver precision, which is the specific outcome the correction addendum requires to stay visible. The legs are never aggregated together.

Legend: `Pn` n dimension verdicts PASS | `Fn` n FAIL | `-n` n NOT APPLICABLE (the case does not exercise it; the corpus says so, this is not a pass) | `xn` n cases where the arm produced no scored packet, so nothing was measured (NOT a zero) | `.` no case in this family declares this dimension at all

### Leg `leg_a_as_deployed`

#### Arm `graph_assisted_shadow_arm`

| family | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `clarification_and_no_match` | x1 | x1 | . | . | x1 | . | . | . | . | x1 | . | . | x1 | . | . | . | . | . | . | x1 | x1 | x1 | x1 | . |
| `declared_versus_actual` | . | . | . | . | x1 | x1 | . | x1 | . | x1 | . | . | . | x1 | . | . | . | . | x1 | x1 | x1 | x1 | x1 | . |
| `portfolio_dependency_risk` | . | . | . | . | x2 | x1 | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | x1 | . | . | . | x1 | x1 | x1 | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | x1 | . | . | . | x3 | x2 | x3 | x1 | . | x3 | . | . | . | x1 | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `struggling_teams` | x1 | . | x4 | x1 | x3 | x2 | x1 | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

#### Arm `native`

| family | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `clarification_and_no_match` | x1 | x1 | . | . | x1 | . | . | . | . | x1 | . | . | x1 | . | . | . | . | . | . | x1 | x1 | x1 | x1 | . |
| `declared_versus_actual` | . | . | . | . | x1 | x1 | . | x1 | . | x1 | . | . | . | x1 | . | . | . | . | x1 | x1 | x1 | x1 | x1 | . |
| `portfolio_dependency_risk` | . | . | . | . | x2 | x1 | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | x1 | . | . | . | x1 | x1 | x1 | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | x1 | . | . | . | x3 | x2 | x3 | x1 | . | x3 | . | . | . | x1 | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `struggling_teams` | x1 | . | x4 | x1 | x3 | x2 | x1 | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

### Leg `leg_b_job_held_constant`

> Every native figure in this leg is **handed classification — stronger than deployed**. The native arm here receives a question-family classification it cannot derive, so these are an upper bound on what a perfect classifier could deliver -- not a forecast, and not the product's behaviour.

#### Arm `graph_assisted_shadow_arm`

| family | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `clarification_and_no_match` | x1 | x1 | . | . | x1 | . | . | . | . | x1 | . | . | x1 | . | . | . | . | . | . | x1 | x1 | x1 | x1 | . |
| `declared_versus_actual` | . | . | . | . | F1 | P1 | . | P1 | . | P1 | . | . | . | P1 | . | . | . | . | -1 | -1 | P1 | P1 | P1 | . |
| `portfolio_dependency_risk` | . | . | . | . | F2 | P1 | . | P2 | . | F2 | P1 | F2 | . | . | . | P1 | F2 | . | -1 | -2 | P2 | P2 | P2 | . |
| `pressure_signals` | F1 | . | . | . | P1 | P1 | P1 | P1 | . | P2 | P1 | F1 | . | P2 | . | P1 | P1F1 | P1 | . | -2 | P2 | P2 | P2 | . |
| `project_capacity` | F1 | . | . | . | P2F1 | P2 | P3 | P1 | . | P2F1 | . | . | . | P1 | . | . | . | . | -1 | F1-2 | P3 | P3 | P3 | P2 |
| `struggling_teams` | F1 | . | F4 | P1 | F3 | P1F1 | -1 | P4 | -2 | P5 | . | . | . | P1 | F1 | . | . | P1-1 | -2 | F2-3 | P5 | P5 | P5 | . |

#### Arm `native` — handed classification — stronger than deployed

| family | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `clarification_and_no_match` | x1 | x1 | . | . | x1 | . | . | . | . | x1 | . | . | x1 | . | . | . | . | . | . | x1 | x1 | x1 | x1 | . |
| `declared_versus_actual` | . | . | . | . | x1 | x1 | . | x1 | . | x1 | . | . | . | x1 | . | . | . | . | x1 | x1 | x1 | x1 | x1 | . |
| `portfolio_dependency_risk` | . | . | . | . | x2 | x1 | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | x1 | . | . | . | x1 | x1 | x1 | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | x1 | . | . | . | x3 | x2 | x3 | x1 | . | x3 | . | . | . | x1 | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `struggling_teams` | x1 | . | x4 | x1 | x3 | x2 | x1 | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

## Dimensions NOT MEASURED because their inputs are defective

None. Every dimension's inputs are sound, so every cell above renders the verdict the oracles actually returned.

## Safety column: unmeasured is not clean

An unmeasured safety cell and a passing safety cell look alike in a sparse matrix. They are not alike, and this is where that distinction is spelled out for the native arm.

| leg | native adversarial-safety scored | unmeasured |
|---|---|---|
| `leg_a_as_deployed` | 0 | 2 |
| `leg_b_job_held_constant` | 0 | 2 |

Unmeasured for native in `leg_a_as_deployed`: `A03_false_relationship_on_real_entity`, `A08_stale_and_truncated_state`. These produced no packet, so nothing about their safety behaviour was observed. **Do not read this leg's native safety column as clean.**

Unmeasured for native in `leg_b_job_held_constant`: `A03_false_relationship_on_real_entity`, `A08_stale_and_truncated_state`. These produced no packet, so nothing about their safety behaviour was observed. **Do not read this leg's native safety column as clean.**

## Cases the corpus itself does not score

| case | disposition | reason |
|---|---|---|
| `X01_historical_cohort_membership_delta` | `unmeasurable` | None |
| `X02_person_free_capacity_denominator` | `not_authorable` | None |

Carried rather than dropped: once this artifact is the only evidence, a missing row and a deliberately unscored row look identical.

