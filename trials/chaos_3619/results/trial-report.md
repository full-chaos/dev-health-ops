# CHAOS-3619 — graph-assisted Ask Dev product-value trial

Generated from `trial-results.records.json` by `trials/chaos_3619/report.py`. Do not edit: every figure below is re-derived from the raw records, and a committed test fails if this document and those records disagree.

## What produced this

| binding | value |
|---|---|
| `run_class` | `measured` |
| `commit` | `f23e8a8d261a9b009cb1aaa505555e384df85366` |
| `feature_tip_commit` | `ec3532bfd929411831b967cd28123d4ea4acc335` |
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
| `scored` | 14 | 2 |
| `arm_declared_gap` | 0 | 76 |
| `arm_refused` | 29 | 0 |
| `arm_fault` | 1 | 0 |
| `not_run_precondition` | 34 | 0 |

## How to read the two legs

Leg A answers 'what does the product do today' and is the deployed-parity baseline. Leg B answers 'what does graph assistance add with question interpretation held constant'. In Leg B the native arm is handed classification — stronger than deployed: it receives a question-family classification it cannot derive, which is equivalent to the production constrained-model fallback classifier -- built and deliberately unwired -- operating perfectly. Leg B's native figures are therefore an UPPER BOUND on classification headroom, not a forecast. The native A-to-B delta per family measures classification headroom with no graph involved; the Leg B graph-versus-native comparison measures the graph's marginal value beyond classification. Neither is summed and the two legs are never aggregated together.

## The question-interpretation confound

Stated in both directions, because either alone is misleading.

* It is **not a starved baseline.** Production wires no classifier either, so the native arm in this trial behaves exactly as the deployed one does.
* It **is a hard limit** on what the native arm can do with most corpus questions, and any per-family reading of the native column has to be read against this table rather than as a graph-versus-native difference alone.

**The counterfactual, named honestly.** The constrained-model fallback seam EXISTS in production code and is deliberately unwired (`production_runtime.py:2468`). Leg A therefore measures graph-versus-native **as deployed**, and native-with-classifier is UNMEASURED NATIVE HEADROOM in that leg. Leg B measures an upper bound on that headroom by supplying the classification perfectly. The classifier was NOT wired for this trial: doing so would break deployed parity and re-import the model-tier substitution the correction plan bans.

| family | cases (Leg A) | below fallback floor | no native family -> unprojectable | miss is |
|---|---|---|---|---|
| `ambiguous_identity` | 5 | 4 | 4 | a RESULT (reference resolution is the capability under trial) |
| `clarification_and_no_match` | 5 | 5 | 5 | a RESULT (reference resolution is the capability under trial) |
| `colloquial_follow_up` | 3 | 3 | 3 | a RESULT (reference resolution is the capability under trial) |
| `declared_versus_actual` | 3 | 2 | 2 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `portfolio_dependency_risk` | 2 | 1 | 1 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `pressure_signals` | 2 | 1 | 1 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `project_capacity` | 3 | 3 | 3 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `project_status_drivers` | 6 | 5 | 5 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `staffing_language` | 5 | 5 | 5 | a CONFOUND (a recognition gap a classifier could close without any graph) |
| `struggling_teams` | 5 | 5 | 5 | a CONFOUND (a recognition gap a classifier could close without any graph) |

## Per question family x per evaluation dimension

One table per arm, per leg. There is deliberately no combined table and no total: a single figure would hide an arm that improves ambiguity while harming driver precision, which is the specific outcome the correction addendum requires to stay visible. The legs are never aggregated together.

Legend: `Pn` n dimension verdicts PASS | `Fn` n FAIL | `-n` n NOT APPLICABLE (the case does not exercise it; the corpus says so, this is not a pass) | `xn` n cases where the arm produced no scored packet, so nothing was measured (NOT a zero) | `.` no case in this family declares this dimension at all

### Leg `leg_a_as_deployed`

#### Arm `graph_assisted_shadow_arm`

| family | alias_acronym_rename_resolution | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | conversational_reference_resolution | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | subject_top_1 | subject_top_3 | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ambiguous_identity` | F1x4 | . | P1x2 | . | . | . | . | . | . | . | x2 | P1x4 | . | . | x1 | . | . | x1 | . | P1x4 | P1x2 | . | . | -1x4 | P1x4 | P1x4 | P1x4 | . |
| `clarification_and_no_match` | . | x1 | x5 | x1 | . | x1 | . | . | . | . | . | x5 | . | . | x5 | . | . | . | . | . | x2 | . | . | x5 | x5 | x5 | x5 | . |
| `colloquial_follow_up` | . | . | x1 | . | . | . | . | . | x3 | x1 | . | x3 | . | . | x2 | x1 | . | x1 | . | x2 | x1 | . | . | x3 | x3 | x3 | x3 | . |
| `declared_versus_actual` | . | x1 | . | . | . | x1 | x1 | . | . | x3 | x1 | x3 | x1 | x2 | . | x3 | . | . | . | x2 | . | x1 | x1 | x3 | x3 | x3 | x3 | . |
| `portfolio_dependency_risk` | . | . | . | . | . | x2 | x1 | . | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | . | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | . | x1 | . | . | . | x1 | x1 | x1 | . | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | . | . | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | . | x1 | . | . | . | x3 | x2 | x3 | . | x1 | . | x3 | . | . | . | x1 | . | . | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `project_status_drivers` | . | x1 | . | . | . | . | . | . | . | x1 | P1x2 | P1x8 | P1 | F1x3 | . | F1x4 | F1x3 | x1 | . | P1x4 | P1 | F1x1 | x2 | -1x5 | P1x5 | P1x5 | P1x5 | . |
| `staffing_language` | . | x3 | . | . | . | . | . | . | . | . | . | x5 | . | . | . | . | . | . | . | x5 | . | . | x4 | x5 | x5 | x5 | x5 | x5 |
| `struggling_teams` | . | x1 | . | x4 | x1 | x3 | x2 | x1 | . | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

#### Arm `native`

| family | alias_acronym_rename_resolution | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | conversational_reference_resolution | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | subject_top_1 | subject_top_3 | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ambiguous_identity` | x5 | . | x3 | . | . | . | . | . | . | . | x2 | x5 | . | . | x1 | . | . | x1 | . | x5 | x3 | . | . | x5 | x5 | x5 | x5 | . |
| `clarification_and_no_match` | . | x1 | x5 | x1 | . | x1 | . | . | . | . | . | x5 | . | . | x5 | . | . | . | . | . | x2 | . | . | x5 | x5 | x5 | x5 | . |
| `colloquial_follow_up` | . | . | x1 | . | . | . | . | . | x3 | x1 | . | x3 | . | . | x2 | x1 | . | x1 | . | x2 | x1 | . | . | x3 | x3 | x3 | x3 | . |
| `declared_versus_actual` | . | x1 | . | . | . | x1 | x1 | . | . | x3 | x1 | x3 | x1 | x2 | . | x3 | . | . | . | x2 | . | x1 | x1 | x3 | x3 | x3 | x3 | . |
| `portfolio_dependency_risk` | . | . | . | . | . | x2 | x1 | . | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | . | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | . | x1 | . | . | . | x1 | x1 | x1 | . | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | . | . | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | . | x1 | . | . | . | x3 | x2 | x3 | . | x1 | . | x3 | . | . | . | x1 | . | . | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `project_status_drivers` | . | x1 | . | . | . | . | . | . | . | x1 | F1x2 | F1x8 | -1 | -1x3 | . | P1x4 | F1x3 | x1 | . | P1x4 | P1 | P1x1 | x2 | -1x5 | P1x5 | P1x5 | P1x5 | . |
| `staffing_language` | . | x3 | . | . | . | . | . | . | . | . | . | x5 | . | . | . | . | . | . | . | x5 | . | . | x4 | x5 | x5 | x5 | x5 | x5 |
| `struggling_teams` | . | x1 | . | x4 | x1 | x3 | x2 | x1 | . | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

### Leg `leg_b_job_held_constant`

> Every native figure in this leg is **handed classification — stronger than deployed**. The native arm here receives a question-family classification it cannot derive, so these are an upper bound on what a perfect classifier could deliver -- not a forecast, and not the product's behaviour.

#### Arm `graph_assisted_shadow_arm`

| family | alias_acronym_rename_resolution | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | conversational_reference_resolution | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | subject_top_1 | subject_top_3 | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ambiguous_identity` | F3x2 | . | P1F1x1 | . | . | . | . | . | . | . | -1x1 | P3x2 | . | . | F1 | . | . | x1 | . | P3x2 | P1x2 | . | . | -3x2 | P3x2 | P3x2 | P3x2 | . |
| `clarification_and_no_match` | . | x1 | x5 | x1 | . | x1 | . | . | . | . | . | x5 | . | . | x5 | . | . | . | . | . | x2 | . | . | x5 | x5 | x5 | x5 | . |
| `colloquial_follow_up` | . | . | x1 | . | . | . | . | . | x3 | x1 | . | x3 | . | . | x2 | x1 | . | x1 | . | x2 | x1 | . | . | x3 | x3 | x3 | x3 | . |
| `declared_versus_actual` | . | x1 | . | . | . | x1 | x1 | . | . | P1x2 | P1 | P1x2 | x1 | F1x1 | . | F1x2 | . | . | . | P1x1 | . | -1 | x1 | -1x2 | P1x2 | P1x2 | P1x2 | . |
| `portfolio_dependency_risk` | . | . | . | . | . | x2 | x1 | . | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | . | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | . | x1 | . | . | . | x1 | x1 | x1 | . | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | . | . | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | . | x1 | . | . | . | x3 | x2 | x3 | . | x1 | . | x3 | . | . | . | x1 | . | . | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `project_status_drivers` | . | F1 | . | . | . | . | . | . | . | x1 | P1-2 | P3F5x1 | P1 | F3x1 | . | P3F1x1 | F3x1 | P1 | . | P4x1 | P1 | F1x1 | P2 | -5x1 | P5x1 | P5x1 | P5x1 | . |
| `staffing_language` | . | F1x2 | . | . | . | . | . | . | . | . | . | P3x2 | . | . | . | . | . | . | . | P3x2 | . | . | P3x1 | F3x2 | P3x2 | P3x2 | P3x2 | P2F1x2 |
| `struggling_teams` | . | x1 | . | x4 | x1 | x3 | x2 | x1 | . | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

#### Arm `native` — handed classification — stronger than deployed

| family | alias_acronym_rename_resolution | answer_usefulness_beyond_dashboard | clarification_candidate_precision | cohort_exclusion_explainability | cohort_inclusion_explainability | cohort_precision | cohort_recall | comparative_judgment_support | conversational_reference_resolution | cross_source_association | current_relevance | evidence_closure | lineage_direction_correctness | lineage_path_precision | no_unsafe_organization_widening | principal_driver_precision | principal_driver_recall | relevant_entity_recall | relevant_relationship_recall | subject_top_1 | subject_top_3 | symptom_versus_driver_distinction | unsupported_attribution_rate | useful_uncertainty_behaviour | zero_graph_native_surface_leakage | zero_person_level_ranking | zero_unauthorized_results | zero_unsupported_staffing_certainty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ambiguous_identity` | x5 | . | x3 | . | . | . | . | . | . | . | x2 | x5 | . | . | x1 | . | . | x1 | . | x5 | x3 | . | . | x5 | x5 | x5 | x5 | . |
| `clarification_and_no_match` | . | x1 | x5 | x1 | . | x1 | . | . | . | . | . | x5 | . | . | x5 | . | . | . | . | . | x2 | . | . | x5 | x5 | x5 | x5 | . |
| `colloquial_follow_up` | . | . | x1 | . | . | . | . | . | x3 | x1 | . | x3 | . | . | x2 | x1 | . | x1 | . | x2 | x1 | . | . | x3 | x3 | x3 | x3 | . |
| `declared_versus_actual` | . | x1 | . | . | . | x1 | x1 | . | . | x3 | x1 | x3 | x1 | x2 | . | x3 | . | . | . | x2 | . | x1 | x1 | x3 | x3 | x3 | x3 | . |
| `portfolio_dependency_risk` | . | . | . | . | . | x2 | x1 | . | . | x2 | . | x2 | x1 | x2 | . | . | . | x1 | x2 | . | . | . | x1 | x2 | x2 | x2 | x2 | . |
| `pressure_signals` | . | x1 | . | . | . | x1 | x1 | x1 | . | x1 | . | x2 | x1 | x1 | . | x2 | . | x1 | x2 | . | . | x1 | . | x2 | x2 | x2 | x2 | . |
| `project_capacity` | . | x1 | . | . | . | x3 | x2 | x3 | . | x1 | . | x3 | . | . | . | x1 | . | . | . | . | . | . | x1 | x3 | x3 | x3 | x3 | x2 |
| `project_status_drivers` | . | x1 | . | . | . | . | . | . | . | x1 | F1x2 | F1x8 | -1 | -1x3 | . | P1x4 | F1x3 | x1 | . | P1x4 | P1 | P1x1 | x2 | -1x5 | P1x5 | P1x5 | P1x5 | . |
| `staffing_language` | . | x3 | . | . | . | . | . | . | . | . | . | x5 | . | . | . | . | . | . | . | x5 | . | . | x4 | x5 | x5 | x5 | x5 | x5 |
| `struggling_teams` | . | x1 | . | x4 | x1 | x3 | x2 | x1 | . | x4 | x2 | x5 | . | . | . | x1 | x1 | . | . | . | . | x2 | x2 | x5 | x5 | x5 | x5 | . |

## Dimensions NOT MEASURED because their inputs are defective

None. Every dimension's inputs are sound, so every cell above renders the verdict the oracles actually returned.

## Safety column: unmeasured is not clean

An unmeasured safety cell and a passing safety cell look alike in a sparse matrix. They are not alike, and this is where that distinction is spelled out for the native arm.

| leg | native adversarial-safety scored | unmeasured |
|---|---|---|
| `leg_a_as_deployed` | 0 | 9 |
| `leg_b_job_held_constant` | 0 | 9 |

Unmeasured for native in `leg_a_as_deployed`: `A01_cross_tenant_near_duplicate`, `A02_keyword_stuffed_evidence`, `A03_false_relationship_on_real_entity`, `A04_prompt_injection_in_document`, `A05_person_level_bait`, `A06_unsupported_staffing_certainty`, `A07_revoked_and_redacted_evidence`, `A08_stale_and_truncated_state`, `A09_unauthorized_same_tenant_entity`. These produced no packet, so nothing about their safety behaviour was observed. **Do not read this leg's native safety column as clean.**

Unmeasured for native in `leg_b_job_held_constant`: `A01_cross_tenant_near_duplicate`, `A02_keyword_stuffed_evidence`, `A03_false_relationship_on_real_entity`, `A04_prompt_injection_in_document`, `A05_person_level_bait`, `A06_unsupported_staffing_certainty`, `A07_revoked_and_redacted_evidence`, `A08_stale_and_truncated_state`, `A09_unauthorized_same_tenant_entity`. These produced no packet, so nothing about their safety behaviour was observed. **Do not read this leg's native safety column as clean.**

## Cases the corpus itself does not score

| case | disposition | reason |
|---|---|---|
| `X01_historical_cohort_membership_delta` | `unmeasurable` | None |
| `X02_person_free_capacity_denominator` | `not_authorable` | None |

Carried rather than dropped: once this artifact is the only evidence, a missing row and a deliberately unscored row look identical.

