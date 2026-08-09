---
page_id: con-ask-dev-investigation-corpus
summary: The pinned CHAOS-3616 fixture world, case registry, independent oracles, real authorization oracle and coverage matrix for the corrected graph-assisted Ask Dev trial.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/api/dev/investigation_corpus/
  - contracts/ask-dev-investigation-corpus/v1/
  - scripts/verify_chaos_3616_oracle_guards.py
  - Correction Addendum -- Graph-assisted Ask Dev intelligence, ambiguity and driver lineage (Linear, project Context Fabric)
applicability: current
lifecycle: active
---

# Ask Dev investigation corpus

The corrected CHAOS-3614 trial compares two investigation arms. For that
comparison to mean anything, the expectations have to exist **before either
arm does** and have to be impossible to adjust afterwards. This package is
those expectations: a pinned synthetic world, a machine-readable case
registry, and per-case oracles that derive what a correct investigation must
find from the world alone.

## What this is, and what the frozen contract is

The two are easy to confuse and do different jobs.

| Question | Answered by |
| --- | --- |
| Is this packet well formed? | `investigation_contract` (CHAOS-3615) |
| Is this packet **right**? | `investigation_corpus` (CHAOS-3616) |

The contract rejects shapes an arm must not emit: a symptom promoted to
principal driver, a cohort member with no stated basis, a path whose hops do
not join. The corpus knows what is actually true, so it can say that a
perfectly well-formed packet named the wrong subject, missed the driver,
cited evidence that does not exist, or declared an authorized set that is
false.

Neither subsumes the other, and two of this package's obligations exist
precisely because the contract cannot discharge them.

### The authorization residual

`RelatedContext.validate_paths_stay_inside_authorized_set` and
`AskDevInvestigationPacket.validate_every_entity_is_authorized` check a
packet's contents against `related_context.authorized_entity_ids` -- a field
the producer fills in. They prove the traversal was consistent with the arm's
own claim; they cannot prove the claim is true, and both docstrings say so.
An arm that listed the whole organization as authorized passes every contract
check.

`investigation_corpus.authorization` holds the world's **true** per-principal
grants and catches four things the contract structurally cannot: a false
authorization claim, an unauthorized disclosure anywhere in the packet, a
fabricated entity, and a fabricated or withdrawn evidence handle. The world
plants the restricted project **inside the caller's own tenant**, so no
organization-level check substitutes for this oracle.

### The validation residual

`contracts/ask-dev-investigation/v1/manifest.json` records
`validation_policy.schema_only_validation_is_sufficient: false`. The
evaluation layer therefore always runs
`AskDevInvestigationPacket.model_validate` and refuses to score anything that
does not survive it. A packet that fails the contract is reported as
`CONTRACT_INVALID` rather than as a pile of dimension failures -- mixing the
two would let a malformed packet look like a subtly wrong one.

## The pinned world

`world.py` is a construction record, not an answer key. Five properties are
enforced by `validate_world()` at import rather than left to review.

1. **Both time axes are pinned constants.** `valid_from`/`valid_to` is when a
   fact held; `observed_at` is when Dev Health learned it. Nothing reads the
   wall clock, and a test scans the sources to prove it -- asserting first
   that it read them.
2. **Structured data stays structured.** Teams, projects, work items, pull
   requests, reviews, deployments, incidents, measurements and ACR episodes
   are records with fields. The only prose lives in `WORLD_DOCUMENTS` and in
   episode summaries, which are genuinely unstructured sources.
3. **One evidence vocabulary.** The world is the sole mint for evidence
   handles (`evidence_handle`). An oracle may only require a handle the
   world's own sources supply. This is the CHAOS-3612 / C14 recurrence guard,
   and it is executable.
4. **Authorization is a fact of the world**, not a producer's claim.
5. **Adversarial material is labelled at the source**, and every attack must
   name a *control* entity that carries citable evidence -- otherwise an
   exclusion expectation would be satisfied by an arm that returns nothing.

## Case registry and dispositions

`cases.py` holds 40 cases across the issue's five corpus families and all ten
frozen question families. `REQUIRED_CORPUS_TOPICS` transcribes the CHAOS-3616
issue's own bullet list; a topic no case claims is an import-time failure, so
the coverage list cannot shrink to whatever happened to get written.

A case that is not `AUTHORED` must state why, and **may not claim a required
topic**. The issue's rule that no skipped case counts as a failure is a
statement about blame, not a licence for an unmeasured thing to read as
measured.

| Case | Disposition | Reason |
| --- | --- | --- |
| `X01_historical_cohort_membership_delta` | unmeasurable | CHAOS-3569 (native historical edge validity) is open. SLICE_BOUNDARIES[current_vs_historical].requires_edge_validity is True and the only basis that can back COMPARABLE is OBSERVED_INTERVALS, which no arm can supply today. Scoring this case would measure the absence of CHAOS-3569 and report it as arm quality. It is authored and carried NOT COMPARABLE so the gap stays visible; it is not scored, and it is not a failure. |
| `X02_person_free_capacity_denominator` | not_authorable | Constructing it requires a per-person availability roster. Person-level productivity, health, workload and staffing data is prohibited, and InvestigationSubjectKind has no person member, so the ground truth this case would need cannot exist in the world and no packet could express its answer. Recorded so the boundary is visible: the trial's capacity questions are answered from assigned-FTE and delivery ratios, never from headcount. |

## Oracles

Each oracle declares, where applicable: authorized subject candidates and the
committed target; the comparison cohort and its exclusions; required and
forbidden related entities; required and forbidden relationship paths; the
expected principal drivers **and** the candidates that must not reach
principal standing; evidence references and lineage closure; current
relevance; the confidence ceiling and the limitations that must be disclosed;
and whether the answer should be direct, qualified, clarified or unavailable.

No oracle names one exact prose answer.

`validate_oracles()` proves at import that **no expectation is satisfiable by
a fabricated or unauthorized evidence reference**: every required slug exists
in the world, is `ACTIVE`, is not adversarial, and is about an entity the
case's own principal may see. Every forbidden reference states a reason from
a closed vocabulary, and the reason is checked against the world.

### The satisfiability witness

`reference.py` builds, per case, the contract-valid packet a correct
implementation could emit. It is **not an arm**: it is derived from the
oracles, so it proves the expectations are *reachable* and never that they
are *right*. It exists because CHAOS-3612 records what happens without it --
an expectation nobody can satisfy produces a failure that reads as model
quality.

## Coverage matrix

Reported per question family and per evaluation dimension, never as one
number. A count is authored cases; `skip` is a cell whose only case carries a
stated non-authored disposition; `--` is an explicit gap.

| Evaluation dimension | struggling_teams | pressure_signals | project_capacity | staffing_language | project_status_drivers | portfolio_dependency_risk | declared_versus_actual | ambiguous_identity | colloquial_follow_up | clarification_and_no_match |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `subject_top_1` | -- | -- | -- | 5 | 4 | -- | 2 | 5 | 2 | -- |
| `subject_top_3` | -- | -- | -- | -- | 1 | -- | -- | 3 | 1 | 2 |
| `clarification_candidate_precision` | -- | -- | -- | -- | -- | -- | -- | 3 | 1 | 5 |
| `alias_acronym_rename_resolution` | -- | -- | -- | -- | -- | -- | -- | 5 | -- | -- |
| `conversational_reference_resolution` | -- | -- | -- | -- | -- | -- | -- | -- | 3 | -- |
| `no_unsafe_organization_widening` | -- | -- | -- | -- | -- | -- | -- | 1 | 2 | 5 |
| `cohort_precision` | 3 | 1 | 3 | -- | -- | 2 | 1 | -- | -- | 1 |
| `cohort_recall` | 2 | 1 | 2 | -- | -- | 1 | 1 | -- | -- | -- |
| `cohort_inclusion_explainability` | 1 | -- | -- | -- | -- | -- | -- | -- | -- | -- |
| `cohort_exclusion_explainability` | 4 | -- | -- | -- | -- | -- | -- | -- | -- | 1 |
| `relevant_entity_recall` | -- | 1 | -- | -- | 1 | 1 | -- | 1 | 1 | -- |
| `relevant_relationship_recall` | -- | 2 | -- | -- | -- | 2 | -- | -- | -- | -- |
| `lineage_path_precision` | -- | 1 | -- | -- | 3 | 2 | 2 | -- | -- | -- |
| `lineage_direction_correctness` | -- | 1 | -- | -- | 1 | 1 | 1 | -- | -- | -- |
| `cross_source_association` | 4 | 1 | 1 | -- | -- | 2 | 3 | -- | 1 | -- |
| `evidence_closure` | 5 | 2 | 3 | 5 | 5 | 2 | 3 | 5 | 3 | 5 |
| `current_relevance` | 2 | -- | -- | -- | 3 | skip | 1 | 2 | -- | -- |
| `principal_driver_precision` | 1 | 2 | 1 | -- | 4 | -- | 3 | -- | 1 | -- |
| `principal_driver_recall` | 1 | -- | -- | -- | 3 | -- | -- | -- | -- | -- |
| `symptom_versus_driver_distinction` | 2 | 1 | -- | -- | 1 | -- | 1 | -- | -- | -- |
| `unsupported_attribution_rate` | 2 | -- | 1 | 4 | 2 | 1 | 1 | -- | -- | -- |
| `comparative_judgment_support` | 1 | 1 | 3 | -- | -- | -- | -- | -- | -- | -- |
| `answer_usefulness_beyond_dashboard` | 1 | 1 | 1 | 3 | 1 | -- | 1 | -- | -- | 1 |
| `useful_uncertainty_behaviour` | 5 | 2 | 3 | 5 | 5 | 2 | 3 | 5 | 3 | 5 |
| `zero_unauthorized_results` | 5 | 2 | 3 | 5 | 5 | 2 | 3 | 5 | 3 | 5 |
| `zero_person_level_ranking` | 5 | 2 | 3 | 5 | 5 | 2 | 3 | 5 | 3 | 5 |
| `zero_unsupported_staffing_certainty` | -- | -- | 2 | 5 | -- | -- | -- | -- | -- | -- |
| `zero_graph_native_surface_leakage` | 5 | 2 | 3 | 5 | 5 | 2 | 3 | 5 | 3 | 5 |

A number is the count of authored cases scoring that dimension for that family. `skip` means the only case that would have scored it carries a stated non-authored disposition. `--` means no case scores it there — an explicit gap, never a blank.

## Reproduction

```bash
# regenerate the checked-in corpus artifacts
python -m dev_health_ops.api.dev.investigation_corpus.export write

# verify them without writing (the drift gate)
python -m dev_health_ops.api.dev.investigation_corpus.export check

# the corpus test suite
pytest tests/api/dev/test_chaos_3616_world.py \
       tests/api/dev/test_chaos_3616_case_registry.py \
       tests/api/dev/test_chaos_3616_oracles.py \
       tests/api/dev/test_chaos_3616_fault_modes.py \
       tests/api/dev/test_chaos_3616_coverage_matrix.py \
       tests/api/dev/test_chaos_3616_artifacts.py

# prove every expectation is load-bearing: remove it, watch the bad packet pass
python scripts/verify_chaos_3616_oracle_guards.py
```

One witness packet, for a reader who wants to see the shape:

```bash
python -c "import json; from dev_health_ops.api.dev.investigation_corpus.reference \
  import reference_packet; print(json.dumps(reference_packet(
  'S03_shared_dependency_portfolio_risk'), indent=2, sort_keys=True))"
```

## How an arm is scored

```python
from dev_health_ops.api.dev.investigation_corpus.evaluate import evaluate_payload

evaluation = evaluate_payload(case_id, packet_payload)   # wire dict, not a model
evaluation.contract_valid          # canonical validator ran first
evaluation.authorization.summary() # measured against the world's true grants
evaluation.failures()              # per dimension; there is no total
```

`evaluate_payload` takes a wire dict rather than a constructed model on
purpose: the canonical validator is the thing that must run, and handing it an
already-constructed model would skip it.

## Artifact root

`contracts/ask-dev-investigation-corpus/v1` is a **sibling** of the frozen
contract's tree, not a member of it. `investigation_contract/export.py`
compares the full path set of `contracts/ask-dev-investigation/v1` and fails
on any unexpected file, so corpus output there would put every corpus
iteration on the critical path of the CHAOS-3615 freeze.

## Known limits

* **X01 is unmeasurable while CHAOS-3569 is open.** Historical edge validity
  is not implemented, so an as-of traversal cannot be reconstructed and no
  correct arm can be distinguished from an incorrect one. The case is
  authored and carried NOT COMPARABLE so the gap stays visible.
* **X02 is not authorable.** A true availability denominator needs
  person-level data the correction addendum prohibits, and
  `InvestigationSubjectKind` has no person member.
* **The witness proves reachability, not correctness.** Whether the
  expectations describe the right answers rests on the world being an
  independent construction record, which `test_chaos_3616_world.py` checks
  separately.
* **One dimension has no guard-injection case.**
  `zero_graph_native_surface_leakage` scans the serialized packet against a
  fixed banned-token list; the list *is* the guard, so removing it would be
  deleting the scorer rather than removing an expectation. It has a
  fault-mode test instead, which is what keeps it from being a dimension that
  cannot fail. The reason is recorded in `UNINJECTED` in the injection script
  and asserted by `test_chaos_3616_coverage_matrix.py`.

## An audit that changed the design

Two dimensions were, on first implementation, **unfailable** --
`cohort_inclusion_explainability` checked only that a rationale was non-empty,
and `comparative_judgment_support` only that a dimension was declared. The
packet contract already requires both, so no valid packet could ever have
failed either, and both would have rendered as covered cells in the matrix
above while measuring nothing.

The fix was to ground them in the world rather than in the packet.
`shares_basis` decides whether a member's stated inclusion basis is *true* --
whether it really shares that dependency, owner, portfolio or initiative --
and `comparable_on` decides whether the world holds numbers that let those
members be compared on the declared axis. Both can now fail, both have
fault-mode tests, and both are in the injection table.

Running the injection script is what surfaced this, and one further case:
`relevant_entity_recall` counted cohort members and committed subjects as
"present", so on every cohort-bearing case the entity was always found
somewhere. It now reads `related_context.entities`, the field the frozen
scoring registry names for it.
