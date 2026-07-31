---
page_id: con-ask-dev-v2
summary: The Ask Dev Wave 3.1 v2 wire contracts -- intent, resolution, plan, frame, narrative, and answer-v2.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/api/dev/contracts_v2/
  - src/dev_health_ops/api/dev/contract_fixtures_v2.py
  - contracts/ask-dev/v2/
  - Amendment TRD v2 -- Ask Dev Wave 3.1 (Linear, project Ask Dev)
  - Amendment PRD v2 -- Ask Dev Wave 3.1 (Linear, project Ask Dev)
applicability: current
lifecycle: active
---

# Ask Dev v2 contracts (Wave 3.1)

CHAOS-3294 lands the contract-first foundation for the Wave 3.1 request
lifecycle: server-owned intent interpretation, per-mention resolution,
investigation planning, a canonical answer frame, an optional presentation
narrative, and a safe public outcome vocabulary. It is **contracts only** --
schemas, Pydantic models, positive/negative fixtures, and semantic
validators. No orchestrator, router, or `scope_service` behavior changed;
those are separate Wave 3.1 issues (CHAOS-3292, CHAOS-3295, CHAOS-3297,
CHAOS-3301) that consume these contracts.
{: .fc-page-lede }

v1 (`dev_answer.v1` / `DevMessageRequest`) is untouched. v2 lives alongside
it in a new `contracts_v2` package and a new `contracts/ask-dev/v2`
generated-artifact tree, using the exact same producer pattern
[Stable contracts](contracts.md) already documents for v1: canonical
Pydantic models produce Draft 2020-12 schemas and golden fixtures, checked
in and verified for drift.

## Why intent moves server-side

v1 makes the browser submit `question_class` and treats the model-authored
answer as the first complete result. `dev_question_intent.v1` replaces that:
a closed, twelve-member registry (`entity_status`, `portfolio_status`,
`remaining_work`, `observed_change`, `registered_statistics`,
`metric_comparison`, `data_trust`, `project_health`, `team_health`,
`team_workload_balance`, `operational_deficiency_inventory`,
`bounded_investigation`) interpreted deterministically server-side.
`dev_message_request.v2` keeps the client's `question_class` only as a
`question_class_hint` field; whenever it is present, the paired
`DevQuestionIntent.client_hint_deprecation_warning` is required, so the
"ignored for planning, diagnostic recorded" requirement is structurally
enforced rather than a convention.

## The contract groups

| Contract | Purpose |
| --- | --- |
| `dev_question_intent.v1` | Authoritative intent, cardinality, subject kinds, comparison mode, clarification requirement. |
| `dev_subject_mention.v1` | One extracted, bounded text span with a requested entity kind. |
| `dev_resolution_ledger.v1` | Append-only, per-mention resolution history -- see below. |
| `dev_subject_set.v1` | A homogeneous, 1-25 entity, authorization-safe committed cohort for plural questions. |
| `dev_investigation_plan.v1` / `dev_source_requirement.v1` | A named, versioned plan (`status.entity.v2`, `health.team.v1`, ...) and its per-source mandatory/conditional/optional/not-applicable requirements. |
| `dev_investigation_result.v1` / `dev_source_observation.v1` | What actually happened when a plan ran, preserving measured-zero vs. no-data. |
| `dev_answer_frame.v1` | The server-owned canonical answer: outcome, subject, completion, readiness, ordered sections/facts, metrics, evidence, coverage, versions. Independent of whether a narrative exists. |
| `dev_narrative.v1` | Optional, presentation-only text referencing existing frame fact/section IDs. Cannot introduce new subjects, numbers, or claims. |
| `dev_answer.v2` | Projects the frame, optional narrative, and safe outcome to both the app-wide window and `/dev`. |

Amended stream events (`dev_stream_event.v2`) replace the v1 `scope.resolved`
event with `resolution.updated` (a resolution ledger update -- a single
"scope resolved" event cannot represent per-mention outcomes) and add the
Wave 3.1 progress stages (`interpreting_question`, `resolving_subjects`,
`checking_project_status`, `checking_team_health`, `comparing_portfolio`,
`checking_workload`, `checking_operational_controls`, `preparing_answer`).

## TEAM is a v2-only first-class kind

`EntityKind` (in `contracts_v2/base.py`) adds `TEAM` to the v1 `EntityType`
value set, but v1's `EntityType`/`DirectScope` enums are **not** mutated --
giving `TEAM` real v1 scope semantics is runtime behavior (a matching
`DevScope` validator branch, orchestrator/`scope_service` wiring), which is
explicitly out of scope for CHAOS-3294. The compatibility projector (below)
returns a safe `feature_not_enabled` v1 `DevError` for a team-subject v2
answer rather than mislabeling it as organization- or repository-scoped.

## The resolution ledger is append-only by construction

`dev_resolution_ledger.v1` must make it structurally impossible for a later
resolution to erase an earlier unresolved mention. Two mechanisms enforce
that jointly:

1. Every v2 contract is frozen (`extra="forbid"`, `frozen=True`) **and holds
   its collections as tuples** -- a ledger cannot be mutated in place, only
   superseded by constructing a new one. `frozen=True` alone is not enough
   for this claim: it blocks rebinding, not mutation of a `list` field's
   contents. See "Immutability is structural" below.
2. `entries` must carry contiguous, strictly increasing `entry_ordinal`
   values starting at zero. A ledger is validated as a whole, so dropping or
   rewriting an existing entry breaks contiguity and fails validation.

`validate_ledger_extends(previous, candidate)` enforces the explicit
cross-snapshot half of the guarantee for callers (persistence) holding two
ledger snapshots: `candidate` must equal `previous` plus zero or more
appended entries, never a rewrite.

## Measured-zero vs. no-data

`dev_source_observation.v1.data_semantics` (`measured_zero | no_data |
not_measured`) is validated jointly against `observed_state`
(`available_current | available_stale | available_unknown | unconfigured |
unavailable | unauthorized_or_not_visible | not_applicable | truncated`): a
source that was never actually queried can never claim `measured_zero`.
This is the structural form of the guardrail "no missing or unconfigured
data represented as zero."

## Completion is never inferred

`DevAnswerFrame.completion` (`DevCompletionBlock`) can only be
`calculable=true` with a full `numerator`, `denominator`, `rule_id`, and
`rule_version` all present and mutually consistent; a `calculable=false`
block can never carry a `rate`. Readiness (`DevReadinessBlock`) is a
separate, independently validated concept -- an answer can be complete about
a subject that is itself `not_ready`.

## Public outcome vocabulary and the five semantic validators

`dev_answer.v2`'s `public_outcome` is exactly: `answered`,
`answered_with_gaps`, `needs_clarification`, `not_found`,
`temporarily_unavailable`, `unsupported`, `denied`, `failed`. `refused` is
deliberately absent -- Amendment PRD v2 reserves it for genuinely prohibited
requests, never a normal status/health question that could not be resolved.

`contracts_v2/validators.py` implements the five CHAOS-3294
acceptance-criteria semantic validators as free functions (not inline
`@model_validator` methods), each independently disableable via
`monkeypatch` against the *module object* -- see that module's docstring for
why the indirection matters for mutation testing:

- `validate_no_internal_leakage` -- public copy fields (`direct_answer`,
  section titles, fact text, limitations, follow-ups, translated readiness
  reasons) may never contain internal codes like `forbidden_or_not_found`,
  `scope_forbidden`, or a versioned rule/plan ID pattern.
- `validate_outcome_consistency` -- a "no content" outcome cannot carry
  sections/facts; an "answered" outcome cannot carry limitations or a
  non-calculable completion; `answered_with_gaps` requires one of the two.
- `validate_completion_denominator` -- a calculable completion requires a
  full numerator/denominator/rule; a non-calculable one can never carry a
  rate.
- `validate_narrative_fact_references` -- a narrative's referenced fact/
  section IDs must exist in its paired frame (cross-object; wired into
  `DevAnswerV2`'s own validation).
- `validate_relationship_refs_within_frame` -- a fact's relationship-path
  references must exist in the frame, and every relationship path must chain
  back to the frame's committed subject.

## Post-merge hardening (adversarial review, CHAOS-3294)

Two adversarial-review rounds ran against the merged contracts. Round 1
reproduced six counterexamples the original five validators did not catch.
Round 2 then bypassed three of those fixes through adjacent variants, and
found one new gap. Because that made three rounds on the same defect
classes, the round-2 fixes are deliberately **class closures** rather than
further patches: each replaces the round-1 check's polarity or scope, and
each carries an explicit closure argument -- the partition of the input
space and why every cell is covered -- recorded in the sections below and
in the `validators` module docstring.

### No-content outcomes are rebuilt from an allowlist (`validate_no_answer_projection`)

`validate_outcome_consistency`'s original "no content" check only inspected
`sections`/`facts` -- a `denied` frame validated with a subject reference, a
3/4 completion rate, evidence, and source observations all still present.
The first fix added a *denylist* of those field names; adversarial review
round 2 walked around it through the fields the denylist did not name --
`direct_answer`, `conflicts`, `limitations`, `safe_follow_up_questions`, and
a whole `narrative` -- each of them producer-authored free text. A `denied`
answer disclosed a private project's existence, its completion percentage,
and a cross-provider conflict about it, and the v1 projector re-emitted that
copy verbatim as `DevError.safe_message`.

The guard is now an **allowlist projection**, not a scrub. `NO_ANSWER_OUTCOMES`
(`not_found`, `temporarily_unavailable`, `unsupported`, `denied`, `failed`)
is the exact outcome set that projects to a v1 `DevError` rather than a v1
`DevAnswer` (see `compat.py`'s `_ERROR_OUTCOME_CODES` below). For those five,
every field of `DevAnswerFrame` and `DevAnswerV2` carries an explicit
classification in `NO_ANSWER_FRAME_FIELD_POLICY` /
`NO_ANSWER_ANSWER_FIELD_POLICY`:

| Class | Meaning |
| --- | --- |
| `ABSENT` | `None` or empty. Nothing about the subject survives. |
| `CANONICAL` | Exactly the server-owned constant for this outcome (`CANONICAL_NO_ANSWER_COPY`, `CANONICAL_NO_ANSWER_DISPLAY_LABELS`). Producer text is replaced, never reused. |
| `IDENTIFIER` | Every string the field reaches must be a whitespace-free identifier token, checked **at runtime on the value**. Classifying a free-text field this way is not an escape hatch: prose in it fails validation. |
| `NON_TEXT` | Reaches no string at all. |
| `SELF_VALIDATED` | A nested v2 contract with its own registered policy. |

**Closure argument.** The classification partitions the fields of both
models, and the partition is total by construction:
`validators.assert_no_answer_policy_is_total` runs at **import time** from
`frame.py` and `answer.py`, so a field added without a classification breaks
the package import. `test_round2_no_answer_policy_classifies_every_field`
re-derives the same enumeration from `model_fields` (rather than from a
hand-written list) and additionally proves `NON_TEXT` is only used where the
annotation truly reaches no string;
`test_round2_every_absent_frame_field_is_individually_rejected` requires a
populating payload for *every* `ABSENT` field and proves each is rejected,
so a field cannot be classified without also being enforced. `denied`
additionally may not carry `subject_ref`/`subject_set_ref` -- the outcome
must not confirm or deny which subject was asked about.

`needs_clarification` is **deliberately excluded** from `NO_ANSWER_OUTCOMES`:
unlike the five error outcomes above, it projects to a v1 `DevAnswer` with
`insufficient_evidence` status (`compat.py::_project_needs_clarification`),
and its frame may legitimately carry a disambiguation-relevant `subject_ref`
(used to build a v1 `DevDisambiguationCandidate`). Only its answer
*content* -- `sections`/`facts` -- is required to stay empty, which the
original `validate_outcome_consistency` check already enforced.

The v1 projector no longer reads any text off a no-answer frame at all:
`_project_error` builds `safe_message` and `remediation` from
`CANONICAL_NO_ANSWER_COPY` / `CANONICAL_NO_ANSWER_REMEDIATION` and
`dev_error_remediation`. That is deliberately redundant with the frame's own
`CANONICAL` pinning -- the v1 boundary was where the disclosure actually
reached a client, so it does not depend on an upstream invariant staying
true.

### Immutability is structural, not just `frozen=True`

`ConfigDict(frozen=True)` blocks attribute *rebinding*; it leaves a `list`
field's contents mutable. Adversarial review cleared `entries` and
`mention_ids` in place on a validated `DevResolutionLedger` snapshot, which
silently defeated `validate_ledger_extends` -- the baseline it compares
against became empty, so any rewrite passed as an "extension".

Every collection field on every v2 contract is now a `tuple`.

**Closure argument.** The property is asserted over the whole model space
rather than the one model reviewed:
`test_round2_no_v2_model_has_a_mutable_collection_field` walks every
`ContractModelV2` subclass and fails on any field whose annotation reaches a
`list`, `set`, or `dict`, so a new mutable field anywhere in the package
fails the suite.  `test_round2_validated_ledger_cannot_be_emptied_in_place`
is the regression form: the in-place clear now raises, and the rewrite it
was used to launder is still rejected afterwards. The JSON Schema output is
unchanged -- a variable-length tuple and a list both render as
`{"type": "array"}` with the same `minItems`/`maxItems`.

**Boundary.** v1 contract models embedded in v2 frames (`DevCoverage`,
`DevEvidenceRef`, `DevMetricRef`, ...) still use `list` fields, because
CHAOS-3294 is additive and does not modify v1.
`test_round2_v1_models_embedded_in_v2_are_an_acknowledged_boundary` pins the
exact set of embedded v1 models, so a *new* v1 embedding surfaces as a
failure and gets a decision instead of quietly widening the gap.

### Narrative/frame consistency is bounded, not general (`validate_narrative_frame_consistency`)

A narrative claiming "100% complete, ready, no open work" previously
validated against a frame declaring a 75% completion rate, `not_ready`
readiness, and an open blocking issue -- narrative text was never checked
against the frame it is paired with. `validate_narrative_frame_consistency`
adds four narrow, deterministic checks, each independently a source of
negative fixtures (`dev_answer.v2` cases `narrative_contradicts_number`,
`_readiness`, `_subject`, `_recommendation`):

1. **Numeric containment** -- every bare numeral/percentage token in the
   narrative body must match (within floating-point tolerance) a number the
   frame renders. Round 1 drew that value set from the *whole* frame, which
   review bypassed: an unrelated comparison value of `100` legitimized a
   "100% complete" claim against a 3/4 completion block. The value set is
   now **bound to the facts the narrative declares it is narrating** (its
   `referenced_fact_ids`, plus the facts of any `referenced_section_ids`),
   the frame's own canonical copy, and the completion block. Comparison
   values are no longer in the global pool at all -- a comparison's values
   are admitted only within a sentence that names that comparison's label.
   A second, independent rule covers the specific claim: a sentence using
   completion vocabulary may only cite a percentage the completion block
   itself supports, and none at all when there is no calculable completion.
2. **Readiness polarity** -- a bare "ready" claim is rejected unless
   `frame.readiness.state == "ready"`; a "not ready" claim is rejected if the
   frame's state actually is `"ready"`. Deterministic because `state` is a
   closed three-value enum.
3. **Subject identity** -- if the frame commits to a single subject, the
   narrative must name it by **canonical identity**: one of the subject's
   token sequences (full `display_label`, its last path segment, or
   `entity_id`) must occur as a *contiguous run of whole tokens*. Round 1
   used substring containment per token, so "billing-health" satisfied a
   frame committed to "full-chaos/dev-health" -- both contain the token
   `health`. Sequence containment rejects it while still accepting the
   ordinary shorthand "dev-health".
4. **Recommendation grounding** -- if the narrative reads as making a
   recommendation ("recommend(s/ed)", "recommendation(s)"), it must
   reference the specific recommendation fact by ID in
   `referenced_fact_ids`. Round 1 accepted the mere existence of *some*
   recommendation fact anywhere in the frame, so a narrative could recommend
   one thing while the frame recommended another. A section reference does
   not stand in for the fact reference here.

**Closure argument.** The three bypasses are the same defect -- a claim
validated against the frame *globally* instead of against the specific thing
it references -- so each check is now bound to a declared reference rather
than tightened in place. `test_round2_narrative_binding_rejects_each_bypass`
covers the reproduced variants;
`test_round2_each_narrative_binding_rule_is_individually_load_bearing`
disables one rule at a time and requires the other two payloads to stay
rejected, so no single over-broad rule can make the others read as covered.
Four paired tests prove the binding did not become a blanket refusal: a
narrative may still cite the frame's own completion percentage, cite the
*same* value `100` in a sentence that names the comparison it came from,
recommend a fact it references, and name the subject by its short form.

**This is not general semantic contradiction detection.** These checks catch
exactly the pattern-matchable subset of narrative/frame disagreement; an
arbitrary false claim that isn't reducible to a number, a readiness word,
the subject's canonical identity, or a recommendation reference cannot be
verified without a model in the loop. Residuals that are explicitly out of
contract-level reach: a narrative that names the committed subject *and*
some other subject; recommendation prose that references the right fact ID
but paraphrases it into a different recommendation; and any claim carrying
no number, readiness word, or name. Full narrative/frame semantic
consistency checking is owned by the TRD v2 §11 layer-6
narrative-consistency validator, tracked as **CHAOS-3297**.

### `subject_set_ref` never widens to organization scope

`compat.py::_build_resolved_scope`'s "no `subject_ref`" fallback branch
previously fired for *any* frame without a `subject_ref` -- including a
cohort (`subject_set_ref`) frame -- silently projecting cohort-specific
facts as `DirectScope.ORGANIZATION` / `ScopeResolutionOutcome.ORGANIZATION_FALLBACK`.
`dev_answer_frame.v1.subject_set_ref` is only an *opaque pointer* to a
`dev_subject_set.v1`; the frame does not embed the committed entity list, so
there is no way to build a v1 `DevScope` that faithfully names the cohort.
`project_answer_v2_to_v1` now intercepts `subject_set_ref` before
scope-building, exactly like the existing team-subject case, and returns the
same safe `feature_not_enabled` `DevError` instead of a mislabeled answer.

### `run_id` closure across frame, answer, and stream boundary

Nothing previously required the frame embedded in a `dev_answer.v2` to have
been produced by the *same* run as the answer itself. `run_id` closure is
now enforced end to end:

- `DevAnswerV2.validate_answer_invariants` requires `frame.run_id ==
  self.run_id` (in addition to the pre-existing `narrative.run_id ==
  self.run_id` check).
- `DevStreamEventV2.validate_event_payload` requires an `answer.completed`
  event's embedded `DevAnswerV2.run_id` to equal the event's own `run_id`.

(`DevError.request_id` is deliberately **not** included in this closure --
it identifies the original client request, a distinct concept from the
execution `run_id`, and the two are expected to differ.)

### The run lifecycle is one invariant, stated once

The lifecycle was checked as a handful of positional rules, each added when
its own counterexample arrived: the last event is `done`; the terminal
result precedes it; `done` appears exactly once. Every such rule covered one
marker and left its neighbour open -- a premature `done` slipped past the
"last event" rule, and then a duplicate `run.started` slipped past all of
them.

`stream._validate_run_lifecycle` replaces them with a single statement: a
stream is exactly one `run.started` at index 0, then any number of interior
events, then exactly one terminal result (`answer.completed` or `error`),
then exactly one `done` as the final event whose `terminal_kind` matches
that result.

**Closure argument.** The defect class is "a lifecycle marker occurring
somewhere other than its one allowed position", which partitions as
(each of the three markers) x (wrong count | wrong position). The invariant
requires every marker to occur exactly once *and* at exactly one index, so
both cells fail for all three markers by construction rather than by
enumeration. `test_round2_every_lifecycle_marker_misplacement_is_rejected`
runs the reproduced instances (duplicate start, duplicate done, premature
done, duplicate terminal, missing done) against it.

### Resolution ledger snapshots must extend across stream updates

`validate_stream_v2` validated each `resolution.updated` event's ledger
independently; `validate_ledger_extends` (see above) was defined but never
actually applied *between* successive ledger snapshots in the same stream,
so a later `resolution.updated` event could rewrite an earlier one's entry
instead of only appending. `validate_stream_v2` now tracks the previous
ledger snapshot across the event loop and, for every `resolution.updated`
event after the first, requires both a non-decreasing `updated_at` and a
passing `validate_ledger_extends(previous, candidate)` call.

## Compatibility: one backend projector

`contracts_v2/compat.py::project_answer_v2_to_v1` is the single backend
projection from `dev_answer.v2` to the retained v1 vocabulary (`DevAnswer` /
`DevError`); web code must not implement a second mapping. `answered` /
`answered_with_gaps` project to a v1 `DevAnswer` (never claiming `COMPLETE`
unless the frame's own coverage satisfies v1's completeness invariant);
`needs_clarification` projects to `DevAnswer` with status
`insufficient_evidence`; every "no content" outcome (`NO_ANSWER_OUTCOMES`,
above) projects to a v1 `DevError` with a safe code (`not_found` &rarr;
`scope_not_found`, `temporarily_unavailable` &rarr; `source_unavailable`,
`unsupported` &rarr; `feature_not_enabled`, `denied` &rarr; `forbidden`,
`failed` &rarr; `internal_error`); a team-subject or subject-set-cohort
frame projects to a `feature_not_enabled` `DevError` regardless of outcome
(v1 cannot faithfully represent either). Existing retained v1 transcripts
are read by the unmodified v1 module and are never reinterpreted as v2
evidence.

## TRD ambiguities resolved for this issue

- The Linear issue's own "Required contracts" list names
  `dev_question_intent.v1`, while Amendment TRD v2 §4.1 labels the same
  contract `dev_question_intent.v2`. This module uses `dev_question_intent.v1`
  literally, following the issue text (a first version of a brand-new
  contract type).
- The "required-source states enum" (`available_current` ... `truncated`)
  is attached to `dev_source_observation.v1.observed_state` (the *executed*
  outcome) rather than `dev_source_requirement.v1` (the *a-priori*
  `mandatory | conditional | optional | not_applicable` declaration), since
  it describes what was observed, not what was required.
- `.github/docs-legacy/architecture/` no longer exists in this branch (it
  was removed and folded into `docs/` before this branch's base commit).
  This document lives in the current, mkdocs-governed home instead:
  `docs/contribute/architecture/`, listed in `mkdocs.yml` nav and
  `docs-data/ia/contribute.tsv` like every other page in this section.
