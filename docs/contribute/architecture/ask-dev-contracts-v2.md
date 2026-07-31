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

Four adversarial-review rounds ran against the merged contracts. Round 1
reproduced six counterexamples the original five validators did not catch.
Round 2 then bypassed three of those fixes through adjacent variants, and
found one new gap. Round 3 cleared most of the round-2 closures but narrowed
three cells that were still open. Round 4 found the last identifier cell one
level out from where round 3's partition test was looking, and an
over-rejection introduced by round 3's own fix. Because that made repeated
rounds on the same defect classes, the fixes are deliberately **class
closures** rather than further patches: each replaces the previous check's
polarity or scope, and each carries an explicit closure argument -- the
partition of the input space and why every cell is covered -- recorded in the
sections below and in the `validators` module docstring.

Two lessons generalise beyond the individual fixes, and both came from a
closure being *stated* more broadly than it was *checked*:

* **A partition test certifies exactly what it enumerates.** Round 3's claim
  ("every field is absent, canonical, closed, or one of two named
  identifiers") was true of `DevAnswerFrame` and false of the answer envelope
  that carries it, because the test walked the frame's policy table rather
  than the object graph. Round 4's version derives the reachable model set and
  fails if it is not exactly what the claim assumes.
* **A guard that over-rejects is a defect, not a conservative default.**
  Round 3 gated completion numbers on a five-word regex; it refused truthful
  narration of the frame's own completion block. No fixture caught it because
  every fixture used a word from the list. Tightening is only free when the
  positive path is tested in the same breath -- hence the paired
  truthful/false variants in `_COMPLETION_NARRATION_PAIRS`.

The three round-3 cells share one shape, worth naming because it is what the
earlier closures kept getting wrong: **a constraint on a value's form is not
a constraint on where the value came from.** An identifier-shaped token can
be a subject's name; a `frozen=True` object can hold a mutable list; a number
that appears somewhere in the frame is not thereby a number that grounds the
sentence citing it. Each fix replaces a predicate over form with membership
of a server-owned set, or with a structural impossibility.

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
| `CLOSED_VOCABULARY` | Every string the field reaches must be a member of a server-owned closed set registered with the policy. The producer picks *from* the vocabulary and cannot contribute *to* it. |
| `IDENTIFIER` | Every string the field reaches must be a whitespace-free identifier token, checked **at runtime on the value**. Classifying a free-text field this way is not an escape hatch: prose in it fails validation. |
| `NON_TEXT` | Reaches no string at all. |
| `SELF_VALIDATED` | A nested contract with its own registered policy. |

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

#### Round 3: identifier *shape* is not identifier *provenance*

The round-2 partition left two `IDENTIFIER` cells, justified on the grounds
that the fields carried "platform tokens, not copy". `IDENTIFIER` constrains
a token's shape, and a subject's name is a perfectly well-shaped token:
review round 3 put `"private/Nightfall"` into
`coverage.unavailable_required_sources` and into `versions.plan_id` on a
`denied` frame, and both validated and serialized. Neither is prose, so
nothing in the projection objected -- the field said "no whitespace", and the
disclosure had none.

Both cells are replaced by membership rather than by a tighter shape:

* **`coverage`** is now `DevCoverageV2` (`contracts_v2/embedded.py`), whose
  `unavailable_required_sources` / `stale_required_sources` are the closed
  `base.SourceClass` enum rather than `OpaqueID`. The field is reached
  through `SELF_VALIDATED`, so its counts and timestamp are separately
  classified `NON_TEXT`. A denial can still answer "how many sources were
  required" while no leaf can carry a producer-chosen string. Because the
  vocabulary is enforced by the *type*, it holds for every outcome, not only
  no-answer ones.
* **`versions`** is `ABSENT`: a no-answer outcome carries no provenance block
  at all. `DevFrameVersions` is seven version strings plus a plan ID;
  constraining each is a weaker statement than not emitting the block, and a
  denial's provenance is recoverable from `run_id` server-side. The field is
  optional on the model *only* so this is expressible;
  `validators.validate_versions_presence` requires it for every outcome that
  carries content, with its own fail-before/pass-after mutation pair
  (`answered_without_versions`), so optionality did not become droppability.
  Independently, every `DevFrameVersions` field is now a
  `base.PlatformVersionToken` (a dotted, lowercase, version-suffixed token)
  and `plan_id` a `PlanRegistryID`, so a subject-derived identifier is not
  spellable in the block even on the answered path where it *is* emitted.
* `schema_version` and `public_outcome` move from `IDENTIFIER` to
  `CLOSED_VOCABULARY`, which is what they always meant. The
  `schema_version` vocabulary is read off the model's own `Literal`
  annotation (`validators.literal_vocabulary`) rather than hand-copied.

**Closure argument (round 3).** For a no-answer projection, every field of
`DevAnswerFrame` is now `ABSENT`, `CANONICAL` server copy, `NON_TEXT`, a
`CLOSED_VOCABULARY` the server owns, or `SELF_VALIDATED` into a nested policy
that is itself only those classes -- with exactly two exceptions, `frame_id`
and `run_id`. `test_round3_denied_projection_admits_no_producer_chosen_string`
asserts that partition against the policy table itself, naming those two, so
a *third* `IDENTIFIER` cell cannot be added without failing.

Because both closed vocabularies are enforced by the type as well as the
policy, the type would mask the policy and make the classification read as
covered while doing nothing. Two tests construct objects past validation
(`model_construct`) and call `validate_no_answer_projection` directly, so the
`CLOSED_VOCABULARY` and `IDENTIFIER` layers are each shown to reject on their
own -- a later widening of an annotation cannot silently reopen the channel.

#### Round 4: the partition was scoped to the frame, and the residual is closed

Round 3 recorded `frame_id` / `run_id` as a residual and asserted its partition
against `NO_ANSWER_FRAME_FIELD_POLICY`. Review round 4 showed the claim was
**inaccurate at the envelope level**: `DevAnswerV2.answer_id` and
`conversation_id` sit one level further out, were never enumerated by the
partition test, and accepted `"private/Nightfall"` on a denied answer --
user-visible on the outer envelope, not the frame. A partition test that
enumerates only part of the object it describes will keep certifying the part
it looks at.

The residual is now closed rather than re-documented. `base.ServerHandle` is a
canonical hyphenated UUID, **pinned to what the persistence layer actually
mints**: every Ask Dev correlation ID in `models/dev_persistence.py` is a
`GUID` column with `default=uuid.uuid4` (`DevConversation.id` line 45,
`DevMessage.id`/`client_message_id`/`answer_id` 106-113,
`DevRun.id`/`request_id`/`retry_of_run_id`/`answer_id` 171-182), the router
serializes them with `str(...)`, and `orchestrator_persistence.py:149` parses
`uuid.UUID(answer.answer_id)` on the way back in -- a non-UUID `answer_id` was
already a runtime failure, so this makes the wire contract state what the
system already required. Hex digits and hyphens cannot spell a project name.

It is applied **uniformly across every outcome**, deliberately: a run ID is
minted at `run.started` before the outcome is known, so a grammar that applied
only to no-answer outcomes would make a legal server behaviour unrepresentable
depending on how the run ended.

**Scope: which identifiers are handles.** Not every `*_id` is a mint, and
forcing one grammar on all 66 identifier fields would be wrong -- `entity_id`
is `repo_dev_health` by design. The families are:

| Family | Grammar | Examples |
| --- | --- | --- |
| Server-minted correlation handles | `ServerHandle` (UUID) | `run_id`, `frame_id`, `answer_id`, `conversation_id`, `narrative_id`, `request_id`, `client_message_id`, `retry_of_run_id`, `result_id`, `ledger_id`, `set_id`, `mention_id`, `observation_id` |
| Provider entity keys | `OpaqueID` -- subject-derived *by design*; `ABSENT` on a no-answer outcome | `entity_id`, `repository_id`, `team_id`, `organization_id` |
| Intra-document reference keys | `OpaqueID` -- scoped to one document, meaningless outside it; `ABSENT` on a no-answer outcome | `fact_id`, `section_id`, `evidence_ref_id`, `path_id`, `step_id` |
| Closed vocabularies / registries | enum or token grammar | `intent_id`, `metric_id`, `route_id`, `plan_id`, `rule_id`, `adapter_id` |

**Closure argument (round 4).**
`test_round4_every_identifier_on_a_denied_envelope_is_an_opaque_handle` walks
the **answer envelope**, not the frame: it computes which models a no-answer
outcome can carry by following only fields the policy does not blank (exactly
`DevAnswerV2`, `DevAnswerFrame`, `DevCoverageV2`), enumerates every identifier
cell that survives, pins the set to those five, and requires every one to be a
server handle. There are **no named exceptions left**.
`test_round4_every_v2_identifier_is_classified` then covers the other 61 cells
across all outcomes: each must be a handle, a closed vocabulary, a provenance
token, or named in `_NON_HANDLE_IDENTIFIER_REASONS` with the reason it cannot
be a mint -- and the reasons table is itself checked against the live models,
so it cannot name a field that no longer exists.

That enumeration immediately paid for itself: it found
`DevInvestigationPlan.intent_id` typed as a free `OpaqueID` while
`DevQuestionIntent.intent_id` was the closed `QuestionIntentID` enum -- the
same concept with two types, one of them unconstrained. It is now the enum.

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

#### Round 3: the "acknowledged boundary" was inside the closure

Round 2 recorded the v1 models embedded in v2 frames (`DevCoverage`,
`DevEvidenceRef`, `DevMetricRef`, ...) as an acknowledged boundary, on the
grounds that CHAOS-3294 is additive and does not modify v1. Review round 3
showed that was not a boundary but a hole, because it sat *inside* the object
graph the v2 validators had just certified:

```python
frame = DevAnswerFrame.model_validate(payload)   # fully validated
frame.coverage.unavailable_required_sources.append("private/Nightfall")
frame.model_dump(mode="json")                    # ...and it serializes
```

Twelve mutable collection fields were reachable from the v2 contracts at that
point, across `DevCoverage`, `DevError`, `DevEvidenceRef`, `DevMetricRef`,
`DevScope` and `DevSurfaceContext`.

`contracts_v2/embedded.py` now defines a deeply immutable mirror of each --
`DevCoverageV2`, `DevErrorV2`, `DevEvidenceRefV2`, `DevMetricRefV2`,
`DevScopeV2`, `DevSurfaceContextV2` -- and the v2 contracts embed those.
Each mirror inherits from **both** `ContractModelV2` and its v1 original and
overrides only the mutable fields, so every v1 field constraint,
`field_validator` and `model_validator` (`DevScope`'s direct-scope and
surface-context invariants are substantial) is inherited rather than
duplicated: the mirror cannot drift from the original the way a
reimplementation would. v1 itself is untouched, so the issue stays additive.

**Closure argument (round 3).** The predicate is no longer "no v2 model has a
mutable collection" plus a list of exceptions, but: *no model reachable from
any v2 contract, at any depth, whether or not it is a v2 model, declares a
mutable collection.*
`test_round3_no_mutable_collection_anywhere_in_the_v2_closure` walks that
whole closure by introspection and asserts it is empty, so a newly embedded
v1 model with a `list` field fails there instead of being added to an
exception list. `test_round3_validated_frame_cannot_be_mutated_after_the_fact`
is the regression form: the exact `append` review used now raises, and the
serialized output is unchanged by the attempt.

A v1 model with no collection at any depth (`DevEntityRef`, `DevTimeRange`,
`DevCitationLink`, `DevEvidenceFlags`, `DevMetricPoint`, `DevModelMetadata`)
is already immutable in fact under `frozen=True` and needs no mirror -- the
recursive predicate covers them rather than exempting them.

**Boundary (the real one).** A mirror `isinstance`-passes as its v1 original,
so pydantic would accept it into a v1-typed field untouched and then ask v1's
`list` serializer to emit a `tuple`. The v2-to-v1 projector therefore converts
explicitly (`compat._as_v1`), and
`test_round3_v1_projection_still_emits_plain_v1_collections` asserts the
projected v1 objects are exactly their v1 types with `list` collections.

### Narrative/frame consistency is bounded, not general (`validate_narrative_frame_consistency`)

A narrative claiming "100% complete, ready, no open work" previously
validated against a frame declaring a 75% completion rate, `not_ready`
readiness, and an open blocking issue -- narrative text was never checked
against the frame it is paired with. `validate_narrative_frame_consistency`
adds four narrow, deterministic checks, each independently a source of
negative fixtures (`dev_answer.v2` cases `narrative_contradicts_number`,
`_readiness`, `_subject`, `_recommendation`):

1. **Numeric containment** -- every bare numeral/percentage token in the
   narrative body must be grounded by that sentence's own references. Round 1
   drew the value set from the *whole* frame, which review bypassed: an
   unrelated comparison value of `100` legitimized a "100% complete" claim
   against a 3/4 completion block. Round 2 narrowed the set to the referenced
   facts, the frame's canonical copy and the completion block -- but it was
   still a **pool**, unioned once and offered to every sentence, and round 3
   showed that pool failing in both directions at once (see below). There is
   now no pool. See "Per-sentence numeric admission". A second, independent
   rule covers the specific claim: a sentence using completion vocabulary may
   only cite a percentage the completion block itself supports, and none at
   all when there is no calculable completion.
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

#### Per-sentence numeric admission (round 3)

Review round 3 reproduced the round-2 pool failing in both directions from
the same cause. It **over-accepted**: `_bound_numeric_values` unioned the
completion numerator, denominator and rate into the set offered to every
sentence, so a frame with a 3/4 completion block accepted the narrative claim
"Repository dev-health has 4 open security incidents" -- `4` was in the pool,
as a denominator, and the sentence was about something else entirely. It also
**over-rejected**: a subject genuinely named `project-42` could not be named
in its own narrative, because `42` appeared in no fact.

Both are one bug: a number was admitted or refused by *membership in a global
set* rather than by *the citing sentence's own grounding*. The pool is gone.
Each sentence admits exactly:

| Source | Condition |
| --- | --- |
| Numerals in the text of facts the narrative references | always (`referenced_fact_ids`, plus the facts of any `referenced_section_ids`) |
| Numerals in the committed subject's canonical identity forms | always -- server-committed, not producer-chosen |
| A comparison's `current_value` / `comparison_value` | only in a sentence that names that comparison's label |
| The completion **proportion** -- rate or its complement -- written with its unit (`75%`, `25%`) or as a bare decimal (`0.75`, `0.25`) | always |
| The completion **counts** -- numerator, denominator | only in a sentence that renders the block's own ratio (`3 of 4`, `3/4`, `3 out of 4`) |

The last two rows are the round-4 revision; see below.

`direct_answer`, `limitations`, `safe_follow_up_questions` and the readiness
reasons were in the round-2 pool and are **not** in this list: they are
frame-level free text, and a number appearing in one of them grounds nothing
about the sentence citing it. A narrative that wants to restate a number must
reference the fact carrying it.

**Closure argument (round 3).** Each admission rule names both a *source* and
the *condition* under which that source applies to a given sentence, so there
is no cell where a value is admitted without a stated reason. The direction
pairs are what make it binding rather than merely stricter, and each is a
fail-before/pass-after pair against the pre-fix source:
`test_round3_completion_numbers_are_not_a_global_narrative_token_pool`
(the incidents claim, previously accepted, now rejected) against
`test_round2_narrative_may_cite_the_frames_own_completion_percentage` (the
same completion values, in a completion sentence, still accepted);
`test_round3_narrative_may_name_a_subject_whose_identity_contains_a_number`
(previously rejected, now accepted); and
`test_round3_frame_free_text_no_longer_grounds_a_narrative_number`, which
rejects a number carried only by `limitations` and then accepts the same
sentence once a fact carrying it is referenced.

#### Round 4: the completion gate was a five-word regex

Round 3 gated admission of completion values on
`\b(complete|completed|completion|done|finished)\b`. Review round 4 showed
that gate rejecting truthful narration of the frame's *own* block --
"Repository dev-health has made 75% progress" and "has passed 3 of 4 required
checks" were both refused, while "finished" passed. That is over-rejection on
the positive path, which is worse than the leak it replaced: a validator that
refuses correct answers makes the feature unusable, and there is no fixture
that would have caught it, because every fixture used a word from the list.

Lengthening the word list is not a fix -- it is the same defect with a longer
list, and the next truthful phrasing outside it fails the same way. The gate
is removed instead. **Admission is now by citation shape, not by vocabulary:**

* the completion **proportion** (the rate and its complement) is admitted
  wherever it is written *with its unit* -- `75%`, `25%`, or the bare decimals
  `0.75` / `0.25`. A proportion carrying its unit is not a plausible count, so
  it needs no gate. A bare `75` is not admitted by this rule.
* the completion **counts** (numerator, denominator) are admitted only in a
  sentence that renders the block's own ratio -- `3 of 4`, `3/4`,
  `3 out of 4`, `3 of the 4`. Mere co-occurrence is deliberately not enough:
  "4 open security incidents and 3 unresolved alerts" contains both integers
  and grounds neither, and admitting on co-occurrence would reopen the round-3
  counterexample.

**Stated limitation, with an escape hatch.** A *bare* completion count in any
other position is not admitted, because nothing distinguishes "3 required
checks passed" from "3 open incidents" -- the contract cannot see which one
the sentence means. A narrative that wants to cite a bare count must reference
the fact that carries it, which is the same rule already stated for frame free
text. `test_round4_bare_completion_count_needs_a_referenced_fact` proves both
halves, so the limitation is not an unqualified refusal.

**Where vocabulary survives, and why that is safe.** One check still needs to
know whether a sentence makes a completion claim: a percentage that is
grounded some *other* way (an unrelated fact carrying `100`) must not be
reusable as a false completion percentage. That check uses an explicit,
documented grammar (`_COMPLETION_CLAIM_STEMS`: complete/completes/completed/
completion, done, finish/finishes/finished, progress/progressed, pass/passes/
passed, close/closes/closed, deliver/delivers/delivered, ship/ships/shipped,
remaining, outstanding). The asymmetry is the point: this vocabulary only
decides whether a *stricter* check applies, so a missing word can weaken the
check but can never open an admission channel -- the opposite of its round-3
use, where a missing word caused a refusal. A percentage in such a sentence
must match the completion proportion or a comparison the sentence names.

**Closure argument (round 4).** `_COMPLETION_NARRATION_PAIRS` carries a
truthful/false pair for each supported verb (progress, passed, closed,
delivered, remaining) and for the ratio and decimal renderings, asserted in
both directions against the same 3/4 block, so the fix cannot degenerate into
"admit any number near a completion word".
`test_round4_completion_admission_does_not_depend_on_vocabulary` validates a
sentence containing *no* stem from the grammar and asserts that absence
explicitly, which is what distinguishes removing the gate from widening it.

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
