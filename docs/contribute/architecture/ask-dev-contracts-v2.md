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

1. Every v2 contract is frozen (`extra="forbid"`, `frozen=True`) -- a ledger
   cannot be mutated in place, only superseded by constructing a new one.
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

## Compatibility: one backend projector

`contracts_v2/compat.py::project_answer_v2_to_v1` is the single backend
projection from `dev_answer.v2` to the retained v1 vocabulary (`DevAnswer` /
`DevError`); web code must not implement a second mapping. `answered` /
`answered_with_gaps` project to a v1 `DevAnswer` (never claiming `COMPLETE`
unless the frame's own coverage satisfies v1's completeness invariant);
`needs_clarification` projects to `DevAnswer` with status
`insufficient_evidence`; every "no content" outcome projects to a v1
`DevError` with a safe code (`not_found` &rarr; `scope_not_found`,
`temporarily_unavailable` &rarr; `source_unavailable`, `unsupported` &rarr;
`feature_not_enabled`, `denied` &rarr; `forbidden`, `failed` &rarr;
`internal_error`). Existing retained v1 transcripts are read by the
unmodified v1 module and are never reinterpreted as v2 evidence.

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
