---
page_id: con-ask-dev-preflight
summary: How Ask Dev interprets a question and resolves every named subject before any evidence tool runs.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/api/dev/question_interpreter.py
  - src/dev_health_ops/api/dev/subject_preflight.py
  - src/dev_health_ops/api/dev/preflight_outcomes.py
  - src/dev_health_ops/api/dev/orchestrator.py
  - Amendment TRD v2 -- Ask Dev Wave 3.1 (Linear, project Ask Dev)
applicability: current
lifecycle: active
---

# Ask Dev subject preflight

Ask Dev decides what a question is about, and which authorized entity it
names, **on the server and before the model is asked anything**. A named
subject that the catalog cannot confirm stops the run: no status, change,
metric, or evidence tool executes without an exact, committed subject.
{: .fc-page-lede }

This page describes the runtime behavior CHAOS-3292 introduced. The wire
contracts it produces are described in
[Ask Dev v2 contracts](ask-dev-contracts-v2.md).

## Why it exists

The previous design asked the model to resolve a name first and then judged
the finished answer afterwards. Both halves were advisory. The prompt told the
model to call `resolve_scope.v1` before any status tool, and a post-hoc check
compared entity names extracted from the question against the model's own
answer text. That check could only ever act by **deleting an answer that had
already been produced**: a miss left a fabricated premise standing, and a false
positive destroyed a good answer.

Committing the subject before the model round inverts this. The model is
*given* a resolved scope in its prompt instead of being asked to earn one, so
there is no opportunity to narrate an unresolved name in the first place.

## The phases

A run now passes through two new states between scope authorization and the
first model round:

| State | What happens |
| --- | --- |
| `interpreting` | The question is classified into one of twelve launch intents, and every named subject is extracted as an ordered *mention*. |
| `resolving_subjects` | Each mention is resolved against the organization's authorized catalog, and the outcome is appended to a resolution ledger. |

Both are non-terminal, so a run parked in either one correctly reads as still
in flight.

### Interpretation

Intent comes from deterministic recognizers over a normalized question, with
an optional constrained model fallback for low-confidence cases. The
recognizers do lexical matching, and the important property is not the
technique but the failure mode: a recognizer only picks a member of a closed
enum, so a miss degrades to a bounded investigation — the behavior Ask Dev
already had — and **can never delete an answer or widen a scope**.

The client's `question_class` is recorded for audit and ignored for planning.

The optional model fallback has no tools, no catalog access, and no scope
authority. It receives the raw question and nothing else, and every span it
proposes must be a literal substring of that question, which is what makes
"it cannot author an entity name" structural rather than aspirational. Any
provider error, timeout, or rejected proposal asks the user to clarify; it
never falls back to organization scope.

### Subject resolution

Each mention resolves to exactly one of five outcomes: an exact match,
ambiguous candidates, no authorized match, a temporarily unavailable catalog,
or an unsupported kind. Every catalog query is filtered by organization in
SQL, so an entity belonging to another tenant is simply absent — it reads as
"no authorized match", never as "forbidden", and nothing about it appears in
any user-visible string.

Resolutions are recorded on an **append-only ledger**. A later success cannot
erase an earlier unresolved mention: entries carry contiguous ordinals, the
objects are immutable, and every update is checked against the previous
snapshot. When a question names several subjects, the outcome is decided by
the lowest-ordinal unresolved mention — "the first thing you named" — which is
stable and independent of how fast the catalog answered.

## What the model is offered

Once a subject is committed there is nothing left for the model to resolve, so
`resolve_scope.v1` is withheld for the rest of the run. This is a per-run tool
allowlist, applied both to the tools advertised to the provider and re-checked
when a tool call is dispatched. When no subject was committed — an
organization-wide question, or a run with the feature off — every tool is
advertised exactly as before.

## Outcomes

| Situation | What the user sees |
| --- | --- |
| Every named subject resolved | The answer, scoped to that subject |
| The question named nothing | An organization-wide answer, as before |
| A named subject does not exist here | "No matching subject was found" |
| A name matches several entities | A request to say which one |
| The catalog is briefly unavailable | "Temporarily unavailable", retryable |
| A named team, or several named subjects at once | "Not supported yet" |

The last row is a deliberate interim. A resolved team is recorded honestly as
an exact match — the team demonstrably exists — but the current scope
vocabulary cannot carry a team subject, and a cohort of several subjects has
no faithful single-subject representation either. Answering under organization
scope in either case would reintroduce exactly the defect this work closes.
Both become real answers when team and multi-subject support land.

## Rollout

The whole path is behind the `ask_dev_wave_3_1` entitlement, which is
explicit-enable and off by default. With it off, a run behaves exactly as it
did before: no interpretation, no preflight, every tool advertised, and the
previous named-entity backstop still terminating.

With it on, that backstop is kept as defense in depth but is **telemetry
only** — it records a content-free reason code on the run row and does not
change the outcome. A firing there means the new path missed something and is
worth investigating, not worth acting on mid-run.

## Diagnostics

Two closed-vocabulary columns on the run row record what happened:
`preflight_outcome` (which branch the preflight took) and
`legacy_guard_reason` (whether the retained backstop fired anyway). Neither
can carry question text, an entity name, or catalog content.
