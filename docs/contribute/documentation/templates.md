---
page_id: con-doc-templates
summary: Copyable page skeletons for landings, task guides, workflow guides, concepts, troubleshooting, runbooks, reference, and deprecations.
content_type: reference
owner: documentation
applicability: current
lifecycle: active
---

# Documentation authoring templates

Copy the closest template, delete every instruction that does not apply, and write with real Dev Health content. These templates are page anatomy, not mandatory decorative components.

Placeholders are written in upper case. Replace each one, including every `LINK TO ...` line, with a real Markdown link to the canonical page.

## Section landing

```markdown
---
page_id: DOMAIN-ID
summary: One sentence defining the work covered by this section.
content_type: landing
owner: TEAM
source_of_truth: []
applicability: current
lifecycle: active
---

# Section name

One or two sentences: what this section helps the reader do and what belongs elsewhere.

## Common tasks

- LINK TO A TASK WRITTEN AS A VERB — one-line outcome.
- LINK TO A TASK WRITTEN AS A VERB — one-line outcome.

## Find exact information

- LINK TO THE RELEVANT REFERENCE
- LINK TO THE TROUBLESHOOTING INDEX
```

## Task guide

```markdown
---
page_id: PAGE-ID
summary: Complete OUTCOME.
content_type: task-guide
owner: TEAM
source_of_truth:
  - SOURCE
applicability: current
lifecycle: active
---

# Complete OUTCOME

Use this procedure when CONDITION.

## Before you begin

- Required role:
- Required source or environment:
- Data or safety prerequisite:

## Procedure

1. Do the first supported action.
2. Do the next action.

## Verify the result

State the exact UI state, response, log, or health signal that confirms success.

## If it does not work

- LINK TO DIAGNOSE SYMPTOM

## Related information

- LINK TO THE EXACT CONCEPT
- LINK TO THE EXACT REFERENCE
```

## Workflow or view guide

```markdown
# Answer READER QUESTION with WORKFLOW

Use this workflow when ...

## Set the context

State route, required role, scope, time window, coverage, and availability.

## Read the result

Explain the sequence in which to inspect the workflow. Define what each visible value does and does not mean.

## Follow the evidence

Describe the supported drill-down and confidence/coverage cues.

## Decide the next action

Give team- or workflow-level next actions. Do not prescribe a person-level verdict.

## Empty, incomplete, stale, or failed states

Link each state to the exact recovery page.

## Exact definitions

Link to canonical metric, taxonomy, schema, or API reference.
```

## Concept

```markdown
# Concept name

A precise definition.

## Why it matters

Explain which tasks depend on the concept.

## Model

Explain the durable parts of the concept and its boundaries.

## Example

Use one concrete example with stated assumptions.

## What this does not mean

Prevent the most likely incorrect conclusion.

## Use this concept

Link to representative tasks and exact reference.
```

## Troubleshooting

```markdown
# Diagnose SYMPTOM

## Symptom

Describe what the reader sees, including relevant status or error text.

## Scope and safety

State required role, environment, and actions not to take yet.

## Check the cause

1. Check the lowest-risk, highest-signal condition.
2. Branch to the next condition.

## Resolve it

Give the supported resolution for each confirmed cause.

## Verify recovery

State the expected signal and timing.

## Escalate

State what evidence to retain and where to send it.
```

## Operator runbook

```markdown
# Recover from INCIDENT

## Trigger and impact
## Prerequisites and authority
## Immediate safety checks
## Diagnosis
## Decision branches
## Recovery
## Verification
## Rollback
## Escalation and communication
## Evidence to retain
## Follow-up
```

## API or configuration reference

````markdown
# Reference family

**Support:** ...
**Required role or entitlement:** ...
**Applicability:** ...

## Item

| Field | Value |
| --- | --- |
| Name | |
| Type | |
| Required | |
| Default | |
| Secret | |
| Reload or restart | |
| Source | |

### Example

```language
MINIMAL EXAMPLE
```

### Errors or limits

Exact supported behavior.
````

## Deprecation

```markdown
# OLD capability is deprecated

**Status:** Deprecated
**Replacement:** LINK TO THE NEW CAPABILITY
**Affected versions:** ...
**Removal or review date:** ...

## What changes
## Migrate
## Compatibility and rollback
## Redirect and retained-history behavior
```

## `/get-started/` brief

Do not copy the current onboarding pages.

```markdown
# Get started

One precise sentence explaining the product's supported purpose and one sentence explaining what it does not do.

## Choose the task in front of you

- LINK TO USE DEV HEALTH
- LINK TO ADMINISTER DEV HEALTH
- LINK TO INSTALL AND OPERATE
- LINK TO INTEGRATE AND EXTEND
- LINK TO REFERENCE
- LINK TO CONTRIBUTE

## Check prerequisites

Only the conditions that block the first real task.

## Read a minimal shared concept

Only concepts proven necessary by task testing.
```

Keep this branch only while reader-task testing shows it improves first-task completion. Otherwise fold its material into the root and `/use/`, or reduce it to a task chooser.
