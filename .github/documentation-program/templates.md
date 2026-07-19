# Documentation authoring templates

Copy the closest template, delete every instruction that does not apply, and write with real Dev Health content. These templates are page anatomy, not mandatory decorative components.

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

- [Task written as a verb](task-url.md) — one-line outcome.
- [Task written as a verb](task-url.md) — one-line outcome.

## Find exact information

- [Relevant reference](reference-url.md)
- [Troubleshooting index](troubleshooting/index.md)
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

- [Diagnose SYMPTOM](../troubleshooting/symptom.md)

## Related information

- [Exact concept](...)
- [Exact reference](...)
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

```markdown
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
```

## Deprecation

```markdown
# OLD capability is deprecated

**Status:** Deprecated
**Replacement:** [NEW capability](...)
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

- [Use Dev Health](../use/)
- [Administer Dev Health](../admin/)
- [Install and operate](../operate/)
- [Integrate and extend](../integrate/)
- [Look up exact information](../reference/)
- [Contribute](../contribute/)

## Check prerequisites

Only the conditions that block the first real task.

## Read a minimal shared concept

Only concepts proven necessary by task testing.
```

The vertical slice decides whether this branch remains separate, collapses into the root and `/use/`, or becomes a task-chooser redirect.
