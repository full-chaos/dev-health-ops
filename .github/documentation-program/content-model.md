# Documentation content model

This is the Phase 3 authoring contract for the **User Guides & Development Documentation** remediation. It is internal project guidance until the Contribute section is migrated.

## Principles

1. A page exists to help a reader complete one task, understand one concept, diagnose one failure, or look up one class of exact facts.
2. Content type, audience, product area, and lifecycle are separate dimensions.
3. A public page has one canonical URL and navigation location.
4. Task pages link to canonical concepts and reference; they do not copy large definitions or tables.
5. Internal plans, PRDs, QA specifications, evidence artifacts, and implementation notes use a separate internal schema and never become public by omission.
6. Metadata is present only when it drives navigation, search, ownership, lifecycle, or factual validation.
7. `/get-started/` content is authored from blank reader-task briefs. Existing onboarding titles, sequences, and prose are not templates.

## Public page types

| Type | Reader intent | Required structure | Not this |
| --- | --- | --- | --- |
| `landing` | Choose a task within a domain | Scope, child task groups, one-sentence exclusions, troubleshooting entry | Marketing home, prose essay, complete sitemap |
| `tutorial` | Reach a first successful outcome while learning | Outcome, prerequisites, ordered path, checkpoints, result, next task | General reference, feature tour |
| `task-guide` | Complete a specific action | Outcome, prerequisites, steps, expected result, failure path, related exact reference | Concept essay |
| `workflow-guide` | Use and interpret a product workflow or view | Reader question, context, current UI path, interpretation sequence, evidence path, limitations, next actions, failure states | Screenshot gallery, feature specification |
| `concept` | Understand a durable model or distinction | Definition, why it matters, model, examples, boundaries, linked tasks/reference | Procedure |
| `troubleshooting` | Recover from a symptom | Symptom, scope, safety check, diagnostics, causes, resolution, verification, escalation | General FAQ |
| `runbook` | Diagnose and recover an operational failure | Trigger, impact, prerequisites, immediate safety, diagnosis, decision branches, recovery, verification, rollback, escalation, retained evidence | User-facing troubleshooting |
| `api-reference` | Look up an exact supported API contract | Support/applicability, auth, endpoint/field, request, response, errors, limits, examples | Integration tutorial |
| `cli-reference` | Look up exact command behavior | Command tree, syntax, arguments, defaults, environment, output, exit status, examples | Operations procedure |
| `configuration-reference` | Look up exact configuration behavior | Key, type, required/default, secret, reload/restart, applicability, source | Deployment narrative |
| `generated-reference` | Read code/schema-derived exact facts | Generated facts, source link, generation rule, applicability | Generated prose |
| `architecture` | Understand stable contributor boundaries | Problem, durable boundary, components, data flow, invariants, change guidance, source links | Project implementation plan |
| `deprecation` | Move from an old supported path to a replacement | Status, affected versions, replacement, migration, deadline, compatibility, redirect/retention | Changelog dump |

## Internal artifact types

The following live outside the public docs source unless a durable reader need is separately approved:

* project charter and PRD;
* IA drafts and migration matrices;
* ADRs about delivery implementation that do not help a supported reader;
* QA/e2e specifications;
* browser evidence, screenshot manifests, hashes, and fixture receipts;
* implementation plans, rollout plans, and issue-specific notes;
* raw audit output;
* temporary design explorations;
* internal agent test fixtures.

## Minimal metadata

The IA manifest owns stable placement facts. Front matter does not repeat them unless a build integration requires it.

### Required for active public pages

```yaml
page_id: use-investment-investigate-effort
summary: Determine where effort appears to be going and follow the result to evidence.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/queries/investment.py
applicability: current
lifecycle: active
```

### Optional when used

```yaml
prerequisites:
  - connected source data
  - repository or team scope
review_interval: P90D
aliases:
  - /user-guide/investment-view/
deprecated_by: /use/investment/investigate-effort/
```

### Prohibited duplication

Do not repeat these in front matter when the IA manifest already supplies them:

* canonical URL;
* navigation parent and order;
* navigation label;
* public/internal state;
* product area;
* redirect sources.

## Risk and review

| Risk | Examples | Review requirement |
| --- | --- | --- |
| Critical | Credentials, destructive operations, upgrades, rollback, security incidents, data loss | Source owner, operations/security, content, and accessibility |
| High | Permissions, provider setup, API contracts, calculations, AI/analytics interpretation | Source owner, content, IA, accessibility |
| Medium | Ordinary feature workflows and concepts | Product/source owner and content |
| Low | Glossary, navigation index, non-behavioral explanation | Content/IA owner |

Age alone does not make a page incorrect. Review is triggered by source changes, product changes, support findings, or the risk-based interval.

## Source-of-truth policy

* Literal keys, enums, defaults, schemas, and taxonomies should be generated or checked from code.
* Procedures are verified against the supported UI, CLI, API, or deployment artifact.
* Narrative interpretation is human-authored and reviewed.
* A page may cite several sources, but it has one accountable owner.
* A source link is not a substitute for explaining the supported reader task.

## Page creation gate

Before creating a page, answer:

1. What exact reader task, concept, symptom, or lookup does it serve?
2. Which locked domain owns that outcome?
3. Does a canonical page already answer it?
4. Which content type fits?
5. What is the source of truth?
6. What failure or limitation must be visible?
7. Does the page require a new URL, or should an existing page be extended?
8. Is the material public, contributor-facing, or internal?
