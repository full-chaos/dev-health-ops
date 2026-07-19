# Phase 6 migration packet template

Create one packet per canonical workflow family before writing public content. Delete instructions that do not apply; do not fill unknowns with assumptions.

## Identity

- Linear issue:
- Workstream:
- Canonical page ID:
- Canonical URL:
- Content type:
- Primary reader task or symptom:
- Accountable owner:
- Risk tier:

## Reviewed inventory inputs

- Current source rows:
- Proposed disposition for each source:
- Duplicate group and canonical survivor:
- Legacy URLs:
- Redirect, archive, internal, or removal behavior:
- Relevant visual/static asset rows:

## Current product verification

- Product revision or commit:
- Supported route:
- Current UI labels:
- Required role or permission:
- Required source connection:
- Scope behavior:
- Time-window behavior:
- Filters and comparison behavior:
- Data coverage/freshness prerequisite:
- Feature availability or entitlement:
- Unsupported, renamed, planned, or removed behavior found:

## Exact sources of truth

- UI/source route implementation:
- API/query/schema contract:
- Metric/computation code:
- Taxonomy/enums/defaults:
- Provider or data-source behavior:
- Test or fixture that demonstrates supported behavior:
- Facts suitable for generation or drift validation:

## Interpretation and state model

Describe only states the product supports:

- observed facts:
- derived measures:
- model-assisted estimates or generated narrative:
- confidence/evidence/coverage behavior:
- measured zero:
- unavailable/null:
- incomplete/partial:
- stale:
- delayed/processing:
- unsupported:
- no detected association:
- what the reader must not conclude:

## Page anatomy

- Opening outcome/question:
- Prerequisites:
- Shortest supported procedure or reading sequence:
- Expected result:
- Evidence/drill-down path:
- Limitations:
- Safe next actions:
- Failure/recovery path:
- Exact concept/reference links:
- Related task links:
- Edit/source and feedback behavior:

## Visual decision

- Is a visual necessary? Why does prose, code, table, or a simple diagram not suffice?
- Source environment:
- Product revision:
- Viewport and state:
- Sanitization:
- Alt text or text equivalent:
- Owner and review trigger:

A screenshot is not required merely because the old page had one.

## Search and navigation

- Canonical navigation parent:
- Natural-language queries that should find this page:
- Competing legacy/duplicate results to remove:
- Breadcrumb and local-contents expectations:
- Contextual inbound links:
- Contextual outbound links:

## Review and gate

Before CHAOS-2995 approval, complete source verification and the migration brief only. After approval, record:

- source/product reviewer:
- content reviewer:
- IA reviewer:
- accessibility reviewer:
- design reviewer when a visual or pattern changes:
- strict build and link result:
- natural-language search result:
- mobile/keyboard/zoom result:
- redirect verification:
- accepted debt issues:

## Rollback

- URL/navigation rollback:
- Content rollback:
- Generated/reference rollback:
- Visual rollback:
- Evidence retained for diagnosis:
