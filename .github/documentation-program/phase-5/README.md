# Phase 5 — Home-to-Investment vertical slice

**Status:** active  
**Implementation issues:** CHAOS-2993 and CHAOS-2994  
**Scale decision:** CHAOS-2995

Phase 5 proves one complete reader journey before the project migrates the rest of the documentation corpus. The accepted IA, content model, Full Chaos design, and reviewed disposition matrix are inputs; this phase may reveal corrections, but it may not silently redefine them.

## Authority

Use these sources in descending order when they disagree:

1. Current supported product and code behavior.
2. The locked IA manifest in `.github/documentation-program/ia/`.
3. The reviewed inventory and disposition in `.github/documentation-program/inventory/`.
4. The Phase 3 content model, templates, and style guide.
5. The approved Full Chaos MkDocs prototype.
6. Current WIP pages only as factual leads that must be verified.

## Reader outcome

A first-time reader can enter through `/`, choose the Investment task, determine where effort appears to be going, inspect the relevant evidence and exact calculation reference, recover from incomplete data, and choose the next supported action without learning the repository or documentation program.

## Canonical journey

```text
/
└── /use/
    └── /use/investment/investigate-effort/
        ├── /use/investment/investment-mix/
        ├── /use/investment/follow-evidence/
        ├── /reference/taxonomies/investment/
        ├── /reference/metrics/weighting-and-aggregation/
        └── /use/troubleshooting/no-or-incomplete-data/
            ├── /admin/sync-and-coverage/status-and-freshness/
            └── /operate/runbooks/ingestion-failure/
```

The home and section landings orient the reader. The task page owns the procedure. Workflow pages explain how to read the current product. Reference pages own exact facts. Troubleshooting owns recovery. Administration and operations pages are escalation destinations, not duplicated diagnostics.

## Provisional Get Started experiment

Test two routes to the same supported task:

```text
Direct:   / → /use/ → Investment task
Optional: / → /get-started/prerequisites/ or /get-started/choose-a-task/ → Investment task
```

Every Get Started sentence is newly authored from a validated prerequisite or routing need. Do not reuse the old “first ten minutes,” quick-start, Start Here, onboarding sequence, titles, structure, or prose.

CHAOS-2995 must decide whether `/get-started/` remains a small router, collapses into `/`, collapses into `/use/`, or is removed.

## Allowed public implementation scope

Phase 5 may implement or correct only:

- the documentation home and shell needed to reach the slice;
- the `/use/` and relevant Investment section landings;
- the Investment task, Investment Mix workflow, evidence path, exact taxonomy, exact weighting/aggregation reference, and incomplete-data troubleshooting pages;
- the minimum optional Get Started prerequisite/router pages needed for the comparison;
- contextual admin/operator escalation stubs only when the complete destination is not yet migrated;
- legacy redirects required by the reviewed disposition for pages in the slice;
- edit/source, feedback, search, responsive navigation, and accessible page behavior required to test the journey.

Do not migrate unrelated user guides, AI views, reports, administration, operations, integrations, APIs, or contributor sections in this phase.

## Delivery sequence

### 1. Production-shaped shell

- Generate the visible slice navigation from the locked IA source.
- Retain the approved hero-and-card home.
- Implement section landings, breadcrumbs, local contents, search entry, edit/source, feedback, and responsive navigation.
- Keep current WIP pages behind an explicit non-canonical boundary.
- Do not create a second design system or publication framework.

### 2. Source verification

Complete `source-verification.tsv` before a page is treated as reviewed content. For each page record:

- current product route and labels;
- required role, source connection, scope, time window, and data coverage;
- exact product/code/taxonomy sources;
- supported zero, unavailable, incomplete, stale, delayed, estimated, and error states;
- legacy sources and redirect behavior;
- screenshot or diagram need;
- source, content, IA, accessibility, and design reviewers.

### 3. Content implementation

- Rewrite from the reader outcome, not from old page boundaries.
- Keep task, workflow, concept, reference, and troubleshooting responsibilities separate.
- Link to exact facts rather than copying them.
- Preserve calibrated evidence language and state distinctions.
- Use a visual only when it removes reader effort and can be reproduced and sanitized.

### 4. Independent validation

Run the scenarios in `acceptance-scenarios.md` with reviewers who did not author the pages. Record defects and the final scale/no-scale decision in CHAOS-2995.

## Pull-request policy

Keep the phase independently reviewable:

1. Shell and source-verification packet.
2. Source-verified Investment content and redirects.
3. Validation evidence and the explicit Get Started/scale decision.

A pull request must not combine this slice with broad Phase 6 migration, Cloudflare production architecture, or removal of the entire WIP site.

## Exit gate

Phase 5 closes only when:

- every canonical page in the journey is reachable without a direct URL;
- current product behavior and exact reference facts are source-verified;
- the incomplete-data recovery path works end to end;
- natural-language search reaches the canonical task and troubleshooting pages;
- no critical or high IA, accuracy, accessibility, security, or redirect defect remains;
- the maintenance footprint is smaller than the WIP implementation;
- CHAOS-2995 records a human decision on `/get-started/` and explicitly approves or rejects scaling Phase 6–9 migration.
