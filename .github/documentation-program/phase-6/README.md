# Phase 6 — Use Dev Health migration preparation

**Status:** source verification and migration planning active  
**Public implementation gate:** CHAOS-2995  
**Workstreams:** CHAOS-2996, CHAOS-2997, and CHAOS-2998

Phase 6 preparation begins now so the team can verify current product behavior, assemble source packets, identify duplicate groups, plan redirects, and define asset needs. Broad public migration must not merge until the Phase 5 vertical slice is independently approved.

## Gate rule

Allowed before CHAOS-2995 approval:

- verify current product routes, labels, permissions, filters, state behavior, and availability;
- map reviewed inventory rows to canonical target pages;
- identify authoritative code, schema, taxonomy, metric, and UI sources;
- group duplicates and confirm canonical survivors;
- draft page briefs, review checklists, redirect sets, and screenshot/diagram needs;
- identify unsupported, removed, planned, or ambiguous behavior;
- create internal migration packets and small source-checking utilities when they validate objective facts.

Not allowed before CHAOS-2995 approval:

- merge broad migrated user guides into the public v2 navigation;
- publish AI, Reports, metric, planning, Work Graph, or other workflow pages as final;
- build a large screenshot corpus;
- create new IA nodes, alternate canonical URLs, or duplicate page placements;
- expand the shell, component system, browser matrix, or CI framework beyond Phase 5 evidence;
- treat a passing build as authorization to scale.

The Phase 5 Investment pages are the only public workflow slice allowed to advance before this gate.

## Workstream A — Core workflows and views

**Linear:** CHAOS-2996

Prepare source-verified migration packets for:

- Investment Flows and Investment Expense after the Phase 5 Investment pattern is approved;
- PR Flow and Quadrants;
- Code Hotspots and flame diagrams/current equivalent;
- Work Graph and evidence/relationship drill-down;
- Capacity Planning;
- Cockpit/navigation, shared scope, time, filter, comparison, and data-state behavior required by these workflows.

Every packet must identify:

- the reader question and canonical target URL;
- current source pages and duplicate group;
- supported route and UI labels;
- required role, source data, scope, time window, and coverage;
- exact product/code/metric sources;
- supported interpretation, evidence, limitations, and recovery states;
- redirect and removal behavior;
- visual need and reproducible source environment;
- required product, source, content, IA, accessibility, and design review.

## Workstream B — AI, Reports, and shared metrics

**Linear:** CHAOS-2997

Prepare source packets for:

- AI Impact, AI Review Load, AI Risk, and AI Attribution;
- Reports Center creation, cloning, running, scheduling, output, and provenance;
- shared metric interpretation required by approved user tasks.

Do not infer product availability from old documentation. Verify each workflow and label observed facts, derived measures, model-assisted estimates, generated narrative, confidence, insufficient coverage, unavailable data, and no detected association distinctly.

No packet may recommend person ranking, employment decisions, or unsupported causal conclusions.

## Workstream C — Journeys, troubleshooting, and visuals

**Linear:** CHAOS-2998

Prepare:

- shared navigation, scope, time, filter, comparison, and data-state journeys;
- planning and team-conversation journeys that connect approved views without inventing conclusions;
- user-visible troubleshooting for permissions, coverage, loading, empty, stale, delayed, failed, and report states;
- contextual escalation boundaries between user, administrator, and operator content;
- the minimum visual asset list with reader purpose, source environment, revision, viewport, sanitization, alt text/text equivalent, owner, and review trigger.

Prefer no visual until the page brief shows that prose, code, table, or diagram cannot communicate the task efficiently.

## Review units

After CHAOS-2995 approval, implement Phase 6 in small workflow-family pull requests rather than one section rewrite:

1. shared navigation, scope, time, filters, and data states;
2. delivery-flow workflows;
3. code and relationship workflows;
4. planning and improvement workflows;
5. AI workflows;
6. reports;
7. user-visible troubleshooting and section acceptance.

Investment Flows and Investment Expense may follow the approved Phase 5 Investment pattern but must remain separately reviewable.

## Required evidence per implementation PR

- inventory rows and canonical targets covered;
- current product/code sources and verification date;
- exact legacy redirects/removals;
- screenshots or diagrams added, changed, or intentionally omitted;
- source accuracy, content, IA, accessibility, and design reviewers;
- strict build and reader-critical checks;
- natural-language search and contextual-link expectations;
- rollback plan for URL or navigation changes.

## Completion gate

Phase 6 closes only when:

- CHAOS-2995 approved scaling;
- every approved Use Dev Health workflow is implemented or has an explicit gap/roadmap disposition;
- one canonical page exists per task, concept, symptom, and exact reference;
- all legacy user-guide rows have implemented redirects, archive, internal, or removal behavior;
- shared state and responsible-interpretation language is consistent;
- user-visible troubleshooting keeps readers out of operator internals until escalation is justified;
- all visuals have a reader purpose and maintenance owner;
- product, content, IA, accessibility, and design reviewers approve the complete section.
