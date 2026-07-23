# Recent documentation source sync — 2026-07-23

## Scope

This review compares the documentation foundation commit `33a09d0544c2382e221802745e96a2a98476cef3` with current `main` after the incident-ingestion and Go-worker foundation work. The former documentation tree is preserved under `.github/docs-legacy/`; current source documents with stronger or newer content are synchronized into their canonical destinations under `docs/`.

The strongest source documents are migrated directly instead of being compressed into summary pages. Their Markdown bodies remain aligned with the archived source documents. Canonical placement, navigation, source-relative links, and inaccessible empty anchors are adapted by the documentation build.

## Directly synchronized documents

| Archived current source | Canonical destination | Reader boundary |
| --- | --- | --- |
| `.github/docs-legacy/user-guide/pagerduty-oauth-app-setup.md` | `/admin/data-sources/incident-response/` | Supported administrator setup and troubleshooting |
| `.github/docs-legacy/providers/jira-service-management.md` | `/admin/data-sources/jira-atlassian/` | Provider contract and explicit blocked release status |
| `.github/docs-legacy/architecture/pagerduty-contract.md` | `/integrate/webhooks/pagerduty/` | PagerDuty REST/Webhooks V3 backend and integration contract |
| `.github/docs-legacy/ops/workers.md` | `/operate/run/workers-and-jobs/` | Active Celery operations and additive Go foundation |
| `.github/docs-legacy/ops/database-connection-pooling.md` | `/operate/configure/database-connection-pooling/` | PgBouncer, direct River queue control, and migration-role boundaries |
| `.github/docs-legacy/architecture/go-worker-runtime-trd.md` | `/contribute/architecture/go-worker-runtime/` | Full technical requirements and runtime design |
| `.github/docs-legacy/decisions/chaos-3034-river-compatibility.md` | `/contribute/architecture/river-compatibility/` | Accepted River compatibility and enqueue-boundary decision |
| `.github/docs-legacy/product/go-worker-migration-prd.md` | `/contribute/architecture/go-worker-migration-prd/` | Product requirements for the migration |
| `.github/docs-legacy/plans/go-worker-migration-implementation-plan.md` | `/contribute/architecture/go-worker-migration-plan/` | Full phased implementation plan |
| `.github/docs-legacy/ops/cli-reference.md` | `/reference/cli/` | Current Python and Go command reference and safety boundaries |

The mapping is versioned in `.github/documentation-program/content/migrated-source-pages.json`. `scripts/mkdocs_migrated_source_links.py` resolves links relative to each archived source document, routes mapped sources to canonical documentation URLs, and links remaining evidence to the archived repository source.

## Related canonical updates

The directly synchronized documents are supported by targeted updates to existing landing, reference, troubleshooting, deployment, and contributor pages, including:

- provider connection and credential lifecycle guidance;
- synchronization status and freshness;
- webhook authentication, replay, and rotation;
- environment and feature-availability reference;
- worker health, metrics, capacity, and recovery guidance;
- deployment examples and migration ordering;
- repository ownership and development commands.

## Important status boundaries retained from the source documents

- PagerDuty canonical incident ingestion is the supported current incident-response path.
- JSM incident ingestion remains blocked for release until live tenant proof is recorded; the direct provider contract states that limitation rather than presenting a false setup workflow.
- Celery remains the production owner of current jobs and schedules.
- Go/River profiles remain coexistence foundations with zero minimum replicas and Celery routes unless a later migration gate changes ownership.
- Direct PostgreSQL queue control, pooled domain access, and one-shot migration access remain separate database responsibilities.

## Material that remains source-only

The canonical site does not publish raw benchmark captures, generated compatibility JSON, local resource snapshots, or other evidence artifacts as standalone navigation destinations. The synchronized TRD, ADR, PRD, implementation plan, worker guide, and database guide link to that archived evidence when it is relevant.
