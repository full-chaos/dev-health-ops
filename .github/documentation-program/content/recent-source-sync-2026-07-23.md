# Recent non-prototype documentation sync — 2026-07-23

## Scope

This review compares the documentation foundation commit `33a09d0544c2382e221802745e96a2a98476cef3` with current `main` after the incident-ingestion and Go-worker foundation work. It inventories new and materially revised documents under `docs/` and identifies their publication destination in `docs-prototype/`.

The implementation direction changed during review: the strongest new source documents are migrated directly instead of being compressed into new summary pages. Their Markdown bodies remain aligned with the source documents. Canonical placement, navigation, source-relative links, and inaccessible empty anchors are adapted by the documentation build.

## Directly migrated documents

| Current source | v2 destination | Reader boundary |
| --- | --- | --- |
| `docs/user-guide/pagerduty-oauth-app-setup.md` | `/admin/data-sources/incident-response/` | Supported administrator setup and troubleshooting |
| `docs/providers/jira-service-management.md` | `/admin/data-sources/jira-atlassian/` | Provider contract and explicit blocked release status |
| `docs/architecture/pagerduty-contract.md` | `/integrate/webhooks/pagerduty/` | PagerDuty REST/Webhooks V3 backend and integration contract |
| `docs/ops/workers.md` | `/operate/run/workers-and-jobs/` | Active Celery operations and additive Go foundation |
| `docs/ops/database-connection-pooling.md` | `/operate/configure/database-connection-pooling/` | PgBouncer, direct River queue control, and migration-role boundaries |
| `docs/architecture/go-worker-runtime-trd.md` | `/contribute/architecture/go-worker-runtime/` | Full technical requirements and runtime design |
| `docs/decisions/chaos-3034-river-compatibility.md` | `/contribute/architecture/river-compatibility/` | Accepted River compatibility and enqueue-boundary decision |
| `docs/product/go-worker-migration-prd.md` | `/contribute/architecture/go-worker-migration-prd/` | Product requirements for the migration |
| `docs/plans/go-worker-migration-implementation-plan.md` | `/contribute/architecture/go-worker-migration-plan/` | Full phased implementation plan |

The mapping is versioned in `.github/documentation-program/content/migrated-source-pages.json`. `scripts/mkdocs_migrated_source_links.py` resolves links relative to each original source document and renders them as stable repository links where the supporting source has not yet been migrated.

## Related prototype updates

The direct documents are supported by targeted updates to existing landing, reference, troubleshooting, deployment, and contributor pages, including:

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

The migration does not publish raw benchmark captures, generated compatibility JSON, local resource snapshots, or other evidence artifacts as standalone navigation destinations. The migrated TRD, ADR, PRD, implementation plan, worker guide, and database guide link to that evidence in the repository when it is relevant.
