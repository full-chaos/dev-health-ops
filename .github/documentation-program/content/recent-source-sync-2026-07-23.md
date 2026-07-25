# Recent documentation source sync — 2026-07-23

## Scope

This review compares the documentation foundation commit `33a09d0544c2382e221802745e96a2a98476cef3` with current `main` after the incident-ingestion and Go-worker foundation work. The former documentation tree is preserved under `.github/docs-legacy/`; current source documents with stronger or newer content are synchronized into their canonical destinations under `docs/`.

Only source documents that are themselves supported reader references are migrated directly. Internal plans, technical requirements, decisions, operational source material, and evidence stay in `.github/docs-legacy/`; public destinations summarize the supported task or reference boundary instead of publishing those internal documents verbatim.

## Directly synchronized documents

| Archived current source | Canonical destination | Reader boundary |
| --- | --- | --- |
| `.github/docs-legacy/user-guide/pagerduty-oauth-app-setup.md` | `/admin/data-sources/incident-response/` | Supported administrator setup and troubleshooting |
| `.github/docs-legacy/ops/cli-reference.md` | `/reference/cli/` | Current Python and Go command reference and safety boundaries |

The mapping is versioned in `.github/documentation-program/content/migrated-source-pages.json`. `scripts/mkdocs_migrated_source_links.py` resolves links relative to each directly migrated archived source document, routes mapped sources to canonical documentation URLs, and links remaining evidence to the archived repository source.

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
- JSM incident ingestion remains blocked for release until live tenant proof is recorded; the archived provider contract states that limitation and the public task guide does not present a false setup workflow.
- Celery remains the production owner of current jobs and schedules.
- Go/River profiles remain coexistence foundations with zero minimum replicas and Celery routes unless a later migration gate changes ownership.
- Direct PostgreSQL queue control, pooled domain access, and one-shot migration access remain separate database responsibilities.

## Material that remains source-only

The canonical site does not publish raw benchmark captures, generated compatibility JSON, local resource snapshots, or other evidence artifacts as standalone navigation destinations. The archived TRD, ADR, PRD, implementation plan, worker guide, and database guide retain links to that evidence when it is relevant.
