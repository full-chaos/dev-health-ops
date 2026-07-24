# AGENTS.md — Dev Health Platform

## Mission

Dev Health is open source analytics for team operating modes and developer
health: where human effort is being invested, and what it costs people when
certain work dominates. Accessibility over extraction; signals not judgment;
trends over absolutes; every metric traces to evidence.

## Repository boundaries

| Project | Purpose | Stack |
| --- | --- | --- |
| `ops/` | Ingest, metrics, API, and jobs | Python, FastAPI, Strawberry; additive Go worker runtime |
| `web/` | Visualization and exploration | Next.js, React, TypeScript |
| `dev-health-panels/` | Grafana panel plugin | TypeScript, React |
| `dev-health-examples/` | Demo-data seeding | Python, Terraform |

Providers own raw fetch, authentication, pagination, retry, and normalization.
Processors orchestrate provider calls. Sinks are the only persistence path.
ClickHouse is the analytics backend; PostgreSQL is the semantic layer.

### Ops runtime ownership

Python owns the FastAPI and GraphQL surfaces, provider fetch and normalization,
processors, domain behavior, and every currently routed Celery job. The Go
worker runtime is additive infrastructure for bounded background execution; it
does not move API or provider ownership and it does not authorize a second
normalization or persistence path.

A job remains on Celery until its migration issue implements the versioned
contract and handler, changes routing behind rollback controls, and passes the
documented shadow, parity, and canary gates. A compiling or healthy Go process
is foundation evidence only, not proof that any production job has migrated or
that a canary may start.

River queue control requires a small direct PostgreSQL connection introduced
with the River migrations and dual-pool work in CHAOS-3037. Transaction-mode
PgBouncer remains valid for domain access but is not, by itself, a
production-ready River queue-control path. Long-running processes never apply
River migrations.

## Product contracts

- WorkUnits are evidence containers, not categories.
- The fixed themes are Feature Delivery, Operational/Support,
  Maintenance/Tech Debt, Quality/Reliability, and Risk/Security.
- LLMs choose subcategory distributions only at compute time. Theme roll-up is
  deterministic, categorization never returns `unknown`, and evidence plus
  quality are persisted.
- The user experience renders persisted distributions and edges. It may use
  LLM output for explanations only, labelled as estimates that appear, lean, or
  suggest rather than definitive conclusions.
- No person-to-person rankings. Individual views are for reflection and
  coaching; heatmap cells trace to evidence; quadrants show raw values.

## Team attribution contract

ClickHouse is the system of record for analytics team attribution. Do not add
PostgreSQL-based team or identity mappings. Manual mappings are ClickHouse
fallback records, not overrides. PR/MR attribution requires an actual linked
issue donor row; an issue-key prefix is never linked-issue inheritance. Every
attribution result emits source, confidence, and evidence provenance.

Changes to attribution behavior update matching documentation and assert the
documented precedence. Coverage remains provider-agnostic across Jira, GitLab,
GitHub, and Linear for teams, projects, members, and issues.

## Documentation delivery contract

The public documentation site is `https://docs.fullchaos.dev`; the separately
hosted product demo is `https://demo.fullchaos.dev`. Both use Cloudflare Workers Static Assets.
GitHub Actions is the only deployment authority and
Workers Builds remains disabled. Previews use a shared Cloudflare Access
service-token policy; production custom domains are anonymously readable.

Use `wrangler@4.107.0` with compatibility date `2026-07-16`. Preserve
deterministic version artifacts, deployment metadata, security headers,
redirects, and rollback evidence. Cloudflare Pages is not a deployment path.

## Change discipline

Never commit to `main`. Create a scoped branch and worktree, use focused tests,
and stage only intended changes. Push with an explicit refspec before opening a
pull request. Do not add agent or AI attribution to commits. For backend
changes, run `bash ci/local_validate.sh` from the owning worktree before push.

The detailed unified-documentation decision and issue coverage are versioned in
`docs/decisions/unified-docs-cloudflare.md` and `docs/coverage-matrix.md`.
