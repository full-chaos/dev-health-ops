---
page_id: ref-cli
summary: Current Python and Go command families, input precedence, validation, worker ownership, and operator safety behavior.
content_type: cli-reference
owner: engineering
source_of_truth:
  - current CLI command implementation and --help output
  - docs/ops/cli-reference.md
  - cmd/dev-health-workerctl/
  - cmd/worker-contractcheck/
applicability: current
lifecycle: active
---

# CLI reference

Dev Health has one Python application CLI and two Go worker-foundation operator tools. They serve different trust boundaries and must not be treated as interchangeable entry points.
{: .fc-page-lede }

## `dev-hops`

`dev-hops` is the Python platform CLI. Current command families include:

- `sync` — provider ingestion;
- `teams` — team catalog operations;
- `metrics` — analytics and materialization;
- `audit` — schema, completeness, performance, and coverage diagnostics;
- `fixtures` — synthetic data generation and validation;
- `work-graph`, `investment`, and `recommendations` — derived product computation;
- `admin`, `billing`, and `ai` — administrative or governance operations;
- `migrate` — PostgreSQL and ClickHouse migrations;
- `api` — API server;
- `workers` — Celery worker and Beat scheduler;
- `maintenance` and service-credential operations.

### Input precedence

Command-line flags override environment variables. Common global inputs include:

- `--db` / `POSTGRES_URI` for semantic PostgreSQL;
- `--analytics-db` / `CLICKHOUSE_URI` for analytics;
- `--org` / `ORG_ID` for organization-scoped commands;
- command-specific provider, model, billing, and path settings.

`--db` and `--analytics-db` are not aliases. Some legacy subcommands define their own `--db` with an analytics meaning; inspect the current command help before running it.

### Preflight behavior

Commands validate declared database, organization, path, and credential inputs before execution. A missing required input exits with usage status `2` and identifies the requirement. Read the command's `Requires:` output from the reviewed release.

### Inline execution boundary

Bare `dev-hops` commands run in the caller's process. Some sync and job operations rely on worker-side credentials and configuration that the inline command does not fully enforce. Prefer the supported API and active Celery path for production synchronization, backfills, and report execution where the current operations guide directs it.

Do not treat the dormant Go scheduler or River runtime as a substitute: current production routes remain Celery-owned.

## `dev-health-workerctl`

`dev-health-workerctl` is the payload-redacted operator surface for the Go coexistence foundation. It requires a service credential with `workers:read` and, for mutations, `workers:operate`.

Read commands include:

```text
status
jobs list
jobs inspect
queues
streams status
contracts
```

Mutations require both a reason and correlation ID. The tool validates exact state transitions and writes a bounded audit intent before modifying River state.

Operator outcomes include:

- `outcome_unknown` when a PostgreSQL commit result is ambiguous—inspect before retrying;
- `audit_pending` when the mutation committed but audit finalization is delayed.

Secrets, DSNs, encoded arguments, and payload contents are not printed. Cancel and retry remain unavailable for foundation kinds without authoritative semantic links.

## `worker-contractcheck`

`worker-contractcheck` validates the versioned Go job and deployment contract:

- registry and schemas;
- job versions and handler capability;
- migration and route state;
- deployment profiles;
- role and database exposure rules;
- connection budgets;
- container/runtime compatibility.

Run it through the current Make contract:

```bash
make go:contract
```

Use direct help only to inspect the exact current subcommands:

```bash
go run ./cmd/worker-contractcheck --help
```

## Safety rules

- Read `--help` and `Requires:` from the checked-out revision.
- Use dry-run, status, or read-only commands before mutation where available.
- Treat migration, repair, fixture, backfill, retry, cancel, queue pause/resume, and profile drain as high-risk operations.
- Do not mix synthetic fixtures with a live synchronized organization.
- Do not retry an ambiguous operator mutation until the resource and audit state are inspected.
- Do not change job route ownership outside the checked-in migration contract.
- Do not run migrations through transaction-mode PgBouncer or from a long-running worker.

The generated command trees and help output are the exact option source; this page documents stable families and safety boundaries rather than freezing every option in prose.
