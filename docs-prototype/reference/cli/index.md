---
page_id: ref-cli
summary: Current `dev-hops` command families, input precedence, validation, and safety behavior.
content_type: cli-reference
owner: engineering
source_of_truth:
  - current CLI command implementation and --help output
  - docs/cli.md
  - docs/ops/cli-reference.md
applicability: current
lifecycle: active
---

# CLI reference

The `dev-hops` CLI includes command families for synchronization, metrics, fixtures, migrations, Customer Push, and other operator or contributor workflows.

## Input precedence

Command-line flags override environment variables. Current commands commonly use `CLICKHOUSE_URI`, `POSTGRES_URI`, `ORG_ID`, and command-specific provider or API settings.

## Preflight validation

Commands validate required database, organization, path, or credential inputs before work begins. Missing required input exits with usage status `2` and identifies the missing requirement.

## Safety

- Read each command's `--help` and `Requires:` line from the reviewed release.
- Use dry-run or preview behavior where supported.
- Treat commands with `--apply`, repair, migration, fixture, backfill, or destructive effects as high risk.
- Do not mix synthetic fixtures into a live synchronized organization without an explicit safe test plan.
- Run data-store migrations before sync or metrics work when the release requires it.

The generated command tree and help output are the exact source; this page does not freeze every option in prose.
