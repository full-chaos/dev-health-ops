# `dho` CLI Reference

Complete reference for `dho`, the command-line interface of dev-health-ops.

---

## Overview

`dho` is one Go binary. `dho help` lists the commands, and `dho <command> --help` prints the flags and the environment of one command. Command groups:

- `sync` : ingest provider data (git, prs, blame, cicd, deployments, incidents, security, tests) and sync team catalogs (teams). Work items are synced by the native Go provider-sync route, with no command (see `sync work-items` below)
- `metrics` : `metrics validate-flags` is the read-only diagnostic; metric runs are dispatched with `dho workers metrics ...`
- `fixtures` : frozen synthetic data for CI and local work (generate, product-telemetry, load-synthetic, finalize-synthetic-sync)
- `work-graph` / `investment` / `recommendations` : no `dho` verb of their own: native Go jobs compute them, and `dho workers` enqueues them (see the Work Graph, Investment and Recommendations sections below)
- `admin` : users, orgs, licenses, feature flags, billing plans, feature bundles, LLM settings
- `billing` : Stripe reconciliation
- `ai` : AI governance allowlist
- `migrate` : PostgreSQL and ClickHouse schema migrations
- `api`, `query-api`, `worker`, `scheduler`, `reconciler`, `stream-runner` : the long-running services
- `workers` : the operator CLI for the Go job runtime
- `backfill` : historical backfill
- `maintenance` : operational cleanup
- `service-credentials`, `mint` : internal service credentials and credential minting
- `push` : customer push ingestion client
- `goapi`, `contracts` : Go API rollout and job-contract operator verbs

### Running a verb

`dho` verbs run in your terminal session and exit when they finish. The Go worker fleet runs scheduled work on its own schedules, in an environment where credentials and configuration are validated before a job is admitted. Prefer letting the scheduled run do the work, and treat an inline invocation as a diagnostic you supply every input to yourself. See [Run workers and jobs](../../operate/run/workers-and-jobs.md) for the Go worker, scheduler, reconciler, and stream-runner processes.

---

## Global Arguments

Global flags go **after** the verb (`dho sync git --org <org-id> ...`).

| Argument | Environment Variable | Description |
|----------|---------------------|-------------|
| `--db` | `POSTGRES_URI` | PostgreSQL connection (semantic: users, settings) |
| `--analytics-db` | `CLICKHOUSE_URI` | ClickHouse connection (analytics: metrics, data) |
| `--org` | `ORG_ID` | Organization id. If it is not set, the verb uses the first organization in PostgreSQL |

`--db` and `--analytics-db` are **not** aliases. They point to different databases serving different roles (see Dual-Database Architecture below). If `POSTGRES_URI` is not set, `--db` falls back to `DATABASE_URI`. Each verb's `--help` lists the flags and environment variables it reads.

Verbs that write analytics accept `--sink`; ClickHouse is the only supported analytics backend, and any other value is refused.


### Dual-Database Architecture

Dev Health Ops uses two databases:

| Layer | Database | Env Var | Purpose |
|-------|----------|---------|---------|
| **Semantic** | PostgreSQL | `POSTGRES_URI` | Users, orgs, settings, credentials |
| **Analytics** | ClickHouse | `CLICKHOUSE_URI` | Commits, PRs, work items, metrics |

See [Data and storage boundaries](../../contribute/architecture/data-and-storage.md) for details.

### Database Connection Strings

| Backend | Format | Example |
|---------|--------|---------|
| PostgreSQL | `postgresql://` | `postgresql://postgres:postgres@localhost:5555/postgres` |
| ClickHouse (native protocol) | `clickhouse://` | `clickhouse://ch:ch@localhost:9000/default` |

The `dho` verbs that read ClickHouse speak the native protocol (port 9000 on the local compose stack). `fixtures generate` also accepts a DSN on the HTTP port (8123, or 8443 with TLS) and speaks HTTP to it.

---

## Input Validation (Preflight)

Before a verb runs, `dho` checks that the inputs it needs are present. A missing required input ends the verb with exit code **2** and a message that names what is missing, and nothing runs:

```bash
$ dho sync git --provider github
dho sync git: missing required input(s): ClickHouse analytics database: pass --analytics-db or set CLICKHOUSE_URI
```

When a preflight check says no (for example a refused environment or a refused parameter set), the verb exits with code **3** and writes nothing. See [Exit Codes](#exit-codes).

**Requirement matrix:**

| Requirement | Commands |
|-------------|----------|
| ClickHouse (`--analytics-db` / `CLICKHOUSE_URI`) | `sync git`, `sync prs`, `sync blame`, `sync cicd`, `sync deployments`, `sync incidents`, `sync security`, `sync tests`, `sync teams`; `metrics validate-flags`; `ai allowlist list/set` (+org); `migrate clickhouse upgrade/status/repair`; `fixtures generate` (`--sink`) |
| PostgreSQL (`MIGRATION_DATABASE_URI`, else `POSTGRES_URI`) | `admin ...`; `billing reconcile`; `maintenance ...`; `service-credentials ...`; `migrate postgres ...` |
| Organization (`--org` / `ORG_ID`) | `ai allowlist list/set`, `sync teams` |

> The org id of the `sync <target>` verbs falls back to the first organization in PostgreSQL when `--org` and `ORG_ID` are omitted.

> Read-only commands that do not open a connection (`migrate postgres heads`, `migrate postgres history`) need no environment.

---

## Sync Commands

> **Warning:** `dho sync <target>` runs in your terminal session and needs provider credentials (such as `GITHUB_TOKEN`, `GITLAB_TOKEN`, or the GitHub App flags). Running it without them ends the verb with a message that names the missing credential.
>
> **Scheduled alternative:** trigger the sync via `POST /api/v1/admin/sync-configs/{config_id}/trigger`. The API plans the `SyncRun` and commits a durable reference-discovery wakeup; the reconciler publishes it through the active sync-dispatch route.

`dho sync git|prs|blame|cicd|deployments|incidents|security|tests` run in-process, without a scheduler, through the same provider routes the worker runs. They work on a single GitHub repository (`--owner/--repo`), a single GitLab project (`--project-id`), or every repository or project a `--search PATTERN` batch lists (`--group`, `--max-repos`, `--batch-size`, `--max-concurrent`). Exit codes: 0 ok, 1 refused by the verb or failed, 2 usage error, 3 not available in `dho`.

- **Credentials.** Give a credential with `--auth` (or `GITHUB_TOKEN` / `GITLAB_TOKEN`, or the GitHub App flags) and an organization with `--org` (or `ORG_ID`). With no organization, `dho` uses the oldest organization in PostgreSQL (`--db`, `POSTGRES_URI` or `DATABASE_URI` names the database). For GitHub with no token or App flags, it uses that organization's `default` GitHub integration credential (decrypted with `SETTINGS_ENCRYPTION_KEY` and `SETTINGS_ENCRYPTION_SALT`). When neither read finds a usable credential, the verb ends with `Missing GitHub credentials`.
- **Targets.** `incidents` runs for GitLab only: GitHub has no native incident source, and the verb exits 1 with a message that says so.
- **Local repositories.** `--provider local` runs `git`, `prs` and `blame` on a local repository (`--repo-path`, default `.`) with the `git` binary: the repository row (its id is the digest of the origin remote URL, or of the absolute path; `REPO_UUID` overrides it), the commits, the per-file commit stats, and the merged and open pull requests inferred from merge-commit messages and `refs/pull|merge-requests/<n>/head`. `blame` writes the working tree's files (`git_files`) and the line-by-line `git blame` of every file that is not binary, media, dependency or build output (`git_blame`).
- **Failures.** A failed ClickHouse insert makes the verb exit 1. A commit, pull request or blame line timestamp after the year 2262 (which the ClickHouse client cannot write as a `DateTime64`) also makes it exit 1.
- **Chunked targets.** `cicd` and `tests` run the worker's chunked routes (bounded chunks, each written and read back before the next). The chunk checkpoint is held in memory for the run: nothing is resumable across processes, so an interrupted run starts again from the beginning of its window, and rows written by the earlier run converge to the same logical rows in ClickHouse.
- **Batches.** A failed repository or dataset never stops the batch and is never swallowed: each failure is named on stderr (repository, dataset, cause), and the command exits 1 if any failed. `--max-concurrent` bounds repositories in flight. `--use-async` is accepted and ignored.
- **Refused flags.** `--max-commits-per-repo` on `git` and `blame` (the routes fetch the whole window and stop at their own page caps) and `--rate-limit-delay` on a `prs` batch (the routes back off on the provider's own rate-limit signals) are refused with a usage error (exit 2, nothing listed, opened or written).
- **Repository row.** The routes write the repository row from the provider's repository metadata. The `git` target does not also run blame: blame is the separate `blame` target.
- **Synthetic data.** `--provider synthetic` is not a source of real data. The CI synthetic targets load with `dho fixtures load-synthetic`.

### `sync git`

Sync git repository data. Uses `CLICKHOUSE_URI` (analytics layer).

```bash
# Local repository
dho sync git --provider local \
  --repo-path /path/to/repo

# GitHub
dho sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# GitHub App
dho sync git --provider github \
  --github-app-id "$GITHUB_APP_ID" \
  --github-app-key-path "$GITHUB_APP_PRIVATE_KEY_PATH" \
  --github-app-installation-id "$GITHUB_APP_INSTALLATION_ID" \
  --owner my-org \
  --repo my-repo

# GitLab
dho sync git --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964
```

**Options:**
| Option | Description |
|--------|-------------|
| `--provider` | `local`, `github`, `gitlab` (`synthetic` is not a source of real data) |
| `--auth` | GitHub/GitLab token override (PAT mode for GitHub) |
| `--github-app-id`, `--github-app-key-path`, `--github-app-installation-id` | GitHub App auth flags. Mutually exclusive with PAT auth. |
| `--repo-path` | Path to local repo |
| `--owner`, `--repo` | GitHub owner/repo |
| `--project-id` | GitLab project ID |
| `--gitlab-url` | GitLab instance URL (default `$GITLAB_URL` or `https://gitlab.com`) |
| `--since` | Start date, inclusive (ISO `YYYY-MM-DD`). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, ISO `YYYY-MM-DD`, default: tomorrow) |
| `--backfill N` | Backfill N days ending before `--before` (default 1). Mutually exclusive with `--since` |
| `--sink` | Analytics backend (`clickhouse` only; default) |

GitHub authentication precedence is CLI flags > environment variables > stored database credentials. Use either PAT auth (`--auth` or `GITHUB_TOKEN`) or GitHub App auth, not both. See [Connect GitHub](../../admin/data-sources/github.md).

### `sync prs`

Sync pull request data. Uses `CLICKHOUSE_URI`.

```bash
dho sync prs --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync work-items` (deleted, CHAOS-5351)

This command no longer exists. Work items are synced automatically by the
native Go provider-sync route (`internal/workerservice/provider_sync.go`'s
work-items dataset case, one per provider, dispatched via the river
`sync_provider` queue) and by webhooks — there is no manual per-provider sync
trigger. To force a backfill for a specific sync configuration and window,
use `dho backfill run --config-id <uuid> [--since ...] [--before ...]`,
which dispatches through the same native route. To inspect what the queue is
doing, use `dho workers jobs list --queue sync_provider --kind
<kind>` / `jobs inspect <id>`.

### `sync cicd`

Sync CI/CD pipeline data. Uses `CLICKHOUSE_URI`.

The sync also writes the versioned, provider-neutral CI acceptance projection
used by Ask Dev status checks. GitHub derives required check names only from the
target branch's required-status-check policy; GitLab derives the required
pipeline outcome only from the project's merge policy. A denied, missing, or
unsupported policy is stored as `unknown`, never optional. Required work that
the provider reports as skipped remains skipped even when the enclosing
pipeline is green.

```bash
# GitHub
dho sync cicd --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo

# GitLab
dho sync cicd --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

Rows ingested before ClickHouse migration `070_ci_acceptance_checks` remain
explicitly unknown to Ask Dev. Re-run the existing CI sync for only the required
repository and time window to populate them; there is no automatic unbounded
history backfill. Replaying the same range is idempotent. Rolling a producer
back preserves prior projection rows and their freshness timestamps, so stale
rows degrade status instead of silently proving completion.

### `sync deployments`

Sync deployment events. Uses `CLICKHOUSE_URI`.

```bash
dho sync deployments --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync incidents`

Sync incident data. Uses `CLICKHOUSE_URI`.

```bash
dho sync incidents --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

GitLab selects native `issue_type=incident` rows. GitHub is intentionally unsupported:
ordinary GitHub issues, including label-bearing issues, remain work items.

### `sync blame`

Sync git blame data only (line-level authorship). Uses `CLICKHOUSE_URI`.

```bash
dho sync blame --provider local --repo-path /path/to/repo
```

Accepts the same provider, auth, single-repo, batch-mode, and date-range options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

Planner-managed GitHub/GitLab integrations seed the heavy `blame` dataset when
the legacy `git` target is selected. This keeps ownership and bus-factor metrics
reachable from normal code-host onboarding while the per-sync GitHub blame crawl
remains capped (`BLAME_BACKFILL_MAX_FILES=500`) and coverage-aware. Existing dev
or support fixtures created before that seed can enable blame by adding an
`integration_datasets` row for the integration with `dataset_key='blame'`,
`is_enabled=true`, and options mirroring the integration's existing `git` dataset
row (for example `{"legacy_targets":["git"]}`); after the row exists, the admin
dataset endpoint can toggle it like any other dataset.

### `sync security`

Sync security and dependency alerts (Dependabot, code-scanning, advisories, GitLab vulnerability/dependency findings). Uses `CLICKHOUSE_URI`.

```bash
dho sync security --provider github \
  --auth "$GITHUB_TOKEN" --owner org --repo repo
```

Accepts the same provider/auth/batch options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

### `sync tests`

Sync CI test results and coverage (TestOps). Uses `CLICKHOUSE_URI`.

```bash
dho sync tests --provider github \
  --auth "$GITHUB_TOKEN" --owner org --repo repo
```

Accepts the same provider/auth/batch options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

### `sync teams`

Sync team catalogs. ClickHouse is the system of record for teams (CHAOS-2600 CS5): `dho sync teams` writes ClickHouse only, tags every team row with `org_id`, and does not write PostgreSQL or call any Postgres-to-ClickHouse bridge. `--provider` is one of `jira`, `github`, `gitlab` or `linear`, and `--org <org-id>` is required. `CLICKHOUSE_URI` (or `--analytics-db`) is required. By default the verb exits 1 when discovery or persistence results in zero teams; use `--allow-empty` only when an empty sync is expected.

```bash
# Atlassian Teams of a Jira organization
dho sync teams --provider jira --org "$ORG_ID"

# From a GitHub org (requires --owner and a token)
dho sync teams --provider github --org "$ORG_ID" \
  --owner my-org \
  --auth "$GITHUB_TOKEN"

# From a GitLab group (fetches group + subgroups)
dho sync teams --provider gitlab --org "$ORG_ID" \
  --owner my-group/path \
  --auth "$GITLAB_TOKEN"

# From a Linear workspace
dho sync teams --provider linear --org "$ORG_ID" \
  --auth "$LINEAR_API_KEY"
```

Team rows for Jira **projects** come from the automatic team import that runs after a Jira sync (`internal/providersync/jira_team_catalog.go`). There is no command for it, and `--provider jira` here syncs Atlassian Teams, which is different data.

- **`--provider jira`** syncs the organization's **Atlassian Teams** (structure, members, active projects; `--structure`, `--members`, `--projects`; with none of the three, all are synced; `--allow-empty`). Members and project links that a team no longer has are retracted (closed); an empty result is refused, so a permissions problem retracts nothing, unless `--allow-empty`. Its Atlassian credential (email, api token, tenant URL) is resolved from the org's **stored jira integration** in PostgreSQL (`POSTGRES_URI` or `--db`), the same integration row and credential that work-items sync and the worker's post-sync team auto-import use, decrypted with `SETTINGS_ENCRYPTION_KEY`. The integration's `atlassian_organization_id` config key is an **override only**: when absent, it is derived live from the AGG (Atlassian GraphQL gateway) `tenantContexts(cloudIds: [...])` query using the same stored credential (`internal/atlassianteams.ResolveOrganizationID`). A set value must be UUID- or ARI-shaped (`ari:cloud:platform::org/<uuid>`), validated on save by the admin credentials API (`PATCH /api/v1/admin/credentials/jira/{name}`). `atlassian_cloud_id` is likewise optional and otherwise derived live from the tenant's own site (its `_edge/tenant_info` endpoint). `ATLASSIAN_ORGANIZATION_ID`, `ATLASSIAN_CLOUD_ID`, `ATLASSIAN_EMAIL`, `ATLASSIAN_API_TOKEN` and `ATLASSIAN_JIRA_BASE_URL` (`_FILE` accepted; `JIRA_EMAIL`, `JIRA_API_TOKEN` and `JIRA_BASE_URL` as fallbacks) each override the corresponding stored or resolved value. When `ATLASSIAN_EMAIL`, `ATLASSIAN_API_TOKEN` and `ATLASSIAN_JIRA_BASE_URL` are ALL set, the verb runs fully from the environment with no database at all (an offline or test path only). The same collection also runs automatically as an additional step of the jira team-catalog auto-import, whenever team import is selected (`auto_import_teams`, `auto_import_projects`, `auto_import_members`), so a normal scheduled jira sync produces Atlassian Teams rows on its own. A resolution failure (no organization context, or no permission) degrades non-strict: it logs and keeps the project-as-team result. See `architecture/team-attribution.md` §0.2a.
- **`--provider github`** `--owner <github-org> [--auth <token>]` (the token else `GITHUB_TOKEN`; a GitHub Enterprise base URL from `GITHUB_URL` or `GITHUB_BASE_URL`) runs the provider's **team catalog**: the producer the worker's post-sync team autoimport runs. It writes the ClickHouse team dimensions the team-attribution contract prescribes (`teams` rows of `provider_access`, `team_memberships`, `team_repo_ownership`; see `architecture/team-attribution.md` §0). A missing owner, a missing token, or an empty result without `--allow-empty` exits 1; a provider or write failure exits 1 with nothing written. A `403` on one team's repositories fails the run with exit 1 and writes nothing. A team with a non-zero `sync_policy` in `team_sync_policies` is left untouched and reported as `teams_skipped_policy`, not as an empty catalog. A team row has `id` `gh:<slug>`, a trimmed `name`, the provider's `description`, `members` as provider-scoped identity facets (`github:<login>` plus the member's public email, lower-cased), a stable `team_uuid` (uuid5 of the team id), `provider` `github`, `native_team_key` (the slug), `repo_patterns` (the team's repositories as `owner/name`), and `manual_members` (an admin override survives). The catalog writes its tables in order and not in one transaction: a write that fails part way leaves the earlier tables written, and the next run finishes them.
- **`--provider gitlab`** `--owner <group-path> [--auth <token>]` (the token else `GITLAB_TOKEN`; the instance URL from `GITLAB_URL`, default `https://gitlab.com`) runs the same team catalog seam for GitLab. The group and its direct subgroups become `teams` rows (`provider_access`, `id` `gl:<full_path>`, `native_team_key` the group's `full_path`, `project_keys` the group's own `path_with_namespace` values). The group's own projects populate `team_project_ownership`, members populate `team_memberships` (identity facets `gitlab:<username>` plus the member's email), and the group's full project tree, including subgroups, populates the native `projects` catalog (CHAOS-3380).
- **`--provider linear`** `[--auth <token>]` (the token else `LINEAR_API_KEY`; `LINEAR_URL` points the verb at a fake API for testing only) needs no `--owner`: an API key scopes the whole Linear workspace, so every team is in scope. Teams become `teams` rows (`provider_access`, `id` the bare team key, `native_team_key` the team's key). Members populate `team_memberships` and the shared `members` dimension (identity facets `linear:<email-or-id>`, lower-cased), and an inactive member is excluded. Linear's own projects populate the native `projects` catalog and `team_project_ownership` when `--projects` is selected. An org-archived Linear team is included, as the workspace returns it. No description is invented when the provider has none.

---

## Teams Commands

> **Removed in CHAOS-2600 CS5:** the `dev-hops teams reconcile` command is **deleted**. It reconciled org-scoped ClickHouse teams back into PostgreSQL `team_mappings` and re-bridged via `bridge_teams_to_clickhouse` — both the Postgres team control plane and the bridge are gone. ClickHouse is now the system of record for teams, written directly by `sync teams` and the admin team surface, so no reconcile step is needed.

---

## Metrics Commands

> ⚠️ **Warning (CHAOS-2475):** Metrics commands run inline and require database connections and configurations that the CLI doesn't enforce at startup. Running them inline can cause silent failures or incomplete computations.
>
> **Interim Workaround:** Prefer the scheduled Go run over an inline invocation; it validates the same inputs before admitting the job. See [Run workers and jobs](../../operate/run/workers-and-jobs.md).

### `metrics daily` / `metrics rebuild` (deleted)

These two `dev-hops` verbs only executed the Go operator verb and were
deleted at spec S2. Run it directly:

```bash
# Single day, every org repository
dho workers metrics daily-start --org <org-uuid> --day 2025-02-01 --reason <code> --correlation-id <id>

# Day range (inclusive)
dho workers metrics daily-start --org <org-uuid> --day 2025-01-26 --to 2025-02-01 --reason <code> --correlation-id <id>

# Scope to specific repositories (repeatable)
dho workers metrics daily-start --org <org-uuid> --day 2025-02-01 --repo-id <uuid> --reason <code> --correlation-id <id>
```

See [`metrics daily-start`](#metrics-daily-start-chaos-5055) for the full
contract.

### `metrics dora` / `complexity` / `capacity` / `release-impact` (deleted)

These `dev-hops` verbs only executed the Go operator verb and were deleted at
spec S2. Run it directly, one dispatch per family (and per day for the
day-scoped families):

```bash
dho workers metrics remaining trigger-backstop --family dora --org <org-uuid> \
  --day 2026-08-01 --review-evidence "CHAOS-1234 -- routine trigger, no automatic run yet today" \
  --reason <code> --correlation-id <id>

dho workers metrics remaining trigger-backstop --family capacity --org <org-uuid> \
  --team <team-uuid> --review-evidence "CHAOS-1234 -- routine trigger" \
  --reason <code> --correlation-id <id>
```

`--day` defaults to yesterday UTC, targeting today requires `--today`, and
`--review-evidence` is always required. See `metrics remaining
trigger-backstop` below for the full contract.

### `metrics validate-flags`

Run feature-flag pipeline validation checks against recent data. Uses `CLICKHOUSE_URI`.

```bash
dho metrics validate-flags --lookback 30
```

**Options:**
| Option | Description |
|--------|-------------|
| `--lookback N` | Number of days to inspect (default: 30) |
| `--org` | Organization to validate (default: `ORG_ID`, else the empty organization) |

---

## Fixtures Commands

### `fixtures generate`

Load a frozen synthetic world into ClickHouse. Uses `CLICKHOUSE_URI`. `dho fixtures generate` loads a **frozen parameter set**, not a free generator: the worlds were written once into a real ClickHouse at the migration head, dumped table by table, committed (digest-pinned) and embedded in `dho`. The verb loads the one that matches the flags, moved by whole days so that its last generated day is today, and written for the given organization (`--org`, else `ORG_ID`, else the default demo organization; it must be a UUID). `--seed` is required: an unseeded run is not repeatable. A parameter set that was not frozen is refused (exit 3), and the refusal names the ones that were.

```bash
dho fixtures generate \
  --sink "$CLICKHOUSE_URI" \
  --provider synthetic \
  --repo-name acme/live-e2e \
  --repo-count 1 \
  --days 14 \
  --commits-per-day 6 \
  --pr-count 24 \
  --team-count 10 \
  --seed 20260219 \
  --with-metrics \
  --with-work-graph
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--sink` | `$CLICKHOUSE_URI` | ClickHouse DSN. A DSN on the HTTP port (8123, or 8443 with TLS) is spoken to over HTTP |
| `--org` | `$ORG_ID`, else the default demo organization | Organization id (a UUID) the rows are written for |
| `--repo-name` | `acme/demo-app` | Base repository name |
| `--repo-count` | `1` | Number of repos |
| `--days` | `30` | Number of days of data |
| `--commits-per-day` | `5` | Average commits per day |
| `--pr-count` | `20` | Total pull requests |
| `--team-count` | `10` | Number of teams |
| `--seed` | required | Seed of the frozen world |
| `--provider` | `synthetic` | Provider label: `synthetic`, `github`, `gitlab`, `jira` |
| `--with-metrics` | off | Also the derived metrics |
| `--with-work-graph` | off | Also the work graph |
| `--db-type` | — | Must be `clickhouse` when given |
| `--skip-coherence-validation`, `--allow-mixed-org` | off | Skip the coherence check; allow an organization that holds synced rows |

Frozen parameter sets:

- `--provider synthetic --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 --seed 20260219 --with-metrics --with-work-graph` (the acr end-to-end run).
- `--provider github --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 --seed 20260219` (the live backend end-to-end run).
- `--provider synthetic --repo-name ci-metrics-executed-proof/repo --repo-count 1 --days 7 --commits-per-day 5 --pr-count 20 --team-count 1 --seed 4276` (the metrics executed proof; one team keeps the repository single-owner).

The ClickHouse schema must be at the head first (`dho migrate clickhouse upgrade`). The verb writes **analytics rows only**: it writes no users, organization or license to PostgreSQL, and a `DATABASE_URI`, `POSTGRES_URI` or `DATABASE_URL` in the environment is refused (exit 3) so that a missing account is never silent. Create the first user and organization with the [admin verbs](#admin-commands). `--overwrite-real-users` is not accepted. The mixed-organization guard is kept: an organization that holds synced `github`, `gitlab`, `jira`, `linear` or `bitbucket` rows is refused unless `--allow-mixed-org`. Running the verb twice for the same organization inserts the rows twice, so use a fresh organization or database per run. Before any row is written, the verb checks the embedded world's content digest and the destination server's timezone (it must be UTC, as the world was captured); a table missing partway through the frozen order is also caught before any row is written. A JSON summary (`org_id`, `params`, `rows` per table) goes to stdout.

Every fixture run also seeds synthetic security alert rows into
`security_alerts` for each generated repo. These rows include Dependabot,
code-scanning, advisory, GitLab vulnerability, and GitLab dependency-style
sources so the security GraphQL resolvers and UI have demo data without a
separate flag. Verify them with:

```sql
SELECT count(), countDistinct(severity) FROM security_alerts;
```

When `--with-metrics` is enabled against ClickHouse, AI workflow intelligence
tables are also seeded: `ai_attribution`, `ai_workflow_runs`,
`ai_workflow_artifact_edges`, and `ai_workflow_issue_edges`. The daily metrics
job then computes `ai_impact_metrics_daily`, `ai_governance_coverage_daily`,
and `ai_policy_events` from those source rows.

`--with-metrics` also writes the Cockpit/Govern risk inputs used by Compounding
Risk and TestOps Delivery Risk: `repo_complexity_daily`, `repo_metrics_daily`,
`compounding_risk_daily`, `testops_pipeline_metrics_daily`,
`testops_test_metrics_daily`, and `testops_coverage_metrics_daily`. A base
fixture run without this flag is suitable for raw-ingest checks, but those risk
surfaces should be expected to report missing inputs until metrics are computed.

### `fixtures product-telemetry`

Seed `product_telemetry_events` across one or more orgs so the platform-admin dashboard and per-org product views have data locally. Uses `CLICKHOUSE_URI`, and reads org IDs from PostgreSQL when `--org` is not supplied.

```bash
# Seed the first 3 orgs from Postgres, 30 days each
dho fixtures product-telemetry --orgs 3 --days 30

# Seed explicit orgs (repeatable --org)
dho fixtures product-telemetry \
  --org 550e8400-e29b-41d4-a716-446655440000 \
  --days 60 --sessions-per-day 50 --seed 42
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--orgs` | `5` | Number of orgs to seed when `--org` is not provided (first N from Postgres `organizations`; falls back to synthetic UUIDs) |
| `--org` | — | Explicit org id to seed (repeatable; overrides `--orgs`) |
| `--days` | `30` | Days of data per org (capped at the table's TTL horizon minus its safety margin: values above 149 seed 149) |
| `--sessions-per-day` | `50` | Average synthetic sessions per day per org |
| `--seed` | none | Deterministic seed (mixed with org_id) for repeatable runs; it must be an integer that fits 64 bits |

It reads `CLICKHOUSE_URI` and, without `--org`, the first `--orgs` rows of `organizations` through `POSTGRES_URI` (or the `DEV_HEALTH_PG_DOMAIN_*` component form). When PostgreSQL cannot be read, it falls back to the synthetic ids `uuid5(NAMESPACE_URL, "seed-org-<i>")` with a warning. A JSON summary (`orgs`, `rows` per org, `total`) goes to stdout, and the log lines are JSON on stderr. A positional argument is a usage error (exit 2).

---

## Admin Commands

User and organization management commands. These use PostgreSQL: the database is `MIGRATION_DATABASE_URI`, else `POSTGRES_URI`.

> **Important:** Users must belong to an organization to log in. Always create an organization after creating a user.

### Create the first admin

A stack on empty volumes has no user. Create the first user and its organization with the two admin verbs below. On the Compose stack, run them in the `migrate` service: its image is `dho`, so the arguments after the service name are the `dho` arguments, and it has the database login these verbs use. Read the password from standard input (the first line of a file here), never from an argument:

```bash
docker compose run --rm --no-deps -T migrate admin users create \
  --email admin@example.com --full-name "Admin User" --superuser --password-stdin < password.txt
docker compose run --rm --no-deps -T migrate admin orgs create \
  --name "My Organization" --owner-email admin@example.com
```

Outside Compose, run the same verbs with `dho` and the database environment set (`dho admin users create ...`, then `dho admin orgs create ...`). Remove the password file afterwards.

### `admin users create`

Create a new user.

```bash
dho admin users create \
  --email admin@example.com \
  --password-stdin \
  --full-name "Admin User" \
  --superuser < password.txt
```

**Options:**
| Option | Description |
|--------|-------------|
| `--email` | User email (required) |
| `--password` | Password, min 8 chars. It shows in the process list: prefer `--password-stdin` |
| `--password-stdin` | Read the password from the first line of standard input. One of `--password` and `--password-stdin` is required |
| `--username` | Optional username |
| `--full-name` | User's full name |
| `--superuser` | Grant superuser privileges |

### `admin orgs create`

Create a new organization.

```bash
dho admin orgs create \
  --name "My Organization" \
  --owner-email admin@example.com \
  --tier community
```

**Options:**
| Option | Description |
|--------|-------------|
| `--name` | Organization name (required) |
| `--slug` | URL-safe slug (auto-generated if omitted) |
| `--description` | Organization description |
| `--tier` | Subscription tier (default: `community`) |
| `--owner-email` | Email of initial owner |

### `admin users list`

List users (`--limit`, default 100; `--include-inactive`).

```bash
dho admin users list --limit 50
```

### `admin orgs list`

List organizations (`--limit`, default 100; `--include-inactive`).

```bash
dho admin orgs list --include-inactive
```

### `admin users update`

Update an existing user (identified by `--id`, `--email`, or `--username`) and optionally manage org memberships.

```bash
dho admin users update \
  --email user@example.com \
  --full-name "New Name" \
  --no-active

# Add to an org with a role
dho admin users update \
  --email user@example.com --org my-org --role admin
```

**Options:**
| Option | Description |
|--------|-------------|
| `--id` / `--email` / `--username` | Identify the user to update |
| `--new-email` / `--new-username` | Change email/username (empty string clears username) |
| `--full-name` | Set the user's full name |
| `--password` / `--password-stdin` | Set a new password (min 8 chars; revokes existing sessions) |
| `--verified` / `--no-verified` | Set verified status |
| `--superuser` / `--no-superuser` | Set superuser status |
| `--active` / `--no-active` | Set active status |
| `--org` | Org slug or ID: add the user or update their role |
| `--role` | Membership role with `--org`: `owner`, `admin`, `member`, `viewer` |
| `--remove-from-org` | Org slug or ID: remove the user's membership |

### `admin orgs delete`

Delete an organization and all of its scoped data.

```bash
# Preview the deletion plan
dho admin orgs delete --org-id <uuid> --dry-run

# Delete
dho admin orgs delete --org-id <uuid>
```

**Options:**
| Option | Description |
|--------|-------------|
| `--org-id` | Organization ID (required) |
| `--dry-run` | Return the deletion plan without deleting data |
| `--analytics-db` | ClickHouse URI (default: `CLICKHOUSE_URI`) |

### `admin licenses`

Offline license key management (Ed25519-signed). `create` uses PostgreSQL.

```bash
# Generate a signing key pair
dho admin licenses keygen

# Create a signed license key
dho admin licenses create \
  --org-id <uuid> --tier enterprise --duration-days 365 \
  --org-name "Acme" --contact-email billing@acme.com
```

| Subcommand | Description |
|------------|-------------|
| `keygen` | Generate an Ed25519 key pair for license signing |
| `create` | Create a signed license key (`--org-id`, `--tier {community,team,enterprise}`, `--duration-days` (default 365), `--org-name`, `--contact-email`) |

### `admin features`

Feature flag management.

```bash
dho admin features seed
```

| Subcommand | Description |
|------------|-------------|
| `seed` | Seed standard feature flags into the database |

### `admin billing`

Billing plan management and Stripe synchronization.

```bash
dho admin billing seed
dho admin billing list
dho admin billing pull-stripe --dry-run
dho admin billing sync-stripe
```

| Subcommand | Description |
|------------|-------------|
| `seed` | Seed standard billing plans (Community, Team, Enterprise) with prices |
| `list` | List all billing plans with prices and Stripe sync status |
| `pull-stripe` | Pull billing plans from Stripe into the database (`--dry-run` to preview) |
| `sync-stripe` | Push unsynced billing plans to Stripe |

### `admin bundles`

Feature bundle management (groups of feature keys mapped to plans/orgs).

```bash
dho admin bundles create \
  --key pro --name "Pro" --features "metrics,investment,reports"
dho admin bundles list
dho admin bundles assign-plan --bundle-key pro --plan-key team
dho admin bundles assign-org --org-id <uuid> --feature-key reports
```

| Subcommand | Description |
|------------|-------------|
| `create` | Create a bundle (`--key`, `--name`, `--features` comma-separated, `--description`) |
| `list` | List all bundles with features and plan assignments |
| `assign-plan` | Assign a bundle to a billing plan (`--bundle-key`, `--plan-key`) |
| `assign-org` | Grant an org a feature override (`--org-id`, `--feature-key`, `--reason`, `--expires-days`) |

### `admin llm-settings`

Read and change the LLM settings of an organization (`get`, `set`, `delete`). Run `dho admin llm-settings <verb> --help` for the flags.

---

## Backfill Commands

### `backfill run`

> **Warning:** the `backfill run` verb starts a run through the scheduler. Only a planner-managed configuration, or one pinned to a single source, can be started this way.
>
> **API alternative:** trigger the backfill via `POST /api/v1/admin/sync-configs/{config_id}/backfill`. The API plans a backfill-mode `SyncRun` and commits the same durable reference-discovery wakeup used by full and continuation syncs.

Start a historical data backfill for a sync configuration over a window of whole days. Data is synced in chunked 7-day windows. The verb validates the configuration, writes one scheduled occurrence and its backfill trigger, and the scheduler plans and dispatches the run. It waits for the plan (30 seconds by default), or prints the occurrence id at once with `--no-wait`. Uses `MIGRATION_DATABASE_URI` (or the `DEV_HEALTH_PG_*` component form).

```bash
dho backfill run \
  --config-id "550e8400-e29b-41d4-a716-446655440000" \
  --since 2024-01-01 \
  --before 2024-03-01
```

**Which datasets run.** For a configuration that covers a whole integration, the backfill names the datasets that are enabled for that integration (its dataset rows), not the configuration's `sync_targets` list. A dataset that is off, or that has no row, is not backfilled and is not switched on. When the integration has no enabled dataset, the verb refuses the run (exit code 3) and writes nothing; enable a dataset in the connection's synchronization settings first. A configuration pinned to a single source still names the datasets its own `sync_targets` select, and so does a PagerDuty configuration (the platform manages the PagerDuty datasets as one set, so its list must be exactly `operational`).

**Options:**

| Option | Description |
|--------|-------------|
| `--config-id` | Sync configuration UUID (required) |
| `--since` | Start date (ISO `YYYY-MM-DD`). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: today). The window ends the day before |
| `--backfill N` | Backfill N days ending before `--before` (default 1). Mutually exclusive with `--since` |
| `--org` | Organization UUID |
| `--no-wait` | Print the occurrence id at once instead of waiting for the plan |
| `--wait-seconds N` | Seconds to wait for the plan |

Backfill depth is limited by organization tier:

| Tier | Max Backfill Depth |
|------|-------------------|
| Community | 30 days |
| Team | 90 days |
| Enterprise | Unlimited |

> **Important:** Backfill never updates SyncWatermarks. Incremental sync state is preserved.

## API Server

### `api`

Run the Dev Health Ops API server (Go), which serves REST for `dev-health-web`. GraphQL is served by the read-only query plane (`dho query-api`); on the local Compose stack a router on port 8000 sends each request to the right one. The verb is a long-running service: it is configured flag-first, every flag also has an environment variable, and a flag wins over the environment. An unknown flag is rejected at startup.

```bash
dho api --api-addr :8000
dho api --help
```

**Options (the main ones; `dho api --help` lists all):**
| Option | Environment | Description |
|--------|-------------|-------------|
| `--api-addr` | `DEV_HEALTH_API_ADDR` | `host:port` of the api HTTP server (default `:8000`) |
| `--http-addr` | `DEV_HEALTH_HTTP_ADDR` | `host:port` of the operator HTTP server: `/healthz`, `/readyz`, `/metrics` (default `:8080`); it must differ from `--api-addr` |
| `--log-level` | `DEV_HEALTH_LOG_LEVEL` | `debug`, `info`, `warn`, `error` or `critical` (default `info`) |
| `--cors-allowed-origins` | `CORS_ALLOWED_ORIGINS` | Comma-separated CORS allow-list (default `http://localhost:3000`) |

The api has no auto-reload, and it does not serve an OpenAPI or `/docs` page.

---

## Workers

**Celery is retired (CHAOS-4026, 2026-08-21): zero Python celery services run
in prod since the 2026-08-19 stop.** `workers start-worker`/`workers
start-scheduler` (which booted a real `celery worker`/`celery beat` process)
were deleted along with it, and `workers inspect` (a Celery control-plane
reader) went with the Celery app (CHAOS-7059). Go owns every periodic
maintenance cadence; see [Run workers and jobs](../../operate/run/workers-and-jobs.md)
for the Go worker/scheduler/reconciler/stream-runner processes.

### Worker operator CLI

`dho workers` is the Go operator CLI. Read this before the verb reference
below — its requirements are not discoverable from the verbs themselves.

**No token.** `dho workers` takes no operator token. Whoever can run it in a
worker pod with that pod's database DSNs is the operator. Every mutation
still requires `--reason` and `--correlation-id`, and it writes an audit
row (principal `operator/dho-workers`). Name yourself in those two flags:
they and the cluster's exec audit log are the record of who acted.

**Every write verb is audited first.** That covers the job, queue and route
verbs, and also the verbs that write outside the operator service:
`metrics daily-start`, `daily-redrive`, `daily-finalize`, `finalize-redrive`,
`partition-recompute`, `execution-repair`, `metrics remaining start`,
`trigger-backstop` and `redrive`, `workgraph trigger` and `repair`,
`investment trigger`, `external-recompute replay`, both `providersync retire-*`
verbs and `sync-dispatch-outbox close-backlog`. Each one writes its
`worker_operator_audits` row before it writes anything else, and completes it
`succeeded` or `failed`. If the row cannot be written, the command stops with
`audit_unavailable` and changes nothing. `--dry-run` writes nothing, so it
needs neither flag and writes no row. `--review-evidence` keeps its own
meaning; it is not the reason code.

**Flags must precede the positional argument.** Go's `flag` package stops
parsing at the first positional, so an id-first invocation fails with a generic
`invalid_request` that names neither the cause nor the fix:

```bash
dho workers jobs retry 9457 --reason r --correlation-id c   # invalid_request
dho workers jobs retry --reason r --correlation-id c 9457   # parses
```

**`COORDINATOR_DATABASE_URI` is required, and not every image carries it.**
The route controllers and the audit table run on the coordinator pool, so
without that DSN the CLI is entirely non-functional and returns
`configuration_error`. A `dho workers` invocation needs
all three database URIs — `POSTGRES_URI`, `WORKER_DATABASE_URI`, and
`COORDINATOR_DATABASE_URI` — plus the session-safe mode settings. The
`go-reconciler` container carries the coordinator DSN; `go-worker-heavy` does
not, so reaching for the nearest worker container returns `configuration_error`
with no hint that a different container would work.

!!! warning "`jobs retry` and `jobs cancel` cannot succeed in Phase 1"
    Both verbs are advertised and both are refused unconditionally. The Phase-1
    domain guard returns an unsupported-precondition error from every branch,
    ignores the requested action, and inspects no domain state, so no amount of
    configuration makes either verb work. The refusal is deliberate: the frozen
    contracts name domain links that have no authoritative semantic table yet.
    Treat them as unavailable until CHAOS-4030 lands. There is currently no
    generic supported path to re-drive a stranded job by hand — `metrics
    daily-redrive` below is a narrow, daily-metrics-specific exception, not a
    counterexample to this warning.

### `dho workers metrics`

#### `metrics daily-start` (CHAOS-5055)

Dispatch a daily-metrics run for one (organization, day-range[, repository
set]) through the same `StartRunTx` coordinator transaction the post-sync
and fixed-schedule fanout paths use. It replaced the deleted
`dev-hops metrics daily`/`rebuild` verbs: the worker
decides the native/bridge split per family exactly as it does for any other
trigger source, so there is no separate, unguarded Python write path. Not
restricted to historical days (unlike `metrics remaining start` below) --
`--to` may be today.

**Coverage check (codex adversarial review round 2, P1):** a deferred-discovery
request (no `--repo-id`) is refused with `already_covered` if the same
(org, day) already has a succeeded run from a DIFFERENT trigger -- the nightly
fixed schedule, a post-sync re-drive, or an earlier manual trigger under a
different generation. This prevents duplicate-writing every native daily
family (`file_hotspots` included) for a day that already computed. A retried
CLI invocation for the identical logical request is unaffected (it reuses the
same deterministic generation and lands on the ordinary idempotency path, not
the coverage refusal). A `--repo-id`-scoped request skips this check --
narrower, deliberately-scoped repository recomputes are not what this guards
against. This does NOT protect the reverse direction (a manual trigger fired
BEFORE that day's fixed-schedule occurrence) -- closing that would mean
changing the nightly schedule's own behavior, deliberately out of scope here.

**Organization from stdin (CHAOS-8892):** `--org-stdin` replaces `--org`. The
verb reads ONE line from stdin, drops one trailing newline, and uses it as the
organization id, so the id is on no command line. This is the route on a
distroless worker pod, where the only way to hand over a value is the stdin of
`kubectl exec -i`. `--org` and `--org-stdin` together, empty stdin, more than
one line, a line over 128 bytes, no end of input within 10 seconds, or a malformed id is a usage error (exit 2) raised before any database is opened
whose text gives a fixed reason and a byte length, never the value. With
`--org-stdin` the verb prints no organization id on stdout, stderr or in its own
log lines (the audit row still holds it). Without the flag nothing changes.

```bash
kubectl exec -i <worker-pod> -- dho workers metrics daily-start \
  --org-stdin --day 2026-09-01 --reason <code> --correlation-id <id> < org-id-source
```

`already_covered` is a normal answer, not a fault: the day already has a succeeded
run from another trigger, and nothing was started for it. It keeps its exit
code.

```bash
dho workers metrics daily-start \
  --org 70d529e0-3c06-4597-8480-794fd02328b6 \
  --day 2026-09-01 \
  --to 2026-09-04 \
  --reason <code> --correlation-id <id>

# Scope to specific repositories (repeatable --repo-id); omit for every
# org repository (deferred discovery, resolved by the worker)
dho workers metrics daily-start \
  --org 70d529e0-3c06-4597-8480-794fd02328b6 \
  --day 2026-09-04 \
  --repo-id 550e8400-e29b-41d4-a716-446655440000 \
  --reason <code> --correlation-id <id>
```

Repair a daily-metrics run stranded by CHAOS-4358: every `daily_partition`
River job for it already failed and was discarded, and nothing else ever
re-enqueues work for that run on its own (a fresh `metrics.daily_dispatch`
run still hits the SAME permanent per-partition outbox dedupe key its
original dispatch used, so a bare re-dispatch alone is not enough — see
[job-recovery-lifecycle.md](../../operate/run/job-recovery-lifecycle.md)).

```bash
dho workers metrics daily-redrive \
  --org 70d529e0-3c06-4597-8480-794fd02328b6 \
  --from 2026-08-08 \
  --to 2026-08-27 \
  --review-evidence "confirmed via ClickHouse readback that testops_test/dora/cicd have zero rows for these repo+day scopes; safe to re-run" \
  --reason <code> --correlation-id <id>
```

`--review-evidence` is **required**, with no default (codex review round 3):
"ambiguous" means a progress-having failure MAY have already written real
output, and claim expiration alone is not evidence retry is safe —
`metrics execution-repair` already requires a human to pick `retry_safe` vs
`confirm_succeeded` per execution based on
actual review, and this bulk path must not quietly bypass that by
auto-authorizing every ambiguous row with a generic hardcoded string. State
what you actually checked — e.g. the redriven families' zero-row counters,
or a fresh ClickHouse readback confirming no output landed yet. This matters
most for families whose readers do not `argMax`/dedup by `computed_at` (e.g.
`file_hotspots`/`file_metrics_daily`, which `SUM`s raw rows) — a needless
retry there silently inflates scores rather than landing a harmless
duplicate.

Scoped to one organization and an inclusive UTC calendar-day range, in two
steps that MUST run in this order (codex review, round 1: publishing a
partition job before the ledger repair only reproduces `ambiguous_refused`
and re-terminalizes the partition `failed_permanent`, undoing the reset):

1. **Ledger repair first.** Repairs the ledger through the coordinator
   database role (the operator credential must hold `workers:operate`; no
   API base URL and no separate repair token) for every `running` run in
   scope, applying the SAME `retry_safe` CAS `metrics execution-repair`
   uses to every `ambiguous`/stuck-`executing` ledger row
   underneath them, carrying your `--review-evidence` text. This path only ever authorizes `retry_safe` —
   never `confirm_succeeded`, which needs per-row `output_evidence` a bulk
   call cannot supply; an operator who has confirmed a SPECIFIC execution's
   output already landed correctly should use `metrics execution-repair`
   with `--resolution confirm_succeeded` instead.
2. **Partition redrive second.** Resets any `failed_permanent` partition
   back to `failed` (clearing `failure_reason`), then publishes a fresh
   `metrics.daily_partition` job for every `pending`/`failed` partition in
   scope — plus any `running` partition whose lease has already expired
   (the final River attempt died after claiming it but before releasing
   it; `ClaimPartition` already treats this as reclaimable) — under a
   redrive-scoped dedupe key distinct from the partition's original
   dispatch (CHAOS-4358). A live (unexpired) lease is never touched.

If step 1 reports `skipped_claim_active > 0` (an execution's original claim
still read as active at repair time), step 2 does **not** run at all: the
command returns `{"status":
"ledger_repair_incomplete_retry_after_claims_settle", "partitions": null}`
instead (codex review round 2: publishing a partition job for a run with an
unrepaired ambiguous row is a race — that ledger row can 409
`ambiguous_refused` the instant the job reaches it, re-terminalizing the
partition `failed_permanent`). Re-run the same command once those claims
have settled (their owning job finishes or its lease expires).

Otherwise, returns `{"ledger_repair": {"repaired", "skipped_claim_active"},
"partitions": {"PermanentReset", "RedispatchedRunIDs",
"RedrivenPartitions"}}`. A window spanning many post_sync fanouts is handled
automatically.

**Observability**: `dev_health_daily_metrics_redrive_partitions_total{reason}`
is wired but not live for THIS caller — `workerctl` is a one-shot CLI with no
Prometheus scrape endpoint, so the counter only becomes real if a future
long-lived caller (e.g. an automatic strand-repair reconciler) invokes the
same Go function. The durable, queryable record of a manual redrive today is
the `worker_job_outbox` rows this command commits, under the
`metrics.daily_partition:redrive:<nonce>` dedupe-key prefix:

```sql
SELECT dedupe_key, status, created_at FROM worker_job_outbox
WHERE dedupe_key LIKE 'metrics.daily_partition:redrive:%'
ORDER BY created_at DESC;
```

#### `metrics daily-finalize` (CHAOS-4389)

Repair a daily-metrics run stranded one step LATER than `daily-redrive`
above: every partition has already succeeded, but the single
`metrics.daily_finalize` job `CompletePartition` ever enqueues for the run
(fixed idempotency key `metrics.daily_finalize:<run id>`, permanently
deduped by the outbox) was discarded by River before `CompleteFinalize` ever
ran — so the run sits `status='running'` forever despite 100% partition
success. This is the finalize-side counterpart of the CHAOS-4358 gap
`daily-redrive` closes for partitions/dispatch.

```bash
dho workers metrics daily-finalize \
  --run 6f2caa3e-2a8b-4e46-9c47-6a5a0a5b9a12 \
  --review-evidence "confirmed all partitions succeeded and no user_metrics_daily/ic_landscape_rolling_30d rows exist yet for this run's target_day -- the prior metrics.daily_finalize job never reached CompleteFinalize" \
  --reason <code> --correlation-id <id>
```

`--run` additionally repairs the named run's finalize ledger
row through the coordinator database role before publishing (see
"Finalize-ledger repair" below); the operator credential must hold
`workers:operate`, and a failed ledger repair fails closed with
`{"error": {"code": "ledger_repair_unavailable"}}` before any write.
`--all-complete` does NOT repair ledger rows: it never touches a run whose finalize
ledger row could be stuck (see that flag's own note below for why).

`--review-evidence` is **required**, with no default, mirroring
`daily-redrive`'s bar: finalize writes `user_metrics_daily`/
`ic_landscape_rolling_30d` directly, so state what you verified before
authorizing a repeat run — e.g. confirmed no rows exist yet for this run's
`target_day`, or that the prior job's `finalization_status` never reached
`succeeded`.

Exactly one of two scopes is required:

- `--run <id>` repairs one named `daily_metrics_runs` row. May redrive a run
  whose finalize was already claimed at least once (`finalization_status`
  `failed`, or `running` with an expired lease) — appropriate ONLY once you
  have personally checked whether that prior attempt already wrote real
  output (see `--review-evidence` above): `finalization_status='failed'`
  does not mean finalize never ran — it is set both when the compatibility
  call failed AND when it succeeded (writing real
  `user_metrics_daily`/`compounding_risk_daily`/`team_cognitive_load_daily`
  rows) but the bookkeeping write afterward failed.
- `--all-complete` sweeps every organization for runs `status='running'`
  with every partition `succeeded` whose finalize was **never attempted at
  all** (`finalization_status='pending'` only — `--limit` bounds one pass,
  default 500) and redrives each one found. Deliberately narrower than
  `--run`: a bulk, unattended sweep must never risk writing a second full
  set of rows on top of a prior attempt's possibly-already-durable output,
  so it only ever touches the provably-safe "genuinely never ran" subset —
  mirrors `daily-redrive`'s own split between its bulk `retry_safe` path and
  `confirm_succeeded`'s single-execution-only endpoint. A run stuck in the
  riskier `failed`/expired-lease shape still shows up in
  `--all-complete`'s own detection pass (and the `_detected_total` counter
  below) for visibility, but needs `--run` to actually redrive it.
  Before the detection pass itself, `--all-complete` also runs
  `ReconcileOrphanedFinalizeRedriveRuns` (CHAOS-4405, see `metrics
  finalize-redrive` below) to close out any `finalize-redrive`-published job
  that never reached a claim at all — otherwise that run's `'open'`
  provenance row would silently exclude it from this very detection pass
  forever.

Both re-verify eligibility (run still `running`, at least one partition
exists, every partition `succeeded`) under a row lock immediately before
publishing, so a run that settled between being named and this call running
is silently skipped rather than double-published — safe to run repeatedly,
including against a run that already finalized (a no-op) or one still
between dispatch and repository discovery (zero partitions is never treated
as "100% succeeded"). Publishes under a fresh, nonce-scoped dedupe key
(`metrics.daily_finalize:redrive:<run id>:<nonce>`), never the original
permanently-deduped one:

```sql
SELECT dedupe_key, status, created_at FROM worker_job_outbox
WHERE dedupe_key LIKE 'metrics.daily_finalize:redrive:%'
ORDER BY created_at DESC;
```

Each invocation mints its own nonce, so re-running this command against the
same run before its prior redrive has been processed publishes a second,
independently-keyed job rather than recognizing one is already in flight
(codex review, round 2). `ClaimFinalize`'s own fencing still prevents two
finalize attempts from ever *executing* concurrently for the same run (a
live lease reports `LeaseActiveError`, a settled run reports nothing to
claim) — the extra row costs River queue capacity, not correctness. Avoid
re-running `--all-complete` in a tight loop; check the query above for
still-`pending`/`delivered` redrive rows first if in doubt.

**Finalize-ledger repair (CHAOS-4409), `--run` only.** Before publishing,
`--run` repairs the compatibility bridge's own finalize ledger row
for the named run (`metric_compatibility_executions`, `worker_kind='daily'
operation='finalize'`) — the same bulk ledger repair `daily-redrive` above
already runs for partition rows, now scoped to the `finalize` operation (the
repair's operations default to `partition`, so `daily-redrive`'s own repair is
unchanged and never touches a finalize row under its partition-scoped review
evidence). Without this, a run whose finalize ledger row was stuck
`ambiguous`/stuck-`executing` from the original stranding answers
`JobCancelError ambiguous_refused` on every redrive attempt, forever: the
ledger's own `_reserve_execution` check refuses the identical execution
identity regardless of how many times `ClaimFinalize`/`Finalize` retries at
the Go layer — republishing alone can never fix it. Mirrors `daily-redrive`'s
CHAOS-4304 ordering requirement exactly: the repair MUST land before any
finalize job publishes (a stuck row answers `ambiguous_refused` the instant
a redriven job reaches it, undoing this same pass), and a nonzero
`skipped_claim_active` (a row whose original claim still reads as live)
aborts the whole invocation with
`{"status": "ledger_repair_incomplete_retry_after_claims_settle"}` rather
than publishing into a race — re-run once that claim has settled (its owning
job finishes or its lease expires).

**`--all-complete` never runs the ledger repair** (codex review, round 1,
P1): its own candidates come from `FindStrandedFinalizeRuns`, which reports
`'failed'`/expired-lease runs alongside genuinely never-attempted `'pending'`
ones for VISIBILITY — `RedriveStrandedFinalize` already refuses to publish
for anything but `'pending'` here. Running the ledger repair over the FULL
candidate list would silently move an unreviewed `'failed'` run's finalize
ledger row to `retry_authorized` even though this sweep will never redrive
it — authorizing a later, unrelated call to do so without any operator
having reviewed that specific run. A genuinely never-attempted `'pending'`
run cannot have a stuck ledger row in the first place (`ClaimFinalize` never
claimed this generation to attempt `Finalize()` at all), so nothing is lost
by skipping the repair step entirely for this flag.

Returns `{"candidates": [...run ids considered...], "ledger_repair":
{"repaired": N, "skipped_claim_active": N}, "finalize":
{"FinalizedRunIDs": [...run ids a fresh job was actually enqueued for...]}}`.
A `--run` id that turns out ineligible (already finalized, not all
partitions succeeded, a live finalize lease still in flight) appears in
`candidates` but not in `finalize.FinalizedRunIDs` — not an error.

**Observability**: `dev_health_daily_metrics_stranded_finalize_runs_detected_total`
(runs an `--all-complete` sweep found in this stranded shape),
`dev_health_daily_metrics_runs_finalized_by_sweep_total` (runs a fresh job
was actually enqueued for), and
`dev_health_daily_metrics_finalize_ledger_repair_rows_total{outcome}`
(finalize-ledger rows the CHAOS-4409 repair above moved to
`retry_authorized` vs. left alone for a still-live claim) are wired but not
live for THIS caller, for the same reason `daily-redrive`'s own counter
above is not: `workerctl` is a one-shot CLI with no Prometheus scrape
endpoint. All three become real the moment a future long-lived caller (e.g.
an automatic reconciler sweep) invokes the same Go functions. The
`ledger_repair` field in this command's own JSON result (above) is the
actual audit trail for a manual invocation today.

#### `metrics finalize-redrive` (CHAOS-4405)

Historical backfill: re-run `metrics.daily_finalize` for one organization
across `[--from, --to]`, one calendar day at a time — **even for a day whose
run already reached `status='succeeded'`**. Distinct from `daily-finalize`
above, which only ever repairs a run still stuck non-terminal: this
command's whole point is re-executing a day's finalize AFTER it already
completed, because `run_daily_metrics_finalize` now also writes
`compounding_risk_daily`(scope='team') and `team_cognitive_load_daily`
(CHAOS-4399, #1963) — a day finalized before that landed has zero rows in
either table and needs finalize re-run purely to backfill them.

First preview with `--dry-run` — lists the days and current run state with
**zero writes** (no reset, no provenance row, no publish; every transaction
it opens is rolled back, never committed), and does not require
`--review-evidence` since nothing yet needs justifying:

```bash
dho workers metrics finalize-redrive \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --from 2026-05-01 --to 2026-05-31 \
  --dry-run
```

Then run for real:

```bash
dho workers metrics finalize-redrive \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --from 2026-05-01 --to 2026-05-31 \
  --review-evidence "CHAOS-4405: backfilling compounding_risk_daily(team)/team_cognitive_load_daily for days finalized before #1963 landed the team-aggregation write" \
  --reason <code> --correlation-id <id>
```

`--review-evidence` is **required** for a real invocation, with no default —
the same bar every operator-authorized repeat execution in this CLI
mirrors, and if anything more consequential here: this verb deliberately
re-executes days that already completed, across a potentially wide date
range.

Both Go's `ClaimFinalize` and the Python compat bridge's own claim-row query
(`worker_metrics.py`'s `_load_daily_execution`) hard-require
`status='running'` before `Finalize()` can run at all — publishing a fresh
`metrics.daily_finalize` job for an already-`'succeeded'` row through the
ordinary pipeline is a guaranteed silent no-op. `--include-succeeded`
(defaults `true`, since touching already-succeeded days is this verb's whole
purpose) makes this work by transactionally resetting an eligible
`'succeeded'` run back to `status='running'`,
`finalization_status='pending'`, **and a fresh `generation`**, in the SAME
transaction as the publish — it either fully lands (reset + fresh outbox
row) or fully rolls back, never a reset with no way to reach
`FinalizeHandler.Work` again. Pass `--include-succeeded=false` to restrict to
the safe never-attempted/failed/expired-lease subset (`daily-finalize
--all-complete`'s own eligibility, scoped to this org+day-range) instead of
authorizing the state-mutating case.

The `generation` reset is load-bearing, not cosmetic (a finding from this
verb's own design review, posted on CHAOS-4405): the Python compatibility
bridge's execution-ledger identity is `uuid5(run_id, family, generation,
scope_digest)`. A bare `status`/`finalization_status` reset alone would
leave the SAME identity that already reached `'succeeded'` the first time
this run finalized — `_reserve_execution` would find it, return
`{"status": "skipped"}`, and never call `run_daily_metrics_finalize` again
at all, even though Go reports a clean success. The fresh `generation` (a
short `redrive:<nonce>` value, always well inside the column's 64-byte
bound) gives the redriven attempt a genuinely new identity, so the write
this whole command exists for actually happens.

One run per calendar day is targeted (whichever run for that day has every
partition succeeded, preferring the most recently updated when more than
one generation qualifies) — the team-aggregation block re-reads ClickHouse
for the whole org/day, not a specific run's own partition rows, so any one
qualifying run is equally valid to redrive through. Safe to re-run for the
same day: `run_daily_metrics_finalize`'s outputs are read back via
`argMax(computed_at)` dedup, the same convention every other daily-metrics
reader in this schema already uses, so a second full write changes nothing
a correctly-written reader observes (confirmed live: two runs for the same
org/day left 2 raw rows in `team_cognitive_load_daily` but an identical
argMax-deduped read both times; the table is `ReplacingMergeTree(computed_at)`
since migration 096, so a later merge removes the older row and the read stays
the same).

**Provenance.** Every terminal-state reset writes one row to
`daily_metrics_finalize_redrive_events` — `run_id`, `org_id`, `target_day`,
the run's exact prior `status`/`finalization_status`, `actor` (always
`'finalize-redrive'`), the operator's `reason` (their `--review-evidence`
text, verbatim), and the redrive `nonce` — in the SAME transaction as the
reset and the publish. Either all three (provenance + reset + fresh outbox
row) land, or none do; a `--dry-run` pass writes none of them. This is the
durable, queryable record of WHY and BY WHOM a `'succeeded'` row was ever
touched, independent of River/outbox history (which only shows a fresh
dedupe key).

**While this command's own redriven finalize is plausibly still in flight,
`daily-finalize --all-complete`'s sweep steps back from it** —
`FindStrandedFinalizeRuns` excludes a run with an `'open'` row in
`daily_metrics_finalize_redrive_events`, so an unattended sweep running
concurrently cannot double-dispatch it. This is deliberately NOT permanent
(a design correction from this verb's initial review, posted on
CHAOS-4405/#1971): `CompleteFinalize`/`ReleaseFinalize` close this run's own
open row — to `'closed_succeeded'` or `'closed_failed'` — the instant that
SPECIFIC claim resolves, same transaction as the run's own completion. A
**failed** redriven finalize therefore reappears in `FindStrandedFinalizeRuns`
immediately, exactly like any other ordinary CHAOS-4389 discard — an
unattended sweep (or `daily-finalize --run <id>`) can recover it without an
operator needing to remember which runs this verb specifically touched.

**A redriven job that never reaches a claim at all is also recovered
automatically.** A job discarded/cancelled by River, or never delivered
into River at all (a relay-level `'dead'` outbox row), before `ClaimFinalize`
is ever called for it has no completion path to fire
`CompleteFinalize`/`ReleaseFinalize` — nothing would otherwise ever close its
`'open'` row. `daily-finalize --all-complete` now runs
`ReconcileOrphanedFinalizeRedriveRuns` right before its own
`FindStrandedFinalizeRuns` scan: it checks each `'open'` row's outbox
delivery state (and, when delivered, the actual River job state via the
queue-control pool) and closes it `'closed_orphaned'` the moment either
orphan shape is detected, observed as the `"redriven_orphaned"` telemetry
outcome. The run then reappears in `FindStrandedFinalizeRuns` exactly like
any other recovered stranded run — an unattended sweep no longer needs an
operator to notice and fall back to `--run <id>` by hand.
`daily-redrive`'s own partition-level query is unaffected by construction
regardless: the reset never touches `daily_metrics_partitions`, so a
redriven run's partitions stay 100% `'succeeded'` throughout.

Returns `{"finalize_redrive": {"Days": [{"Day": "...", "RunID": "...",
"Outcome": "...", "ResetFromSucceeded": true|false}, ...]}, "dry_run":
true|false}`, one entry per calendar day in the requested range. `Outcome`
is `"redriven"` or `"redriven_reset_from_succeeded"` for a real pass that
touched a day (the latter whenever `ResetFromSucceeded` is true),
`"skipped_ineligible"` for a day with no eligible run, or the `"would_"`-
prefixed equivalents (`"would_redrive"`, `"would_redrive_reset_from_succeeded"`,
`"would_skip_ineligible"`) under `--dry-run` — the prefix makes a preview
result impossible to mistake for a completed write. `RunID` is empty when no
eligible run exists at all for that day (never dispatched, or aged out),
distinguishing "nothing to redrive here" from "found something, chose not
to touch it".

**Observability**: `dev_health_daily_metrics_finalize_redrive_days_total{outcome}`
(`redriven`/`redriven_reset_from_succeeded`/`skipped_ineligible`, one sample
per calendar day considered — `--dry-run` never observes, since it makes no
real work to count) is wired but not live for THIS caller, for the same
reason every other `workerctl` counter above is not: a one-shot CLI with no
Prometheus scrape endpoint. `redriven_orphaned` is observed the identical
way: by `PostgresStore.ReconcileOrphanedFinalizeRedriveRuns`, called from
`daily-finalize --all-complete` right before its own `FindStrandedFinalizeRuns`
scan, whenever it closes an event `'closed_orphaned'` — counted in runs, not
calendar days, and not live for the same reason (this CLI, not a scraped
process). `redriven_failed` is different: it is observed by
`PostgresStore.transitionFinalize`, called from the long-running worker's
`ReleaseFinalize` path whenever it closes a run's finalize-redrive event as
`'closed_failed'` — this one IS live, scraped from the worker process that
actually processes the redriven job, not from this CLI.

#### `metrics partition-recompute` (CHAOS-4459)

Historical recompute: reset an already-`'succeeded'` daily-metrics run's
**partitions** (not its finalize step — see `finalize-redrive` above for
that) back to a claimable state for one organization across `[--from, --to]`,
one calendar day at a time. This is the partition-level gap
`finalize-redrive` does not close and `daily-redrive` cannot reach:
`daily-redrive`'s own eligibility scan only ever targets stranded/failed
partitions on a run still `status='running'` — a run whose partitions are
ALL `'succeeded'` is invisible to it by construction, even when the family
output those partitions wrote is now known to be wrong. Prod-observed
motivating case (CHAOS-4459): the native `repo_user_commit` executor wrote
`org_id=""` on `repo_metrics_daily`/`user_metrics_daily`/`commit_metrics`
before PR #1960 (CHAOS-4341) — every org-scoped read of those tables for a
day computed before the fix sees zero rows, and no operator path could ever
recompute that day once its partition read `'succeeded'`.

`--org-stdin` replaces `--org` here too, with the same rules as for
[`metrics daily-start`](#metrics-daily-start-chaos-5055).

```bash
dho workers metrics partition-recompute \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --from 2026-08-20 --to 2026-08-27 \
  --family repo_user_commit \
  --dry-run
```

Then run for real:

```bash
dho workers metrics partition-recompute \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --from 2026-08-20 --to 2026-08-27 \
  --family repo_user_commit \
  --review-evidence "CHAOS-4459: org-scoped commit_metrics/repo_metrics_daily/user_metrics_daily rows are 0 for this range because the partition succeeded under the pre-#1960 writer that stamped org_id=''" \
  --reason <code> --correlation-id <id>
```

`--family` is restricted to a closed list (`daily.SupportedPartitionRecomputeFamilies`
— today just `repo_user_commit`) and is recorded on the provenance row for
audit/intent, but does **not** narrow the actual recompute: one
`metrics.daily` partition always computes every family in ONE HTTP
compatibility-bridge call plus whichever native executors have cut over
(`job_daily.py`'s `run_daily_metrics_job`), so resetting a partition
recomputes ALL of its families, not just the named one. This is safe —
every family writer stamps a newer `computed_at`, its output table keeps the
newest row per reader key (`ReplacingMergeTree(computed_at)`, migration 096), and
readers dedup by `computed_at` — but it is not free, which is exactly why `--family` stays a
required, closed-vocabulary flag rather than an implied default: it forces
an operator to name which specific gap they are repairing, even though the
mechanism underneath is the same for all of them today.

Same `generation`-reset mechanism as `finalize-redrive`, applied one layer
earlier: `ClaimPartition` hard-requires `run.status='running'` (exactly like
`ClaimFinalize`'s `status='running'` requirement), so a bare partition
status reset alone would never make the run reachable again. This command
transactionally resets the run to `status='running'`,
`finalization_status='pending'`, **and a fresh `generation`** (a short
`recompute:<nonce>` value), and every one of its partitions back to
`status='pending'` with claim/lease/attempt state cleared — all in the SAME
transaction as the provenance row and the fresh per-partition publish. The
fresh `generation` is load-bearing for the same reason `finalize-redrive`'s
is: the compatibility bridge's execution-ledger identity is
`uuid5(run_id, family, generation, scope_digest)`, and the ORIGINAL identity
already reached `'succeeded'` — an unchanged generation would make
`_reserve_execution` find it and skip re-executing.

One run per calendar day is targeted (whichever run for that day has every
partition `'succeeded'`, preferring the most recently updated). A run still
genuinely `status='running'` (mid-dispatch, not yet fully succeeded) is
`skipped_ineligible` — that shape belongs to `daily-redrive`/the automatic
path, not this command, since resetting it here would race whatever is
currently executing it.

**Provenance.** Every reset writes one row to
`daily_metrics_partition_recompute_events` — `run_id`, `org_id`,
`target_day`, `family`, the run's exact prior `status`/`generation`, `actor`
(always `'partition-recompute'`), the operator's `reason` (their
`--review-evidence` text, verbatim), and the redrive `nonce` — in the SAME
transaction as the reset and the publish. Either all three (provenance +
reset + fresh outbox rows) land, or none do; a `--dry-run` pass writes none
of them. Deliberately append-only, unlike `finalize-redrive`'s
`daily_metrics_finalize_redrive_events`: there is no unattended sweep for
this class today that needs an open/closed lifecycle to avoid
double-dispatching an in-flight redrive (see the migration's own doc
comment, `0117_add_daily_metrics_partition_recompute_events.py`) — tracked
as follow-up scope if/when one is added.

Returns `{"partition_recompute": {"Days": [{"Day": "...", "RunID": "...",
"Outcome": "..."}, ...]}, "dry_run": true|false}`, one entry per calendar
day in the requested range. `Outcome` is `"redriven"` for a real pass that
reset and republished a day, `"skipped_ineligible"` for a day with no
eligible run (none exists, partitions not 100% succeeded, or a concurrent
caller already reset it), or the `"would_"`-prefixed equivalents under
`--dry-run`. `RunID` is empty when no run exists at all for that day.

**Observability**: `dev_health_daily_metrics_partition_recompute_days_total{family,outcome}`
is wired but not live for THIS caller, for the same reason every other
`workerctl` counter above is not: a one-shot CLI with no Prometheus scrape
endpoint.

#### `metrics remaining start` (CHAOS-4254)

Dispatch a **new** remaining-metrics run for a historical `(organization,
family, day)` that no automatic trigger ever dispatched at all (CHAOS-4254) —
sync never ran that day, or the row aged out of River's retention. This is
narrower than it sounds: `jobs retry` recovers a `remaining_metric_runs` row
that was dispatched and then discarded, and `metrics daily-redrive` above is
DAILY-family-only (`daily_metrics_runs`/`daily_metrics_partitions`) and never
touches the remaining family's native Go executors (dora, capacity, …) at
all. Neither helps when the day was never computed in the first place — this
command is also the prod recovery path for CHAOS-4384's dora-frozen-at-0
incident, since a day the pre-fix same-day coverage bug froze at 0 rows
already has a "succeeded" partition and needs exactly this bypass.

```bash
dho workers metrics remaining start \
  --family dora \
  --day 2026-08-25 --to 2026-08-27 \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --review-evidence "CHAOS-4384: dora frozen at 0 rows for 08-25..08-27 by the pre-fix same-day coverage bug (5ddab4c65); deployments/incidents have since landed for these closed days" \
  --reason <code> --correlation-id <id>
```

Supported `--family` values are the day-scoped remaining-metrics families
only: `complexity`, `dora`, `release_impact`. `capacity`, `recommendations`,
and `membership_backfill` are real families (`families.json`) but do not
scope by calendar day — `capacity` needs a `GenerationSeed` the CLI has no
flag for, and the other two scope by window/repo set — so `--family capacity`
etc. is `invalid_request`. `--to` defaults to `--day` (a single day); the
`[--day, --to]` span is capped at 31 days — this is a manual, human-invoked
recovery tool for a handful of days, not a bulk backfill mechanism. Both
`--day` and `--to` must be **strictly before today (UTC)** — this is a
historical recovery tool, and dora's automatic triggers (post-sync's first-
sync-of-day, the fixed-schedule `dora_daily_fanout` occurrence) only ever
target the CURRENT day, so allowing "today" would open a race between a
manual and an automatic dispatch for the same day (see the coverage rule
below).

**Coverage rule — deliberately not the same one the automatic dora trigger
applies.** `StartRunTx`'s own `family=="dora"` cross-trigger dedup
(CHAOS-4384) treats ANY succeeded partition for a day that has already
CLOSED as terminal coverage, 0 rows or not, because for the automatic
triggers a genuinely quiet closed day and a day nobody ever computed are
indistinguishable. This command is more precise, because a human is
authorizing each recompute individually, and checks every succeeded
partition whose scope window `[anchor - backfill_days + 1, anchor]`
contains the requested day (`generation` is not part of the check — a
prior run under ANY generation counts):

- An **exact single-day partition** (`backfill_days == 1`, the shape every
  automatic post-sync/fixed-schedule dispatch normally uses) is
  unambiguous: its `rows_written` evidence IS that day's own total. A
  **0-row** exact match is exactly the CHAOS-4384 shape this command
  exists to recompute and is NOT refused; a **non-zero-row** exact match
  is refused (`"already_covered"`).
- A **wide multi-day partition** (`backfill_days > 1`, e.g. a post-sync
  catch-up run) is ambiguous for any day inside its window that is not
  provably isolated: `DORAExecutor.ComputePartition` accumulates ONE
  `rows_written` total across the WHOLE window, so neither a zero nor a
  non-zero aggregate proves what any single interior day got. This command
  refuses (`"already_covered"`) rather than guess in either direction —
  verify via the `readback_hint` query and, if the day is genuinely
  uncovered, request it with a narrower `--day`/`--to` that does not land
  inside the ambiguous run's window.
- A run for the same day still **`pending`/`running`** under a different
  generation (an automatic trigger currently executing) is refused as
  `"in_progress"` — its eventual completion is invisible to the checks
  above, so inserting a manual run alongside it risks a genuine duplicate
  once both finish. Re-run once it settles.

Retrying the **identical** command (same `--family`/`--org`/`--day`/`--to`,
which derives the same base `generation` — see below) is always safe: it
reuses the same run rather than inserting a duplicate, even across an
in-flight retry (`"already_ran"`) or after the run itself legitimately
completed with 0 rows. In the 0-row case, the store mints a numbered
`:retry-N` generation (bounded to 5 bumps) so the identical retry actually
recomputes instead of reloading the exhausted run forever; a later call
while that bumped generation is itself still pending correctly recognizes it
as the same in-flight request (`"already_ran"`), not a foreign collision. If
every generation up to the bound is exhausted (all terminal, none usefully
covering), the day fails as `"exhausted"` rather than silently reporting
success with nothing dispatched.

Returns, per day in the requested span:

```json
{
  "family": "dora", "org": "c6a3...", "generation": "manual-backfill:dora:c6a3...:2026-08-25..2026-08-27",
  "days": [
    {"day": "2026-08-25", "status": "started", "run_id": "...", "partition_id": "...", "generation": "manual-backfill:dora:c6a3...:2026-08-25..2026-08-27"},
    {"day": "2026-08-26", "status": "already_ran", "run_id": "...", "partition_id": "...", "generation": "manual-backfill:dora:c6a3...:2026-08-25..2026-08-27:retry-1"},
    {"day": "2026-08-27", "status": "already_covered", "run_id": "..."}
  ],
  "readback_hint": "ClickHouse: SELECT day, count() FROM dora_metrics_daily WHERE org_id = '...' AND day BETWEEN '2026-08-25' AND '2026-08-27' GROUP BY day ORDER BY day"
}
```

`status` is one of `started` (a new run/partition was inserted), `already_ran`
(idempotent retry, not an error), `already_covered` or `in_progress` (refused,
see above), `exhausted` (the retry-generation budget ran out with nothing
dispatched), or `error` (an unexpected store/publisher failure for that one
day). The command's own exit code is nonzero if ANY day is `error` or
`exhausted`, even though the full per-day JSON is still printed to stdout for
diagnosis. The top-level `generation` is derived deterministically from the
request's own flags (family/org/day-range), never wall-clock time, so a
retried invocation is recognizable as the same logical request -- but each
day's OWN `generation` field is the one that actually matters for a durable
lookup, since a bumped `:retry-N` run's generation differs from the
top-level one. Run the `readback_hint` query (or the daily-redrive query
below, substituted with the printed `run_id`s) to confirm rows actually
landed once the dispatched partition jobs execute — a `"started"` status is
dispatch confirmation, not completion.

**Observability**: `dev_health_remaining_metrics_manual_backfill_total{family,outcome}`
is wired but not live for THIS caller, for the identical reason
`dev_health_daily_metrics_redrive_partitions_total` above is not: `workerctl`
is a one-shot CLI with no Prometheus scrape endpoint. The durable record of a
manual backfill is the `remaining_metric_runs`/`remaining_metric_partitions`
rows themselves, findable by each day's OWN printed `generation` (not the
top-level one — see above):

```sql
SELECT run.id, run.generation, run.status, partition.id, partition.status, partition.output_evidence
FROM remaining_metric_runs run
JOIN remaining_metric_partitions partition ON partition.run_id = run.id
WHERE run.org_id = '<org>' AND run.family = '<family>' AND run.generation = '<that day's printed generation>';
```

### `dho workers routes`

Inspect or control one fixed sync-dispatch transport route through the
authenticated, payload-redacted Go operator binary:

```bash
dho workers routes status dispatch_sync_run

dho workers routes apply \
  --reason deployment \
  --correlation-id change-123 \
  dispatch_sync_run

dho workers routes pause \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run

dho workers routes drain \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run

dho workers routes resume \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run
```

The fixed kinds are `dispatch_sync_run`, `finalize_sync_run`, `post_sync`, and
`reference_discovery`. No operator token is needed (see "No token" above). Mutations
are serialized per semantic database, persist audit intent before changing
state, and may return `outcome_unknown`; inspect the route before retrying.

The checked-in transport for all four sync-dispatch kinds is River, and no
rollback transport is recorded against them (`rollback_route: none`). `routes apply`
converges one unpaused legacy-transport route to its checked-in River transport after proving the
matching capability exists and no live outbox claim remains. It is idempotent
when the route is already active.

A fresh database seeds these four route rows on `celery`. With the rollback
transport retired, `routes apply` reads such a row as drift and does not move
it. `dho migrate river` moves it: a row that is on `celery` with no rollback
transport, not paused and with no live outbox claim goes to River at
`generation + 1`, in the same transaction as the runtime grants. A row in any
other state (already on River, paused, still naming a rollback transport, or
with a live claim) is not changed. The run logs one line for each kind:
`sync dispatch route moved`, `sync dispatch route present`, or the warning
`sync dispatch route left as found` with the row's state. After
`dho migrate river`, `routes apply` on a fresh database finds the route active
and changes nothing.

**`routes resume` no longer takes a `--transport` flag (CHAOS-5626).** The
celery transport is retired: no Celery consumer runs anywhere, and it is not a
supported rollback target (ruling R146). `resume` now always resumes onto
River, the only remaining transport. To roll back, redeploy a previously
deployed Go revision from the rollback tag set.

---

## Maintenance

Operational cleanup tasks. Use `POSTGRES_URI`.

### `maintenance cleanup-tokens`

Delete expired refresh tokens.

```bash
dho maintenance cleanup-tokens
```

### `maintenance cleanup-all`

Run all maintenance cleanup tasks (currently refresh-token cleanup).

```bash
dho maintenance cleanup-all
```

---

## Service Credentials

Internal service credentials are the bearer tokens the ACR entitlement lookup (`acr`) and the worker operator API (`worker-operator`) present to the API. The token is printed **once**, on stdout, when a credential is created or rotated; the database keeps only its SHA-256 and its first 16 characters (`token_prefix`), and `list` never prints a secret. Uses `POSTGRES_URI`. `--service` chooses the service (default `acr`), and a service can hold only its own scopes: `acr` holds `entitlements:read`; `worker-operator` holds `workers:read` and `workers:operate`.

```bash
# Create a credential (the token is printed once)
dho service-credentials create --service acr --scope entitlements:read

# A worker-operator credential with both scopes that lapses at a fixed time
dho service-credentials create --service worker-operator \
  --scope workers:read --scope workers:operate --expires-at 2027-01-01T00:00:00+00:00

# List one service's credentials (metadata only)
dho service-credentials list --service worker-operator

# Rotate: issue a replacement now; the old credential stays valid for 5 more minutes
dho service-credentials rotate <credential-id> --scope entitlements:read --overlap-seconds 300

# Revoke
dho service-credentials revoke <credential-id>
```

| Subcommand | Description |
|------------|-------------|
| `create` | `--service {acr,worker-operator}` (default `acr`), `--scope` (required, repeatable), `--expires-at` (ISO 8601 with a timezone, in the future), `--created-by-user-id` (a user id) |
| `list` | `--service` (default `acr`); one JSON line: `id`, `service_name`, `token_prefix`, `scopes`, `expires_at`, `revoked_at`, `last_used_at` (no computed validity: an expired credential prints like a live one) |
| `rotate` | `<credential_id>` then the `create` options plus `--overlap-seconds` (0 to 3600, default 0). The credential must belong to `--service` (the default is `acr`: pass `--service worker-operator` to rotate a worker-operator credential) and be active; its expiry is set to now + the overlap (even if it had less time left) and a replacement is issued |
| `revoke` | `<credential_id>`; sets `revoked_at` to now, also on an already revoked credential |

> **Notes:** the database is `MIGRATION_DATABASE_URI`, else `POSTGRES_URI` (as every `dho admin` verb); a missing one is exit 1. `rotate` locks the credential row while it checks and changes it. `--help` prints usage on stdout (exit 0). A refusal is one JSON error line on stderr with exit 1. `list` prints no computed validity (CHAOS-4032): an expired credential prints like a live one.

## Billing

### `billing reconcile`

Run billing reconciliation against Stripe. Uses `POSTGRES_URI`.

```bash
# Reconcile all orgs
dho billing reconcile

# Reconcile a single org since a date
dho billing reconcile --org-id <uuid> --since 2025-01-01
```

**Options:**
| Option | Description |
|--------|-------------|
| `--org-id` | Reconcile a single organization (UUID). Omit to reconcile all orgs |
| `--since` | Only reconcile invoices on or after this date (ISO YYYY-MM-DD) |

> **Notes:** the report is one line of JSON (`started_at`, `completed_at`, the three `*_checked` counts, `mismatches`, `missing_local`, `missing_stripe`). The verb needs a database (`MIGRATION_DATABASE_URI` or `POSTGRES_URI`) and `STRIPE_SECRET_KEY`. It lists every subscription, invoice and refund (100 a page, every page followed) and compares them, writes audit rows for a named organization, and runs a second invoice pass for `--since`. A `--since` with no UTC offset is read as UTC. A bad `--org-id` or `--since`, or a missing `STRIPE_SECRET_KEY`, ends the verb with a one-line message and exit 1.

---

## AI Governance

### `ai allowlist`

Manage the org-level AI tool allowlist (which AI tools/models are permitted). Requires `CLICKHOUSE_URI` **and** an organization id.

```bash
# Set a policy for a tool (optionally a specific model)
dho ai allowlist set --tool claude-code --status allowed --reason "approved"
dho ai allowlist set --tool claude-code --model opus --status deprecated

# List the latest allowlist entries for the org
dho ai allowlist list
```

| Subcommand | Description |
|------------|-------------|
| `set` | Create/update an entry (`--tool`, optional `--model`, `--status {allowed,disallowed,deprecated}`, `--reason`) |
| `list` | Show the latest allowlist entries for the org |

---

## Work Graph

> **CHAOS-4924:** the `dev-hops work-graph build` verb was deleted; the Python build compute is gone. To enqueue a fresh `workgraph.build` request, use `dho workers workgraph trigger --org <uuid> [--from <YYYY-MM-DD>] [--to <YYYY-MM-DD>] --review-evidence "<text>" [--dry-run] --reason <code> --correlation-id <id>`, or rely on the scheduled Go run. See [Run workers and jobs](../../operate/run/workers-and-jobs.md).

---

## Investment

> **CHAOS-5173:** the `dev-hops investment materialize` verb was deleted — it was a separate, direct-Python-compute entry point from the `investment.materialize` River kind, which is NATIVE and runs through the same worker dispatch/idempotency every other kind does. Use `dho workers investment trigger --org <uuid> [--from <YYYY-MM-DD>] [--to <YYYY-MM-DD>] --review-evidence "<text>" [--dry-run] --reason <code> --correlation-id <id>` to enqueue a fresh run through the native executor instead. It drops every flag with no Go-side equivalent (`--window-days`, `--repo-id`, `--team-id`, every LLM flag, `--force`, `--persist-evidence-snippets`, `--allow-unscoped`, `--analytics-db`/`--db`) — only an org id and an optional `--from`/`--to` window exist on the request.

During preprocessing, the native materializer emits an `investment repo
attribution` log record scoped by `org_id` and `run_id`. The `own_signal`,
`hierarchy_ancestor`, `hierarchy_children`, and `unassigned` counts partition the
`components` count. The record also includes `cascade_hop1`,
`cascade_hop2_plus`, and `cascade_max_hops` to show transitive inheritance. These
are log fields, not Prometheus counters or a successful-run completion signal.
See [Investment repository inheritance](../data-models/investment.md#repository-inheritance)
for the allocation precedence and persisted evidence. These counts describe the
hierarchy stage, before the final team ownership fallback.

The `investment team repository fallback` log record reports what the final
equal-share fallback did with every component in the run. `allocated`,
`own_repo`, `stronger_allocation`, `direct_repo_evidence`, `no_eligible_owner`
and `window_skipped` partition the `components` count: they always sum to it.
`repo_shares` counts the repository rows the fallback wrote, and `donor_rows`
and `donor_issues` report the read side -- how much eligible ownership evidence
the run loaded. `ownership_as_of` names the timestamp the ownership intervals
were evaluated at, which is the run's own `computed_at`, never wall-clock time.

**Every one of these fields is emitted on every run, including when its value is
zero.** A field that disappears at zero cannot be told apart from a field that
was never computed, so a zero is always written out. This matters for reading
the record: a zero `allocated` on its own is ambiguous. With a high `own_repo`
or `direct_repo_evidence` it means stronger evidence already resolved the units,
which is the healthy case. With a high `no_eligible_owner` AND `donor_rows=0` it
means no eligible ownership reached the run at all, which points at the team and
repository-ownership sync, not at this allocator. A failed donor read is neither
case -- it fails the run before anything is written.

The run statistics expose the same counts as `repo_ownership_fallback`,
`repo_ownership_own_repo`, `repo_ownership_stronger_allocation`,
`repo_ownership_direct_repo_evidence`, `repo_ownership_no_eligible_owner`,
`repo_ownership_window_skipped`, `repo_ownership_repo_shares`,
`repo_ownership_donor_rows` and `repo_ownership_donor_issues`. No new CLI option
is required. Inspect persisted `work_unit_repo_effort` rows from the latest
generation to distinguish `team_ownership` from direct churn and
`hierarchy_cascade`. See
[Team ownership fallback](../data-models/investment.md#team-ownership-fallback).

---

## Recommendations

> **CHAOS-5307:** the `dev-hops recommendations compute` preview verb was deleted — a Python CLI running `RuleEngine` directly is Python compute executing in production tooling, read-only or not (team-lead ruling). There is no `dev-hops` wrapper verb for recommendations. For the persisted, generation-deduped compute, use `dho workers metrics remaining trigger-backstop --family recommendations --team <team-uuid>` (or `--all-teams`) `--window <days> --review-evidence <why> --reason <code> --correlation-id <id>` directly — **not** `metrics remaining start`, which only accepts `complexity`/`dora`/`release_impact` and rejects `recommendations` outright. A Go-native `workerctl recommendations preview` verb is tracked as a follow-up so the read-only preview capability itself is not lost.

---

## Reports

> ℹ️ **Note:** Reports are not managed or triggered via the CLI. They are managed entirely through the GraphQL API or the Report Center UI. See [Reports](../../use/reports/index.md) for details.

AI-generated reports are managed through the GraphQL API and executed by Go's native report runtime. Reports are not triggered via CLI — they are created, triggered, and scheduled through the Report Center UI or GraphQL mutations.

### How Reports Work

1. **Create** a SavedReport via the Report Center UI or `createSavedReport` mutation
2. **Trigger** execution manually ("Run Now") or via a cron schedule
3. The trigger writes a durable outbox row, relayed to River and executed by Go's `report.execute_on_demand`/`report.execute_scheduled` (CHAOS-4440; CHAOS-3093 deleted the last Celery report task, `execute_saved_report`)
4. The engine fetches metrics from ClickHouse, generates insights, and renders markdown
5. Results are persisted as a `ReportRun` with rendered content and provenance records

### Report Plan

Each report requires a `ReportPlan` that defines scope, time range, sections, and metrics. If no explicit plan is provided, a default plan is generated from the report's `parameters` at execution time:

- `scope` → team/repo/org scoping
- `dateRange` → time window (`last_7_days`, `last_30_days`, `last_90_days`)
- `metrics` → requested metric names

### Scheduling

Reports can be scheduled with a five-field cron expression (via `scheduleCron` in the create/update mutation). Create and update validate the field count and value ranges before persistence. Invalid input returns an error that identifies how to correct it. The periodic scan for due reports was `dispatch_scheduled_reports` (a Celery beat task, run every 5 minutes) until CHAOS-4026 (2026-08-21) deleted it -- Go's `report.execute_scheduled` fixed schedule now owns that scan. `execute_saved_report` (the per-report execution work) was not part of that cleanup at the time, but CHAOS-3093 has since deleted it outright -- report execution is now Go-only end to end, dispatched through the durable outbox and River.

### Worker Configuration

Reports execution is Go-only now -- no Celery task remains in this path; see [Run workers and jobs](../../operate/run/workers-and-jobs.md) for how the Go-only runtime is started.

### GraphQL Mutations

| Mutation | Description |
|----------|-------------|
| `createSavedReport` | Create a new report definition |
| `updateSavedReport` | Update name, description, parameters, schedule |
| `cloneSavedReport` | Clone a report with optional overrides |
| `deleteSavedReport` | Delete a report and its schedule |
| `triggerReport` | Manually trigger a report execution |

### GraphQL Queries

| Query | Description |
|-------|-------------|
| `savedReports` | List saved reports for an org |
| `savedReport` | Get a single report by ID |
| `reportRuns` | List execution history for a report |

---

## Batch Processing Options

For GitHub/GitLab batch operations:

| Option | Description |
|--------|-------------|
| `-s, --search PATTERN` | Glob pattern for repos |
| `--group NAME` | Organization/group name |
| `--batch-size N` | Records per batch |
| `--max-concurrent N` | Concurrent workers |
| `--max-repos N` | Maximum repos to process |
| `--use-async` | Enable async workers |
| `--rate-limit-delay SECONDS` | Delay between requests |

---

## Environment Variables

### Database

| Variable | Description |
|----------|-------------|
| `POSTGRES_URI` | PostgreSQL connection (semantic layer: users, settings) |
| `CLICKHOUSE_URI` | ClickHouse connection (analytics layer: metrics, data) |
| `DATABASE_URI` | Legacy fallback (deprecated) |
| `DB_ECHO` | Enable SQL logging |

### Provider Auth

| Variable | Provider |
|----------|----------|
| `GITHUB_TOKEN` | GitHub |
| `GITLAB_TOKEN` | GitLab |
| `JIRA_EMAIL` | Jira |
| `JIRA_API_TOKEN` | Jira |
| `JIRA_BASE_URL` | Jira |
| `LINEAR_API_KEY` | Linear |

### Linear Options

| Variable | Default | Description |
|----------|---------|-------------|
| `LINEAR_FETCH_COMMENTS` | `true` | Fetch issue comments |
| `LINEAR_FETCH_HISTORY` | `true` | Fetch status change history |
| `LINEAR_FETCH_CYCLES` | `true` | Fetch cycles as sprints |
| `LINEAR_COMMENTS_LIMIT` | `100` | Max comments per issue |

### Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | 100 | Records per batch |
| `MAX_WORKERS` | 4 | Parallel workers |

---

## Migrate Commands

Database schema migrations for PostgreSQL and ClickHouse. `dho migrate upgrade` runs the whole set in order: the PostgreSQL head (`dho migrate postgres upgrade`), the standard feature flags (`dho admin features seed`), then the ClickHouse head (`dho migrate clickhouse upgrade`). `--river` also applies the River schema right after the PostgreSQL step. The first step that fails stops the run, and the exit code is that step's.

### `migrate postgres`

Run PostgreSQL schema migrations. Uses `MIGRATION_DATABASE_URI` (a direct, elevated DSN), else `POSTGRES_URI`. The bare `dho migrate postgres` prints usage only; the verbs are:

```bash
# Apply all ordinary pending migrations
dho migrate postgres upgrade

# Revert one migration
dho migrate postgres downgrade -1

# Show current applied revision
dho migrate postgres current

# Show recorded, missing and pending revisions
dho migrate postgres status

# Read-only application-schema check (exit 1 while required revisions are pending)
dho migrate status --check

# Print, read-only, what the upgrade will do to this database
dho migrate postgres preflight

# Show migration history
dho migrate postgres history

# Show available heads
dho migrate postgres heads
```

`upgrade` takes no revision argument: it applies the head baseline to an empty database, then every revision after the head. `downgrade` takes a target: a revision id (`0138` to `0145`, which reverts everything above it) or `-N` (revert N steps).

`upgrade` and `status` require `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`, because the head has the River cutover (revision `0066`) applied, as production does, and `RIVER_DATABASE_SCHEMA` set to the head's River schema (`river`). `dho` refuses any other value and names it.

`dho migrate status --check` is read-only. It exits 1 if a required migration is pending and 0 if the application schema is current. Only this flat form takes `--check`: `dho migrate postgres status` takes no flags.

`dho migrate postgres preflight` is read-only and exits 0 when the database is at the head, 10 when the upgrade applies cleanly (1 with `--strict`), 1 when the upgrade needs manual work, and 3 when the measurement did not happen.

**Flat aliases:** `dho migrate current`, `heads`, `history`, `status` and `downgrade` target PostgreSQL. `dho migrate upgrade` is not an alias: it runs the full set described above.

### `migrate clickhouse`

Run ClickHouse schema migrations. Uses `CLICKHOUSE_URI` (native protocol), or the `DEV_HEALTH_CH_*` component form.

ClickHouse migrations are tracked in a `schema_migrations` table in ClickHouse. `upgrade` applies the head baseline to an empty database, then every migration after the head. The bare `dho migrate clickhouse` prints usage only.

```bash
# Apply all pending migrations
dho migrate clickhouse upgrade

# Show applied, missing and pending migrations (read-only)
dho migrate clickhouse status

# Exit 1 unless the database is at the head (read-only wait primitive for deploy tooling)
dho migrate clickhouse status --check

# List stale duplicate repo rows (a dry run unless --apply)
dho migrate clickhouse repair
dho migrate clickhouse repair --apply
```

> **Important:** Run `dho migrate clickhouse upgrade` after setting up a fresh environment, before running any sync or metrics commands. ClickHouse tables are **not** auto-created — they require migrations to be applied first.

---

## Workflow Examples

### Full Sync Pipeline

```bash
# Set environment variables
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default"
export POSTGRES_URI="postgresql://postgres:postgres@localhost:5555/postgres"
export DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 RIVER_DATABASE_SCHEMA=river

# 1. Run migrations
dho migrate postgres upgrade
dho migrate clickhouse upgrade

# 2. Sync git data
dho sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 3. Work items sync automatically via the native Go provider-sync route +
#    webhooks (CHAOS-5351) -- no manual command. To force a backfill window
#    for a specific sync config: dho backfill run --config-id <uuid>
#    [--since ...] [--before ...]

# 4. Compute metrics (needs a running worker and the Postgres coordinator)
dho workers metrics daily-start \
  --org "$ORG_ID" \
  --day "$(date -u -d '30 days ago' +%F)" --to "$(date -u -d yesterday +%F)" \
  --reason <code> --correlation-id <id>
```

### Local Development

```bash
# Start databases
docker compose up -d clickhouse postgres

# Run migrations (the compose `migrate` service runs `dho migrate upgrade`)
docker compose run --rm migrate

# Load a frozen synthetic world with its derived metrics and work graph
# (analytics rows only: create the first user with `dho admin users create`)
dho fixtures generate --sink "$CLICKHOUSE_URI" --provider synthetic \
  --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 \
  --pr-count 24 --team-count 10 --seed 20260219 --with-metrics --with-work-graph
```

### Batch Organization Sync

```bash
# Sync all repos in org
dho sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  -s "myorg/*" \
  --group myorg \
  --max-concurrent 4 \
  --use-async
```

---

## push

`dho push` is the client CLI for [Customer Push](../../integrate/customer-push/overview.md)
— submitting your own data to `/api/v1/external-ingest/*` instead of relying on a FullChaos-managed
connector. See [Register a source](../../integrate/customer-push/register-source.md) for a full first-batch
walkthrough and [Record kinds and enums](../../reference/schemas/record-kinds-and-enums.md)
for the record kinds. `push` subcommands use neither the global `--org` auto-resolution
nor the ClickHouse/PostgreSQL preflight — `validate`/`sample` are fully offline; `batch`/`status`
talk to the FullChaos API over HTTP and resolve their own credentials (below).

### Credentials (`batch` / `status`)

| Flag | Env var | Notes |
|------|---------|-------|
| `--api-url` | `FULLCHAOS_API_URL` | FullChaos API base URL. |
| `--token` | `FULLCHAOS_INGEST_TOKEN` (deprecated alias: `FULLCHAOS_API_TOKEN`) | An `fcpush_...` ingest token — see [Register a source](../../integrate/customer-push/register-source.md). |
| `--org` | `FULLCHAOS_ORG_ID` | Organization id. |

A flag always wins over its env var. If any of the three can't be resolved, the command prints
`error: missing required: ...` to stderr and exits 2 — not an argparse `required=True` error,
so the env-var fallback still works.

### `dho push validate <payload>`

Validates a batch envelope locally — **no network call**. Reads a JSON file, or `-` for stdin.

| Flag | Notes |
|------|-------|
| `--schema` | Schema version to validate against. Default and only supported value: `external-ingest.v1`. |
| `--json` | Emit machine-readable JSON to stdout instead of a human rejection table. |

```bash
dho push validate batch.json
dho push validate - < batch.json --json
```

### `dho push sample`

Prints a canonical sample batch envelope built from the packaged example payloads (the same
files `GET /schemas/{version}` embeds) — no network call.

| Flag | Notes |
|------|-------|
| `--kind KIND` | One record kind, bare or versioned (e.g. `pull_request` or `pull_request.v1`). Mutually exclusive with `--all`. |
| `--all` | Combined batch envelope with one record of every kind. Mutually exclusive with `--kind`. |

```bash
dho push sample --kind pull_request > sample.json
dho push sample --all | dho push validate -
```

### `dho push batch <payload>`

Submits a batch to `POST /api/v1/external-ingest/batches`. Reads a JSON file, or `-` for stdin.

| Flag | Notes |
|------|-------|
| `--api-url`, `--token`, `--org` | See [Credentials](#credentials-batch-status) above. |
| `--poll` | Poll `GET /batches/{id}` until the batch reaches a terminal status, instead of returning immediately after the `202`/`200`. |
| `--poll-interval` | Seconds between polls. Default 5 (an internal floor of 0.5s is enforced). |
| `--poll-timeout` | Give up polling after this many seconds. |
| `--skip-limits-check` | Skip the `GET /schemas` limits pre-flight; enforce hardcoded client defaults (1000 records / 10MB) instead of the server's live limits. |
| `--json` | Emit machine-readable JSON to stdout. |

```bash
dho push batch sample.json --poll
dho push batch - --json < sample.json
```

### `dho push status <ingestion_id>`

Fetches (and optionally polls) a batch's status via `GET /batches/{id}`.

| Flag | Notes |
|------|-------|
| `--api-url`, `--token`, `--org` | See [Credentials](#credentials-batch-status) above. |
| `--poll`, `--poll-interval`, `--poll-timeout` | Same semantics as `push batch`. |
| `--json` | Emit machine-readable JSON to stdout. |

```bash
dho push status b6c1e6b0-...-uuid --poll
```

### `dho push export <provider>`

Reserved extension point for provider-native export helpers (e.g. `github`, `gitlab`). **Not
implemented in v1** — every provider currently prints an error and exits with a data-failure
status. Use `dho push sample` plus a hand-written export, or the provider's native
FullChaos connector, in the meantime.

### `push` exit codes

`push` uses its own exit-code contract, distinct from the rest of `dho` (see
[Exit Codes](#exit-codes) below):

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Data-level failure — invalid payload, or a batch that completed with rejections / reached a terminal `failed` status |
| 2 | Usage error — bad/missing CLI args, or unresolved `--api-url`/`--token`/`--org` |
| 3 | Transport/API error after retries, or `stream_unavailable` |
| 4 | Poll timeout — the batch was still non-terminal when `--poll-timeout` elapsed |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Failure: a dependency is down, the operation failed, or the configuration is invalid |
| 2 | Usage error: an unknown command, flag or positional argument, or a missing required input surfaced by the [preflight](#input-validation-preflight) (e.g. unset `CLICKHOUSE_URI`). Nothing ran |
| 3 | Refused: a preflight said no and nothing was written, or the verb is not available in `dho` |

