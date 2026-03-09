# Task Trackers & Work Items

This repo normalizes Jira issues, GitHub issues/Projects items, GitLab issues, and Linear issues into a unified `WorkItem` model (`src/dev_health_ops/models/work_items.py`) and computes daily aggregates + cycle times.

Jira is used to track associated project work (planning/throughput/WIP). Pull request metrics are computed from PR/MR data synced via the CLI (`dev-hops sync ...`) and are independent of Jira.

## Provider Credentials (env vars)

### Jira (Cloud)
- `JIRA_BASE_URL` (e.g. `your-org.atlassian.net` or `https://your-org.atlassian.net`; normalized to `https://`)
- `JIRA_EMAIL`
- `JIRA_API_TOKEN`
- `JIRA_PROJECT_KEYS` (optional, comma-separated, e.g. `ABC,XYZ`)
- `JIRA_JQL` (optional override; if set, used as-is instead of the built-in windowed query)
- `JIRA_FETCH_ALL` (optional; set to `1` to fetch all issues in the project(s) regardless of date window — can be very slow)
- `JIRA_FETCH_COMMENTS` (optional; set to `0` to disable comment metadata ingestion; default: `1`)

Optional Jira field mappings (instance-specific):
- `JIRA_STORY_POINTS_FIELD` (e.g. `customfield_10016`)
- `JIRA_SPRINT_FIELD` (default: `customfield_10020`)
- `JIRA_EPIC_LINK_FIELD` (e.g. `customfield_10014`)

### GitHub
- `GITHUB_TOKEN`
  - Optional CLI override: `dev-hops sync work-items --provider github --auth "$GITHUB_TOKEN" ...`

Optional Projects v2 ingestion:
- `GITHUB_PROJECTS_V2` as comma-separated `org_login:project_number` entries, e.g.:
  - `GITHUB_PROJECTS_V2="myorg:3,anotherorg:12"`

### GitLab
- `GITLAB_TOKEN`
- `GITLAB_URL` (optional, default: `https://gitlab.com`)
  - Optional CLI override: `dev-hops sync work-items --provider gitlab --auth "$GITLAB_TOKEN" ...`

### Linear
- `LINEAR_API_KEY` (required)

Optional configuration:
- `LINEAR_FETCH_COMMENTS` (default: `true`) - fetch issue comments
- `LINEAR_FETCH_HISTORY` (default: `true`) - fetch status change history for transitions
- `LINEAR_FETCH_CYCLES` (default: `true`) - fetch cycles as sprints
- `LINEAR_COMMENTS_LIMIT` (default: `100`) - max comments per issue

Usage:
```bash
# Sync all teams
dev-hops sync work-items --provider linear --db "$DATABASE_URI"

# Sync specific team by key (e.g., ENG, PROD)
dev-hops sync work-items --provider linear --repo ENG --db "$DATABASE_URI"
```

## Status & Type Normalization

Status normalization is config-driven via `src/dev_health_ops/config/status_mapping.yaml`.

### Status categories
Normalized categories are:
- `backlog`
- `todo`
- `in_progress`
- `in_review`
- `blocked`
- `done`
- `canceled`

### Provider-specific rules
The mapping file supports:
- Jira: `providers.jira.statuses` (maps Jira status names)
- GitHub/GitLab: `providers.<provider>.status_labels` (maps label names to categories)
- Linear: `providers.linear.state_types` (maps Linear state.type to categories)

If no label/status match exists:
- GitHub: `open → todo`, `closed → done`
- GitLab: `opened → todo`, `closed → done`
- Linear: Maps `state.type` directly (`backlog`, `unstarted → todo`, `started → in_progress`, `completed → done`, `canceled`)

## Identity Mapping (optional)

To keep user metrics consistent across providers, populate `src/dev_health_ops/config/identity_mapping.yaml`.

Schema:
- `canonical`: stable identity (prefer email)
- `aliases`: provider-qualified logins (e.g. `github:octocat`) or Jira account IDs (e.g. `jira:accountid:abcd123`)

## Team Mapping (optional)

To enable team filtering in Grafana, you can sync teams from various sources.

### Config-based Mapping
Populate `src/dev_health_ops/config/team_mapping.yaml` (schema: `team_id`, `team_name`, `members`).
Then run:
```bash
dev-hops sync teams --path src/dev_health_ops/config/team_mapping.yaml
```

### Jira Project Mapping
Automatically import Jira projects as teams:
```bash
dev-hops sync teams --provider jira
```

## Running Jira work metrics

Jira work items are fetched via the work item sync job:

```bash
dev-hops sync work-items --provider jira --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
```

Use `-s`/`--search` to filter repos by name (glob pattern), e.g.:

```bash
dev-hops sync work-items --provider github -s "org/*" --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
```

## Atlassian Client Migration

The project is migrating from a legacy custom Jira client to a shared `atlassian` client library. This migration is managed via feature flags and runs in parallel with the legacy implementation.

See [Atlassian Client Integration Plan](./plans/atlassian-client-integration.md) for the full phased roadmap.

### Feature Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `ATLASSIAN_CLIENT_ENABLED` | `false` | Enable the new `atlassian` client library for Jira sync. When `false`, the legacy `JiraClient` is used. |
| `ATLASSIAN_GQL_ENABLED` | `false` | Enable Atlassian GraphQL Gateway (AGG) enrichment (Phase 5). Requires OAuth credentials. |
| `JIRA_USE_PROVIDER` | `false` | Use `JiraProvider` in the work items pipeline instead of the standalone sync function. |

### New Atlassian Environment Variables

When `ATLASSIAN_CLIENT_ENABLED=true`, configure these variables (they fall back to legacy `JIRA_*` vars if unset):

| Variable | Description |
|----------|-------------|
| `ATLASSIAN_EMAIL` | Atlassian account email (falls back to `JIRA_EMAIL`) |
| `ATLASSIAN_API_TOKEN` | API token (falls back to `JIRA_API_TOKEN`) |
| `ATLASSIAN_JIRA_BASE_URL` | Jira Cloud base URL, e.g. `https://your-org.atlassian.net` (falls back to `JIRA_BASE_URL`) |
| `ATLASSIAN_CLOUD_ID` | Atlassian Cloud ID (auto-derived from base URL if omitted) |

#### Optional: Custom field IDs

| Variable | Description |
|----------|-------------|
| `ATLASSIAN_JIRA_STORY_POINTS_FIELD` | Custom field ID for story points (e.g. `customfield_10016`) |
| `ATLASSIAN_JIRA_SPRINT_IDS_FIELD` | Custom field ID for sprint (default: `customfield_10020`) |

#### Optional: GraphQL / OAuth (Phase 5)

| Variable | Description |
|----------|-------------|
| `ATLASSIAN_OAUTH_ACCESS_TOKEN` | OAuth access token for AGG |
| `ATLASSIAN_OAUTH_REFRESH_TOKEN` | OAuth refresh token |
| `ATLASSIAN_CLIENT_ID` | OAuth client ID |
| `ATLASSIAN_CLIENT_SECRET` | OAuth client secret |
| `ATLASSIAN_GQL_BASE_URL` | Tenant GraphQL endpoint (e.g. `https://your-org.atlassian.net/gateway/api/graphql`) |
| `ATLASSIAN_GQL_EXPERIMENTAL_APIS` | Comma-separated experimental APIs (e.g. `jira-software`) |
| `ATLASSIAN_COOKIES_JSON` | Cookie auth for AGG (JSON object) |

### Migration Approach

The migration uses a parallel-run strategy:
1. Both legacy and new client run side by side
2. Feature flag toggles which implementation is active
3. Rollback is instant by setting `ATLASSIAN_CLIENT_ENABLED=false`
4. No database schema changes are required for Phases 1-4

### Quick Jira API smoke test (curl)

Jira Cloud has removed `GET /rest/api/3/search`; use `GET /rest/api/3/search/jql`:

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  --get "https://$JIRA_BASE_URL/rest/api/3/search/jql" \
  --data-urlencode "jql=(updated >= '2025-09-10' OR (statusCategory != Done AND created <= '2025-12-18')) ORDER BY updated DESC" \
  --data-urlencode "maxResults=5" \
  --data-urlencode "fields=key,summary,updated,status"
```
