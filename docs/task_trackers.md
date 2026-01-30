# Task Trackers & Work Items

This repo normalizes Jira issues, GitHub issues/Projects items, GitLab issues, and Linear issues into a unified `WorkItem` model (`models/work_items.py`) and computes daily aggregates + cycle times.

Jira is used to track associated project work (planning/throughput/WIP). Pull request metrics are computed from PR/MR data synced via the CLI (`python cli.py sync ...`) and are independent of Jira.

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
  - Optional CLI override: `python cli.py sync work-items --provider github --auth "$GITHUB_TOKEN" ...`

Optional Projects v2 ingestion:
- `GITHUB_PROJECTS_V2` as comma-separated `org_login:project_number` entries, e.g.:
  - `GITHUB_PROJECTS_V2="myorg:3,anotherorg:12"`

### GitLab
- `GITLAB_TOKEN`
- `GITLAB_URL` (optional, default: `https://gitlab.com`)
  - Optional CLI override: `python cli.py sync work-items --provider gitlab --auth "$GITLAB_TOKEN" ...`

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
python cli.py sync work-items --provider linear --db "$DATABASE_URI"

# Sync specific team by key (e.g., ENG, PROD)
python cli.py sync work-items --provider linear --repo ENG --db "$DATABASE_URI"
```

## Status & Type Normalization

Status normalization is config-driven via `config/status_mapping.yaml`.

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

To keep user metrics consistent across providers, populate `config/identity_mapping.yaml`.

Schema:
- `canonical`: stable identity (prefer email)
- `aliases`: provider-qualified logins (e.g. `github:octocat`) or Jira account IDs (e.g. `jira:accountid:abcd123`)

## Team Mapping (optional)

To enable team filtering in Grafana, you can sync teams from various sources.

### Config-based Mapping
Populate `config/team_mapping.yaml` (schema: `team_id`, `team_name`, `members`).
Then run:
```bash
python cli.py sync teams --path config/team_mapping.yaml
```

### Jira Project Mapping
Automatically import Jira projects as teams:
```bash
python cli.py sync teams --provider jira
```

## Running Jira work metrics

Jira work items are fetched via the work item sync job:

```bash
python cli.py sync work-items --provider jira --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
```

Use `-s`/`--search` to filter repos by name (glob pattern), e.g.:

```bash
python cli.py sync work-items --provider github -s "org/*" --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
```

### Quick Jira API smoke test (curl)

Jira Cloud has removed `GET /rest/api/3/search`; use `GET /rest/api/3/search/jql`:

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  --get "https://$JIRA_BASE_URL/rest/api/3/search/jql" \
  --data-urlencode "jql=(updated >= '2025-09-10' OR (statusCategory != Done AND created <= '2025-12-18')) ORDER BY updated DESC" \
  --data-urlencode "maxResults=5" \
  --data-urlencode "fields=key,summary,updated,status"
```
