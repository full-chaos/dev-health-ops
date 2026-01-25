# Jira Connector

## Overview

The Jira connector fetches work items from Jira Cloud and normalizes them into the unified `WorkItem` model for throughput, WIP, and cycle time analysis.

**Important:** Jira is for planning/throughput/WIP metrics. PR metrics (cycle time, merge frequency) come from Git provider sync.

---

## Authentication

### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `JIRA_BASE_URL` | Jira instance URL | `your-org.atlassian.net` |
| `JIRA_EMAIL` | Account email | `user@example.com` |
| `JIRA_API_TOKEN` | API token | From Atlassian account |

The URL is normalized to `https://` automatically.

### Optional Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `JIRA_PROJECT_KEYS` | Comma-separated project keys | `ABC,XYZ` |
| `JIRA_JQL` | Custom JQL override | `project = ABC AND ...` |
| `JIRA_FETCH_ALL` | Fetch all issues (slow) | `1` |
| `JIRA_FETCH_COMMENTS` | Include comment metadata | `1` (default) |

### Custom Field Mappings

| Variable | Default | Description |
|----------|---------|-------------|
| `JIRA_STORY_POINTS_FIELD` | None | e.g., `customfield_10016` |
| `JIRA_SPRINT_FIELD` | `customfield_10020` | Sprint field ID |
| `JIRA_EPIC_LINK_FIELD` | None | e.g., `customfield_10014` |

---

## Usage

### Sync Work Items

```bash
python cli.py sync work-items --provider jira \
  --date 2025-02-01 \
  --backfill 30 \
  --db "clickhouse://localhost:8123/default"
```

### Quick API Smoke Test

Jira Cloud uses `GET /rest/api/3/search/jql` (not `/search`):

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  --get "https://$JIRA_BASE_URL/rest/api/3/search/jql" \
  --data-urlencode "jql=(updated >= '2025-01-01' OR (statusCategory != Done AND created <= '2025-02-01')) ORDER BY updated DESC" \
  --data-urlencode "maxResults=5" \
  --data-urlencode "fields=key,summary,updated,status"
```

---

## Status Normalization

Jira statuses are normalized to canonical categories via `config/status_mapping.yaml`.

### Canonical Categories

| Category | Meaning |
|----------|---------|
| `backlog` | Not yet scheduled |
| `todo` | Scheduled but not started |
| `in_progress` | Active work |
| `in_review` | Awaiting review |
| `blocked` | Work cannot proceed |
| `done` | Successfully completed |
| `canceled` | Abandoned |

### Configuration

In `config/status_mapping.yaml`:

```yaml
providers:
  jira:
    statuses:
      "To Do": todo
      "In Progress": in_progress
      "In Review": in_review
      "Blocked": blocked
      "Done": done
      "Canceled": canceled
```

---

## Identity Mapping

To keep user metrics consistent across providers, populate `config/identity_mapping.yaml`:

```yaml
identities:
  - canonical: "jane@example.com"
    aliases:
      - "jira:accountid:abcd123"
      - "github:jane-dev"
      - "gitlab:jane.developer"
```

---

## Team Mapping

### Config-Based

Create `config/team_mapping.yaml`:

```yaml
teams:
  - team_id: "platform"
    team_name: "Platform Team"
    members:
      - "jane@example.com"
      - "bob@example.com"
```

Sync:
```bash
python cli.py sync teams --path config/team_mapping.yaml
```

### Jira Project-Based

Auto-import Jira projects as teams:

```bash
python cli.py sync teams --provider jira
```

---

## What Gets Stored

### Work Item Facts (`work_item_cycle_times`)

| Field | Description |
|-------|-------------|
| `provider` | `jira` |
| `work_item_id` | Jira issue key |
| `work_scope_id` | Project key |
| `team_id` | Assigned team |
| `created_at` | Issue creation |
| `started_at` | First in_progress transition |
| `completed_at` | First done/canceled transition |
| `lead_time_hours` | completed_at - created_at |
| `cycle_time_hours` | completed_at - started_at |

### Daily Aggregates (`work_item_metrics_daily`)

| Metric | Description |
|--------|-------------|
| `items_started` | Transitioned to in_progress |
| `items_completed` | Transitioned to done/canceled |
| `wip_count_end_of_day` | Active items at day end |
| `cycle_time_p50_hours` | Median cycle time |
| `story_points_completed` | Sum of story points |

---

## Time-in-State Tracking

When Jira changelog is available, the connector computes time spent in each status.

### Table: `work_item_state_durations_daily`

| Key | Description |
|-----|-------------|
| `day` | Date |
| `provider` | `jira` |
| `work_scope_id` | Project key |
| `team_id` | Team |
| `status` | Normalized status |
| `duration_hours` | Time spent in status |

**Note:** Items without changelog history contribute no rows.

---

## Troubleshooting

### Authentication Errors

1. Verify API token at: https://id.atlassian.com/manage/api-tokens
2. Ensure email matches account
3. Check URL format (no trailing slash)

### No Issues Returned

1. Verify `JIRA_PROJECT_KEYS` are correct
2. Check JQL syntax if using `JIRA_JQL`
3. Try the curl smoke test above

### Missing Fields

Custom fields vary by Jira instance. Use Jira admin or API to find field IDs:

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  "https://$JIRA_BASE_URL/rest/api/3/field" | jq '.[] | {name, id}'
```

### Story Points Not Appearing

1. Find the correct custom field ID for your instance
2. Set `JIRA_STORY_POINTS_FIELD=customfield_XXXXX`
