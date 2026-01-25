# GitHub & GitLab Connectors

## Overview

The CLI exposes target-specific sync jobs with provider selection:

**Sync targets:** `git`, `prs`, `blame`, `cicd`, `deployments`, `incidents`, `work-items`

**Providers:** `local`, `github`, `gitlab`, `synthetic`

---

## Authentication

### GitHub

| Method | Variable | Notes |
|--------|----------|-------|
| Token | `GITHUB_TOKEN` | Personal access token |
| CLI override | `--auth "$GITHUB_TOKEN"` | Takes precedence |

**Required scopes:**
- `repo` — Full control (required for private repos)
- `read:org` — Read org membership (recommended for org repos)

### GitLab

| Method | Variable | Notes |
|--------|----------|-------|
| Token | `GITLAB_TOKEN` | Private token |
| CLI override | `--auth "$GITLAB_TOKEN"` | Takes precedence |
| Self-hosted | `--gitlab-url` | Default: `https://gitlab.com` |

**Required scopes:**
- `read_api` — Read access to API (required for private projects)
- `read_repository` — Read repository data (required for private projects)

---

## Local Repository Mode

Analyze a local git repository.

```bash
# Environment variables
export DATABASE_URI="postgresql+asyncpg://localhost:5432/mergestat"
export REPO_PATH="/path/to/repo"
python cli.py sync git --provider local

# Command-line arguments
python cli.py sync git --provider local \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --repo-path "/path/to/repo"

# With date filtering
python cli.py sync git --provider local \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --repo-path "/path/to/repo" \
  --since 2024-01-01
```

---

## GitHub Connector Mode

Fetch data directly from GitHub without cloning. Supports both public and private repos.

### Single Repository

```bash
# Public repository
python cli.py sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# Private repository (token must have 'repo' scope)
python cli.py sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner your-org \
  --repo your-private-repo
```

### Batch Processing

```bash
python cli.py sync git --provider github \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --auth "$GITHUB_TOKEN" \
  -s "myorg/api-*" \
  --group myorg \
  --batch-size 10 \
  --max-concurrent 4 \
  --max-repos 50 \
  --use-async
```

### What Gets Stored

- Repository metadata (name, URL, default branch)
- Commits (up to 100 most recent)
- Commit statistics (additions, deletions per file for last 50 commits)

---

## GitLab Connector Mode

Fetch data from GitLab (including self-hosted). Supports public and private projects.

### Single Project

```bash
# GitLab.com
python cli.py sync git --provider gitlab \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964

# Self-hosted GitLab
python cli.py sync git --provider gitlab \
  --db "postgresql://..." \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.example.com" \
  --project-id 123
```

### Batch Processing

```bash
python cli.py sync git --provider gitlab \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --group mygroup \
  -s "mygroup/service-*" \
  --batch-size 10 \
  --max-concurrent 4 \
  --max-repos 50 \
  --use-async
```

### Finding Project ID

1. Go to project page on GitLab
2. Look under project name: "Project ID: 12345"
3. Or via API: `curl "https://gitlab.com/api/v4/projects/owner%2Fproject" --header "PRIVATE-TOKEN: <token>"`

---

## Command-Line Reference

### Common Arguments

| Argument | Description |
|----------|-------------|
| `--db DB` | Database connection string (required) |
| `--auth TOKEN` | Authentication token |
| `--since DATE` | Filter commits after date |
| `--date DATE` | Specific date filter |
| `--backfill N` | Days to backfill |

### GitHub-Specific

| Argument | Description |
|----------|-------------|
| `--owner OWNER` | Repository owner |
| `--repo REPO` | Repository name |

### GitLab-Specific

| Argument | Description |
|----------|-------------|
| `--project-id ID` | Numeric project ID |
| `--gitlab-url URL` | GitLab instance URL |

### Batch Processing

| Argument | Description |
|----------|-------------|
| `-s, --search PATTERN` | Glob pattern for repos |
| `--group NAME` | Organization/group name |
| `--batch-size N` | Records per batch |
| `--max-concurrent N` | Concurrent workers |
| `--max-repos N` | Maximum repos to process |
| `--use-async` | Enable async workers |
| `--rate-limit-delay SECONDS` | Delay between requests |

---

## Rate Limits

| Provider | Limit | Handling |
|----------|-------|----------|
| GitHub | 5,000 requests/hour | Automatic retry with backoff |
| GitLab | 10 requests/second | Automatic retry with backoff |

Both connectors:
- Include automatic retry with exponential backoff
- Coordinate concurrent workers via shared backoff gate
- Honor server-provided `Retry-After` / reset delays

---

## Limitations

### GitHub

- Commits: Up to 100 most recent
- Commit stats: Last 50 commits (API rate limits)
- No blame data (requires per-file API calls)
- No file contents

### GitLab

- Commits: Up to 100 most recent
- Commit stats: Last 50 commits (aggregate per commit, not per-file)
- No blame data
- No file contents

### Performance Comparison

| Mode | Best For | Trade-offs |
|------|----------|------------|
| Local | Complete analysis (files, blame, full history) | Requires clone |
| GitHub/GitLab | Quick commits and stats | Limited depth, rate limits |

---

## Troubleshooting

### "Connectors are not available"

Install dependencies:
```bash
pip install PyGithub python-gitlab
# or
pip install -r requirements.txt
```

### GitHub 404 Errors on Private Repos

Token must have `repo` scope. Verify at:
https://github.com/settings/tokens

### GitLab Permission Errors

Token must have `read_api` and `read_repository` scopes.
Check at: Settings → Access Tokens

---

## Environment Variables Summary

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URI` | Database connection string | Required |
| `SECONDARY_DATABASE_URI` | Secondary sink | None |
| `DB_ECHO` | Enable SQL logging | `false` |
| `BATCH_SIZE` | Records per batch | `100` |
| `MAX_WORKERS` | Parallel workers | `4` |
| `GITHUB_TOKEN` | GitHub PAT | None |
| `GITLAB_TOKEN` | GitLab token | None |
| `REPO_PATH` | Local repo path | `.` |
