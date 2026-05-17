# Using `cli.py` with GitHub and GitLab Connectors

The `cli.py` front end exposes target-specific sync jobs with a provider switch:

1. **Sync targets**: `git`, `prs`, `blame`, `cicd`, `deployments`, `incidents`
2. **Providers**: `local`, `github`, `gitlab`, `synthetic`

## Simplified Library Dispatch

The dedicated CLI (`dev-hops sync ...`) now exposes:

- **Unified authentication** with `--auth` (works for both GitHub and GitLab)
- **Auto-detection of database type** from connection string URL scheme
- **Consolidated batch processing arguments** that work with both connectors (`-s/--search`, `--group`, `--batch-size`, `--max-concurrent`)

## Local Repository Mode

This is the original mode that analyzes a local git repository.

```bash
# Using environment variables
export DATABASE_URI="postgresql+asyncpg://localhost:5432/mergestat"
export REPO_PATH="/path/to/repo"
dev-hops sync git --provider local

# Using command-line arguments (auto-detects database type from URL)
dev-hops sync git --provider local --db "postgresql+asyncpg://localhost:5432/mergestat" --repo-path "/path/to/repo"

# Limit to commits and blames since a date (ISO date or datetime)
dev-hops sync git --provider local --db "sqlite+aiosqlite:///mergestat.db" --repo-path "/path/to/repo" --since 2024-01-01

# Explicit connector type (not needed with the CLI mode, but shown for parity)
dev-hops sync git --provider local --db "postgresql://..." --repo-path "/path/to/repo"
```

**How filtering works**: `--since` / `--date` restricts commits and commit stats to changes at or after the timestamp.

## GitHub Connector Mode

Fetch repository data directly from GitHub without cloning. **Fully supports both public and private repositories**.

### Requirements

- GitHub personal access token with appropriate permissions
  - **For public repositories**: Any valid token (for higher rate limits)
  - **For private repositories**: Token must have `repo` scope
- Repository owner and name

### Usage

```bash
# Public repository with unified auth
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
dev-hops sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# Private repository (token must have 'repo' scope)
dev-hops sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner your-org \
  --repo your-private-repo

# Token from environment variable (GITHUB_TOKEN) - no --auth needed
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
dev-hops sync git --provider github \
  --db "postgresql://..." \
  --owner torvalds \
  --repo linux
```

### Batch Processing Multiple Repositories

```bash
# Process repositories matching a pattern
dev-hops sync git --provider github \
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

## GitLab Connector Mode

Fetch project data directly from GitLab (including self-hosted instances). **Fully supports both public and private projects**.

### Requirements

- GitLab private token with appropriate permissions
  - **For public projects**: Token optional (but recommended for rate limits)
  - **For private projects**: Token must have `read_api` and `read_repository` scopes
- Project ID (numeric ID, not path)

### Usage

```bash
# Public project on GitLab.com with unified auth
dev-hops sync git --provider gitlab \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964

# Private project (token must have required scopes)
dev-hops sync git --provider gitlab \
  --db "postgresql://..." \
  --auth "$GITLAB_TOKEN" \
  --project-id 12345

# Self-hosted GitLab
dev-hops sync git --provider gitlab \
  --db "postgresql://..." \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.example.com" \
  --project-id 123

# Token from environment variable (GITLAB_TOKEN)
export GITLAB_TOKEN="glpat-xxxxxxxxxxxx"
dev-hops sync git --provider gitlab \
  --db "mongodb://localhost:27017" \
  --project-id 278964
```

### Batch Processing Multiple Projects

```bash
# Process projects matching a pattern
dev-hops sync git --provider gitlab \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --group mygroup \
  -s "mygroup/api-*" \
  --batch-size 10 \
  --max-concurrent 4 \
  --max-repos 50 \
  --use-async
```

### What Gets Stored

- Project metadata (name, URL, default branch)
- Commits (up to 100 most recent)
- Commit statistics (aggregate additions/deletions for last 50 commits)

### Finding Your Project ID

The project ID is the numeric identifier for your GitLab project:

1. Go to your project page on GitLab
2. Look under the project name - you'll see "Project ID: 12345"
3. Or use the GitLab API: `curl "https://gitlab.com/api/v4/projects/owner%2Fproject" --header "PRIVATE-TOKEN: <token>"`

## Environment Variables

All modes support these environment variables:

| Variable                 | Description                          | Default         |
| ------------------------ | ------------------------------------ | --------------- |
| `DATABASE_URI`           | Database connection string           | None (required) |
| `SECONDARY_DATABASE_URI` | Secondary database for `--sink both` | None            |
| `DB_ECHO`                | Enable SQL logging                   | `false`         |
| `BATCH_SIZE`             | Records per batch insert             | `100`           |
| `MAX_WORKERS`            | Parallel workers                     | `4`             |
| `GITHUB_TOKEN`           | GitHub personal access token         | None            |
| `GITLAB_TOKEN`           | GitLab private token                 | None            |
| `REPO_PATH`              | Path to local repository             | `.`             |

## Command-Line Arguments

The CLI exposes target-specific sync jobs (`sync git`, `sync prs`, `sync blame`, `sync cicd`, `sync deployments`, `sync incidents`) with a `--provider` flag.

### Local provider (`dev-hops sync <target> --provider local`)

- `--db DB` (required): Database connection string (auto-detects the backend from the URL).
- `--repo-path PATH`: Path to the repository (`.` by default).
- `--since / --date / --backfill`: Time filters for commits and stats.
- `--db-type {postgres,mongo,sqlite,clickhouse}`: Optional override when the scheme is ambiguous.

### GitHub provider (`dev-hops sync <target> --provider github`)

Single-repo execution (required: `--owner` and `--repo`):

- `--db DB` (required).
- `--auth TOKEN`: Token overrides `GITHUB_TOKEN`.
- `--owner OWNER` and `--repo REPO`: Target repository.

Batch processing:

- `-s, --search PATTERN` (required for batch).
- `--group NAME`: Organization/user that owns the repositories.
- `--batch-size N`, `--max-concurrent N`, `--rate-limit-delay SECONDS`: Tune pagination and throughput.
- `--max-repos N`: Stop after processing N repositories.
- `--use-async`: Enable async workers.
- Shared options: `--max-commits-per-repo`.

### GitLab provider (`dev-hops sync <target> --provider gitlab`)

- `--db DB` (required).
- `--auth TOKEN`: Token overrides `GITLAB_TOKEN`.
- `--project-id ID`: Required for single-project mode.
- `--gitlab-url URL`: Defaults to `https://gitlab.com`.
- Batch options mirror the GitHub CLI flags (`-s/--search`, `--group`, etc.).
- Shared tuning: `--batch-size`, `--max-concurrent`, `--rate-limit-delay`, `--max-repos`, `--use-async`, `--max-commits-per-repo`.

## Examples

### Analyze Linux Kernel from GitHub

```bash
dev-hops sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux
```

### Analyze GitLab's GitLab from GitLab.com

```bash
dev-hops sync git --provider gitlab \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964
```

### Analyze Local Repository

```bash
dev-hops sync git --provider local \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --repo-path "/home/user/my-project"
```

### Batch Process Multiple Repositories

```bash
# GitHub batch processing
dev-hops sync git --provider github \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --auth "$GITHUB_TOKEN" \
  --group myorg \
  -s "myorg/api-*" \
  --batch-size 5 \
  --max-repos 20 \
  --use-async

# GitLab batch processing
dev-hops sync git --provider gitlab \
  --db "sqlite+aiosqlite:///mergestat.db" \
  --auth "$GITLAB_TOKEN" \
  --group mygroup \
  -s "mygroup/service-*" \
  --batch-size 5 \
  --max-repos 20 \
  --use-async
```

## Limitations

### GitHub Connector Mode

- Fetches up to 100 most recent commits
- Commit stats limited to last 50 commits (API rate limits)
- Does not fetch blame data (requires per-file API calls)
- Does not fetch file contents

### GitLab Connector Mode

- Fetches up to 100 most recent commits
- Commit stats limited to last 50 commits (API rate limits)
- Stores aggregate stats per commit (not per-file breakdowns)
- Does not fetch blame data (requires per-file API calls)
- Does not fetch file contents

### Rate Limits

- **GitHub**: 5,000 requests/hour for authenticated users
- **GitLab**: 10 requests/second (self-hosted may vary)

Both connectors include automatic retry with exponential backoff for rate limit handling.

Batch modes also coordinate concurrent workers using a shared backoff gate to reduce rate-limit stampedes, and honor server-provided `Retry-After`/reset delays when available.

## Troubleshooting

### "Connectors are not available"

The connectors require additional dependencies. Install them:

```bash
pip install PyGithub python-gitlab
```

Or install all requirements:

```bash
pip install -r requirements.txt
```

### GitHub Authentication Errors

Ensure your token has the appropriate scopes:

- **`repo`** - Full control of private repositories (REQUIRED for private repos)
- **`read:org`** - Read org and team membership (recommended for organization repos)

**For private repositories**: The `repo` scope is mandatory. Without it, you'll receive 404 errors when trying to access private repositories.

**To verify your token scopes**:

1. Go to <https://github.com/settings/tokens>
2. Find your token and check which scopes are selected
3. If `repo` is not checked, generate a new token with this scope

### GitLab Authentication Errors

Ensure your token has the appropriate scopes:

- **`read_api`** - Read access to API (REQUIRED for private projects)
- **`read_repository`** - Read repository data (REQUIRED for private projects)

**For private projects**: Both scopes are mandatory. Without them, you'll receive authentication or permission errors.

**To verify your token permissions**:

1. Go to your GitLab instance Settings → Access Tokens
2. Review the token's scopes
3. If needed, create a new token with `read_api` and `read_repository` scopes

### Finding Repository/Project IDs

**GitHub**: Use owner/repo format (e.g., `torvalds/linux`)
**GitLab**: Use the numeric project ID (e.g., `278964`)

## Integration with Existing Workflows

The connector modes integrate seamlessly with the existing storage system:

- All data is stored in the same database schema
- Repository metadata tagged with source (`github` or `gitlab`)
- Can mix local and remote repositories in the same database
- Query data the same way regardless of source

## Performance Considerations

- **Local mode**: Best for comprehensive analysis (files, blame, full history)
- **GitHub/GitLab modes**: Faster for basic commits and stats
- **API rate limits**: GitHub and GitLab have rate limits; local mode has none
- **Network dependency**: Connector modes require internet access

Choose the appropriate mode based on your needs:

- Use **local mode** for complete repository analysis
- Use **connector modes** for quick commit and stats analysis without cloning
