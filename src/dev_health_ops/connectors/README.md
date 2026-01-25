# GitHub and GitLab Connectors

Production-grade connectors for retrieving data from GitHub and GitLab APIs with automatic pagination, rate limiting, and error handling.

## Architecture

All connectors inherit from the `GitConnector` abstract base class, which defines a common interface:

```python
from connectors import GitConnector, GitHubConnector, GitLabConnector, BatchResult

# All connectors implement these methods:
# - list_organizations()
# - list_repositories()
# - get_contributors()
# - get_commit_stats()
# - get_repo_stats()
# - get_pull_requests()
# - get_file_blame()
# - get_repos_with_stats()
# - get_repos_with_stats_async()
# - close()

# Use as context manager
with GitHubConnector(token="your_token") as connector:
    repos = connector.list_repositories(max_repos=10)
```

## Features

- **Base Connector Class**: Common interface for all connectors (`GitConnector`)
- **GitHub Connector**: Uses PyGithub and GraphQL API
  - Fetch repositories for organizations and individual users
  - **Full support for private repositories** with proper token scopes
  - Search and filter repositories by keywords
  - Support for fetching ALL repositories or limiting results
- **GitLab Connector**: Uses python-gitlab and REST API
  - Use project names instead of IDs for easier access
  - **Full support for private projects** with proper token permissions
  - Fetch projects for groups using group names or IDs
  - Search and filter projects by keywords
  - Support for fetching ALL projects or limiting results
- **Batch Processing**: Process multiple repositories with pattern matching
- **Automatic Pagination**: Handles paginated responses seamlessly
- **Rate Limiting**: Exponential backoff for API rate limits
- **Retry Logic**: Automatic retries with configurable backoff
- **Type-Safe Models**: Dataclasses for all data structures
- **Error Handling**: Clear exception types for different errors
- **Context Manager Support**: Use `with` statement for automatic cleanup
- **Backward Compatible**: All legacy parameters still supported
- **Date filtering**: CLI local mode supports `--since`/`--start-date` for commit/stats/blame. Connector methods continue to honor `max_commits` but otherwise operate on the latest history returned by the providers.
- **Planning docs**: Implementation plans in `docs/project.md`, metrics inventory in `docs/metrics-inventory.md`, requirements/roadmap in `docs/roadmap.md`.

## Installation

The connectors are included with this project. Dependencies are listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

Key dependencies:

- `PyGithub` - GitHub API client
- `python-gitlab` - GitLab API client
- `requests` - HTTP library

## Quick Start

### GitHub Connector

```python
from connectors import GitHubConnector

# Initialize with GitHub token
connector = GitHubConnector(token="your_github_token")

# List organizations
orgs = connector.list_organizations(max_orgs=10)

# List repositories for authenticated user
repos = connector.list_repositories(max_repos=20)

# List repositories for a specific user
repos = connector.list_repositories(user_name="octocat", max_repos=20)

# List repositories for an organization
repos = connector.list_repositories(org_name="github", max_repos=20)

# Search for repositories (global search)
repos = connector.list_repositories(search="python machine learning")

# Search within an organization
repos = connector.list_repositories(org_name="github", search="api")

# Get ALL repositories (no limit)
all_repos = connector.list_repositories()

# Get contributors
contributors = connector.get_contributors("owner", "repo")

# Get repository statistics
stats = connector.get_repo_stats("owner", "repo", max_commits=100)

# Get pull requests
prs = connector.get_pull_requests("owner", "repo", state="open")

# Get file blame using GraphQL
blame = connector.get_file_blame("owner", "repo", "path/to/file.py")

# Check rate limit
rate_limit = connector.get_rate_limit()

# Close connector
connector.close()
```

### GitLab Connector

```python
from connectors import GitLabConnector

# Initialize with GitLab token
connector = GitLabConnector(
    url="https://gitlab.com",  # Or your GitLab instance URL
    private_token="your_gitlab_token"
)

# List groups
groups = connector.list_groups(max_groups=10)

# List all accessible projects
projects = connector.list_projects(max_projects=20)

# List projects for a group by name
projects = connector.list_projects(group_name="gitlab-org", max_projects=20)

# List projects for a group by ID (legacy support)
projects = connector.list_projects(group_id=123, max_projects=20)

# Search for projects
projects = connector.list_projects(search="python")

# Search within a group
projects = connector.list_projects(group_name="gitlab-org", search="api")

# Get ALL projects (no limit)
all_projects = connector.list_projects()

# Get contributors using project name
contributors = connector.get_contributors(project_name="group/project")

# Get contributors using project ID (legacy support)
contributors = connector.get_contributors(project_id=123)

# Get project statistics using project name
stats = connector.get_repo_stats(project_name="group/project", max_commits=100)

# Get merge requests using project name
mrs = connector.get_merge_requests(project_name="group/project", state="opened")

# Get file blame using project name
blame = connector.get_file_blame(project_name="group/project", file_path="path/to/file.py")

# Close connector
connector.close()
```

## Private Repository Access

Both connectors fully support private repositories/projects when provided with tokens that have the appropriate permissions.

### GitHub Private Repositories

```python
from connectors import GitHubConnector

# Initialize with token that has 'repo' scope
connector = GitHubConnector(token="ghp_XXXXXXXXXXXXXXXXXXXX")

# List authenticated user's repositories (includes private repos)
my_repos = connector.list_repositories(max_repos=50)

# Access a specific private repository
owner = "your-username"
repo_name = "your-private-repo"

# Get repository statistics
stats = connector.get_repo_stats(owner, repo_name, max_commits=100)
print(f"Private repo has {stats.total_commits} commits")

# Get contributors
contributors = connector.get_contributors(owner, repo_name)
print(f"Private repo has {len(contributors)} contributors")

# Get pull requests
prs = connector.get_pull_requests(owner, repo_name, state="open")
print(f"Private repo has {len(prs)} open PRs")

connector.close()
```

**Important**: Ensure your GitHub token has the `repo` scope. Tokens without this scope will not be able to access private repositories.

### GitLab Private Projects

```python
from connectors import GitLabConnector

# Initialize with token that has appropriate permissions
connector = GitLabConnector(
    url="https://gitlab.com",
    private_token="glpat-XXXXXXXXXXXXXXXXXXXX"
)

# List all accessible projects (includes private projects)
my_projects = connector.list_projects(max_projects=50)

# Access a specific private project by name
project_name = "your-group/your-private-project"

# Get project statistics
stats = connector.get_repo_stats(project_name=project_name, max_commits=100)
print(f"Private project has {stats.total_commits} commits")

# Get contributors
contributors = connector.get_contributors(project_name=project_name)
print(f"Private project has {len(contributors)} contributors")

# Get merge requests
mrs = connector.get_merge_requests(project_name=project_name, state="opened")
print(f"Private project has {len(mrs)} open MRs")

connector.close()
```

**Important**: Ensure your GitLab token has `read_api` and `read_repository` scopes. Tokens without these scopes will not be able to access private projects.

### Verifying Private Access

You can verify your token has access to private repositories by running the integration tests:

```bash
# For GitHub private repositories
export GITHUB_TOKEN=your_token
export GITHUB_PRIVATE_REPO=owner/repo  # Replace with your private repo
pytest tests/test_private_repo_access.py::TestGitHubPrivateRepoAccess -v

# For GitLab private projects
export GITLAB_TOKEN=your_token
export GITLAB_PRIVATE_PROJECT=group/project  # Replace with your private project
pytest tests/test_private_repo_access.py::TestGitLabPrivateProjectAccess -v
```

## Data Models

All connectors use the same data models:

- **Organization**: GitHub organization or GitLab group
- **Repository**: GitHub repository or GitLab project
- **Author**: Contributor or author
- **CommitStats**: Statistics for a single commit
- **RepoStats**: Aggregated statistics for a repository
- **PullRequest**: GitHub PR or GitLab MR
- **BlameRange**: Range of lines with blame information
- **FileBlame**: Blame information for a file

## Configuration

### GitHub Connector

```python
connector = GitHubConnector(
    token="your_token",              # Required: GitHub personal access token
    base_url=None,                   # Optional: For GitHub Enterprise
    per_page=100,                    # Items per page (max 100)
    max_workers=4,                   # Concurrent workers
)
```

#### GitHub Token Scopes

To access repositories, your GitHub personal access token needs appropriate scopes:

- **Public repositories only**: No special scopes required (but token is still recommended for higher rate limits)
- **Private repositories**: Requires `repo` scope for full access to private repositories
- **Organization repositories**: May require `read:org` scope to list organization repositories

**To create a GitHub token with private repository access:**

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Select scopes:
   - ✅ `repo` - Full control of private repositories (includes all repo sub-scopes)
   - ✅ `read:org` - Read org and team membership (if accessing organization repos)
4. Generate and copy the token

**Security Note**: Keep your token secure and never commit it to version control. Use environment variables to store tokens.

### GitLab Connector

```python
connector = GitLabConnector(
    url="https://gitlab.com",        # GitLab instance URL
    private_token="your_token",      # GitLab private token
    per_page=100,                    # Items per page (max 100)
    max_workers=4,                   # Concurrent workers
)
```

#### GitLab Token Permissions

To access projects, your GitLab private token needs appropriate scopes:

- **Public projects only**: No token required, but recommended for higher rate limits
- **Private projects**: Requires a token with the following scopes:
  - `read_api` - Read access to the API
  - `read_repository` - Read access to repository data
- **Write operations**: Would require `write_repository` or `api` scope (not used by this connector)

**To create a GitLab token with private project access:**

1. Go to GitLab Settings → Access Tokens
2. Create a new token with:
   - Name: "MergeStat Sync" (or any descriptive name)
   - Scopes:
     - ✅ `read_api` - Read-only API access
     - ✅ `read_repository` - Read repository content
3. Copy the generated token

**Security Note**: GitLab tokens are sensitive. Store them securely in environment variables and never commit them to your repository.

## Error Handling

The connectors use custom exceptions for clear error handling:

```python
from connectors.exceptions import (
    ConnectorException,        # Base exception
    RateLimitException,        # Rate limit exceeded
    AuthenticationException,   # Authentication failed
    NotFoundException,         # Resource not found
    APIException,             # Generic API error
)

try:
    repos = connector.list_repositories()
except RateLimitException as e:
    print(f"Rate limit exceeded: {e}")
except AuthenticationException as e:
    print(f"Authentication failed: {e}")
except APIException as e:
    print(f"API error: {e}")
```

## Pagination

Pagination is handled automatically by the connectors:

```python
# Get all repositories (paginated automatically)
repos = connector.list_repositories()

# Limit results
repos = connector.list_repositories(max_repos=50)
```

For custom pagination control, use the utility classes:

```python
from connectors.utils import PaginationHandler

handler = PaginationHandler(per_page=100, max_pages=5, max_items=400)
results = handler.paginate_all(fetch_function)
```

## Rate Limiting

Rate limiting is handled automatically with exponential backoff. When APIs return server-provided delays (e.g., `Retry-After` or reset headers), retries will honor those values.

```python
from connectors.utils import retry_with_backoff

@retry_with_backoff(max_retries=5, initial_delay=1.0, max_delay=60.0)
def api_call():
    # Your API call here
    pass
```

## GraphQL (GitHub)

The GitHub connector uses GraphQL for operations not well-supported by PyGithub:

```python
from connectors.utils import GitHubGraphQLClient

client = GitHubGraphQLClient(token="your_token")
result = client.query("""
    query {
        viewer {
            login
        }
    }
""")
```

## REST API (GitLab)

The GitLab connector uses REST API for blame and merge requests:

```python
from connectors.utils import GitLabRESTClient

client = GitLabRESTClient(
    base_url="https://gitlab.com/api/v4",
    private_token="your_token"
)
blame = client.get_file_blame(project_id=123, file_path="file.py")
```

## Examples

See the `examples/` directory for complete usage examples:

- `examples/github_example.py` - GitHub connector examples
- `examples/gitlab_example.py` - GitLab connector examples

Run examples:

```bash
export GITHUB_TOKEN=your_token
python examples/github_example.py

export GITLAB_TOKEN=your_token
python examples/gitlab_example.py
```

## Testing

Run tests:

```bash
# Run all connector tests
pytest tests/test_connectors_*.py -v

# Run specific test file
pytest tests/test_connectors_models.py -v

# Run with coverage
pytest tests/test_connectors_*.py --cov=connectors --cov-report=html
```

### Integration Tests

Integration tests make real API calls to GitHub and GitLab to fetch public repositories:

```bash
# Run integration tests (requires GITHUB_TOKEN for GitHub tests)
export GITHUB_TOKEN=your_github_token
export GITLAB_TOKEN=your_gitlab_token  # Optional, GitLab tests work without token
pytest tests/test_connectors_integration.py -v

# Skip integration tests in CI/CD
export SKIP_INTEGRATION_TESTS=1
pytest tests/test_connectors_integration.py -v
```

Integration tests verify:

- Fetching first 10 public repositories from GitHub organizations and users
- Searching repositories on GitHub
- Fetching first 10 public projects from GitLab
- Fetching projects from GitLab groups
- Searching projects on GitLab

## Integration with Existing Storage

The connectors can be integrated with the existing storage system:

```python
from connectors import GitHubConnector
from storage import SQLAlchemyStore
from models.git import Repo, GitCommit

# Initialize connector and storage
connector = GitHubConnector(token="your_token")
async with SQLAlchemyStore(conn_string="postgresql+asyncpg://...") as store:
    # Get repositories
    repos = connector.list_repositories(max_repos=10)
    
    # Store in database
    for repo in repos:
        db_repo = Repo(
            repo_path=None,
            id=repo.id,
            repo=repo.full_name,
            settings={},
            tags=[]
        )
        await store.insert_repo(db_repo)
```

## Performance Considerations

- **Pagination**: Default page size is 100 (maximum for both GitHub and GitLab)
- **Rate Limits**:
  - GitHub: 5000 requests/hour (authenticated)
  - GitLab: 10 requests/second (self-hosted may vary)
- **Concurrency**: Use `max_workers` parameter for parallel operations
- **Caching**: Consider caching results for frequently accessed data

## Best Practices

1. **Always use authentication tokens** for higher rate limits
2. **Handle rate limit exceptions** gracefully with retries
3. **Limit results** when testing to avoid unnecessary API calls
4. **Close connectors** when done to clean up resources
5. **Use context managers** where applicable
6. **Monitor rate limits** to avoid hitting limits

## License

This code is part of the dev-health-ops project.
