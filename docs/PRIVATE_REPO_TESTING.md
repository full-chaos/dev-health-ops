# Testing Private Repository Access

This document explains how to verify that the GitHub and GitLab connectors work correctly with private repositories and projects.

## Overview

The connectors fully support private repositories when provided with tokens that have the appropriate permissions:

- **GitHub**: Requires `repo` scope for private repository access
- **GitLab**: Requires `read_api` and `read_repository` scopes for private project access

## Quick Test

Run the example script to verify private repository access:

```bash
# Set up GitHub credentials
export GITHUB_TOKEN=ghp_your_token_with_repo_scope
export GITHUB_PRIVATE_REPO=your-username/your-private-repo

# Set up GitLab credentials
export GITLAB_TOKEN=glpat_your_token
export GITLAB_PRIVATE_PROJECT=your-group/your-private-project

# Run the test
python examples/private_repo_example.py
```

## Detailed Testing

### GitHub Private Repository Testing

#### 1. Create a GitHub Token

1. Go to <https://github.com/settings/tokens>
2. Click "Generate new token" → "Generate new token (classic)"
3. Give it a descriptive name: "MergeStat Private Repo Test"
4. Select scopes:
   - ✅ **repo** (Full control of private repositories)
   - ✅ **read:org** (optional, for organization repos)
5. Click "Generate token" and copy it immediately

#### 2. Set Environment Variables

```bash
# Your GitHub token (with 'repo' scope)
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Your private repository (format: owner/repo)
export GITHUB_PRIVATE_REPO=myusername/my-private-repo
```

#### 3. Run Tests

**Option A: Run the example script**

```bash
python examples/private_repo_example.py
```

**Option B: Run integration tests**

```bash
pytest tests/test_private_repo_access.py::TestGitHubPrivateRepoAccess -v
```

**Option C: Test with actual data sync**

```bash
dev-hops sync git --provider github \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITHUB_TOKEN" \
  --owner myusername \
  --repo my-private-repo
```

#### 4. Verify Success

The test should:

- ✅ Successfully list the private repository
- ✅ Fetch repository statistics
- ✅ Get contributors list
- ✅ Access pull requests
- ✅ Check rate limit status

### GitLab Private Project Testing

#### 1. Create a GitLab Token

1. Go to your GitLab instance Settings → Access Tokens
2. Create a new token:
   - Name: "MergeStat Private Project Test"
   - Scopes:
     - ✅ **read_api** (Read API access)
     - ✅ **read_repository** (Read repository content)
3. Click "Create personal access token" and copy it

#### 2. Set Environment Variables

```bash
# Your GitLab token (with required scopes)
export GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx

# Your private project (format: group/project or numeric ID)
export GITLAB_PRIVATE_PROJECT=mygroup/my-private-project
# OR
export GITLAB_PRIVATE_PROJECT=12345

# Optional: Custom GitLab instance URL
export GITLAB_URL=https://gitlab.example.com
```

#### 3. Run Tests

**Option A: Run the example script**

```bash
python examples/private_repo_example.py
```

**Option B: Run integration tests**

```bash
pytest tests/test_private_repo_access.py::TestGitLabPrivateProjectAccess -v
```

**Option C: Test with actual data sync**

```bash
dev-hops sync git --provider gitlab \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --auth "$GITLAB_TOKEN" \
  --project-id 12345
```

#### 4. Verify Success

The test should:

- ✅ Successfully access the private project
- ✅ Fetch project statistics
- ✅ Get contributors list
- ✅ Access merge requests
- ✅ List accessible projects (including private)

## Troubleshooting

### GitHub Issues

**Problem**: `404 Not Found` error

- **Cause**: Token doesn't have access to the repository or lacks `repo` scope
- **Solution**:
  1. Verify the repository exists and you have access
  2. Check token has `repo` scope at <https://github.com/settings/tokens>
  3. Generate a new token with correct scopes if needed

**Problem**: `401 Unauthorized` error

- **Cause**: Invalid or expired token
- **Solution**: Generate a new token

**Problem**: Rate limit exceeded

- **Cause**: Too many API requests
- **Solution**: Wait for rate limit to reset or use a different token

### GitLab Issues

**Problem**: `404 Not Found` or `401 Unauthorized` error

- **Cause**: Token doesn't have access to the project or lacks required scopes
- **Solution**:
  1. Verify the project exists and you have access
  2. Check token has `read_api` and `read_repository` scopes
  3. Generate a new token with correct scopes if needed

**Problem**: Project ID vs Project Path

- **Cause**: Using project path when numeric ID is expected or vice versa
- **Solution**:
- For `cli.py sync git --provider gitlab`: Use numeric project ID
  - For connector methods: Can use either project name or ID

**Problem**: Self-hosted GitLab connection issues

- **Cause**: Incorrect URL or network issues
- **Solution**: Verify `GITLAB_URL` is correct and accessible

## Verifying Token Scopes

### GitHub Token Verification

```python
from connectors import GitHubConnector

token = "ghp_your_token"
connector = GitHubConnector(token=token)

# If this works, token is valid
rate_limit = connector.get_rate_limit()
print(f"Token is valid. Rate limit: {rate_limit['remaining']}/{rate_limit['limit']}")

# If you can list private repos, token has 'repo' scope
repos = connector.list_repositories(max_repos=5)
print(f"Can access {len(repos)} repositories")

connector.close()
```

### GitLab Token Verification

```python
from connectors import GitLabConnector

token = "glpat_your_token"
connector = GitLabConnector(url="https://gitlab.com", private_token=token)

# If this works, token is valid
try:
    projects = connector.list_projects(max_projects=5)
    print(f"Token is valid. Can access {len(projects)} projects")
except Exception as e:
    print(f"Token validation failed: {e}")

connector.close()
```

## Best Practices

1. **Never commit tokens to version control**
   - Use environment variables
   - Add tokens to `.gitignore`
   - Use secret management tools in production

2. **Use minimal required scopes**
   - GitHub: `repo` for private repos, no scope needed for public
   - GitLab: `read_api` + `read_repository` for private projects

3. **Rotate tokens regularly**
   - Generate new tokens periodically
   - Revoke old tokens when no longer needed

4. **Test with both public and private repositories**
   - Ensure your setup works for both cases
   - Verify error handling for insufficient permissions

5. **Monitor rate limits**
   - Check rate limit status regularly
   - Implement backoff strategies for production use

## Integration Tests

Run the full test suite:

```bash
# Run all private repo tests
pytest tests/test_private_repo_access.py -v

# Run specific test classes
pytest tests/test_private_repo_access.py::TestGitHubPrivateRepoAccess -v
pytest tests/test_private_repo_access.py::TestGitLabPrivateProjectAccess -v

# Skip integration tests in CI/CD
export SKIP_INTEGRATION_TESTS=1
pytest tests/test_private_repo_access.py -v
```

## CI/CD Considerations

When running tests in CI/CD pipelines:

1. **Store tokens as secrets**
   - GitHub Actions: Use repository secrets
   - GitLab CI: Use CI/CD variables (masked)
   - Other CI: Use secure secret storage

2. **Skip integration tests by default**

   ```yaml
   # GitHub Actions example
   - name: Run tests
     env:
       SKIP_INTEGRATION_TESTS: 1
     run: pytest
   ```

3. **Optional: Run integration tests with secrets**

   ```yaml
   # Only run on main branch or with specific label
   - name: Run integration tests
     if: github.ref == 'refs/heads/main'
     env:
       GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
       GITHUB_PRIVATE_REPO: ${{ secrets.TEST_PRIVATE_REPO }}
     run: pytest tests/test_private_repo_access.py -v
   ```

## Support

If you encounter issues:

1. Check this documentation for troubleshooting steps
2. Verify your token scopes and permissions
3. Review the connector documentation in `connectors/README.md`
4. Check example scripts in `examples/` directory
5. Review test files for usage patterns
