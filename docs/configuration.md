# Configuration

Use this page as the quick index for environment and deployment configuration.

## Environment variables

- Start with the template in `.env.example` at the repository root.
- For dual-database setup details, see [Database Architecture](./architecture/database-architecture.md).

## Email

Transactional emails (account operations and billing notifications) are sent via a pluggable email provider.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `EMAIL_PROVIDER` | No | `console` | Email backend: `resend` (production) or `console` (dev/test) |
| `EMAIL_API_KEY` | When `resend` | — | Resend API key |
| `EMAIL_FROM_ADDRESS` | No | `dev-health@example.com` | Sender address for all outgoing emails |

See [Email Setup](./email-setup.md) for provider configuration, email types, and troubleshooting.

## Credential Management

Provider credentials (GitHub, GitLab, Jira, Linear, Atlassian) are resolved through a unified credential system that supports two sources:

| Source | When Used | How |
|--------|-----------|-----|
| **Database** (Enterprise) | Multi-tenant deployments | Encrypted credentials stored per-org via the Admin API |
| **Environment** (Dev/OSS) | Single-tenant or local dev | Standard env vars (`GITHUB_TOKEN`, `JIRA_API_TOKEN`, etc.) |

### Resolution Order

1. **Database lookup** -- queries the `integration_credentials` table for the org + provider
2. **Environment fallback** -- reads from provider-specific env vars (enabled by default)

### Provider Env Vars

| Provider | Required Variables |
|----------|--------------------|
| GitHub | `GITHUB_TOKEN` (or `app_id` + `private_key` via API) |
| GitLab | `GITLAB_TOKEN` |
| Jira | `JIRA_API_TOKEN`, `JIRA_EMAIL`, `JIRA_BASE_URL` |
| Linear | `LINEAR_API_KEY` |
| Atlassian | `ATLASSIAN_API_TOKEN`, `ATLASSIAN_EMAIL` (optional: `ATLASSIAN_CLOUD_ID`) |

### Usage

```python
# Async context (API, async workers)
from dev_health_ops.credentials import CredentialResolver
async with get_async_session() as session:
    resolver = CredentialResolver(session, org_id="my-org")
    creds = await resolver.resolve("github")

# Sync context (Celery workers, CLI)
from dev_health_ops.credentials import resolve_credentials_sync
creds = resolve_credentials_sync("github", org_id="my-org", db_url=db_url)
```

For Enterprise deployments, credentials are managed through the Admin API and stored encrypted in PostgreSQL. Environment variable fallback can be disabled by setting `allow_env_fallback=False`.

## Operational reference

- CLI environment variable details: [CLI Reference](./ops/cli-reference.md#environment-variables)
- End-to-end bootstrap flow: [Self-Hosted Quickstart](./self-hosted-quickstart.md)
- Deployment-specific guidance: [Deployment Guide](./ops/deployment-guide.md)
