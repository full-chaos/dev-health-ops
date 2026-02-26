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

## Operational reference

- CLI environment variable details: [CLI Reference](./ops/cli-reference.md#environment-variables)
- End-to-end bootstrap flow: [Self-Hosted Quickstart](./self-hosted-quickstart.md)
- Deployment-specific guidance: [Deployment Guide](./ops/deployment-guide.md)
