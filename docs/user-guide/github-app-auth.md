# GitHub App authentication

Dev Health can authenticate GitHub syncs with either a personal access token (PAT) or a GitHub App installation token. GitHub App auth is useful for organization-wide installs, fine-grained permissions, and higher installation rate limits.

## Required GitHub App permissions

Grant only the permissions needed for the sync target you run:

- Repository contents: read
- Metadata: read
- Pull requests: read (for `sync prs`)
- Issues: read (for GitHub work item sync)
- Actions / deployments / security events: read when using the corresponding sync targets

Install the app on the organization or repositories you want Dev Health to ingest.

## Environment variables

```bash
export GITHUB_APP_ID="123456"
export GITHUB_APP_PRIVATE_KEY_PATH="/secure/path/dev-health-github-app.pem"
export GITHUB_APP_INSTALLATION_ID="987654"
```

Then run a GitHub sync without `--auth`:

```bash
dev-hops sync git --provider github --owner my-org --repo my-repo
```

## CLI flags

CLI flags take precedence over environment variables and stored database credentials:

```bash
dev-hops sync git --provider github \
  --github-app-id "123456" \
  --github-app-key-path "/secure/path/dev-health-github-app.pem" \
  --github-app-installation-id "987654" \
  --owner my-org \
  --repo my-repo
```

## Authentication precedence and compatibility

GitHub sync resolves credentials in this order:

1. CLI flags (`--auth` or the GitHub App flags)
2. Environment variables (`GITHUB_TOKEN` or the GitHub App env vars)
3. Stored organization credentials, when `--org` and `--db`/`POSTGRES_URI` are available

Exactly one auth mode is allowed for the selected source: PAT XOR GitHub App. Existing PAT usage is unchanged:

```bash
export GITHUB_TOKEN="ghp_..."
dev-hops sync git --provider github --owner my-org --repo my-repo

dev-hops sync git --provider github --auth "$GITHUB_TOKEN" --owner my-org --repo my-repo
```

Private keys, app JWTs, and installation tokens are never logged by Dev Health.
