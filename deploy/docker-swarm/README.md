# Docker Swarm Deployment

## Prerequisites

1. Docker Swarm initialized (`docker swarm init`)
2. Docker secrets created (see below)

## Creating Secrets

```bash
echo "ch_password" | docker secret create clickhouse_password -
echo "ghp_your_github_token" | docker secret create github_token -
echo "glpat-your_gitlab_token" | docker secret create gitlab_token -
cat << EOF | docker secret create jira_credentials -
JIRA_BASE_URL=your-org.atlassian.net
JIRA_EMAIL=user@example.com
JIRA_API_TOKEN=your_api_token
EOF
```

## Deploying the Stack

```bash
docker stack deploy -c stack.yml dev-health
```

## Database Migrations (CHAOS-2304)

Schema migrations run as a one-shot `migrate` service
(`deploy.restart_policy.condition: none`). App services run with
`AUTO_RUN_MIGRATIONS=false` and never apply migrations themselves.

Swarm has **no** `depends_on` ordering: on first deploy (and after every image
update) you MUST verify the migrate service completed successfully before
relying on the stack — api/worker will not create the schema for you:

```bash
# Wait for the one-shot task to finish, then check it exited 0
docker service logs dev-health_migrate
docker service ps dev-health_migrate   # DesiredState=Shutdown, no error

# Re-run migrations manually (e.g. after a failed run)
docker service update --force dev-health_migrate
```

Notes:

- Set `MIGRATION_DATABASE_URI` to a dedicated migration-role DSN pointing
  **directly** at Postgres (port 5432) to run Alembic plus the pinned River
  migration. Existing Alembic-only stacks may keep a direct `POSTGRES_URI`;
  without the dedicated DSN the additive River step is skipped. Never send
  migration DDL through transaction-mode PgBouncer. See
  `docs/ops/database-connection-pooling.md`.

## Scaling Services

```bash
docker service scale dev-health_api=4
docker service scale dev-health_worker=4
```

## Viewing Logs

```bash
docker service logs -f dev-health_api
docker service logs -f dev-health_worker
```

## Updating the Stack

```bash
docker stack deploy -c stack.yml dev-health
```

## Removing the Stack

```bash
docker stack rm dev-health
```
