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
