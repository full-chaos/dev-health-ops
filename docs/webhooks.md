# Webhook Setup Documentation

This document describes how to configure webhooks for real-time data synchronization in Dev Health Ops.

## GitHub Configuration

1. Go to your repository or organization **Settings**.
2. Select **Webhooks** from the sidebar.
3. Click **Add webhook**.
4. Set **Payload URL** to `https://your-dev-health-instance.com/api/v1/webhooks/github`.
5. Set **Content type** to `application/json`.
6. Enter a **Secret** (must match `GITHUB_WEBHOOK_SECRET` environment variable).
7. Select **Let me select individual events**:
   - Pushes
   - Pull requests
   - Issues
   - Deployments
   - Workflow runs
8. Click **Add webhook**.

## GitLab Configuration

1. Go to your project or group **Settings** > **Webhooks**.
2. Set **URL** to `https://your-dev-health-instance.com/api/v1/webhooks/gitlab`.
3. Set **Secret token** (must match `GITLAB_WEBHOOK_TOKEN` environment variable).
4. Under **Trigger**, select:
   - Push events
   - Tag push events
   - Merge request events
   - Issue events
   - Pipeline events
   - Job events
5. Click **Add webhook**.

## Jira Configuration

1. Log in as a Jira Administrator.
2. Go to **System** > **Webhooks**.
3. Click **Create a Webhook**.
4. Set **URL** to `https://your-dev-health-instance.com/api/v1/webhooks/jira`.
5. (Optional) Add `?secret=your_secret` to the URL if `JIRA_WEBHOOK_SECRET` is configured.
6. Under **Events**, select:
   - Issue: created, updated, deleted.
7. Click **Create**.

## Environment Variables

Ensure the following variables are set in your deployment environment:

| Variable | Description |
|----------|-------------|
| `GITHUB_WEBHOOK_SECRET` | HMAC secret for GitHub signature validation |
| `GITLAB_WEBHOOK_TOKEN` | Token for GitLab X-Gitlab-Token validation |
| `JIRA_WEBHOOK_SECRET` | Optional secret for Jira validation |
| `REDIS_URL` | Required for webhook delivery idempotency |
