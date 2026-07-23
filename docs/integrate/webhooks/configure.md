---
page_id: int-wh-config
summary: Configure GitHub, GitLab, Jira, or PagerDuty webhook delivery for the managed synchronization path.
content_type: task-guide
owner: platform-api
source_of_truth:
  - .github/docs-legacy/webhooks.md
  - .github/docs-legacy/architecture/pagerduty-contract.md
  - current provider webhook routes and handlers
applicability: current
lifecycle: active
---

# Configure a provider webhook

Provider webhooks notify Dev Health about source changes after the provider has been connected through the supported workspace administration flow. They are part of **Full Chaos-managed synchronization**. They are not the Customer Push API used by a customer-controlled CI pipeline or ETL process.
{: .fc-page-lede }

Use the public HTTPS origin for the Dev Health deployment. Store secrets in the deployment secret manager, configure the corresponding receiver with the same value, and never put a reusable secret in screenshots, issue comments, or ordinary logs.

## Before you begin

Confirm that:

- the provider connection already exists for the intended Dev Health organization;
- the public application origin and TLS route are reachable from the provider;
- the deployment has the required webhook secret or binding configuration;
- the active workers consume the `webhooks` queue;
- the selected event families are supported by the current handler.

A provider webhook supplements managed synchronization. It does not replace the initial source discovery, historical backfill, or reconciliation path.

## GitHub

1. In the GitHub repository or organization, open **Settings → Webhooks**.
2. Select **Add webhook**.
3. Set **Payload URL** to:

   ```text
   https://YOUR_HOST/api/v1/webhooks/github
   ```

4. Set **Content type** to `application/json`.
5. Enter the secret configured as `GITHUB_WEBHOOK_SECRET` in Dev Health.
6. Select the event families supported by the current handler:
   - pushes;
   - pull requests;
   - issues;
   - deployments;
   - workflow runs.
7. Save the webhook and send a provider test delivery.

Dev Health validates the GitHub HMAC signature against the original request body before accepting the event.

## GitLab

1. In the GitLab project or group, open **Settings → Webhooks**.
2. Set **URL** to:

   ```text
   https://YOUR_HOST/api/v1/webhooks/gitlab
   ```

3. Set the secret token to the value configured as `GITLAB_WEBHOOK_TOKEN`.
4. Enable the event families supported by the current handler:
   - push and tag-push events;
   - merge-request events;
   - issue events;
   - pipeline events;
   - job events.
5. Save the webhook and run GitLab's test delivery.

Dev Health validates the `X-Gitlab-Token` value before dispatching the event.

## Jira

1. Sign in as a Jira administrator.
2. Open **System → Webhooks**.
3. Select **Create a Webhook**.
4. Set **URL** to:

   ```text
   https://YOUR_HOST/api/v1/webhooks/jira
   ```

5. Configure the supported secret path when `JIRA_WEBHOOK_SECRET` is enabled for the deployment.
6. Select the supported issue-created, issue-updated, and issue-deleted events.
7. Save the webhook and send a bounded test event.

Jira Service Management incident ingestion has a separate provider contract and is not made release-ready merely by configuring this generic Jira webhook.

## PagerDuty V3

PagerDuty uses an opaque persisted binding rather than a shared environment route. Create and verify the binding through the supported PagerDuty integration flow, then configure the V3 subscription with the binding URL:

```text
https://YOUR_HOST/api/v1/webhooks/pagerduty/{binding_id}
```

The authenticated `pagey.ping` event verifies a candidate binding without creating an incident. Activate the replacement binding before revoking an old one. See [PagerDuty provider and webhook contract](pagerduty.md) for the accepted event set, signature, replay, identity, and rotation rules.

## Verify delivery

After configuring a provider:

1. Send the provider's supported test event.
2. Confirm the receiver accepts the request only with valid authentication.
3. Confirm the event is admitted once and placed on the intended processing path.
4. Verify downstream synchronization or canonical state advances.
5. Repeat the delivery to confirm idempotent replay behavior where the provider supports it.
6. Confirm malformed authentication, unknown events, and revoked authority fail closed.

Continue with [Verify a webhook signature](verify-signatures.md) and [Handle retries and replay](retries-and-replay.md).
