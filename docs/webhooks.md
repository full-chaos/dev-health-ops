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
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret (SaaS billing) |
| `REDIS_URL` | Required for webhook delivery idempotency |

## Stripe Configuration (SaaS Billing)

Stripe webhooks are used for real-time subscription management in SaaS deployments. When a customer subscribes, upgrades, downgrades, or cancels, Stripe sends events directly to `dev-health-ops`.

1. Go to your [Stripe Dashboard](https://dashboard.stripe.com/) > **Developers** > **Webhooks**.
2. Click **Add endpoint**.
3. Set **Endpoint URL** to `https://your-dev-health-instance.com/api/v1/billing/webhooks/stripe`.
4. Under **Events to send**, select:
   - `checkout.session.completed`
   - `customer.subscription.updated`
   - `customer.subscription.deleted`
   - `invoice.payment_succeeded`
   - `invoice.payment_failed`
5. Click **Add endpoint**.
6. Copy the **Signing secret** and set it as `STRIPE_WEBHOOK_SECRET` in your environment.

### Billing Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/billing/webhooks/stripe` | POST | Receives Stripe webhook events |
| `/api/v1/billing/checkout` | POST | Creates a Stripe Checkout Session (auth required) |
| `/api/v1/billing/portal` | POST | Creates a Stripe Customer Portal session (auth required) |
| `/api/v1/billing/entitlements/{org_id}` | GET | Returns current tier, features, and limits for an org |

### Required Environment Variables (Billing)

| Variable | Description |
|----------|-------------|
| `STRIPE_SECRET_KEY` | Stripe API secret key |
| `STRIPE_WEBHOOK_SECRET` | Webhook signing secret from step 6 above |
| `STRIPE_PRICE_ID_TEAM` | Stripe Price ID for the Team tier product |
| `STRIPE_PRICE_ID_ENTERPRISE` | Stripe Price ID for the Enterprise tier product |
| `LICENSE_PRIVATE_KEY` | Ed25519 private key for signing JWT licenses (base64-encoded) |

> **Note**: Stripe webhooks are only relevant for SaaS deployments. Self-hosted deployments use offline Ed25519 license keys and do not require Stripe configuration.
