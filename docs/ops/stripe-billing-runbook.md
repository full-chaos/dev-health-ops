# Stripe Billing Runbook

This runbook covers how to operate Stripe-backed billing in `dev-health-ops` across local development, CI, and production operations.

## Deployment Paths

| Path | Stripe required | Core mechanism |
|---|---|---|
| SaaS | Yes | Stripe checkout, portal, and webhook flow |
| Self-hosted | No | Offline license flow (`DEV_HEALTH_LICENSE`) |

If you are self-hosting only, skip Stripe setup and use `DEV_HEALTH_LICENSE` as documented in the self-hosted guides.

## Billing API Contract (Current)

The runbook assumes the current billing endpoints in `dev_health_ops.api.billing.router`:

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v1/billing/webhooks/stripe` | `POST` | Receives Stripe webhook events |
| `/api/v1/billing/checkout` | `POST` | Creates Stripe Checkout sessions |
| `/api/v1/billing/portal` | `POST` | Creates Stripe Billing Portal sessions |
| `/api/v1/billing/entitlements/{org_id}` | `GET` | Returns org entitlements |
| `/api/v1/billing/audit` | `GET` | Lists billing audit and reconciliation entries (superadmin) |
| `/api/v1/billing/audit/{audit_id}` | `GET` | Gets one billing audit entry (superadmin) |
| `/api/v1/billing/audit/{audit_id}/resolve` | `POST` | Marks mismatch resolution (superadmin) |
| `/api/v1/billing/reconcile` | `POST` | Triggers reconciliation run (superadmin) |

## Required Environment Variables (SaaS)

Set these before running API billing flows:

```bash
export STRIPE_SECRET_KEY="sk_test_..."
export STRIPE_WEBHOOK_SECRET="whsec_..."
export STRIPE_PRICE_ID_TEAM="price_..."
export STRIPE_PRICE_ID_ENTERPRISE="price_..."
export LICENSE_PRIVATE_KEY="<base64-ed25519-private-key>"
```

Optional but recommended for checkout URL validation:

```bash
export APP_BASE_URL="http://localhost:3000"
export ALLOWED_CHECKOUT_DOMAINS="http://localhost:3000,https://staging.example.com"
```

## Local Workflow (SaaS)

### 1) Start API with billing env

```bash
# Example local API startup
export CLICKHOUSE_URI="clickhouse://localhost:8123/default"
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5432/devhealth"
dev-hops api --db "$CLICKHOUSE_URI" --host 0.0.0.0 --port 8000 --reload
```

### 2) Install and authenticate Stripe CLI

```bash
# macOS (Homebrew)
brew install stripe/stripe-cli/stripe

# Authenticate Stripe CLI for your Stripe account
stripe login
```

### 3) Forward Stripe events to local webhook endpoint

```bash
stripe listen \
  --forward-to http://127.0.0.1:8000/api/v1/billing/webhooks/stripe \
  --events checkout.session.completed,customer.subscription.created,customer.subscription.updated,customer.subscription.deleted,invoice.created,invoice.updated,invoice.finalized,invoice.paid,invoice.payment_failed,invoice.voided,charge.refunded,charge.refund.updated
```

Copy the emitted signing secret (`whsec_...`) and set it as `STRIPE_WEBHOOK_SECRET` in your shell where the API runs.

### 4) Trigger local event flows

```bash
# Simulate checkout completion
stripe trigger checkout.session.completed

# Simulate recurring invoice events
stripe trigger invoice.paid
stripe trigger invoice.payment_failed

# Simulate subscription updates
stripe trigger customer.subscription.updated
```

### 5) Validate API-level outcomes

```bash
# Health check
curl http://127.0.0.1:8000/health

# Trigger reconciliation (requires superadmin auth in real environments)
curl -X POST "http://127.0.0.1:8000/api/v1/billing/reconcile"
```

## Webhook Replay, Retry, and Idempotency

### Stripe retry model

- Stripe retries failed deliveries automatically.
- Manual replays can come from Stripe Dashboard or Stripe CLI.
- Your API must tolerate at-least-once delivery.

### Current idempotency behavior in `dev-health-ops`

- Invoice webhook handling checks duplicate Stripe event IDs before processing invoice writes.
- Duplicate invoice events are skipped and logged.
- For subscription/refund mismatches or replay uncertainty, use reconciliation and audit endpoints.

### Safe replay playbook

1. Confirm the webhook endpoint returns non-2xx or missed state change.
2. Replay specific events:

```bash
# List recent events
stripe events list --limit 20

# Replay one event to the local endpoint
stripe events resend evt_123 --webhook-endpoint we_123
```

3. Run reconciliation:

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/billing/reconcile"
```

4. Inspect audit trail for unresolved mismatches:

```bash
curl "http://127.0.0.1:8000/api/v1/billing/audit?org_id=<org-uuid>"
```

## Ops Workflow (SaaS Production)

### Incident triage checklist

1. Verify env vars in runtime (`STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, price IDs, `LICENSE_PRIVATE_KEY`).
2. Verify Stripe webhook endpoint URL exactly matches:
   - `https://<your-domain>/api/v1/billing/webhooks/stripe`
3. Verify Stripe endpoint event subscriptions include the billing lifecycle events listed in this runbook.
4. Confirm webhook delivery status in Stripe Dashboard (response code + body).
5. Trigger reconciliation and review mismatches via audit endpoints.

### Reconciliation commands

Use either API endpoint or CLI:

```bash
# API
curl -X POST "https://<your-domain>/api/v1/billing/reconcile"

# CLI (from ops runtime with DB/env configured)
python -m dev_health_ops.cli billing reconcile

# Scoped reconcile by org
python -m dev_health_ops.cli billing reconcile --org-id <org-uuid>

# Reconcile invoices since timestamp
python -m dev_health_ops.cli billing reconcile --org-id <org-uuid> --since 2026-02-24T00:00:00
```

### Resolving mismatches

1. Pull mismatch entries from `/api/v1/billing/audit`.
2. Investigate local vs Stripe state.
3. Mark resolved when remediation completes:

```bash
curl -X POST "https://<your-domain>/api/v1/billing/audit/<audit-id>/resolve" \
  -H "Content-Type: application/json" \
  -d '{"resolution":"manual correction applied after Stripe replay"}'
```

## Email Notifications

When billing webhook events are processed, the system automatically sends email notifications to the organization owner. This happens after all database operations complete successfully.

### Emails Sent

| Event | Email | Details |
|-------|-------|---------|
| `invoice.paid` | Invoice receipt | Amount, currency, link to hosted invoice |
| `invoice.payment_failed` | Payment failed alert | Amount, currency, retry attempt count |
| `customer.subscription.updated` | Subscription changed | Old tier → new tier (only sent when tier actually changes) |
| `customer.subscription.deleted` | Subscription cancelled | Current tier name |

### Email Delivery Guarantees

- Emails are dispatched **asynchronously via Celery** on the `webhooks` queue — the webhook handler returns immediately after enqueuing.
- Failed email deliveries are **retried up to 3 times** with exponential backoff (30s, 60s, 120s).
- Database state is never affected by email failures — DB commits happen before email dispatch.
- If the Celery broker (Redis) is unavailable, email dispatch is silently skipped — the webhook still succeeds.
- If no organization owner is found (missing `org_id` in metadata or no owner-role member), the email is silently skipped with a warning log.

### Email Provider Configuration

Billing emails use the same email service as account emails (invites, verification, password reset). Configure via:

```bash
export EMAIL_PROVIDER="resend"        # or "console" for dev/test
export EMAIL_API_KEY="re_..."         # Resend API key
export EMAIL_FROM_ADDRESS="noreply@yourdomain.com"
```

See [Email Setup](../email-setup.md) for full provider configuration, troubleshooting, and template details.

### Verifying Email Delivery Locally

1. Start the API with `EMAIL_PROVIDER=console` (default) to log emails to stdout.
2. Forward Stripe events as described in the [Local Workflow](#local-workflow-saas) section.
3. Trigger an event: `stripe trigger invoice.paid`
4. Check API logs for the rendered email content.

To test with real email delivery, set `EMAIL_PROVIDER=resend` with a valid API key and from address.

## CI Workflow

### Secret handling

- Store Stripe values in CI secret manager, never in repo files:
  - `STRIPE_SECRET_KEY`
  - `STRIPE_WEBHOOK_SECRET`
  - `STRIPE_PRICE_ID_TEAM`
  - `STRIPE_PRICE_ID_ENTERPRISE`
  - `LICENSE_PRIVATE_KEY`
- Inject them as environment variables at job runtime.

Example (generic CI shell step):

```bash
export STRIPE_SECRET_KEY="$CI_STRIPE_SECRET_KEY"
export STRIPE_WEBHOOK_SECRET="$CI_STRIPE_WEBHOOK_SECRET"
export STRIPE_PRICE_ID_TEAM="$CI_STRIPE_PRICE_ID_TEAM"
export STRIPE_PRICE_ID_ENTERPRISE="$CI_STRIPE_PRICE_ID_ENTERPRISE"
export LICENSE_PRIVATE_KEY="$CI_LICENSE_PRIVATE_KEY"
pytest -q tests/test_billing.py tests/test_subscriptions.py tests/test_invoices.py tests/test_refunds.py
```

### CI guardrails

- Use Stripe test-mode keys only (`sk_test_...`).
- Avoid printing secret values in logs.
- Keep webhook-signature tests deterministic by stubbing payload/signature where possible.

## Stripe Test Card Matrix

Use Stripe test mode and these cards during checkout/billing validation:

| Scenario | Card number | Notes |
|---|---|---|
| Successful payment | `4242 4242 4242 4242` | Baseline success path |
| Generic decline | `4000 0000 0000 0002` | Payment declined |
| Insufficient funds | `4000 0000 0000 9995` | Insufficient funds path |
| 3DS required | `4000 0025 0000 3155` | Authentication flow required |
| Expired card | `4000 0000 0000 0069` | Expiration failure path |
| Incorrect CVC | `4000 0000 0000 0127` | CVC validation failure |

For all test cards, use any future expiry date, any 3-digit CVC, and any postal code.

## Self-Hosted Note

Self-hosted deployments do not require Stripe endpoint configuration. Use offline license keys and set:

```bash
export DEV_HEALTH_LICENSE="<signed-license-token>"
```

Reference: [`self-hosted-quickstart.md`](../self-hosted-quickstart.md).
