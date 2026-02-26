# Email Setup

Dev Health Ops sends transactional emails for account operations and billing events. This page covers provider configuration, available email types, and troubleshooting.

## Email Provider

The platform uses [Resend](https://resend.com) as its transactional email provider. A console provider is available for local development and testing.

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `EMAIL_PROVIDER` | No | `console` | Email backend: `resend` for production, `console` for dev/test |
| `EMAIL_API_KEY` | When `EMAIL_PROVIDER=resend` | — | Resend API key (starts with `re_`) |
| `EMAIL_FROM_ADDRESS` | No | `dev-health@example.com` | Sender address for all outgoing emails |

### Resend Setup (Production)

1. Create an account at [resend.com](https://resend.com).
2. Verify your sending domain under **Domains** in the Resend dashboard.
3. Create an API key under **API Keys** with sending permission.
4. Configure your environment:

```bash
export EMAIL_PROVIDER="resend"
export EMAIL_API_KEY="re_your_api_key_here"
export EMAIL_FROM_ADDRESS="noreply@yourdomain.com"
```

!!! warning
    `EMAIL_FROM_ADDRESS` must match a verified domain in your Resend account. Emails sent from unverified domains will be rejected.

### Console Provider (Development)

The default `console` provider logs all outgoing emails to stdout instead of sending them. No additional configuration is needed:

```bash
export EMAIL_PROVIDER="console"
# EMAIL_API_KEY is not required
# EMAIL_FROM_ADDRESS defaults to dev-health@example.com
```

This is useful for local development and CI where you want to verify email content without sending real emails.

## Email Types

### Account Emails

| Email | Trigger | Recipient |
|-------|---------|-----------|
| Welcome | User registration | New user |
| Email verification | Account creation or email change | User |
| Password reset | Password reset request | User |
| Organization invite | Org admin invites a member | Invited email address |

### Billing Emails

Billing emails are sent when Stripe webhook events are processed. All billing emails go to the **organization owner** (the first owner by `created_at` if multiple owners exist).

| Email | Stripe Event | Recipient | Template Variables |
|-------|-------------|-----------|-------------------|
| Invoice receipt | `invoice.paid` | Org owner | `full_name`, `org_name`, `amount`, `currency`, `invoice_url` |
| Payment failed | `invoice.payment_failed` | Org owner | `full_name`, `org_name`, `amount`, `currency`, `attempt_count` |
| Subscription changed | `customer.subscription.updated` | Org owner (only if tier changed) | `full_name`, `org_name`, `old_tier`, `new_tier` |
| Subscription cancelled | `customer.subscription.deleted` | Org owner | `full_name`, `org_name`, `tier` |

**Key behaviors:**

- Invoice amounts from Stripe are in cents and automatically converted to display format (e.g., `4900` → `49.00`).
- Subscription change emails are only sent when the tier actually changes (not for other subscription metadata updates).
- All billing email calls are wrapped in try/except — an email delivery failure will never cause a webhook handler to fail.
- If no org owner is found for the `org_id` in Stripe metadata, the email is silently skipped with a warning log.

## Template System

Email templates are plain HTML files in `src/dev_health_ops/templates/email/` using Python `str.format()` placeholders:

```
templates/email/
├── welcome.html
├── email_verification.html
├── password_reset.html
├── invite.html
├── invoice_receipt.html
├── payment_failed.html
├── subscription_changed.html
└── subscription_cancelled.html
```

Templates use `{variable_name}` syntax. No Jinja2, no CSS frameworks — bare HTML only.

## Architecture

```
Stripe Webhook Event
    ↓
billing/router.py (handles event, commits DB changes)
    ↓
Celery task queue (webhooks queue, Redis broker)
    ↓
send_billing_notification worker task (max 3 retries, exponential backoff)
    ↓
billing_emails.py (looks up org owner, calls email service)
    ↓
email.py → EmailService → EmailProvider (Resend or Console)
    ↓
Resend API (production) or stdout (development)
```

Billing email dispatch is **asynchronous via Celery**. The webhook handler enqueues a `send_billing_notification` task on the `webhooks` queue and returns immediately. This ensures:

- Webhook response time is not affected by email delivery latency.
- Failed emails are retried automatically (up to 3 times with exponential backoff).
- DB state is always consistent regardless of email delivery success.
- Stripe always receives a `200 OK` response.

### Worker Requirements

A Celery worker must be running to process billing email tasks:

```bash
# Start worker with webhooks queue
dev-hops workers start-worker --queues webhooks

# Or include webhooks in a multi-queue worker
dev-hops workers start-worker --queues default,webhooks,sync
```

If the Celery broker (Redis) is unavailable when a webhook is processed, the email dispatch is silently skipped — the webhook still succeeds.

## Troubleshooting

### Emails not being sent

1. **Check `EMAIL_PROVIDER`** — Defaults to `console` which only logs. Set to `resend` for real delivery.
2. **Check `EMAIL_API_KEY`** — Required when provider is `resend`. Must be a valid Resend API key.
3. **Check `EMAIL_FROM_ADDRESS`** — Must match a verified domain in your Resend account.
4. **Check application logs** — Failed email sends are logged at `ERROR` level with full exception details.

### Billing emails not being sent

1. **Check Stripe webhook delivery** — Verify events are reaching your endpoint in the Stripe Dashboard.
2. **Check `org_id` in Stripe metadata** — Billing emails require `org_id` in subscription/customer metadata. If missing, emails are skipped.
3. **Check org ownership** — The org must have at least one member with the `owner` role.
4. **Check logs for `"No owner found"`** — This warning indicates the org owner lookup failed.

### Testing email delivery locally

Use the console provider to verify email content without sending:

```bash
EMAIL_PROVIDER=console dev-hops api --host 0.0.0.0 --port 8000 --reload
```

Then trigger a Stripe event:

```bash
stripe trigger invoice.paid
```

Check the API logs for the email content output.

## Related Documentation

- [Webhook Setup](./webhooks.md) — Stripe webhook configuration
- [Stripe Billing Runbook](./ops/stripe-billing-runbook.md) — Full billing operations guide
- [Configuration](./configuration.md) — All environment variables
