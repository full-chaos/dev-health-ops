# Billing Edge Cutover Runbook

This runbook covers migration of Stripe webhook delivery to the dedicated
`billing-edge` service.

## Prerequisites

- `billing-edge` service is deployed and healthy (`/health` returns `status=ok`).
- `POSTGRES_URI`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, and
  `LICENSE_PRIVATE_KEY` are set for `billing-edge`.
- Core API service is private/internal only.

## Stripe Event Subscriptions

Configure the endpoint to receive:

- `checkout.session.completed`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
- `invoice.created`
- `invoice.updated`
- `invoice.finalized`
- `invoice.paid`
- `invoice.payment_failed`
- `invoice.voided`
- `charge.refunded`
- `charge.refund.updated`

## Cutover Steps

1. Deploy compose changes with `billing-edge` exposed and core API internal.
2. Create a new Stripe webhook endpoint:
   - URL: `https://<public-host>/api/v1/billing/webhooks/stripe`
   - Signing secret: set in `STRIPE_WEBHOOK_SECRET`.
3. Keep old endpoint active during the observation window.
4. Trigger Stripe test events and verify:
   - endpoint receives `2xx`
   - billing records persist to Postgres
   - no duplicate side effects
5. Observe production traffic for 30-60 minutes.
6. Disable old endpoint after stable validation.

## Validation Checklist

- `docker compose -f deploy/docker-compose/compose.production.yml config` succeeds.
- `billing-edge` `/health` reports `status=ok`.
- Stripe delivery success >= 99% during validation window.
- No spike in webhook retries or `4xx/5xx` responses.
- License/subscription/invoice/refund state updates persist correctly.

## Rollback

If webhook failure rate exceeds threshold or persistence fails:

1. Re-enable legacy Stripe endpoint (if disabled).
2. Point Stripe deliveries back to legacy endpoint.
3. Scale down or remove `billing-edge` from routing.
4. Keep event logs and retry IDs for reconciliation.
5. Run billing reconciliation after rollback.

## Post-Cutover

- Archive old endpoint secret after verification.
- Keep `billing-edge` runbook and env contract updated with deployment changes.
