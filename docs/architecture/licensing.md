# Licensing & Entitlements Architecture

> **Status**: DECIDED  
> **Model**: SaaS-first with runtime feature gating (GitLab model)  
> **Related**: [ADR-001](adr/001-enterprise-edition.md), [Monetization Strategy](../monetization-strategy.md)

## Overview

Dev Health uses a **single-repository model** with all code visible under the BSL license. Premium features are gated at runtime by entitlement checks.

**Entitlement paths (in order of priority):**
1. **SaaS subscriptions (primary)** — Stripe-managed billing with real-time entitlement sync via webhooks directly to `dev-health-ops`. This is the default for most users.
2. **Self-hosted license keys (secondary)** — Ed25519-signed JWTs for offline validation. Used by organizations requiring data sovereignty or air-gapped environments.

---

## SaaS Licensing (Primary)

In the SaaS model, entitlements are managed dynamically through our billing integration.

### How It Works

1. **Subscription**: Users subscribe to a tier (Team or Enterprise) via Stripe through the `dev-health-web` UI.
2. **Checkout**: `dev-health-web` calls the `dev-health-ops` billing API to create a Stripe Checkout Session.
3. **Webhook**: When a subscription changes, Stripe sends a webhook directly to `dev-health-ops` at `/api/v1/billing/webhooks/stripe`.
4. **Enforcement**: `dev-health-ops` processes the event, signs an Ed25519 JWT license, updates `Organization.tier`, and gates features in real-time.

### SaaS Entitlement Flow

```
┌──────────────┐      ┌──────────────┐
│    Stripe    │─────▶│dev-health-ops│
└──────────────┘      └──────────────┘
       │                     │
1. Subscription       2. Process event,
   event (webhook)       sign JWT license,
   POST /api/v1/         update org tier,
   billing/webhooks/     gate features
   stripe
```

---

## Self-Hosted Licensing (Secondary)

For self-hosted deployments, entitlements are validated offline using Ed25519-signed license keys.

### How It Works

1. **License Key**: The customer receives an Ed25519-signed JWT license key.
2. **Configuration**: The key is provided to the instance via the `DEV_HEALTH_LICENSE` environment variable or application settings.
3. **Validation**: `dev-health-ops` validates the signature and expiration offline using a hardcoded public key.
4. **Enforcement**: Features and limits are unlocked based on the payload of the validated license key.

### Self-Hosted Entitlement Flow

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│ Customer Portal  │─────▶│   Environment    │─────▶│  dev-health-ops  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
         │                         │                         │
1. Purchase license       2. Set env var            3. Offline validation
   & receive key             DEV_HEALTH_LICENSE        & unlock features
```

---

## License Key Format (Self-Hosted Only)

### Ed25519-Signed JWT

Self-hosted license keys use the Ed25519 signature algorithm for fast, secure, and offline validation.

**Why Ed25519:**
- Smaller keys than RSA (32 bytes vs 2048 bits)
- Faster verification
- Modern standard (used by Coder, tldraw)
- No padding attacks

### Payload Schema

```json
{
  "iss": "fullchaos.studio",
  "sub": "org_abc123",
  "iat": 1706745600,
  "exp": 1738281600,
  "tier": "team",
  "features": {
    "sso": true,
    "audit": true,
    "api_access": true,
    "investment_view": false
  },
  "limits": {
    "users": 50,
    "repos": -1,
    "api_rate": 1000
  },
  "deployment_ids": ["self-hosted"],
  "grace_days": 14
}
```

---

## Implementation

### Feature Gating

We use the `@require_feature` decorator in `dev-health-ops` to gate access to premium functionality. This decorator checks the current organization's entitlements (either from the database for SaaS or from the validated license key for self-hosted).

```python
@router.get("/api/investment")
@require_feature("investment_view")
async def get_investment_view():
    # Only available with Team+ license
    ...
```

### Resource Limits

Resource limits (e.g., user count, repository count) are enforced at the service layer by checking the current usage against the entitlements.

```python
async def create_user(org_id: str, user_data: dict):
    current_users = await count_users(org_id)
    if not check_limit("users", current_users):
        raise LimitExceeded("users", get_entitlements().limits["users"])
    ...
```

---

## Related Documents

- [ADR-001: Enterprise Edition Design Decisions](adr/001-enterprise-edition.md)
- [Monetization Strategy](../monetization-strategy.md)
- [Enterprise Overview](enterprise-overview.md)
