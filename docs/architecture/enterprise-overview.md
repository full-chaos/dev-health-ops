# Enterprise Edition Architecture

> **Status**: DECIDED  
> **Tracking Issue**: [#299](https://github.com/full-chaos/dev-health-ops/issues/299)  
> **Design Decisions**: [ADR-001](adr/001-enterprise-edition.md)

## Overview

Dev Health Enterprise Edition adds authentication, authorization, and compliance features to the open-source core. It is designed with a **SaaS-first** architecture: the managed platform is the primary deployment model, providing the fastest path to value with zero infrastructure overhead. Self-hosted enterprise deployments are also supported via offline license keys for organizations requiring data sovereignty or air-gapped environments.

## Deployment Architecture (SaaS Primary)

The primary deployment model is our managed SaaS platform, which consists of three main components:

1. **dev-health-web (PUBLIC)**: The Next.js frontend that provides the user interface, including billing settings and upgrade prompts.
2. **dev-health-ops (PUBLIC)**: The FastAPI backend that handles data collection, analytics, and feature gating.
3. **license-svc (PRIVATE)**: A private service that manages Stripe integration, subscription lifecycles, and license generation.

### SaaS Entitlement Flow

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│    Stripe    │─────▶│  license-svc │─────▶│dev-health-ops│
└──────────────┘      └──────────────┘      └──────────────┘
       │                     │                     │
1. Subscription       2. Process event      3. Update tier
   event (webhook)       & notify ops          & gate features
```

1. **Stripe**: Handles payments and subscription state.
2. **license-svc**: Receives Stripe webhooks, manages entitlements, and notifies `dev-health-ops` of tier changes.
3. **dev-health-ops**: Updates the `Organization.tier` in the database and gates features in real-time using the `@require_feature` decorator.

---

## Self-Hosted Architecture (Secondary)

For self-hosted deployments, entitlements are managed via an offline license key system.

### Self-Hosted Entitlement Flow

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│ Customer Portal  │─────▶│   Environment    │─────▶│  dev-health-ops  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
         │                         │                         │
1. Purchase license       2. Set env var            3. Offline validation
   & receive key             DEV_HEALTH_LICENSE        & unlock features
```

1. **Customer Portal**: Customer purchases a license and receives an Ed25519-signed JWT.
2. **Environment**: Customer sets the `DEV_HEALTH_LICENSE` environment variable on their instance.
3. **dev-health-ops**: Validates the license key offline using a hardcoded public key and unlocks features accordingly.

---

## Repository Responsibilities

| Repository | Visibility | Responsibility |
|------------|------------|----------------|
| **dev-health-ops** | PUBLIC | Open-source analytics platform, feature gating via `@require_feature`, license webhook receiver. |
| **dev-health-web** | PUBLIC | Frontend UI, billing settings, `UpgradeGate` component, Stripe checkout redirects. |
| **license-svc** | PRIVATE | Stripe integration, license generation, entitlements API, billing portal management. |

---

## Tier Comparison

| Feature | Community | Team | Enterprise |
|---------|:---------:|:----:|:----------:|
| **Core Analytics** |
| Basic metrics (commits, PRs, cycle time) | ✅ | ✅ | ✅ |
| Git sync (GitHub, GitLab, local) | ✅ | ✅ | ✅ |
| Work item sync (Jira, Linear, GitHub Issues) | ✅ | ✅ | ✅ |
| Grafana dashboards | ✅ | ✅ | ✅ |
| **Organization** |
| Single organization | ✅ | ✅ | ✅ |
| Up to 3 users | ✅ | - | - |
| Multiple organizations | ❌ | ✅ | ✅ |
| Unlimited users | ❌ | ✅ | ✅ |
| **Advanced Features** |
| API access | ❌ | ✅ | ✅ |
| Webhooks | ❌ | ✅ | ✅ |
| Capacity planning (Monte Carlo) | ❌ | ✅ | ✅ |
| Investment distribution view | ❌ | ❌ | ✅ |
| **Security & Compliance** |
| Email/password auth | ✅ | ✅ | ✅ |
| SSO (SAML/OIDC) | ❌ | ❌ | ✅ |
| Audit logging | ❌ | ❌ | ✅ |
| Data retention policies | ❌ | ❌ | ✅ |
| IP allowlisting | ❌ | ❌ | ✅ |
| **Support** |
| Community support (GitHub) | ✅ | ✅ | ✅ |
| Email support | ❌ | ✅ | ✅ |
| Priority support (SLA) | ❌ | ❌ | ✅ |

---

## Data Model (Enterprise Tables)

### Users & Organizations

```sql
-- Core identity
users (id, email, name, password_hash, auth_provider, ...)
organizations (id, name, slug, tier, settings, ...)
memberships (user_id, org_id, role, ...)

-- RBAC
permissions (name, description, category)
role_permissions (role, permission)
```

### Licensing & Entitlements

```sql
-- Feature flags
feature_flags (name, min_tier, description)
org_feature_overrides (org_id, feature_name, enabled, expires_at)
```

---

## Authentication & Authorization

### Standard Flow (SaaS & Self-Hosted)

1. User authenticates via email/password or OAuth.
2. JWT issued with claims: `{sub, email, org_id, role}`.
3. API validates JWT and populates request context.
4. Feature access is checked via `@require_feature(feature_name)`.

### RBAC Roles

| Role | Description | Typical Use |
|------|-------------|-------------|
| `owner` | Full control including billing and org deletion | Org creator, billing admin |
| `admin` | Manage users and settings, full data access | Team leads, IT admins |
| `editor` | Modify teams, run syncs, write access | DevOps engineers |
| `viewer` | Read-only access to analytics | Most users |

---

## Related Documents

- [ADR-001: Enterprise Edition Design Decisions](adr/001-enterprise-edition.md)
- [Licensing Architecture](licensing.md)
- [Monetization Strategy](../monetization-strategy.md)
