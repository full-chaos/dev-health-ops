# Monetization Strategy: SaaS-First with Self-Hosted Licenses

> **Status**: DECIDED  
> **Model**: SaaS-first with runtime feature gating (GitLab model)  
> **License**: BSL (Business Source License)  
> **Related**: [Licensing Architecture](../architecture/licensing.md), [ADR-001](../architecture/adr/001-enterprise-edition.md)

---

## SaaS Subscription (Primary)

The primary deployment model for Dev Health is our managed SaaS platform. This provides the fastest path to value with zero infrastructure overhead.

### SaaS Billing Flow

```
User signs up → org created (tier=community)
User clicks "Upgrade" → dev-health-web calls dev-health-ops billing API
dev-health-ops creates Stripe Checkout Session → redirect to Stripe
Stripe webhook → dev-health-ops processes event directly
Organization.tier updated → features gated in real-time
```

- **Multi-tenant**: Shared infrastructure with logical isolation via `org_id`.
- **Managed**: Automatic updates, maintenance, and backups.
- **Self-service**: Upgrade/downgrade directly via the web UI.
- **Integrated Billing**: Stripe integration is built into `dev-health-ops` — no external billing service required.

---

## Self-Hosted Licenses (Secondary)

> Most users access Dev Health through the managed SaaS platform above. Self-hosted licensing is available for organizations that require full data sovereignty or air-gapped environments.

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                    dev-health-ops (BSL)                      │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  License     │───▶│  Entitlements│───▶│  Feature     │  │
│  │  Validator   │    │              │    │  Gates       │  │
│  │              │    │  • tier      │    │              │  │
│  │  • Ed25519   │    │  • features  │    │  @require()  │  │
│  │  • Offline   │    │  • limits    │    │  check_limit │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Self-Hosted License Flow

1. Purchase license at fullchaos.dev
2. Receive Ed25519-signed license key
3. Set `DEV_HEALTH_LICENSE` env var
4. Features unlock on startup (offline validation)

**Why Ed25519:**
- Fully offline validation (public key hardcoded in binary)
- Smaller keys than RSA (32 bytes)
- Faster verification
- No padding attacks

See [Licensing Architecture](../architecture/licensing.md) for full implementation details.

---

## License Tiers

| Tier | Price | Target | Key Features |
|------|-------|--------|--------------|
| **Community** | Free | Individuals, small teams | Core analytics, 3 users, 5 repos |
| **Team** | $29/user/mo | Growing teams | SSO, API access, webhooks, unlimited repos |
| **Enterprise** | Custom | Large orgs | SAML, audit logs, retention policies, SLA support |

### Feature Matrix

| Feature | Community | Team | Enterprise |
|---------|:---------:|:----:|:----------:|
| **Core Analytics** |
| Basic metrics (commits, PRs, cycle time) | ✅ | ✅ | ✅ |
| Git sync (GitHub, GitLab, local) | ✅ | ✅ | ✅ |
| Work item sync (Jira, Linear, GitHub) | ✅ | ✅ | ✅ |
| Grafana dashboards | ✅ | ✅ | ✅ |
| **Limits** |
| Users | 3 | Unlimited | Unlimited |
| Repositories | 5 | Unlimited | Unlimited |
| Organizations | 1 | Multiple | Multiple |
| **Advanced Features** |
| API access | ❌ | ✅ | ✅ |
| Webhooks | ❌ | ✅ | ✅ |
| Investment distribution view | ❌ | ✅ | ✅ |
| Capacity planning (Monte Carlo) | ❌ | ✅ | ✅ |
| **Security & Compliance** |
| Email/password auth | ✅ | ✅ | ✅ |
| OAuth SSO (GitHub, GitLab, Google) | ❌ | ✅ | ✅ |
| SAML/OIDC SSO | ❌ | ❌ | ✅ |
| Audit logging | ❌ | ❌ | ✅ |
| Data retention policies | ❌ | ❌ | ✅ |
| IP allowlisting | ❌ | ❌ | ✅ |
| **Support** |
| Community (GitHub) | ✅ | ✅ | ✅ |
| Email support | ❌ | ✅ | ✅ |
| Priority support (SLA) | ❌ | ❌ | ✅ |

---

## Why Single-Repo (GitLab Model)

Dev Health uses a single repository with all code visible, premium features gated by runtime checks.

### Rationale

| Factor | Separate Repos | Single Repo (Chosen) |
|--------|----------------|---------------------|
| **Transparency** | Enterprise code hidden | Users see what they're paying for |
| **Community** | Can only contribute to "core" | Can contribute to all features |
| **Maintenance** | Two codebases, two CIs, sync issues | One codebase, one CI |
| **Trust** | "What are they hiding?" | Full visibility builds trust |
| **Security** | Obscurity (weak) | BSL + license enforcement (strong enough) |

---

## Revenue Model

### Primary Revenue

1. **SaaS subscriptions** — Per-seat pricing via Stripe (primary revenue driver)
2. **Support contracts** — Enterprise SLA agreements
3. **Self-hosted license keys** — Team and Enterprise deployments

### Secondary Revenue

1. **Professional services** — Setup, migration, training
2. **Custom development** — Feature requests, integrations
3. **Training** — Engineering effectiveness workshops

---

## Implementation Roadmap

### Phase 1: License Validation (Complete)

- [x] Research license patterns (GitLab, Coder, tldraw)
- [x] Document licensing architecture
- [x] Implement `LicenseValidator` service
- [x] Implement `get_entitlements()` API
- [x] Add `@require_feature()` decorator
- [x] License generation CLI (`admin licenses create`)

### Phase 2: Feature Gating (In Progress)

- [x] Gate SSO endpoints by tier
- [x] Gate API access by tier
- [x] Implement user/repo limits
- [ ] Add upgrade prompts in UI

### Phase 3: Billing Integration (Complete)

- [x] Stripe integration for SaaS built into `dev-health-ops` (primary billing path)
- [x] Self-service subscription management (upgrade/downgrade/cancel)
- [x] License key purchase flow (self-hosted)
- [ ] Usage-based metering (optional)

---

## Related Documents

- [Licensing Architecture](../architecture/licensing.md) — Technical implementation
- [ADR-001: Enterprise Edition](../architecture/adr/001-enterprise-edition.md) — Design decisions
- [Enterprise Overview](../architecture/enterprise-overview.md) — Full architecture
