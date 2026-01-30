# ADR-001: Enterprise Edition Design Decisions

**Status**: PROPOSED  
**Created**: 2026-01-30  
**Updated**: 2026-01-30  
**Parent Issue**: [#299](https://github.com/full-chaos/dev-health-ops/issues/299)

## Context

Dev Health needs an enterprise edition to support:
- Multi-tenant SaaS offering
- Self-hosted deployments with premium features
- Sustainable open-source business model

This ADR captures the key design decisions that need to be made before implementation.

---

## Decision 1: Org ID vs Tenant ID

### Current State
The codebase uses `org_id` throughout:
- GraphQL context: `context.org_id`
- Headers: `X-Org-Id`
- Database scoping: `WHERE org_id = :org_id`

### Options

| Option | Pros | Cons |
|--------|------|------|
| **A: Keep org_id** | No migration needed, familiar to team | Less clear multi-tenancy semantics |
| **B: Rename to tenant_id** | Clearer SaaS semantics, industry standard | Large refactor, breaking change |
| **C: Support both (alias)** | Backward compatible, gradual migration | Complexity, confusion |

### Recommendation
**Option A: Keep org_id**

Rationale:
- `org_id` is already well-understood and used consistently
- "Organization" is user-facing terminology (users belong to orgs)
- "Tenant" is infrastructure terminology (less user-friendly)
- GitLab, GitHub, and most DevOps tools use "organization"

### Decision
> **PENDING** - Awaiting team input

---

## Decision 2: Self-Hosted Datastore Support

### Question
Should self-hosted customers be able to bring their own ClickHouse/PostgreSQL in v1?

### Options

| Option | Pros | Cons |
|--------|------|------|
| **A: Our infra only (v1)** | Simpler support, controlled environment | Less flexibility, vendor lock-in concerns |
| **B: BYOD from v1** | Maximum flexibility, appeals to enterprise | Complex support matrix, compatibility issues |
| **C: Defer to v2** | Ship faster, learn from v1 usage | May lose enterprise deals requiring BYOD |

### Considerations

**For Option A (Our infra only):**
- SaaS: We manage ClickHouse and PostgreSQL
- Self-hosted: Ships with embedded/bundled database
- Simpler to support, test, and maintain

**For Option B (BYOD):**
- Enterprise customers often have existing data infrastructure
- Compliance requirements may mandate data stays in their environment
- Requires extensive compatibility testing

**For Option C (Defer):**
- Get to market faster
- Collect feedback on actual enterprise requirements
- Risk: May need to retrofit BYOD later

### Recommendation
**Option C: Defer to v2** with the following caveats:
- v1 self-hosted includes bundled PostgreSQL + ClickHouse (Docker Compose)
- Document that BYOD is on roadmap
- Design data layer to be swappable (already using sinks pattern)
- Track enterprise requests for BYOD as signal

### Decision
> **PENDING** - Awaiting team input

---

## Decision 3: Authentication Approach

### Options

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A: Embedded** | NextAuth (web) + JWT middleware (API) | Simpler, single codebase | No centralized auth, harder SSO |
| **B: External Gateway** | Authentik/Keycloak/Auth0 | Enterprise SSO ready, centralized | Additional infrastructure, complexity |
| **C: Hybrid** | Basic auth embedded, SSO via gateway (Enterprise tier) | Best of both, tier separation | Two auth paths to maintain |

### Recommendation
**Option C: Hybrid**

Rationale:
- Community/Team tiers: Simple email/password auth (NextAuth + JWT)
- Enterprise tier: SSO via external IdP integration
- Keeps core simple, adds complexity only for Enterprise
- Follows GitLab model (basic auth + optional SAML/OIDC)

### Implementation
```
Community/Team:
  User → NextAuth (email/password) → JWT → API

Enterprise (SSO):
  User → External IdP (Okta/Azure AD) → SAML/OIDC → NextAuth → JWT → API
```

### Decision
> **PENDING** - Awaiting team input

---

## Decision 4: Licensing Model

### Options

| Model | License | Paid Via | Examples |
|-------|---------|----------|----------|
| **A: AGPL + Commercial** | AGPL for OSS, commercial for EE | License key | GitLab |
| **B: MIT + Hosted** | MIT for all, monetize hosting | SaaS subscription | PostHog |
| **C: BSL (BUSL)** | Source-available, converts to OSS | SaaS + self-hosted licenses | Sentry, HashiCorp |

### Considerations

**AGPL + Commercial:**
- Strong copyleft encourages commercial licensing
- Clear separation between CE and EE features
- Some enterprises avoid AGPL (legal concerns)

**MIT + Hosted:**
- Maximum adoption (no license concerns)
- Monetization relies on SaaS being better than self-hosted
- Risk of AWS/cloud providers offering competing service

**BSL:**
- Protects against cloud providers
- Time-delayed OSS (typically 3-4 years)
- Growing acceptance in enterprise

### Recommendation
**Option C: BSL** (Business Source License)

Rationale:
- Protects against hyperscaler competition
- Allows self-hosting for production use
- Converts to Apache 2.0 after 4 years
- HashiCorp and Sentry have validated this model
- Enterprises are increasingly comfortable with BSL

### Decision
> **PENDING** - Awaiting team input

---

## Decision 5: Public API

### Question
Should there be a documented, stable public API for external integrations?

### Options

| Option | Pros | Cons |
|--------|------|------|
| **A: No public API (v1)** | Less support burden, can iterate freely | Limits integrations, less extensible |
| **B: Public API from v1** | Ecosystem potential, enterprise need | Stability commitment, support burden |
| **C: Internal API, document later** | Ship fast, stabilize API organically | Unclear commitment, fragmented docs |

### Recommendation
**Option C: Internal API, document when stable**

Rationale:
- Current GraphQL API exists but isn't stable
- Gate API access behind Team+ tier (monetization lever)
- Document and version when patterns stabilize
- Allows iteration without breaking external users

### Decision
> **PENDING** - Awaiting team input

---

## Summary of Recommendations

| Decision | Recommendation | Confidence |
|----------|---------------|------------|
| Org ID vs Tenant ID | Keep `org_id` | High |
| Self-hosted Datastore | Defer BYOD to v2 | Medium |
| Auth Approach | Hybrid (basic + SSO for EE) | High |
| Licensing Model | BSL | Medium |
| Public API | Internal for now, document later | High |

---

## Next Steps

1. Review and approve/modify these decisions
2. Update this ADR with final decisions
3. Proceed with P1 implementation
4. Revisit deferred decisions for v2 planning

---

## Changelog

| Date | Change |
|------|--------|
| 2026-01-30 | Initial draft with 5 decisions |
