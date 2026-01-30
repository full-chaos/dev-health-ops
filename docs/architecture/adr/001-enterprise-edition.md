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

## Decision 1: Data Isolation Model

### Terminology

| Term | Meaning | Isolation Level |
|------|---------|-----------------|
| **org_id** | Logical organization identifier | Row-level (shared database) |
| **Tenant** | Physically isolated customer | Database-level (separate DB per customer) |

### Current State
The codebase uses `org_id` for **logical separation** within shared databases:
- GraphQL context: `context.org_id`
- Headers: `X-Org-Id`
- Database scoping: `WHERE org_id = :org_id`
- All orgs share the same ClickHouse and PostgreSQL instances

### Question
Should we support **tenant-level isolation** (separate databases per customer)?

### Options

| Option | Description | Isolation | Complexity |
|--------|-------------|-----------|------------|
| **A: org_id only** | All customers in shared DB, row-level isolation | Logical | Low |
| **B: Tenant isolation (v1)** | Dedicated DB per enterprise customer | Physical | High |
| **C: Tenant isolation (v2)** | Defer physical isolation to v2 | Logical for now | Medium |

### Trade-offs

**Logical isolation (org_id):**
- ✅ Simpler operations (one DB to manage)
- ✅ Efficient resource utilization
- ✅ Faster queries across shared infrastructure
- ❌ Noisy neighbor risk (one org's load affects others)
- ❌ Some compliance requirements mandate physical isolation
- ❌ Data breach affects all orgs in shared DB

**Physical isolation (Tenant):**
- ✅ Complete data isolation (compliance-friendly)
- ✅ Per-tenant performance guarantees
- ✅ Per-tenant backup/restore
- ✅ Can support customer-provided databases
- ❌ Operational complexity (many DBs to manage)
- ❌ Higher infrastructure cost
- ❌ Cross-tenant analytics harder

### Recommendation
**Option C: Keep org_id for v1, design for tenant isolation in v2**

Rationale:
- v1 focuses on shipping core functionality
- Current `org_id` pattern is working and well-tested
- Tenant isolation is an Enterprise-tier feature (can wait)
- Design data layer to be tenant-aware (connection routing) without implementing yet
- Track enterprise requests for physical isolation as signal

### Implementation Notes (for v2)
```
v2 Tenant Isolation Architecture:

┌─────────────────────────────────────────────────────────────┐
│                    Connection Router                         │
│  tenant_id → connection_string mapping                       │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Shared DB      │ │  Tenant A DB    │ │  Tenant B DB    │
│  (Community/    │ │  (Enterprise)   │ │  (Enterprise)   │
│   Team tiers)   │ │                 │ │                 │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### Decision
> **PENDING** - Awaiting team input

---

## Decision 2: Self-Hosted Datastore Support

### Question
Should self-hosted customers be able to bring their own ClickHouse/PostgreSQL in v1?

This is related to but distinct from tenant isolation:
- **Tenant isolation** = We provision separate DBs (managed by us)
- **BYOD (Bring Your Own Database)** = Customer provides their own DB infrastructure

### Options

| Option | Pros | Cons |
|--------|------|------|
| **A: Bundled only (v1)** | Simpler support, controlled environment | Less flexibility |
| **B: BYOD from v1** | Maximum flexibility, appeals to enterprise | Complex support matrix |
| **C: Defer BYOD to v2** | Ship faster, learn from v1 usage | May lose some enterprise deals |

### Considerations

**Bundled (our infrastructure):**
- SaaS: We manage ClickHouse and PostgreSQL
- Self-hosted: Ships with bundled databases (Docker Compose)
- Simpler to support, test, and maintain
- Consistent experience across deployments

**BYOD (customer infrastructure):**
- Enterprise customers may have existing ClickHouse/PostgreSQL
- Compliance requirements may mandate data stays in their environment
- Requires extensive compatibility testing (versions, configs)
- Support burden increases significantly

### Recommendation
**Option C: Defer BYOD to v2** with the following approach:

**v1 Self-Hosted:**
- Bundled PostgreSQL + ClickHouse (Docker Compose)
- Single supported configuration
- Document that BYOD is on roadmap

**v2 Self-Hosted (Enterprise):**
- Support customer-provided ClickHouse (with version requirements)
- Support customer-provided PostgreSQL (with version requirements)
- Compatibility matrix documented
- Premium support for BYOD configurations

**Design Principle:**
- Keep data layer swappable (already using sinks pattern)
- Use connection strings from environment (already done)
- Avoid hardcoding assumptions about DB location

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
| Data Isolation Model | Keep `org_id` (logical) for v1, tenant isolation (physical) in v2 | High |
| Self-hosted Datastore | Bundled for v1, BYOD in v2 | Medium |
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
| 2026-01-30 | Clarified Decision 1: org_id = logical isolation, tenant = physical DB isolation |
