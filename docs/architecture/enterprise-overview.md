# Enterprise Edition Architecture

> **Status**: Planning  
> **Tracking Issue**: [#299](https://github.com/full-chaos/dev-health-ops/issues/299)  
> **Design Decisions**: [ADR-001](adr/001-enterprise-edition.md)

## Overview

Dev Health Enterprise Edition adds authentication, authorization, and compliance features to the open-source core, enabling both multi-tenant SaaS and self-hosted enterprise deployments.

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           ENTERPRISE LAYER                                  │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌─────────────┐ │
│  │    Auth      │   │   License    │   │    Audit     │   │   Feature   │ │
│  │   Service    │   │   Service    │   │   Service    │   │    Gates    │ │
│  │              │   │              │   │              │   │             │ │
│  │ • NextAuth   │   │ • Tier check │   │ • Event log  │   │ • Per-tier  │ │
│  │ • JWT issue  │   │ • License    │   │ • Retention  │   │ • Per-org   │ │
│  │ • SSO/SAML   │   │   validate   │   │ • Export     │   │ • Override  │ │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬──────┘ │
│         │                  │                  │                  │        │
│         └──────────────────┼──────────────────┼──────────────────┘        │
│                            │                  │                           │
│                            ▼                  ▼                           │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │                      API LAYER (FastAPI)                           │   │
│  │                                                                    │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐    │   │
│  │  │  Auth Middleware │  │  Authz Middleware│  │ Rate Limiter   │    │   │
│  │  │  (JWT validate)  │  │  (RBAC check)    │  │ (per-org quota)│    │   │
│  │  └────────┬─────────┘  └────────┬─────────┘  └────────┬───────┘    │   │
│  │           │                     │                     │            │   │
│  │           └─────────────────────┼─────────────────────┘            │   │
│  │                                 ▼                                  │   │
│  │  ┌──────────────────────────────────────────────────────────────┐ │   │
│  │  │                    GraphQL + REST Endpoints                   │ │   │
│  │  │  • @require_auth      • @require_permission("read:metrics")  │ │   │
│  │  │  • @require_feature("investment_view")                        │ │   │
│  │  └──────────────────────────────────────────────────────────────┘ │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                           │
├───────────────────────────────────────────────────────────────────────────┤
│                           DATA LAYER                                       │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐           │
│  │   PostgreSQL    │  │   ClickHouse    │  │     Redis       │           │
│  │                 │  │                 │  │                 │           │
│  │ • users         │  │ • git_commits   │  │ • sessions      │           │
│  │ • organizations │  │ • metrics_*     │  │ • cache         │           │
│  │ • memberships   │  │ • work_items    │  │ • rate_limits   │           │
│  │ • audit_log     │  │ • work_graph    │  │ • pubsub        │           │
│  │ • settings      │  │                 │  │                 │           │
│  │ • licenses      │  │                 │  │                 │           │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘           │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│                           FRONTEND (Next.js)                               │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐           │
│  │   Auth Pages    │  │   Admin Pages   │  │   Analytics     │           │
│  │                 │  │                 │  │                 │           │
│  │ • /auth/login   │  │ • /admin/users  │  │ • /metrics      │           │
│  │ • /auth/logout  │  │ • /admin/settings│ │ • /investment   │           │
│  │ • /auth/signup  │  │ • /admin/audit  │  │ • /work         │           │
│  │                 │  │ • /admin/billing│  │ • /explore      │           │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘           │
│                                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │                      Middleware (route protection)                 │   │
│  │  • Check session → redirect to /auth/login if unauthenticated     │   │
│  │  • Check permissions → show 403 if unauthorized                    │   │
│  │  • Check feature access → show upgrade prompt if gated            │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

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
| **Customization** |
| Custom branding | ❌ | ❌ | ✅ |
| Custom domain | ❌ | ❌ | ✅ |
| **Support** |
| Community support (GitHub) | ✅ | ✅ | ✅ |
| Email support | ❌ | ✅ | ✅ |
| Priority support (SLA) | ❌ | ❌ | ✅ |

## Data Model (Enterprise Tables)

### Users & Organizations

```sql
-- Core identity
users (id, email, name, password_hash, auth_provider, ...)
organizations (id, name, slug, license_tier, settings, ...)
memberships (user_id, org_id, role, ...)

-- RBAC
permissions (name, description, category)
role_permissions (role, permission)
```

### Licensing

```sql
-- Feature flags
feature_flags (name, min_tier, description)
org_feature_overrides (org_id, feature_name, enabled, expires_at)
```

### Audit & Compliance

```sql
-- Audit logging
audit_log (id, timestamp, org_id, user_id, action, resource_type, ...)

-- Data retention
retention_policies (org_id, data_type, retention_days)
legal_holds (id, org_id, reason, created_at, released_at)
```

### SSO Configuration

```sql
-- SSO settings
sso_configs (org_id, provider_type, config, enabled, enforce_sso)
```

## Authentication Flow

### Standard Flow (Community/Team)

```
1. User visits /auth/login
2. Enters email + password
3. NextAuth validates credentials
4. JWT issued with claims: {sub, email, org_id, role}
5. JWT stored in httpOnly cookie
6. Subsequent requests include JWT
7. API validates JWT signature + expiry
8. GraphQL context populated with user info
```

### SSO Flow (Enterprise)

```
1. User visits /auth/login
2. Clicks "Login with SSO"
3. Redirect to IdP (Okta, Azure AD, etc.)
4. User authenticates at IdP
5. IdP redirects back with SAML assertion / OIDC token
6. NextAuth validates assertion
7. JIT provision user if new
8. JWT issued (same as standard flow)
9. Subsequent requests use same JWT flow
```

## Authorization (RBAC)

### Roles

| Role | Description | Typical Use |
|------|-------------|-------------|
| `owner` | Full control including billing and org deletion | Org creator, billing admin |
| `admin` | Manage users and settings, full data access | Team leads, IT admins |
| `editor` | Modify teams, run syncs, write access | DevOps engineers |
| `viewer` | Read-only access to analytics | Most users |

### Permission Categories

- `metrics`: read:metrics
- `teams`: read:teams, write:teams
- `settings`: read:settings, write:settings
- `admin`: manage:users, manage:billing, admin:org

### Enforcement

```python
# Decorator usage
@require_permission("read:metrics")
async def resolve_home(context, filters):
    ...

@require_feature("investment_view")
async def resolve_investment(context, filters):
    ...
```

## Deployment Options

### SaaS (Managed)

- Multi-tenant on shared infrastructure
- Automatic updates and maintenance
- All tiers available
- Data isolated by org_id

### Self-Hosted (Community)

- Docker Compose deployment
- Bundled PostgreSQL + ClickHouse
- Community tier features
- No license key required

### Self-Hosted (Enterprise)

- Same deployment as Community
- License key unlocks Enterprise features
- Optional: Connect to customer's own databases (v2)
- Priority support included

## Implementation Roadmap

| Phase | Scope | Issues | Notes |
|-------|-------|--------|-------|
| **P0** | **Settings Infrastructure** | [#306](https://github.com/full-chaos/dev-health-ops/issues/306) | **Start here** - enables integration setup |
| P1 | User + Org models | [#300](https://github.com/full-chaos/dev-health-ops/issues/300) | Foundation for auth |
| P2 | Authentication | [#301](https://github.com/full-chaos/dev-health-ops/issues/301) | Login/JWT |
| P3 | RBAC | [#302](https://github.com/full-chaos/dev-health-ops/issues/302) | Permissions |
| P4 | Admin UI | [#110](https://github.com/full-chaos/dev-health-web/issues/110) | Settings + user mgmt pages |
| P5 | Licensing | [#303](https://github.com/full-chaos/dev-health-ops/issues/303) | Feature gating |
| P6 | Enterprise Features | [#304](https://github.com/full-chaos/dev-health-ops/issues/304) | SSO, audit, retention |

### Why P0 First?

Settings infrastructure comes before authentication because:
1. **Integration setup** (GitHub/GitLab/Jira tokens) is needed for any useful deployment
2. **Worker configuration** is required for background jobs
3. Current **135+ env vars** make self-hosting painful
4. Settings service provides foundation for **per-org configuration**
5. Can be developed **independently** without auth dependency

## Related Documents

- [ADR-001: Enterprise Edition Design Decisions](adr/001-enterprise-edition.md)
- [Data Pipeline Architecture](data-pipeline.md)
- [GraphQL API Overview](../api/graphql-overview.md)
