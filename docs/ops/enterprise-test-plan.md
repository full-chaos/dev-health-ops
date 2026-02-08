# Enterprise Features Manual Test Plan

> **Document Status**: Active  
> **Last Updated**: 2026-02-05  
> **Related Issues**: [#338](https://github.com/full-chaos/dev-health-ops/issues/338), [#299](https://github.com/full-chaos/dev-health-ops/issues/299)

This document defines step-by-step manual test procedures for validating enterprise features across SaaS and self-hosted deployments.

---

## Table of Contents

1. [Test Environment Setup](#1-test-environment-setup)
2. [License Validation](#2-license-validation)
3. [RBAC Enforcement](#3-rbac-enforcement)
4. [SSO Authentication](#4-sso-authentication)
5. [Audit Logging](#5-audit-logging)
6. [Retention Policies](#6-retention-policies)
7. [IP Allowlisting](#7-ip-allowlisting)
8. [Limit Enforcement](#8-limit-enforcement)
9. [Admin Portal](#9-admin-portal)
10. [Deployment-Specific Tests](#10-deployment-specific-tests)

---

## 1. Test Environment Setup

### 1.1 Required Environment Variables

#### SaaS (Primary) Configuration

```bash
# Core SaaS configuration
export DATABASE_URI="clickhouse://localhost:8123/default"
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5432/devhealth"
export AUTH_SECRET="$(openssl rand -base64 32)"

# SSO test IdP (optional, for SSO tests)
export SSO_TEST_IDP_URL="https://idp.test.local"
```

#### Self-Hosted Testing Configuration

```bash
# License configuration (for self-hosted enterprise features)
export LICENSE_PUBLIC_KEY="<base64-encoded-ed25519-public-key>"
export LICENSE_KEY="<valid-license-key>"  # Generate from ops CLI (see below)
```

> **Note**: SaaS deployments manage entitlements via Stripe webhooks directly to `dev-health-ops` (no license key needed). License keys are for self-hosted testing only.

### 1.2 Test License Keys

> **SaaS Entitlements**: In SaaS deployments, entitlements are managed via Stripe webhooks sent directly to `dev-health-ops` at `/api/v1/billing/webhooks/stripe`. No license key environment variable is needed. The license key generation below is for **self-hosted testing only**.

Generate test licenses for each tier using the `dev-health-ops` CLI:

```bash
# Generate an Ed25519 keypair (one-time setup)
python -m dev_health_ops.cli admin licenses keygen

# Community tier (no license needed - default behavior)
unset LICENSE_KEY

# Team tier license
export LICENSE_KEY="$(python -m dev_health_ops.cli admin licenses create --tier team --org test-org --days 30)"

# Enterprise tier license
export LICENSE_KEY="$(python -m dev_health_ops.cli admin licenses create --tier enterprise --org test-org --days 30)"

# Expired license (for grace period testing)
export LICENSE_KEY="$(python -m dev_health_ops.cli admin licenses create --tier enterprise --org test-org --days -1)"

# Hard-expired license (past grace period)
export LICENSE_KEY="$(python -m dev_health_ops.cli admin licenses create --tier enterprise --org test-org --days -45)"
```

### 1.3 Test Data Setup

```bash
# Create admin user
python -m dev_health_ops.cli admin users create \
  --email admin@test.local \
  --password "TestPassword123!" \
  --superuser

# Create test organization
python -m dev_health_ops.cli admin orgs create \
  --name "Test Organization" \
  --owner-email admin@test.local \
  --tier enterprise

# Create additional test users
python -m dev_health_ops.cli admin users create \
  --email viewer@test.local \
  --password "ViewerPass123!"

python -m dev_health_ops.cli admin users create \
  --email editor@test.local \
  --password "EditorPass123!"
```

### 1.4 Start Services

**Docker Compose:**
```bash
cd dev-health-ops
docker compose up -d
alembic upgrade head
dev-hops api --db "$DATABASE_URI" --reload
```

**Kubernetes:**
```bash
helm install dev-health fullchaos/dev-health-platform \
  -f values-test.yaml \
  --namespace dev-health-test \
  --create-namespace
```

---

## 2. License Validation

### 2.1 Community Tier (No License)

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 2.1.1 | Unset `LICENSE_KEY` and restart API | API starts successfully | [ ] |
| 2.1.2 | Call `GET /api/v1/entitlements` | Returns `tier: "community"` | [ ] |
| 2.1.3 | Verify feature flags | `sso: false`, `audit_log: false`, `retention_policies: false` | [ ] |
| 2.1.4 | Verify limits | `users: 5`, `repos: 3`, `api_rate: 60` | [ ] |

### 2.2 Team Tier License

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 2.2.1 | Set valid Team license and restart | API starts, logs "License validated" | [ ] |
| 2.2.2 | Call `GET /api/v1/entitlements` | Returns `tier: "team"` | [ ] |
| 2.2.3 | Verify feature flags | `team_dashboard: true`, `sso: false`, `audit_log: false` | [ ] |
| 2.2.4 | Verify limits | `users: 25`, `repos: 20`, `api_rate: 300` | [ ] |

### 2.3 Enterprise Tier License

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 2.3.1 | Set valid Enterprise license and restart | API starts, logs "License validated" | [ ] |
| 2.3.2 | Call `GET /api/v1/entitlements` | Returns `tier: "enterprise"` | [ ] |
| 2.3.3 | Verify feature flags | All enterprise features `true` | [ ] |
| 2.3.4 | Verify limits | All limits `-1` (unlimited) | [ ] |

### 2.4 Grace Period Behavior

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 2.4.1 | Set expired license (within grace period) | API starts with warning banner | [ ] |
| 2.4.2 | Call `GET /api/v1/entitlements` | `in_grace_period: true`, features still active | [ ] |
| 2.4.3 | Check audit logs | "license_grace_period_entered" event logged | [ ] |
| 2.4.4 | Set hard-expired license | API starts, downgrades to community | [ ] |
| 2.4.5 | Call enterprise-gated endpoint | Returns 402 Payment Required | [ ] |

### 2.5 Invalid License Handling

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 2.5.1 | Set malformed license key | API starts, falls back to community | [ ] |
| 2.5.2 | Set license with invalid signature | API starts, falls back to community | [ ] |
| 2.5.3 | Set license with wrong issuer | API starts, falls back to community | [ ] |
| 2.5.4 | Check logs | "License validation failed" with error details | [ ] |

---

## 3. RBAC Enforcement

### 3.1 Role Definitions

| Role | Permissions | Test User |
|------|-------------|-----------|
| `owner` | Full control, billing, org deletion | admin@test.local |
| `admin` | Manage users, settings, full data | - |
| `editor` | Modify teams, run syncs, write access | editor@test.local |
| `viewer` | Read-only analytics | viewer@test.local |

### 3.2 Permission Checks

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 3.2.1 | Login as `viewer@test.local` | Login succeeds, JWT includes `role: viewer` | [ ] |
| 3.2.2 | Call `GET /api/v1/metrics` | Returns 200 OK with data | [ ] |
| 3.2.3 | Call `POST /api/v1/admin/settings` | Returns 403 Forbidden | [ ] |
| 3.2.4 | Call `POST /api/v1/admin/users` | Returns 403 Forbidden | [ ] |

### 3.3 Role Assignment

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 3.3.1 | Login as owner | Login succeeds | [ ] |
| 3.3.2 | Add viewer@test.local to org as editor | `POST /api/v1/admin/orgs/{id}/memberships` succeeds | [ ] |
| 3.3.3 | Login as viewer (now editor) | JWT shows updated role | [ ] |
| 3.3.4 | Test editor permissions | Can run syncs, cannot manage users | [ ] |

### 3.4 Ownership Transfer

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 3.4.1 | Transfer ownership from admin to editor | `POST /api/v1/admin/orgs/{id}/transfer-ownership` succeeds | [ ] |
| 3.4.2 | Verify old owner demoted | Previous owner now has `admin` role | [ ] |
| 3.4.3 | Verify new owner promoted | New user has `owner` role | [ ] |

---

## 4. SSO Authentication

> **Prerequisite**: Enterprise license with `sso: true` feature enabled

### 4.1 SAML Provider Setup

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 4.1.1 | Call `POST /api/v1/auth/sso/providers` with SAML config | Provider created, status: pending | [ ] |
| 4.1.2 | Configure IdP with SP metadata | IdP accepts metadata | [ ] |
| 4.1.3 | Activate provider | `POST /sso/providers/{id}/activate` returns active provider | [ ] |
| 4.1.4 | List providers | `GET /api/v1/auth/sso/providers` shows active provider | [ ] |

**SAML Provider Create Payload:**
```json
{
  "name": "Test SAML IdP",
  "protocol": "saml",
  "saml_config": {
    "entity_id": "https://idp.test.local/metadata",
    "sso_url": "https://idp.test.local/sso",
    "certificate": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "attribute_mapping": {
      "email": "email",
      "full_name": "displayName"
    }
  },
  "auto_provision_users": true,
  "default_role": "viewer"
}
```

### 4.2 SAML Login Flow

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 4.2.1 | Initiate SAML login | `POST /api/v1/auth/sso/saml/init` returns redirect URL | [ ] |
| 4.2.2 | Authenticate at IdP | User authenticates successfully | [ ] |
| 4.2.3 | Process SAML callback | `POST /api/v1/auth/sso/saml/acs` returns JWT | [ ] |
| 4.2.4 | Verify user created | New user exists with SSO link | [ ] |
| 4.2.5 | Verify JWT claims | JWT contains org_id, role, email | [ ] |

### 4.3 OIDC Provider Setup

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 4.3.1 | Call `POST /api/v1/auth/sso/providers` with OIDC config | Provider created | [ ] |
| 4.3.2 | Verify discovery endpoint fetched | Provider has token_endpoint, userinfo_endpoint | [ ] |
| 4.3.3 | Activate provider | Provider status becomes active | [ ] |

**OIDC Provider Create Payload:**
```json
{
  "name": "Test OIDC IdP",
  "protocol": "oidc",
  "oidc_config": {
    "client_id": "dev-health-client",
    "client_secret": "test-secret",
    "issuer": "https://oidc.test.local",
    "scopes": ["openid", "email", "profile"]
  },
  "auto_provision_users": true,
  "default_role": "viewer"
}
```

### 4.4 OIDC Login Flow

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 4.4.1 | Initiate OIDC login | `POST /api/v1/auth/sso/oidc/init` returns auth URL | [ ] |
| 4.4.2 | Authenticate at IdP | User authenticates, grants consent | [ ] |
| 4.4.3 | Process OIDC callback | `POST /api/v1/auth/sso/oidc/callback` with code | [ ] |
| 4.4.4 | Verify token exchange | Access token obtained, user info fetched | [ ] |
| 4.4.5 | Verify JWT issued | User logged in, JWT returned | [ ] |

### 4.5 SSO Edge Cases

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 4.5.1 | SSO login without enterprise license | Returns 402 Payment Required | [ ] |
| 4.5.2 | SAML response with invalid signature | Returns 401, login fails | [ ] |
| 4.5.3 | OIDC callback with invalid state | Returns 400, login fails | [ ] |
| 4.5.4 | User from disallowed domain | Returns 403 if domain filtering enabled | [ ] |
| 4.5.5 | SSO provider disabled | Login redirects to standard auth | [ ] |

---

## 5. Audit Logging

> **Prerequisite**: Enterprise license with `audit_log: true` feature enabled

### 5.1 Audit Event Generation

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 5.1.1 | User logs in | Audit event: `user.login` | [ ] |
| 5.1.2 | User created | Audit event: `user.created` | [ ] |
| 5.1.3 | Setting changed | Audit event: `setting.updated` with changes | [ ] |
| 5.1.4 | SSO provider configured | Audit event: `sso_provider.created` | [ ] |
| 5.1.5 | License validated | Audit event: `license_validated` | [ ] |

### 5.2 Audit Log Queries

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 5.2.1 | List all logs | `GET /api/v1/admin/audit-logs` returns paginated results | [ ] |
| 5.2.2 | Filter by user | `?user_id=xxx` returns only that user's actions | [ ] |
| 5.2.3 | Filter by action | `?action=user.login` returns login events | [ ] |
| 5.2.4 | Filter by date range | `?start_date=...&end_date=...` works | [ ] |
| 5.2.5 | Filter by resource | `?resource_type=user&resource_id=xxx` works | [ ] |

### 5.3 Audit Log Details

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 5.3.1 | Get single audit log | `GET /api/v1/admin/audit-logs/{id}` returns full details | [ ] |
| 5.3.2 | Verify changes captured | `changes` field shows before/after values | [ ] |
| 5.3.3 | Verify request metadata | IP address, user agent captured | [ ] |
| 5.3.4 | Get resource history | `GET /api/v1/admin/audit-logs/resource/{type}/{id}` works | [ ] |
| 5.3.5 | Get user activity | `GET /api/v1/admin/audit-logs/user/{id}` works | [ ] |

### 5.4 Audit Feature Gating

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 5.4.1 | Query audit logs without enterprise license | Returns 402 Payment Required | [ ] |
| 5.4.2 | Events still logged internally | Logs contain audit entries | [ ] |
| 5.4.3 | Upgrade to enterprise | Audit history becomes accessible | [ ] |

---

## 6. Retention Policies

> **Prerequisite**: Enterprise license with `retention_policies: true` feature enabled

### 6.1 Policy Management

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 6.1.1 | List available resource types | `GET /api/v1/admin/retention-policies/resource-types` | [ ] |
| 6.1.2 | Create retention policy | `POST /api/v1/admin/retention-policies` succeeds | [ ] |
| 6.1.3 | List policies | `GET /api/v1/admin/retention-policies` shows new policy | [ ] |
| 6.1.4 | Update policy | `PATCH /api/v1/admin/retention-policies/{id}` updates retention_days | [ ] |
| 6.1.5 | Deactivate policy | Set `is_active: false`, policy stops running | [ ] |

**Create Policy Payload:**
```json
{
  "resource_type": "audit_log",
  "retention_days": 90,
  "description": "Keep audit logs for 90 days"
}
```

### 6.2 Policy Execution

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 6.2.1 | Manually execute policy | `POST /api/v1/admin/retention-policies/{id}/execute` | [ ] |
| 6.2.2 | Verify records deleted | Response shows `deleted_count` | [ ] |
| 6.2.3 | Verify policy updated | `last_run_at`, `last_run_deleted_count` updated | [ ] |
| 6.2.4 | Delete policy | `DELETE /api/v1/admin/retention-policies/{id}` | [ ] |

### 6.3 Legal Hold (Future)

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 6.3.1 | Create legal hold | Policy execution skips affected data | [ ] |
| 6.3.2 | Release legal hold | Data becomes eligible for deletion | [ ] |

---

## 7. IP Allowlisting

> **Prerequisite**: Enterprise license with `ip_allowlist: true` feature enabled

### 7.1 Allowlist Management

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 7.1.1 | Add IP range | `POST /api/v1/admin/ip-allowlist` with CIDR | [ ] |
| 7.1.2 | List entries | `GET /api/v1/admin/ip-allowlist` shows entries | [ ] |
| 7.1.3 | Update entry | `PATCH /api/v1/admin/ip-allowlist/{id}` works | [ ] |
| 7.1.4 | Deactivate entry | Set `is_active: false` | [ ] |
| 7.1.5 | Delete entry | `DELETE /api/v1/admin/ip-allowlist/{id}` | [ ] |

**Add IP Range Payload:**
```json
{
  "ip_range": "192.168.1.0/24",
  "description": "Office network"
}
```

### 7.2 IP Checking

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 7.2.1 | Check allowed IP | `POST /api/v1/admin/ip-allowlist/check` returns `allowed: true` | [ ] |
| 7.2.2 | Check blocked IP | Returns `allowed: false` | [ ] |
| 7.2.3 | Check with expired entry | Expired entries not considered | [ ] |

---

## 8. Limit Enforcement

### 8.1 User Limits

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 8.1.1 | Community tier: add 6th user | Returns 402 with `limit_exceeded` | [ ] |
| 8.1.2 | Team tier: add 26th user | Returns 402 with `limit_exceeded` | [ ] |
| 8.1.3 | Enterprise tier: unlimited users | User creation succeeds | [ ] |
| 8.1.4 | Check audit log | `limit_exceeded` event logged | [ ] |

### 8.2 Repository Limits

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 8.2.1 | Community tier: sync 4th repo | Returns 402 with `limit_exceeded` | [ ] |
| 8.2.2 | Team tier: sync 21st repo | Returns 402 with `limit_exceeded` | [ ] |
| 8.2.3 | Enterprise tier: unlimited repos | Sync succeeds | [ ] |

### 8.3 API Rate Limits

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 8.3.1 | Community tier: exceed 60 req/min | Returns 429 Too Many Requests | [ ] |
| 8.3.2 | Team tier: exceed 300 req/min | Returns 429 | [ ] |
| 8.3.3 | Enterprise tier: no rate limit | All requests succeed | [ ] |

---

## 9. Admin Portal

### 9.1 User Management

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 9.1.1 | List users | `GET /api/v1/admin/users` returns all org users | [ ] |
| 9.1.2 | Create user | `POST /api/v1/admin/users` creates user | [ ] |
| 9.1.3 | Update user | `PATCH /api/v1/admin/users/{id}` updates user | [ ] |
| 9.1.4 | Set password | `POST /api/v1/admin/users/{id}/set-password` works | [ ] |
| 9.1.5 | Deactivate user | User cannot log in | [ ] |

### 9.2 Organization Management

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 9.2.1 | Get org details | `GET /api/v1/admin/orgs/{id}` returns org | [ ] |
| 9.2.2 | Update org | `PATCH /api/v1/admin/orgs/{id}` updates settings | [ ] |
| 9.2.3 | List memberships | `GET /api/v1/admin/orgs/{id}/memberships` works | [ ] |
| 9.2.4 | Add member | `POST /api/v1/admin/orgs/{id}/memberships` works | [ ] |
| 9.2.5 | Remove member | `DELETE /api/v1/admin/memberships/{id}` works | [ ] |

### 9.3 Settings Management

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 9.3.1 | List setting categories | `GET /api/v1/admin/settings/categories` | [ ] |
| 9.3.2 | Get settings by category | `GET /api/v1/admin/settings/{category}` | [ ] |
| 9.3.3 | Set a setting | `PUT /api/v1/admin/settings/{category}/{key}` | [ ] |
| 9.3.4 | Set encrypted setting | Setting stored encrypted, displayed as `[ENCRYPTED]` | [ ] |
| 9.3.5 | Delete setting | `DELETE /api/v1/admin/settings/{category}/{key}` | [ ] |

### 9.4 Integration Credentials

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 9.4.1 | Add GitHub credential | `POST /api/v1/admin/credentials` stores token | [ ] |
| 9.4.2 | Test connection | `POST /api/v1/admin/credentials/{id}/test` validates token | [ ] |
| 9.4.3 | List credentials | Secrets masked in response | [ ] |
| 9.4.4 | Update credential | Token updated, old token invalidated | [ ] |
| 9.4.5 | Delete credential | Credential removed | [ ] |

---

## 10. Deployment-Specific Tests

> **SaaS (Primary)**: Sections 10.1 (Docker Compose) and 10.2 (Kubernetes) cover the primary SaaS deployment paths. Section 10.3 covers self-hosted enterprise deployments.

### 10.1 Docker Compose

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 10.1.1 | Fresh install | `docker compose up -d` starts all services | [ ] |
| 10.1.2 | Migrations run | `alembic upgrade head` completes | [ ] |
| 10.1.3 | API accessible | `curl http://localhost:8000/health` returns OK | [ ] |
| 10.1.4 | Web accessible | `curl http://localhost:3000` returns HTML | [ ] |
| 10.1.5 | Grafana accessible | `curl http://localhost:3001` returns login page | [ ] |
| 10.1.6 | License applied | Restart with LICENSE_KEY env var works | [ ] |
| 10.1.7 | Data persists | Restart containers, data still present | [ ] |

### 10.2 Kubernetes (Helm)

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 10.2.1 | Helm install | `helm install` completes without errors | [ ] |
| 10.2.2 | Pods running | All pods reach Ready state | [ ] |
| 10.2.3 | Ingress configured | External URL accessible | [ ] |
| 10.2.4 | License via secret | Create Kubernetes secret, pods pick up license | [ ] |
| 10.2.5 | Rolling update | `helm upgrade` completes without downtime | [ ] |
| 10.2.6 | Horizontal scaling | Increase replicas, load balanced | [ ] |

### 10.3 Self-Hosted Enterprise

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 10.3.1 | Offline installation | No internet required after images pulled | [ ] |
| 10.3.2 | License offline validation | Public key embedded, no phone-home | [ ] |
| 10.3.3 | Custom database | Connect to external PostgreSQL/ClickHouse | [ ] |
| 10.3.4 | TLS termination | Works behind reverse proxy with TLS | [ ] |

---

## Test Result Summary

| Section | Tests | Passed | Failed | Blocked |
|---------|-------|--------|--------|---------|
| License Validation | 16 | | | |
| RBAC Enforcement | 11 | | | |
| SSO Authentication | 20 | | | |
| Audit Logging | 14 | | | |
| Retention Policies | 9 | | | |
| IP Allowlisting | 7 | | | |
| Limit Enforcement | 10 | | | |
| Admin Portal | 19 | | | |
| Deployment Tests | 13 | | | |
| **Total** | **119** | | | |

---

## Pass/Fail Criteria

- **Pass**: All steps complete successfully with expected results
- **Fail**: Any step produces unexpected result or error
- **Blocked**: Cannot execute due to missing dependency or environment issue

### Overall Pass Criteria

- All critical path tests (licensing, auth, RBAC) must pass
- No security-related tests may fail
- At least 95% of total tests must pass

---

## Related Documentation

- [Enterprise Overview](../architecture/enterprise-overview.md)
- [Licensing Architecture](../architecture/licensing.md)
- [SSO Setup Guide](./sso-setup.md) (TODO)
- [ADR-001: Enterprise Edition](../architecture/adr/001-enterprise-edition.md)
- [Self-Hosted Quickstart](../self-hosted-quickstart.md)
