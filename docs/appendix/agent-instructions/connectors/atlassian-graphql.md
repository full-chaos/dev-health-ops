# Atlassian GraphQL Gateway (AGG) Client

## Overview

The `atlassian/` directory implements a **production-grade Atlassian GraphQL Gateway client** for Jira and related Atlassian products.

**Core principle:** Schema-driven, not guess-driven.

---

## Architecture

### 1. API Layer (Fast-moving, Generated)

**Purpose:** Represent Atlassian GraphQL schema as it exists today.

**Locations:**
- `python/atlassian/graph/gen/`
- `python/atlassian/rest/gen/`
- `go/atlassian/graph/gen/`
- `go/atlassian/rest/gen/`

**Source:** `graphql/schema.introspection.json`

**Rules:**
- Never hand-edit generated files
- Regeneration must be deterministic
- Missing schema fields → generator failure

### 2. Transport & Client Layer (Stable, Defensive)

**Purpose:** Safely execute GraphQL operations.

**Locations:**
- `python/atlassian/graph/client.py`
- `python/atlassian/rest/client.py`
- `go/atlassian/graph/client.go`
- `go/atlassian/rest/client.go`

**Responsibilities:**
- Authentication
- Rate limiting
- Retries (429 only)
- Logging
- Strict vs non-strict error handling
- Beta headers (`X-ExperimentalApi`)

### 3. Canonical Analytics Layer (Slow-moving, Stable)

**Purpose:** Define data model for developer health metrics.

**Source of truth:** `openapi/jira-developer-health.canonical.openapi.yaml`

**Characteristics:**
- API-agnostic
- Versioned by this repo
- Backward-compatible
- Designed for analytics, not transport

**Examples:**
- JiraUser
- JiraProject
- JiraIssue
- JiraChangelogEvent
- JiraWorklog

**Rules:**
- Must NOT leak API shapes (edges, nodes, cursors)
- IDs are strings
- Timestamps are RFC3339
- Prefer optional fields over brittle requirements

### 4. Mapping Layer (Explicit, Validated)

**Purpose:** Convert API models → canonical analytics models.

**Locations:**
- `python/atlassian/graph/mappers/`
- `python/atlassian/rest/mappers/`
- `go/atlassian/graph/mappers/`
- `go/atlassian/rest/mappers/`

**Rules:**
- Required canonical fields MUST be validated
- Missing required data → explicit error
- No implicit defaults for semantic fields
- Positive conditionals preferred
- No business logic — mapping only

---

## Rate Limiting (Non-Negotiable)

Atlassian AGG uses **cost-based rate limiting**:

| Parameter | Value |
|-----------|-------|
| Default budget | 10,000 points/minute |
| Enforcement | HTTP 429 |
| `Retry-After` format | **TIMESTAMP** (not seconds) |

### Required Behavior

```
Retry ONLY on HTTP 429
Compute wait = retry_after_timestamp - now
Cap wait using MaxWait
After MaxRetries429 → fail with RateLimitError
Do NOT retry 4xx (except 429)
Do NOT retry >= 500
```

**Violating this will get your code reverted.**

---

## Pagination Rules

- Assume **every connection paginates**
- Support `pageInfo.hasNextPage` and `pageInfo.endCursor`
- Handle nested pagination (e.g., projects → opsgenie teams)
- Never assume single page
- Never hardcode page sizes

---

## Authentication

### Supported Modes

| Mode | Header | Use Case |
|------|--------|----------|
| OAuth Bearer | `Authorization: Bearer <token>` | Primary |
| Basic Auth | `Authorization: Basic <base64>` | Tenant gateway |
| Cookie Auth | `Cookie: <value>` | Explicit opt-in |

### Rules

- Auth MUST be injectable
- Never hardcode tokens
- Never log auth headers or cookies
- Tests MUST mock auth

---

## Testing Contract

### Unit Tests

Must mock HTTP and cover:
- Pagination
- Rate limiting (429)
- Beta headers
- Mapping validation
- Error paths

### Integration Tests

- Must be env-gated
- Skip cleanly if env vars missing
- Do NOT intentionally trigger rate limits
- Assert shape, not volume

### Required Env Vars (Integration)

- `ATLASSIAN_GQL_BASE_URL`
- One of:
  - `ATLASSIAN_OAUTH_ACCESS_TOKEN`
  - `ATLASSIAN_EMAIL` + `ATLASSIAN_API_TOKEN`
  - `ATLASSIAN_COOKIES_JSON`

---

## AI Agent Rules

If you are an AI agent:

| DO | DON'T |
|----|-------|
| Fetch introspection | Invent GraphQL fields |
| Inspect schema | Assume schema stability |
| Generate models | Collapse API → analytics models |
| Map explicitly | Remove rate-limiting safeguards |
| | Weaken error handling |
| | Introduce silent fallbacks |

**If schema details are unclear:**

1. Fetch introspection
2. Inspect schema
3. Generate models
4. Map explicitly

**Guessing is a failure.**

---

## Schema Evolution

### When Schema Changes

1. Fetch new introspection
2. Regenerate API models
3. Update mappers if needed
4. Update tests
5. Verify canonical layer unchanged (or version bump if changed)

### Canonical Layer Changes

- Require explicit versioning
- Maintain backward compatibility when possible
- Document breaking changes
- Update all downstream consumers

---

## What This Repo Is NOT

- Not a thin demo client
- Not a static schema wrapper
- Not Jira-only
- Not tolerant of silent data corruption

---

## Authority

If there is conflict between code comments, README, and `AGENTS.md`:

**AGENTS.md wins.**

If unsure:
- Preserve correctness
- Preserve explicitness
- Preserve future schema evolution
