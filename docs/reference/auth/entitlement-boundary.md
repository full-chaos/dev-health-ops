---
page_id: ref-auth-entitlement-boundary
summary: Where entitlement ends and authorization begins - the two independent gates, why no entitlement travels in a token or a decision today, the bounded entitlement adapter, and what is not built yet.
content_type: reference
owner: engineering
source_of_truth:
  - docs/contribute/architecture/auth-control-plane-adrs.md
  - internal/auth/entitlement/entitlement.go
  - internal/auth/entitlement/source_pg.go
  - internal/auth/entitlement/deps_test.go
  - internal/auth/decisioncache/cache.go
  - internal/api/licensing/orgfeature.go
  - contracts/auth/v1/endpoint-profile.schema.json
  - contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json
applicability: current
lifecycle: active
---

# Entitlement boundary reference

Authorization answers "may this principal take this action on this resource". Entitlement answers "does this organization's plan include this product feature". They are two gates. Both must pass, and neither grants what the other grants.
{: .fc-page-lede }

The rules come from ACP-ADR-07 in the [Auth Control Plane ADRs](../../contribute/architecture/auth-control-plane-adrs.md). This page states the rules, then what is on `main` and what is not.

!!! warning "The authorization decision does not check entitlement yet"
    No decision on `main` reads an entitlement. The `authorization_decision.v1` schema has no entitlement obligation, and no auth package imports the entitlement adapter. Product gates run in the licensing code of each service, as before. Read [Status](#status) before you rely on this page.

## Rules

| Rule | Meaning |
| --- | --- |
| Billing and licensing are the authority | The control plane holds a bounded projection with a source revision and an expiry. It is never a second source of truth. |
| Entitlement is an input, never a claim | An authorization decision may carry an entitlement-derived obligation. An access token may not carry an entitlement. The envelope's `tier` and `licensed_features` are deprecated at its next schema version. |
| Two independent gates | A paid entitlement grants no action. A role grants no product. `agent_context_runtime` stays a product entitlement, independent of ACR credential scope and of a repository grant. |
| Fail closed | An unavailable or expired entitlement state denies a gated action. The community tier is the model of "closed": a defined minimum, not a denial of service. |

## What a client does today

A resource service that gates a product feature must pass two gates (G-6):

1. It asks the decision point for the action. See [decision clients](decision-clients.md).
2. It checks the feature with its own licensing code. In go-api this is `licensing.OrgHasFeature` (`internal/api/licensing/orgfeature.go`): an `org_licenses` row decides first, by its tier and its `features_override`; with no row, `organizations.tier` decides; an unknown tier or an invalid organization id denies. A database error is returned and the caller denies.

A deny from the first gate is final. An allow from it does not mean the feature is licensed, and a licensed feature does not mean the action is allowed.

## The entitlement adapter

`internal/auth/entitlement` is the auth service's bounded view of an organization's plan.

| Property | Value |
| --- | --- |
| Source | `PGSource` reads `public.org_licenses` and `public.organizations`, the columns the auth runtime role may select. With no `org_licenses` row, it falls back to `organizations.tier`. With no row in either table, or with an unknown tier, it holds nothing (`internal/auth/entitlement/source_pg.go`, `Load`). A database error is returned, never read as "no entitlement". |
| Snapshot | Per organization: tier, features, source revision, fetched time, expiry. |
| Staleness bound | `MaxTTL` is 60 s. A longer TTL is refused. A failed refresh never extends a snapshot. |
| Size | 4096 organizations by default, least recently used first. Refreshes of one organization share one read. |
| Check | `Check(ctx, org, feature)` answers `entitled`, `entitlement_not_granted` or `entitlement_unavailable`. Without a valid snapshot it denies with `entitlement_unavailable`. |

These three reasons are not decision reason codes. They are not in `decision_reason_code` ([reason codes](reason-codes.md)), and a decision with one of them is invalid.

Three tests hold the boundary in code (`internal/auth/entitlement/deps_test.go`):

- The adapter does not import `internal/auth/authorization`, and `authorization` does not import the adapter.
- The adapter imports no token, session, credential or claims package.
- No package under `internal/auth` imports the adapter. A search of `internal/auth/authapi`, `authruntime` and `authorization` finds no call to it, so no path from a decision reaches it today.

## Status

| Part | State |
| --- | --- |
| Adapter and Postgres source | On `main` as a library. Nothing in the authorize path calls it. |
| Entitlement-derived obligation | Not built. ACP-ADR-07 allows one. The decision schema defines none, so adding one is a schema change ([obligations](obligations.md#status)). |
| Entitlement revision in the decision cache key | The key type has the field. The authorize route does not set it (`internal/auth/authapi/authorize_cache.go:123-133`), so it is 0, and no entitlement aggregate or feed event exists yet. When one exists it becomes a dependency like the others. |
| `entitlement_requirement` of an endpoint profile | The field exists in the profile schema. No row of the go-api, query-api or auth-service profiles has a value on `main`. See [endpoint profiles](endpoint-profiles.md#absent-versus-null). |
| Entitlement in tokens | The query-api principal envelope still has `tier` and `licensed_features` (`internal/queryapi/principal/claims.go`). ADR-07 removes them at the next version of the envelope. Until then the 60 s token lifetime is the compensating control. |
| Billing and licensing enforcement point | Planned, CHAOS-10862. |

When an entitlement obligation or an entitlement-aware decision lands, this page is the one to change first: the Status table, then the rules that moved from "planned" to "enforced".
