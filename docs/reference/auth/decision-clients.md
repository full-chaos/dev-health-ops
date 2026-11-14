---
page_id: ref-auth-decision-clients
summary: The Go (dev-health-go pep) and TypeScript (dev-health-web) clients of the authorize API - how to build a request, how every failure becomes a deny, how to read a decision and its obligations, and the rules of the Go allow cache.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/openapi/auth-v1.openapi.json
  - contracts/auth/v1/jsonschema/authorization_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_response.v1.schema.json
  - internal/auth/authorization/grant.go
  - internal/auth/authapi/authorize_wire.go
  - go.mod
applicability: current
lifecycle: active
---

# Authorization decision clients reference

Two clients ask the auth-service decision point the question of [authorization decisions](obligations.md): the Go package `pep` of `github.com/full-chaos/dev-health-go` and the server-only TypeScript module `web:src/lib/auth/authorize` of `dev-health-web`. Both deny on every failure.
{: .fc-page-lede }

Their source is in the `dev-health-go` and `dev-health-web` repositories, not in this one. `go.mod` here pins `dev-health-go` at v0.46.0. The facts on this page are from that tag and from `dev-health-web` main.

## The API

| Call | Request document | Response document |
| --- | --- | --- |
| `POST /v1/authorize` | `authorization_request.v1` | `authorization_decision.v1` |
| `POST /v1/authorize:batch` | `authorization_batch_request.v1` (1 to 100 requests) | `authorization_batch_response.v1` (one decision per request, each with its zero-based `index`) |
| `POST /v1/authorize:repositories` | `authorization_request.v1` with `resource` `{type: repository, key: "*", organization_id}` and `acr.context.read` or `acr.evidence.read` | `authorization_decision.v1`: the repositories the principal may read under that action, the same set its per-key decisions allow: an allow carries `repository_scope` `organization` (every repository) or `repository_allowlist` (the principal's own keys, at most 256); more keys is the deny `repository_allowlist_too_large`. |

The caller sends the workload token of its service for the auth-service audience. The request fields, the closed resource types and the decision members are in [authorization decisions and obligations](obligations.md). The OpenAPI document lists the failure statuses 400, 401, 413, 429 and 503. A 503 is also the answer when the decision point could not read the state it needs ([reason codes](reason-codes.md#state-failures-are-not-decisions)). A failure is never a decision document.

A request has these limits in both clients:

| Field | Rule |
| --- | --- |
| `principal_reference` | `^[A-Za-z0-9._~-]{16,1000}$`. Opaque. The decision point resolves it. |
| `action` | `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$` and a catalog action. |
| `resource.type` | One of the ten closed types. |
| `resource.key` | A repository: `owner/repo` or `*`. Every other type: `^[a-z0-9][a-z0-9._:-]{0,255}$`. |
| `resource.organization_id` | The resource's organization. Empty for a platform resource. The decision point verifies it. |
| `context.request_id` | `^req_[A-Za-z0-9_-]{26}$`. Names one call for the client. The decision point does not store it: the audit row of a high-risk decision carries the server-minted request id from `requestid.Audit(ctx)` (`internal/auth/authapi/authorize_audit.go:70`). A low-risk decision writes no audit row (`highRiskAction`, `internal/auth/authapi/authorize_audit.go:19`, read by `auditDecisions`, `internal/auth/authapi/authorize_audit.go:36`). The response never echoes it. |
| `context.client_service` | A label from a closed set. It confers nothing. The Go set is `dev-health-ops-api`, `dev-health-ops-billing-edge`, `dev-health-web`, `dev-health-acr-api`, `dev-health-acr-mcp`, `dev-health-query-api` and `dev-health-auth-service`. The TypeScript client sends `dev-health-web`. |

A request that fails these rules is a local deny. It is never sent.

## Two kinds of deny

A caller must tell "the decision point said no" from "I could not get an answer". Both clients keep them apart. Both are a deny.

| | Decision point verdict | Local deny |
| --- | --- | --- |
| Go | `Decision.Local()` is false. `Reason()` is a wire code. | `Decision.Local()` is true. `Reason()` starts with `pdp_`. `Err()` is a `*pep.Error`. |
| TypeScript | `{ allowed: false, source: "pdp", decision }` | `{ allowed: false, source: "client", reason }` |

The local reasons are in [reason codes](reason-codes.md#local-reasons-of-the-go-client).

## Go client (`pep`)

### Build the client

`pep.New(pep.Config)` checks the configuration and returns an error at start. A bad configuration is a startup failure, not a runtime deny.

| Field | Rule |
| --- | --- |
| `BaseURL` | Required. `https` with a host and a port only. No path, query, fragment or user. `http` is refused unless `AllowPlainHTTP` is true (tests and loopback). |
| `HTTPClient` | Required. It must carry the caller's workload token for the auth-service audience. `New` copies it and refuses redirects on the copy. |
| `ClientService` | Required. A value of the closed set above. |
| `Timeout` | Optional. Default 2 s, on top of the caller's context. Negative is refused. |
| `Now` | Optional. The clock that decides whether a decision is cacheable. Default `time.Now`. |

### Ask

`Client` implements `pep.Decider`:

```go
type Decider interface {
    Decide(ctx context.Context, req Request) Decision
    DecideBatch(ctx context.Context, reqs []Request) []Decision
}
```

Neither method returns an error. There is nothing a caller can drop and then read a zero value as an allow. The zero `Decision` denies, a nil `*Client` denies, and a `Decision` has no exported field. The only allowed `Decision` is one built from a document that passed every check.

`DecideBatch` takes up to `pep.MaxBatch` (100) requests and returns one `Decision` per request, in order. An empty slice makes no call. A batch over 100, or with one invalid request, is denied whole and never sent. A failure of the call, one bad item or one bad binding in the response denies every item.

### Failure to reason

| Event | Local reason |
| --- | --- |
| HTTP 200 with `application/json` | The body is parsed (below). |
| HTTP 429 | `pdp_rate_limited` |
| HTTP 5xx | `pdp_unavailable` |
| Any other status | `pdp_refused` |
| Any other media type | `pdp_malformed` |
| Body over 256 KiB (single) or 4 MiB (batch) | `pdp_too_large` |
| Context canceled, or deadline passed | `pdp_canceled`, `pdp_timeout` |
| Transport error | `pdp_unreachable` (a timeout is `pdp_timeout`) |

The transport error never reaches the caller. It can hold the URL or the host.

### Parse

The parse rules are those of [authorization decisions](obligations.md#parsing-rules-in-the-go-client): strict JSON with no repeated member at any depth, no data after the document, `allowed` a JSON boolean that agrees with the reason code, integer tokens for the policy revision and the batch index, and closed obligations. An unknown obligation, redaction class or assurance level denies the whole decision (`pdp_unknown_obligation`). A decision whose organization is not the request's is `pdp_mismatch`.

### Read a decision

| Method | Meaning |
| --- | --- |
| `Allowed()` | True only for a valid allow. |
| `Reason()` | A wire code or a local reason. |
| `Local()`, `Err()` | A local deny and its typed cause. |
| `DecisionID()`, `PrincipalID()`, `OrganizationID()` | Members of the decision. |
| `PolicyRevision()` | The revision of the active policy set. |
| `ExpiresAt()`, `Cacheable()` | Reuse bound. `Cacheable()` is false for a decision whose `expires_at` is not after the time it was read. Use it once. |
| `Obligations()` | The obligations of an allow. The caller must enforce them. The zero value grants nothing. |

`Obligations` has `TenantScope()` and `TenantAllows(org)`, `RepositoryScopeOrganization()`, `RepositoryRestricted()`, `RepositoryAllowlist()` and `RepositoryAllowed(key)`, `Redaction()` and `Redacts(class)`, `MinimumAssurance()` and `AssuranceSatisfiedBy(level)`, and `StepUpRequired()`. See [obligations](obligations.md#obligations) for what each means. `RepositoryAllowed(key)` is false when the allow carries neither `repository_scope` nor `repository_allowlist`, and for `*` or a key that is not a repository key: an absent member never means every repository. A consumer that ignores an obligation ignores a denial.

### Test double

`pepfake.Fake` implements `Decider`. It allows exactly the requests its rules name, each by exact principal reference, action, resource type and resource key. The zero value, a nil `*Fake` and `New()` with no rules deny. No constructor allows all requests, and a census test pins the exported API.

## TypeScript client

The module is server-only: each file starts with `import "server-only"`, so a client bundle cannot import it. It exports `authorize(request, deps)` and `authorizeBatch(requests, deps)`.

`AuthorizeDeps` has `transport` and `timeoutMs`. Both are required. No module default exists that could allow. `nowMs` is optional. The transport takes `{ path, body }` and returns `{ status, body }`, so the caller owns the HTTP stack and the workload token.

The request is `{ principalReference, action, resource: { type, key, organizationId? }, requestId }`. There is no member for a role, a scope or an actor.

The result is an `AuthzOutcome`:

- `{ allowed: true, decision }`: a valid, unexpired allow.
- `{ allowed: false, source: "pdp", decision }`: a verdict.
- `{ allowed: false, source: "client", reason }`: a local deny.

A non-200 status is `non_200`. A time over `timeoutMs` is `timeout`. A transport throw is `transport_error`. A parse refusal is one of the `ParseFailure` values: `bad_json`, `duplicate_member`, `non_integer_number`, `service_error`, `bad_shape`, `unknown_reason`, `reason_mismatch`, `expired`, `batch_count_mismatch` and `batch_index_mismatch`. `authorizeBatch` takes up to 100 requests. A batch over 100 is `batch_bound`. One bad entry refuses the whole batch.

!!! note "Differences between the clients"
    The TypeScript client treats an expired decision as `expired` and denies. The Go client accepts an expired decision for the request it answers and marks it not cacheable. The TypeScript client has no cache.

## Cache rules

Only the Go client has a cache. `pep.NewCachedClient(client, pep.CacheConfig)` wraps a ready `Client` and implements `Decider`. The cache is a bound, not a source of authority.

| Rule | Behaviour |
| --- | --- |
| Allow only | A deny is never stored, so a cached deny cannot keep a principal out after a grant. A verdict that is not a cacheable allow removes the entry for its key and leaves a tombstone, so an older allow still in flight cannot land over it. |
| Expiry | An entry ends at the earlier of the decision's `expires_at` and `MaxTTL` after the request was sent. `MaxTTL` defaults to 30 s and may only be lowered. A clock that steps back is a miss. |
| Key | The SHA-256 of the principal reference, action, resource type, resource key and organization. The request id is not in the key. One principal and one session are one reference at the decision point. |
| Size | `MaxEntries`, default 10000, tombstones included. |
| Revision watermark | The cache keeps the highest `policy_revision` it has seen. When it rises, every entry below it is dropped. An allow below the watermark goes to its caller and is not stored. |
| Invalidation | The wire carries no resource, membership or grant revision. `Invalidate` and `InvalidateAll` drop every entry. Without a call, expiry bounds reuse. `Close` drops all entries, and afterwards every call goes to the decision point. |
| No single flight | Each miss is its own call with its own request id. A high-risk decision writes its own audit row. A low-risk decision writes none. A hit makes no call. |
| Batch | Hits are served and misses go as one batch. If a hit is below the watermark once the misses are answered, every hit is asked again. A failure of either call denies every item. |
| Obligations | An allow with an unknown obligation is a local deny, so it is never stored. When the client learns an audit obligation, an allow that carries it must not be stored. |

### What the server lets the cache keep

The client has no copy of the catalog. It trusts the server's `expires_at`. The decision point sets it as follows (`decisionExpiry` in `internal/auth/authapi/authorize_wire.go`, `repositoryAllow` in `internal/auth/authorization/grant.go`, the loop of `internal/auth/authapi/authorize.go` that sets `decisioncache.TTL`):

| Decision | `expires_at` | Cacheable |
| --- | --- | --- |
| Any deny | The decision time. | No. |
| An allow of an action with cache class `none`, which is every high-risk action | The decision time. | No. A high-risk allow always reaches the decision point and its audit row. |
| A repository allow (by grant or by membership default) of a low-risk action | The decision time plus 30 s (`RepositoryAllowBoundNanos`). | Yes, up to 30 s. |
| An allow of an action with cache class `decision_bound` that has no expiry of its own | The decision time plus 30 s (`decisioncache.TTL`), while the server's decision cache and its revision feed are ready. Otherwise the decision time. | Yes for 30 s while the cache is ready. Otherwise no. |

The 30 s is the ceiling for a repository read of TRD section 14. No environment value, configuration or snapshot changes it. The cache classes of each action are in the [policy catalog](policy-catalog.md#risk-class-and-cache-class).

A cached allow is bound to its policy revision. The class names `decision_bound` and `resource_revision_bound` in the catalog are classes of the server's rule. The Go client cannot check the class. It sees only the expiry and the revision.

### The server's own cache

The decision point has its own allow cache (`internal/auth/decisioncache`, used by the authorize routes). It stores allows only, never an allow of a high-risk or `none` action, and never an allow that carries an audit obligation. Each entry is bound three ways: expiry (at most 30 s), eviction when a revision feed event moves an aggregate the decision read, and a watermark check. The key type has fields for the principal, the session, an actor chain, the organization, the action, the resource, an assurance level, the policy revision, an entitlement revision and a digest of the row versions read. The authorize route fills the principal, the session, the organization, the action, the resource, the policy revision, the values no feed event covers and the state digest. It leaves the actor chain, the assurance level and the entitlement revision empty or 0 (`cacheKey`, `internal/auth/authapi/authorize_cache.go:123-133`). A delegated session, and any call while the feed is not ready, bypass the cache. The cache and its entries are not visible to a client, except through `expires_at`.

### Rules for a future TypeScript cache

`dev-health-web` has no cache. Its README gives three rules for the first cache reader. The key must carry the session. An allow that carries an audit obligation is never served from a cache, and the wire has no audit member yet. Stale-revision rows join the golden corpus with that reader.

## Related pages

- [Authorization decisions and obligations](obligations.md)
- [Reason codes](reason-codes.md)
- [Policy catalog](policy-catalog.md)
- [Entitlement boundary](entitlement-boundary.md)
