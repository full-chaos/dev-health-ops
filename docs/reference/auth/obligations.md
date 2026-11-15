---
page_id: ref-auth-obligations
summary: The authorization decision wire contract - the fields of a decision, the six closed obligations, the redaction class registry, the fail-closed parsing rules the Go client enforces, and the status of the decision point that issues them.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_response.v1.schema.json
  - contracts/auth/v1/jsonschema/principal.v1.schema.json
  - contracts/auth/v1/vocabulary/reason-codes.json
  - contracts/auth/v1/vocabulary/redaction-classes.v1.json
  - internal/auth/contracts/authorization.go
applicability: current
lifecycle: active
---

# Authorization decisions and obligations reference

An authorization decision answers one question: may this principal perform this action on this resource. A decision that allows may carry obligations. An obligation is a condition the resource service must enforce on the allowed request.
{: .fc-page-lede }

!!! warning "No consumer enforces an obligation yet"
    `POST /v1/authorize`, `POST /v1/authorize:batch` and `POST /v1/authorize:repositories` are served by the auth-service (`contracts/auth/v1/openapi/auth-v1.openapi.json`). Every allow of an organization or repository action carries `tenant_scope` and, for an action that reads repository-bound data, `repository_scope` or `repository_allowlist`. A delegated (impersonation) allow also carries `minimum_assurance`. The redaction rules are empty, so no allow carries `redaction` yet. No service enforces an obligation yet. See [Status](#status).

## Request

`authorization_request.v1` has these required members: `schema_version`, `principal_reference`, `action`, `resource` and `context`.

- The principal is an opaque reference that the decision point resolves on the server. The request has no `roles`, `permissions`, `scopes` or `is_superuser` member, and `additionalProperties` is false, so a caller cannot add a claim.
- `action` is a catalog action from the [policy catalog](policy-catalog.md). A bare `admin` is invalid.
- `resource` has a `type` and a canonical lowercase `key`. The closed `type` values are `organization`, `repository`, `membership`, `service_account`, `grant`, `credential`, `session`, `signing_key`, `subscription` and `platform`. A repository key is a lowercase path of 2 to 20 segments in at most 255 bytes (`owner/repo`, or `owner/group/repo` for nested GitLab groups) or `*` for organization-wide. A segment but the last holds 1 to 100 characters, starts and ends with a letter or digit and never starts with a dot; the last may start with a dot (`.github`). An empty segment, `.`, `..` and a trailing slash are refused.
- `context` has a `request_id` and a `client_service`. The `client_service` is a label for metrics and audit. It confers nothing.

A batch request holds 1 to 100 requests. A batch response holds 1 to 100 decisions, each with the zero-based `index` of the request it answers.

## Decision

`authorization_decision.v1` (`contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json`) requires these members: `schema_version`, `decision_id`, `allowed`, `reason_code`, `principal_id`, `organization_id`, `policy_revision` and `expires_at`. `obligations` is optional.

| Member | Rule |
| --- | --- |
| `allowed` | Required. A strict JSON boolean. No default. Missing, null, `"true"` or `1` is invalid. |
| `reason_code` | A closed enum: `decision_reason_code` in `contracts/auth/v1/vocabulary/reason-codes.json`, equal to the schema enum (26 values, including `impersonation_denied` and `delegation_invalid`). See [reason codes](reason-codes.md). |
| `decision_id` | Minted by the server (`dec_` plus 26 characters). Never copied from the caller. |
| `organization_id` | Null only for a platform or pre-organization decision. |
| `policy_revision` | The monotonic revision of the active policy set. Every allow-cache key binds it. |
| `expires_at` | Required for an allow and a deny. After this instant the caller must not reuse the decision for a later request and must ask again. The allow of a delegated session expires at the decision time: use it once. |
| `obligations` | Present only on an allow. A deny that carries `obligations` is invalid. |

## Obligations

The set is closed (six members). `additionalProperties` is false. A decision that carries an obligation the contract does not define is invalid, and the consumer denies.

| Obligation | Type | The resource service must |
| --- | --- | --- |
| `tenant_scope` | organization id | Keep every read and write inside that organization. |
| `repository_allowlist` | array of at most 256 repository keys (a path of 2 to 20 segments; the contract also admits `*`) | Touch only those repositories. An empty array means none, not unrestricted. The decision point never emits `*` and never truncates a list: a principal with more than 256 readable repositories is denied with `repository_allowlist_too_large`. A consumer still accepts `*` as organization-wide. A consumer that reads an `owner/*` scope (a grant on every repository below one owner) matches every depth below `owner/`, never `owner2/...`; this contract defines no `owner/*` key, so the decision point states each repository. |
| `repository_scope` | `organization` | Read every repository of the tenant: nothing limits the read. A decision carries this member or `repository_allowlist`, never both. On an allow of an action that filters by repository, a consumer that finds neither member denies: an absent member never means unrestricted. |
| `redaction` | array of `personal_identifiers`, `contact_details`, `free_text` | Remove those field classes from the response. An unknown class makes the whole decision invalid. |
| `minimum_assurance` | `aal1`, `aal2` or `aal3` | Deny when the principal's authenticator assurance level is lower. |
| `step_up` | boolean | Make the caller complete step-up authentication before the action proceeds. |

Ignoring an obligation is ignoring a denial (guardrail G-29 in the schema text).

### Allow rule of the decision point

The decision point (`internal/auth/authorization/scope.go`) sets the obligations of an allow from the snapshot it already read. No extra read is made.

| Allow | Obligations |
| --- | --- |
| Platform action | None. |
| Organization action whose catalog `repository_scope` is `none` | `tenant_scope` (the question's organization). |
| Organization action whose catalog `repository_scope` is `filtered`, principal with no grant row of any state (membership default) or with a live `*` grant | `tenant_scope` and `repository_scope: organization`. |
| Organization action whose catalog `repository_scope` is `filtered`, other principals | `tenant_scope` and `repository_allowlist`: the keys of the live grants (not revoked, not expired) whose role holds the action, sorted. `[]` when none is live. One grant row of any state ends the membership default, for an owner too. |
| Repository-kind action (`acr.context.read`, `acr.evidence.read`) | `tenant_scope` and `repository_allowlist` with the one requested key. A request for `*` is refused with `invalid_request`. |
| Repository listing (`POST /v1/authorize:repositories`, `acr.context.read` or `acr.evidence.read`, resource `*`) | The same standing as a repository decision for that action, then the set: `tenant_scope` and `repository_scope: organization` for the membership default or a live `*` grant, else `tenant_scope` and `repository_allowlist` (the canonical keys of the live grants; an id-bearing grant lists the live path of its id). More than 256 keys is the deny `repository_allowlist_too_large`. A platform role gives no listing without a membership (`no_membership`, as the per-key decision). A delegated session gets the same set with `minimum_assurance`, as its per-key reads. No catalog is read. |
| Delegated session | The obligations above, plus `minimum_assurance`. |

The allowlist is the same rule as a repository decision: a key is in it exactly when a repository question for that key would allow. More than 256 keys is the deny `repository_allowlist_too_large`. The list is never truncated. The catalog actions marked `filtered` are listed in the [policy catalog](policy-catalog.md).

### Redaction classes

The class set is closed and lives in `contracts/auth/v1/vocabulary/redaction-classes.v1.json`. The file also holds the (role, action) rules that say which role gets which class on which action. It equals the schema enum, and a test holds both equal. The rules ship empty: no policy is set until a decision names rows.

| Class | Covers |
| --- | --- |
| `personal_identifiers` | Fields that name or identify a person: a display name, a login, an author id, an external user id. |
| `contact_details` | Fields that reach a person: an email address, a phone number, a chat handle. |
| `free_text` | Fields that hold text a person typed: titles and bodies of pull requests and issues, commit messages, comments. |

A class is added in this order: schema enum, registry file, then the dev-health-go client release. The client compiles the class list in and denies a decision with a class it does not know. Which response field belongs to which class is the table of the consumer, not of the registry.

In Go (`internal/auth/contracts/authorization.go`, `AuthorizationObligations`) every member is a pointer. Absent and empty stay different, so a client that re-marshals obligations sends what it received.

The clients that send these requests and read these decisions are in [decision clients](decision-clients.md).

## Parsing rules in the Go client

`ParseAuthorizationDecision` and `ParseAuthorizationBatchResponse` fail closed. Any failure means "no valid decision", and the caller denies.

1. Duplicate object members are refused on the raw bytes first. A repeated `allowed` keeps the last value in every JSON parser, so the schema alone would accept an allow the sender never clearly sent.
2. The document is validated against the schema.
3. The document is decoded strictly, with unknown fields refused.
4. A decision whose `expires_at` is not after `now` is still valid for the request it answers, allow or deny. The parser marks it not cacheable (`Cacheable` is false): use it once and do not store it. `expires_at` bounds reuse, never the answer. `now` is a parameter.
5. For a batch, the response must hold exactly one decision per request, and the decision at position `i` must declare index `i`. A response that fails either rule is refused whole. The caller denies every item.

A decision point failure is an `error.v1` document (503). It is a different schema from a decision, so "the decision point failed" and "the decision point said no" are distinguishable.

## Status

| Part | State |
| --- | --- |
| Wire schemas and Go parser | On `main`. |
| `POST /v1/authorize` and `POST /v1/authorize:batch` | On `main`. The decision document carries the `obligations` a decision holds. A decision whose obligations are outside the contract (a deny or platform allow with obligations, an organization allow without its tenant scope, a repeated, malformed or `*` key, more than 256 keys, a `repository_scope` other than `organization` or one beside an allowlist, an unregistered class, an unknown assurance level, a tenant scope other than the question's organization) is not served: the call answers 503. |
| Actor chain, delegation limits and obligations in the decision point | Rules in the core. The route reads the delegated row, the delegating session and the actor in one transaction and sets `tenant_scope`, `minimum_assurance` and `repository_allowlist` on the allow of a delegated session. The policy that sets `tenant_scope`, `repository_allowlist` and `repository_scope` on every allow: see [Allow rule of the decision point](#allow-rule-of-the-decision-point). The redaction rules ship empty. The PEP client reads `repository_scope` and the reason `repository_allowlist_too_large` from dev-health-go v0.44.0. The owner-exemption audit event is written by a follow-up change; until then the exemption is a log line. |
| Decision clients | On `main`: the Go client `pep` and the web TypeScript client. See [decision clients](decision-clients.md). |
| Policy enforcement points that read obligations | query-api: the REST routes and the GraphQL root fields on `/graphql`, `/query`, `/query/run-operation` and the MCP listener read `tenant_scope` and the repository scope while `QUERY_API_AUTHZ_ENFORCE` or `QUERY_API_HEADERS_AUTHZ_ENFORCE` names the action's namespace (unset: off); see [configuration](configuration.md). Planned: web server actions use single decisions. |
| `step_up` and `minimum_assurance` enforcement | Planned. go-api (on a read) and query-api carry out `minimum_assurance` `aal1` only: every session they accept is `aal1`; a higher or unknown level stays no verdict. No MFA factor exists. The vocabulary and enforcement path ship before a factor does, and the high-risk list is denied during impersonation until step-up exists. |
| Entitlement-derived obligation | Planned (billing and licensing enforcement point). The entitlement adapter is on `main` as a library that no decision calls ([entitlement boundary](entitlement-boundary.md)). ADR text (`docs/contribute/architecture/auth-control-plane-adrs.md`) allows a decision to carry one. The decision schema defines no entitlement obligation, so adding one is a schema change. |
| Tenant scope as the go-api and query-api rule | Today the organization comes from `OrgScope` and `OperationOrgGuard`. See [authentication and authorization](authn-authz.md). |

Until a policy enforcement point reads them, treat every obligation in this page as a contract that a consumer must meet, not as enforced behaviour.
