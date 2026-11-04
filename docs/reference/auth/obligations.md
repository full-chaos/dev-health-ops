---
page_id: ref-auth-obligations
summary: The authorization decision wire contract - the fields of a decision, the five closed obligations, the redaction class registry, the fail-closed parsing rules the Go client enforces, and the status of the decision point that issues them.
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

!!! note "Delegated sessions only"
    `POST /v1/authorize` and `POST /v1/authorize:batch` are served by the auth-service (`contracts/auth/v1/openapi/auth-v1.openapi.json`; CHAOS-10841), and the decision document has an `obligations` member. The decision point puts obligations on the allow of a delegated (impersonation) session only: `tenant_scope`, `minimum_assurance` and, for a repository request, a one-key `repository_allowlist`. A plain session's allow carries none, and `redaction` is not set yet (CHAOS-12656). No service enforces an obligation yet. See [Status](#status).

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
| `reason_code` | A closed enum: `decision_reason_code` in `contracts/auth/v1/vocabulary/reason-codes.json`, equal to the schema enum (24 values, including `impersonation_denied` and `delegation_invalid`). See [reason codes](reason-codes.md). |
| `decision_id` | Minted by the server (`dec_` plus 26 characters). Never copied from the caller. |
| `organization_id` | Null only for a platform or pre-organization decision. |
| `policy_revision` | The monotonic revision of the active policy set. Every allow-cache key binds it. |
| `expires_at` | Required for an allow and a deny. After this instant the caller must not reuse the decision for a later request and must ask again. The allow of a delegated session expires at the decision time: use it once. |
| `obligations` | Present only on an allow. A deny that carries `obligations` is invalid. |

## Obligations

The set is closed. `additionalProperties` is false. A decision that carries an obligation the contract does not define is invalid, and the consumer denies.

| Obligation | Type | The resource service must |
| --- | --- | --- |
| `tenant_scope` | organization id | Keep every read and write inside that organization. |
| `repository_allowlist` | array of at most 256 repository keys (a path of 2 to 20 segments), or `*` | Touch only those repositories. `*` means organization-wide. An empty array means none, not unrestricted. Unrestricted is the absence of the member plus a tenant scope. The decision point states an organization-wide allow as the explicit `["*"]` and sends the member (an empty one included) on every allow of an action that filters by repository; a consumer reads an absent allowlist on such an action as a deny. A list is never truncated. A consumer that reads an `owner/*` scope (a grant on every repository below one owner) matches every depth below `owner/`, never `owner2/...`; this contract defines no `owner/*` key, so the decision point states each repository or `*`. |
| `redaction` | array of `personal_identifiers`, `contact_details`, `free_text` | Remove those field classes from the response. An unknown class makes the whole decision invalid. |
| `minimum_assurance` | `aal1`, `aal2` or `aal3` | Deny when the principal's authenticator assurance level is lower. |
| `step_up` | boolean | Make the caller complete step-up authentication before the action proceeds. |

Ignoring an obligation is ignoring a denial (guardrail G-29 in the schema text).

### Redaction classes

The class set is closed and lives in `contracts/auth/v1/vocabulary/redaction-classes.v1.json`. The file also holds the (role, action) rules that say which role gets which class on which action. It equals the schema enum, and a test holds both equal. The rules ship empty: no policy is set until a ruling names rows.

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
| `POST /v1/authorize` and `POST /v1/authorize:batch` | On `main` (CHAOS-10841). The decision document carries the `obligations` a decision holds. A decision whose obligations are outside the contract (a deny with obligations, a repeated or malformed key, more than 256 keys, an unregistered class, an unknown assurance level, a tenant scope other than the question's organization) is not served: the call answers 503. |
| Actor chain, delegation limits and obligations in the decision point | Rules in the core (CHAOS-10839). The route reads the delegated row, the delegating session and the actor in one transaction and sets `tenant_scope`, `minimum_assurance` and `repository_allowlist` on the allow of a delegated session (CHAOS-12438). The policy that sets `redaction`, and the obligations of an ordinary allow, is the next change of CHAOS-12656. The owner-exemption audit event is written by a follow-up change; until then the exemption is a log line. |
| Decision clients | On `main`: the Go client `pep` (CHAOS-10844) and the web TypeScript client (CHAOS-10846). See [decision clients](decision-clients.md). |
| Policy enforcement points that read obligations | Planned. web: CHAOS-10883 (server actions use single decisions). |
| `step_up` and `minimum_assurance` enforcement | Planned, CHAOS-10839. No MFA factor exists. The decision sheet (`docs/contribute/architecture/auth-control-plane-decision-sheet.md`) records the decision that the vocabulary and enforcement path ship before a factor does, and that the high-risk list is denied during impersonation until step-up exists. |
| Entitlement-derived obligation | Planned, CHAOS-10862 (billing and licensing enforcement point). The entitlement adapter of CHAOS-10837 is on `main` as a library that no decision calls ([entitlement boundary](entitlement-boundary.md)). ADR text (`docs/contribute/architecture/auth-control-plane-adrs.md`) allows a decision to carry one. The decision schema defines no entitlement obligation, so adding one is a schema change. |
| Tenant scope as the go-api and query-api rule | Today the organization comes from `OrgScope` and `OperationOrgGuard`. See [authentication and authorization](authn-authz.md). |

Until a policy enforcement point reads them, treat every obligation in this page as a contract that a consumer must meet, not as enforced behaviour.
