---
page_id: ref-auth-obligations
summary: The authorization decision wire contract - the fields of a decision, the five closed obligations, the fail-closed parsing rules the Go client enforces, and the status of the decision point that would issue them.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_request.v1.schema.json
  - contracts/auth/v1/jsonschema/authorization_batch_response.v1.schema.json
  - contracts/auth/v1/jsonschema/principal.v1.schema.json
  - contracts/auth/v1/vocabulary/reason-codes.json
  - internal/auth/contracts/authorization.go
applicability: current
lifecycle: active
---

# Authorization decisions and obligations reference

An authorization decision answers one question: may this principal perform this action on this resource. A decision that allows may carry obligations. An obligation is a condition the resource service must enforce on the allowed request.
{: .fc-page-lede }

!!! warning "Contract only"
    The wire contract and the Go parser exist on `main`. No service issues a decision and no service enforces an obligation. The auth-service has no authorize route (`contracts/auth/v1/openapi/auth-v1.openapi.json` lists the session, identity and discovery routes only). The decision point is planned in CHAOS-10841 (`POST /v1/authorize` and `POST /v1/authorize:batch`) and obligations in CHAOS-10839. See [Status](#status).

## Request

`authorization_request.v1` has these required members: `schema_version`, `principal_reference`, `action`, `resource` and `context`.

- The principal is an opaque reference that the decision point resolves on the server. The request has no `roles`, `permissions`, `scopes` or `is_superuser` member, and `additionalProperties` is false, so a caller cannot add a claim.
- `action` is a catalog action from the [policy catalog](policy-catalog.md). A bare `admin` is invalid.
- `resource` has a `type` and a canonical lowercase `key`. The closed `type` values are `organization`, `repository`, `membership`, `service_account`, `grant`, `credential`, `session`, `signing_key`, `subscription` and `platform`. A repository key is `owner/repo` or `*` for organization-wide.
- `context` has a `request_id` and a `client_service`. The `client_service` is a label for metrics and audit. It confers nothing.

A batch request holds 1 to 100 requests. A batch response holds 1 to 100 decisions, each with the zero-based `index` of the request it answers.

## Decision

`authorization_decision.v1` (`contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json`) requires these members: `schema_version`, `decision_id`, `allowed`, `reason_code`, `principal_id`, `organization_id`, `policy_revision` and `expires_at`. `obligations` is optional.

| Member | Rule |
| --- | --- |
| `allowed` | Required. A strict JSON boolean. No default. Missing, null, `"true"` or `1` is invalid. |
| `reason_code` | A closed enum: `allowed_by_repository_grant`, `grant_absent`, `repository_not_granted`, `platform_role_absent`. |
| `decision_id` | Minted by the server (`dec_` plus 26 characters). Never copied from the caller. |
| `organization_id` | Null only for a platform or pre-organization decision. |
| `policy_revision` | The monotonic revision of the active policy set. Every allow-cache key binds it. |
| `expires_at` | Required for an allow and a deny. After this instant the caller must ask again. |
| `obligations` | Present only on an allow. A deny that carries `obligations` is invalid. |

## Obligations

The set is closed. `additionalProperties` is false. A decision that carries an obligation the contract does not define is invalid, and the consumer denies.

| Obligation | Type | The resource service must |
| --- | --- | --- |
| `tenant_scope` | organization id | Keep every read and write inside that organization. |
| `repository_allowlist` | array of `owner/repo` keys, or `*` | Touch only those repositories. `*` means organization-wide. An empty array means none, not unrestricted. Unrestricted is the absence of the member plus a tenant scope. |
| `redaction` | array of `personal_identifiers`, `contact_details`, `free_text` | Remove those field classes from the response. An unknown class makes the whole decision invalid. |
| `minimum_assurance` | `aal1`, `aal2` or `aal3` | Deny when the principal's authenticator assurance level is lower. |
| `step_up` | boolean | Make the caller complete step-up authentication before the action proceeds. |

Ignoring an obligation is ignoring a denial (guardrail G-29 in the schema text).

In Go (`internal/auth/contracts/authorization.go`, `AuthorizationObligations`) every member is a pointer. Absent and empty stay different, so a client that re-marshals obligations sends what it received.

## Parsing rules in the Go client

`ParseAuthorizationDecision` and `ParseAuthorizationBatchResponse` fail closed. Any failure means "no valid decision", and the caller denies.

1. Duplicate object members are refused on the raw bytes first. A repeated `allowed` keeps the last value in every JSON parser, so the schema alone would accept an allow the sender never clearly sent.
2. The document is validated against the schema.
3. The document is decoded strictly, with unknown fields refused.
4. A decision whose `expires_at` is not after `now` is refused, allow or deny. `now` is a parameter.
5. For a batch, the response must hold exactly one decision per request, and the decision at position `i` must declare index `i`. A response that fails either rule is refused whole. The caller denies every item.

A decision point failure is an `error.v1` document (503). It is a different schema from a decision, so "the decision point failed" and "the decision point said no" are distinguishable.

## Status

| Part | State |
| --- | --- |
| Wire schemas and Go parser | On `main`. |
| `POST /v1/authorize` and `POST /v1/authorize:batch` | Planned, CHAOS-10841. No route exists. |
| Actor chain, delegation limits and obligations in the decision point | Planned, CHAOS-10839. |
| Policy enforcement points that read obligations | Planned. web: CHAOS-10846 (TypeScript server-only decision client) and CHAOS-10883 (server actions use single decisions). |
| `step_up` and `minimum_assurance` enforcement | Planned, CHAOS-10839. No MFA factor exists. The decision sheet (`docs/contribute/architecture/auth-control-plane-decision-sheet.md`) records the decision that the vocabulary and enforcement path ship before a factor does, and that the high-risk list is denied during impersonation until step-up exists. |
| Entitlement-derived obligation | Planned, CHAOS-10837 (entitlement adapter) and CHAOS-10862 (billing and licensing enforcement point). ADR text (`docs/contribute/architecture/auth-control-plane-adrs.md`) allows a decision to carry one. The decision schema defines no entitlement obligation, so adding one is a schema change. |
| Tenant scope as the go-api and query-api rule | Today the organization comes from `OrgScope` and `OperationOrgGuard`. See [authentication and authorization](authn-authz.md). |

Until the decision point exists, treat every obligation in this page as a contract that a future producer and consumer must meet, not as behaviour of a running service.
