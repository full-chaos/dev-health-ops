---
page_id: ref-auth-reason-codes
summary: The closed reason codes of an authorization decision (26 wire values) and the closed local reasons a client gives when it could not get a decision, with the check that produces each wire value.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/reason-codes.json
  - contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json
  - internal/auth/authorization/types.go
  - internal/auth/authorization/evaluate.go
  - internal/auth/authorization/grant.go
  - internal/auth/authorization/delegation.go
  - internal/auth/authapi/authorize_wire.go
applicability: current
lifecycle: active
---

# Authorization reason codes reference

Every authorization decision has a `reason_code`. The set is closed. A decision with a value outside the set is invalid, and the consumer denies.
{: .fc-page-lede }

There are two sets that must not be mixed. A wire reason code is a verdict of the decision point. A local reason is why a client denied without a verdict. See [decision clients](decision-clients.md#two-kinds-of-deny).

## Wire reason codes

`decision_reason_code` in `contracts/auth/v1/vocabulary/reason-codes.json` lists 26 values. The schema enum of `authorization_decision.v1` equals the list, and a test holds both equal. A value is added only with the change of the producer that emits it.

The reason `allowed_by_platform_admin` allows an action flagged `platform_admin: allowed` in the [policy catalog](policy-catalog.md) to a principal whose platform role holds `admin.orgs`. The wire sends the same code, `allowed_by_platform_admin`, that the auth-service decision log line carries in its `reason` field. The `auth.authorize.high_risk_decided` and `auth.authorize.delegated_decided` audit rows of such an allow carry it as `decision_reason_code`. Audit rows written before the wire named this reason carry `decision_reason_code` `allowed_by_role` and the legacy attribute `decision_internal_reason` = `allowed_by_platform_admin`; no row is written with `decision_internal_reason` any more, and the audit schema keeps it as an optional legacy attribute for those stored rows. A policy-enforcement client must accept the code as an allow before the producer sends it: the Go client from dev-health-go v0.45.0 accepts it ahead of this list, and the web client does the same.

`allowed` must agree with the code. Exactly four codes mean an allow. Every other code means a deny. A decision where `allowed` and the code disagree is invalid.

| Code | Allow | Produced when |
| --- | --- | --- |
| `allowed_by_role` | yes | An organization or platform action: the principal's role holds the action. |
| `allowed_by_platform_admin` | yes | An organization action flagged `platform_admin: allowed`: the principal holds `admin.orgs` and needs no membership role for the action. |
| `allowed_by_membership_default` | yes | A repository action: the principal is an active member with no grant row, and the membership role holds the action. |
| `allowed_by_repository_grant` | yes | A repository action: a live grant of the organization, for this repository or `*`, holds the action. |
| `invalid_request` | no | The request does not normalize, a repository request names `*`, or the evaluator has no usable snapshot or catalog. |
| `unknown_action` | no | The action is not in the [policy catalog](policy-catalog.md), or has an unknown scope. |
| `resource_mismatch` | no | The request's resource type is not the resource kind of the action. |
| `principal_inactive` | no | The principal is not active. |
| `organization_inactive` | no | The organization is not active. |
| `no_membership` | no | The principal has no membership in the organization. |
| `org_mismatch` | no | The membership or service account row belongs to another organization. |
| `service_account_inactive` | no | A service account row is not active. |
| `platform_role_absent` | no | A platform action: the principal holds no platform role. |
| `role_unknown` | no | The role is not in the catalog. |
| `role_scope_mismatch` | no | The role's scope is not the scope of the action. |
| `role_lacks_action` | no | The role does not hold the action. |
| `grant_absent` | no | A repository action where a service account or a missing grant snapshot has no grant rows. |
| `repository_not_granted` | no | Grant rows exist, and none covers the repository with a role that holds the action. |
| `repository_unresolved` | no | A grant holds a repository id, and the live path of the request has no row in the organization's repository identities, so no id can be compared. The next provider sync resolves it. |
| `grant_expired` | no | A matching grant is past its expiry. |
| `grant_revoked` | no | A matching grant is revoked. |
| `repository_allowlist_too_large` | no | An organization action that reads repository-bound data: the principal can read more than 256 repositories by live grants, more than an allowlist holds. The list is never truncated. |
| `workload_audience_denied` | no | A workload principal's token audience is not one its registration lists. |
| `workload_action_denied` | no | A workload principal's registration does not list the action. |
| `impersonation_denied` | no | A delegated session: the chain has more than one actor, the request is a platform action, the action is not in the actor's permitted actions, the action is not allowed under impersonation (unless the actor is a platform owner), or a repository request is organization-wide (`*`). |
| `delegation_invalid` | no | A delegated session: the chain is empty or malformed, or the request is for another organization than the session's target. |

The order of the checks decides which code a request gets when several apply. The order is the order of `Evaluate` and `evaluateBase` in `internal/auth/authorization`. This page does not restate it. For the grant rules see [authentication and authorization](authn-authz.md).

### State failures are not decisions

The decision core has 27 reasons. Two of them, `clock_unavailable` and `grant_read_failed`, mean the decision point could not read the state it needs. They are never a wire reason code. The call answers 503 with an `error.v1` document (`authorizeStateFailures` in `internal/auth/authapi/authorize_wire.go`). A client treats a 503 as a local deny.

## Local reasons of the Go client

The Go client (`pep`) gives a deny it made itself one of 13 reasons. Each starts with `pdp_`, and `Reason.Local()` is true for it. None is a verdict.

| Local reason | The client denied because |
| --- | --- |
| `pdp_not_configured` | The client is nil or the zero value. |
| `pdp_invalid_request` | The request, or a batch of more than 100, is invalid. Nothing was sent. |
| `pdp_canceled` | The caller's context was canceled. |
| `pdp_timeout` | The call exceeded the timeout or the context deadline. |
| `pdp_unreachable` | The call failed at the transport. |
| `pdp_rate_limited` | The decision point answered HTTP 429. |
| `pdp_unavailable` | The decision point answered HTTP 5xx. |
| `pdp_refused` | The decision point answered another status than 200, 429 or 5xx. |
| `pdp_malformed` | The answer is not valid: wrong media type, not strict JSON, wrong shape, an unknown reason code, a reason that disagrees with `allowed`, or an `expires_at` outside the contract's range. An expired `expires_at` is not malformed: the decision is valid and is not cacheable. |
| `pdp_too_large` | The answer is larger than 256 KiB (single) or 4 MiB (batch). |
| `pdp_mismatch` | The decision's organization is not the request's, or a batch has the wrong count or index. |
| `pdp_unknown_obligation` | The decision carries an obligation, redaction class or assurance level the client does not know. |
| `pdp_no_decision` | The deny has no typed error. This is the reason a nil `*pep.Error` reports. |

## Local reasons of the TypeScript client

The TypeScript client returns `source: "client"` with a `LocalDenyReason`. `ParseFailure` values are `bad_json`, `duplicate_member`, `non_integer_number`, `service_error`, `bad_shape`, `unknown_reason`, `reason_mismatch`, `expired`, `batch_count_mismatch` and `batch_index_mismatch`. The other local values are `invalid_request`, `internal_error`, `batch_bound`, `transport_error`, `timeout` and `non_200`. See [decision clients](decision-clients.md).

## Related vocabularies

`audit-event.v1` `attributes.reason_code` is a different, larger list in the same file. `error.v1` `reason_code` is shape-only and is not governed by the file. See [policy catalog](policy-catalog.md#related-closed-vocabularies).
