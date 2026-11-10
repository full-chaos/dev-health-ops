---
page_id: ref-auth-acr-credential-authority
summary: Who owns ACR credentials, which bearers the internal routes accept, and how auth-service records credential lifecycle events.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# ACR credential authority

This page describes behavior built on the main branch. Enable the required settings before using a route in a deployment. For operator steps, see [Migrate ACR service credentials](../../operate/security/acr-service-credential-migration.md).

## Who decides

- ACR is the `acr` repository and its `acr-api` and `acr-mcp` services. ACR owns browser and MCP credential issuance and its own credential tables.
- The auth control plane is `auth-service`. It holds a projection of ACR credentials in `credential_registry` and `credential_projection`. It does not validate those credentials or write to ACR's credential store.
- The projection is a record, not a gate. A late lifecycle event does not change whether ACR accepts a credential.

## Credential kinds that reach ACR and ops

| Credential | Accepted route or use |
| --- | --- |
| ACR client credential (`acr_client_credential`) | ACR owns this credential. It is distinct from the workload token used for calls to ops. |
| `acr-api` workload token, header `typ` = `workload+jwt`, audience `dev-health-api` | The four go-api internal ACR routes listed below. The subject must match the workload registry. |
| `acr-api` workload token, audience `auth-service` | `POST /internal/v1/acr/credential-events`, when enabled. Its live workload row must allow `credentials.project`. |
| Service credential, such as `svc_acr_<redacted>` | Retired class. None of these routes accepts it, and no endpoint profile may list it. The `dho service-credentials` commands are removed, so no build issues, rotates or revokes it. |

For the full class inventory, see [Credential reference](credential-reference.md). For the control plane as a whole, see [Auth control plane](../../operate/architecture/auth-control-plane.md).

The four go-api routes use `GET`:

- `/api/v1/internal/acr/health`
- `/api/v1/internal/acr/entitlements/{org_id}`
- `/api/v1/internal/acr/orgs/{org_id}/llm-config`
- `/api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access`

They accept only the `acr-api` workload token. Any other bearer, including `svc_acr_`, gets the uniform `401` body `{"detail":"Unauthorized"}` before any credential lookup. This is the authentication refusal; an unavailable dependency can return `503`, and an exhausted request budget returns `429`. A valid token from another workload gets `403`.

Health and entitlements share a budget of 100 attempts per minute per client address. Principal access has its own budget of 6,000 attempts per minute per client address. Entitlements writes an `internal_service_credential_audits` row for each audited outcome. A verified request records the workload subject in `principal_id`, with no `credential_id`. A refusal before or at verification records no principal.

go-api reads `auth.workloads` joined to `auth.principals`. It selects the platform workload named `acr-api`, with `organization_id` NULL. Exactly one row must be live, with a live principal and audience `dev-health-api`. No matching live row, or several, gives `401` with reason `not_configured`. A registry read error gives `503`. The reader caches an answer for 30 seconds. On a read error, it serves the last good answer only while that answer is less than 60 seconds old.

The retired `AUTH_ACR_WORKLOAD_SUBJECT` setting does not select the subject for these routes. A token over `contracts.WorkloadTokenMaxBytes` (2,048 bytes) is refused as `contract` before verification. A bearer with another credential prefix is refused as that other class without decoding it.

## Projection model

When enabled, `acr-api` posts one credential lifecycle event per request to `POST /internal/v1/acr/credential-events`. The body follows `acr-credential-lifecycle-event.v1` and is at most 16 KiB. It carries no credential secret or verifier.

Admission runs before the body is read: bearer class, audience and subject, signature, then the live workload row. The row must be enabled and list audience `auth-service` and action `credentials.project`. The token carries no action claim.

The event kinds are:

| Event | Projected effect |
| --- | --- |
| `acr_credential.issued` | Create an `active` credential. |
| `acr_credential.device_redeemed` | Create an `active` credential. |
| `acr_credential.rotated` | Create a new `active` credential that names its rotation parent. |
| `acr_credential.revoked` | End the credential in state `revoked`. |
| `acr_credential.expired` | End the credential in state `expired`. |

The route maps each event through `pushlifecycle.FromAcr`. The engine in `outboxprojection` commits the projection state, registry change and decision audit row in one transaction. It stores the ACR class and owner subject as data. All projected ACR credentials have registry class `acr_client_credential`. The engine derives a public fingerprint from the credential id; it stores no secret verifier.

- Duplicate, stale and out-of-order events never reactivate a revoked or expired credential. Events cannot widen scopes.
- A terminal event applies once. A later terminal event cannot change that credential again.
- A valid terminal event can close an active credential across a revision gap. Other gaps and unseen rotation parents require a retry.
- The same revision with different posted bytes is a conflict. The engine hashes the whole posted body.
- The engine checks the restore fence before writing. A registry revocation or earlier expiry also narrows the projected state.

The External Push reader shares this engine. Its reaper guard keeps outbox rows above the reader's low-water mark. Its restore-fence floor includes the highest applied or parked revision per External Push credential. These reader controls do not turn the ACR events route into an outbox reader.

## Events route answers

| Status | Reason or result | Publisher action |
| --- | --- | --- |
| `204` | Applied, duplicate or stale | Delivery is complete. |
| `409` | `projection_retry`: revision gap, parent pending or state changed during the decision | Keep the event and retry later. |
| `409` | `projection_conflict`: the same revision has different bytes | Treat this delivery as final. Investigate the conflict. |
| `409` | `projection_refused`: a final lifecycle or input refusal | Treat this delivery as final. Investigate the refusal. |
| `400` | Malformed body, including an oversized body or a noncanonical organization id | Correct the event. |
| `401` | Caller authentication failed | Check the credential class, subject, audience, signature and token times. |
| `403` | The live workload is absent, disabled or lacks the audience or action | Check the workload registry. |
| `503` | Key source, store or restore fence unavailable | Keep the event. Restore the dependency before retrying. |

## Settings

| Setting | Where | Default | Effect |
| --- | --- | --- | --- |
| `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL` | acr-api | `service_token` | `workload` selects the workload token for ops calls. A failed exchange sends no request and never falls back to the service token. |
| `ACR_DEV_HEALTH_ENTITLEMENT_URL` | acr-api | unset | Sets the go-api origin. Required in workload mode. |
| `ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE` | acr-api | unset | Names the service credential file in `service_token` mode. Workload mode does not read it. |
| `ACR_AUTH_WORKLOAD_EXCHANGE_URL` | acr-api | unset | Sets auth-service's origin for token exchange. Required when any workload client setting is set. |
| `ACR_AUTH_WORKLOAD_ISSUER` | acr-api | unset | Sets the expected token issuer. Required with the workload client. |
| `ACR_AUTH_WORKLOAD_SUBJECT_FILE`, `ACR_AUTH_WORKLOAD_BOOTSTRAP_FILE` | acr-api | unset | Select exactly one: a projected ServiceAccount token file or a bootstrap credential file. |
| `ACR_AUTH_WORKLOAD_ALLOW_PLAIN_HTTP` | acr-api | `false` | Allows an HTTP exchange origin when enabled. |
| `ACR_AUTH_PROJECTION_URL` | acr-api | unset | Sets auth-service's origin and enables lifecycle event publishing. Needs the workload client. |
| `AUTH_ACCESS_TOKEN_ISSUER` | go-api | unset | Selects the issuer and JWKS for workload verification. Required when `API_DATABASE_URI` is set. The internal ACR verifier accepts audience `dev-health-api` only. |
| `AUTH_WORKLOAD_READER_ROLES` | `auth-migrate` / `auth-grants` | empty | Grants go-api's login the narrow workload and principal reads. Each login must also be in `AUTH_SESSION_READER_ROLES`. Leave query-api's login out. Without these grants, the registry read fails and the routes answer `503`. Removing a grant requires the printed digest with `-ack-grant-narrowing`. |
| `AUTH_ACR_EVENTS_WORKLOAD_SUBJECT` | auth-service | unset | Enables the events route when its endpoint-profile row is present. Set it to the `acr-api` workload principal id: `prn_` plus 22 base64url characters. Needs `AUTH_TOKEN_ISSUER_URL`. This setting does not select the subject for go-api's internal ACR routes. |
| `AUTH_TOKEN_ISSUER_URL` | auth-service | unset | Sets the token issuer. Required to configure the events route's workload subject. |

For workload exchange and binding settings, see [Workload registry](workload-registry.md) and [Auth service configuration](configuration.md#workload-token-exchange).

## Source of each fact

- `acr/internal/auth/service.go`
- `acr/internal/auth/browser_refresh.go`
- `acr/internal/storage/postgres/credentials.go`
- `acr/internal/authprojection/client.go`
- `acr/internal/runtime/hosted/credential_outbox_publisher.go`
- `acr/internal/config/auth_projection.go`
- `acr/internal/config/auth_workload.go`
- `acr/internal/config/entitlement.go`
- `acr/internal/entitlements/credential.go`
- `contracts/auth/v1/credential-classes.json`
- `contracts/auth/v1/endpoint-profiles.auth-service.json`
- `internal/apiservice/accessverify.go`
- `internal/apiservice/acr_workload_source.go`
- `internal/apiservice/acr/handlers.go`
- `internal/apiservice/acr/internalauth.go`
- `internal/apiservice/acr/workloadauth.go`
- `internal/apiservice/acr/workloadsubject.go`
- `internal/apiservice/acr/llmconfig.go`
- `internal/apiservice/acr/access.go`
- `internal/pgmigrate/archive/sql/0157_add_internal_service_credential_audits_principal_id.sql`
- `internal/auth/contracts/workload_token.go`
- `internal/auth/contracts/acr_credential_lifecycle_event.go`
- `internal/auth/authapi/acr_events.go`
- `internal/auth/authconfig/workload.go`
- `internal/auth/authruntime/acr_events.go`
- `internal/auth/outboxprojection/project.go`
- `internal/auth/outboxprojection/pure.go`
- `internal/auth/outboxprojection/store.go`
- `internal/auth/pushlifecycle/acr.go`
- `internal/auth/pushlifecycle/projection.go`
- `internal/auth/pushprojection/floorinput.go`
- `internal/audit/reaper.go`
- `cmd/auth-migrate/main.go`
- `internal/storage/postgres/authschema/publicreads.go`
- `internal/storage/postgres/authschema/apply.go`
- `internal/storage/postgres/authschema/defer_narrowing.go`
