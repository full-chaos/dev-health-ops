---
page_id: op-acr-service-credential-migration
summary: Move acr-api calls to the workload token, verify the change, and retire old service credentials with a bounded rollback path.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Migrate ACR service credentials

Move `acr-api` calls to go-api from a `svc_acr_` bearer to the `acr-api` workload token. These steps describe built behavior. Check which server build a deployment runs before changing its consumer. See [ACR credential authority](../../reference/auth/acr-credential-authority.md) for the route and projection contracts.

## Prerequisites

1. Prepare the `acr-api` platform workload in the [workload registry](../../reference/auth/workload-registry.md). Exactly one live row must have name `acr-api`, `organization_id` NULL and audience `dev-health-api`. Its joined principal must also be live.
2. Configure the workload exchange and the binding or bootstrap credential for this deployment. Follow [Kubernetes federation](../runbooks/auth-workload-kubernetes-federation.md) or [Workload bootstrap credentials](../runbooks/auth-workload-bootstrap-credential.md). In acr-api, set `ACR_AUTH_WORKLOAD_EXCHANGE_URL` to auth-service's origin and `ACR_AUTH_WORKLOAD_ISSUER` to its token issuer. Set exactly one of `ACR_AUTH_WORKLOAD_SUBJECT_FILE` and `ACR_AUTH_WORKLOAD_BOOTSTRAP_FILE`. Confirm that the consumer can obtain a token with `typ` = `workload+jwt`, audience `dev-health-api` and the registry principal as subject.
3. Set `AUTH_WORKLOAD_READER_ROLES` for `auth-migrate` / `auth-grants` to include go-api's database login. Also list that login in `AUTH_SESSION_READER_ROLES`. Do not list query-api's login as a workload reader. Apply the grants before switching the consumer. Without them, the internal ACR routes answer `503`. Grant narrowing needs the printed digest with `-ack-grant-narrowing`.
4. Check go-api's internal listener and its issuer/JWKS configuration (`AUTH_ACCESS_TOKEN_ISSUER`). Confirm that the deployment serves the health, entitlements, organization LLM-config and principal access routes listed in the authority page. These routes need their stores configured and reachable.

go-api resolves the workload subject from the registry. Do not use the retired `AUTH_ACR_WORKLOAD_SUBJECT` pin. Registry answers are cached for 30 seconds. A read error can use the last good answer only while it is less than 60 seconds old.

## Switch the consumer

1. Set `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=workload` in acr-api. Keep `ACR_DEV_HEALTH_ENTITLEMENT_URL` pointed at go-api's internal origin. The client obtains and sends its workload token with audience `dev-health-api` on all four internal ACR routes. A failed exchange sends no request and never falls back to the service token.
2. Replace the consumer's `svc_acr_<redacted>` bearer. Check a health call and an entitlement call for a known organization. Exercise LLM-config and principal access through their normal consumer calls.
3. Check the results below before retiring the old credentials.

When lifecycle event publishing is enabled with `ACR_AUTH_PROJECTION_URL`, its destination is different: `POST /internal/v1/acr/credential-events` takes audience `auth-service`. It needs `AUTH_ACR_EVENTS_WORKLOAD_SUBJECT`, `AUTH_TOKEN_ISSUER_URL` and a live workload row with action `credentials.project`. Do not send the `dev-health-api` audience token to that route. Projection delivery does not control whether ACR accepts a client credential.

## Check

- Watch `devhealth_acr_workload_accepted_total` by `route`. Exercise each route so the counter proves use of the new credential.
- Watch `devhealth_acr_workload_refused_total` by `route` and `reason`. The closed reasons are `contract`, `unknown_kid`, `signature`, `issuer`, `audience`, `not_yet_valid`, `expired`, `principal`, `not_configured` and `unavailable`.
- Read the `acr internal: workload token refused` log lines by route and reason. `principal` means a valid token belongs to another workload (`403`). `not_configured` means no usable verifier or single live registry row (`401`). `unavailable` means the key source or registry cannot be read (`503`). Other verification refusals are `401`. A token above 2,048 bytes is refused as `contract` before verification.
- Check `internal_service_credential_audits` for the entitlement outcomes. Accepted authentication records the workload subject in `principal_id`, with `credential_id` NULL. Refusals before or at verification have no principal. Older rows also have no principal; the schema added this column in migration `0157`.
- Check `devhealth_acr_internal_requests_total` for other outcomes. A `svc_acr_` refusal does not enter the workload verifier. Health and entitlements share 100 attempts per minute per client address; principal access has its own 6,000-per-minute budget. An exhausted budget returns `429`.

## Stop issuing

Stop operational issuance of `svc_acr_` credentials after the workload calls pass these checks. Remove the old bearer from the consumer's secret input. The commands still allow `service-credentials create` and `service-credentials rotate` for service `acr`; this step changes the operator workflow, not the mint implementation.

The old scopes are `entitlements:read`, `llm_config:read` and `principal_access:read`. They do not make a `svc_acr_` bearer valid on the current internal ACR routes.

## Revoke the old credentials

1. Run `dho service-credentials list --service acr` against the intended semantic database. Identify each old row by its credential id and prefix.
2. Run `dho service-credentials revoke <credential_id>` for each old credential.
3. List the rows again and check `revoked_at`. Revoke sets this timestamp even when a row was already revoked.

Create and rotate show the token once. The table stores its SHA-256 and first 16 characters. Keep token values out of logs, tickets and chat.

## Rollback

Rollback changes which credential the `acr-api` consumer sends. It can point to a still-valid service credential only if the server build still accepts that credential on the target routes.

On such a server build, set `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=service_token` and point `ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE` at that valid credential. Keep the internal go-api origin. Workload mode can leave this file mounted, but it never reads it.

After the server stops accepting `svc_acr_` on the internal ACR routes, a `svc_acr_` credential is refused with the uniform `401` body `{"detail":"Unauthorized"}` whatever its state. Consumer rollback to this credential therefore needs a server build that still accepts it. Changing the consumer alone cannot restore that path.

A revoked or expired service credential is never reactivated. `service-credentials rotate` refuses both. Revoke is final. If rollback needs a credential after the old one has ended, mint a **new** credential with `dho service-credentials create --service acr --scope entitlements:read`, adding only the other supported scopes the consumer needs. Use it only with a server build that accepts it. Never clear `revoked_at` or extend an expired row to undo retirement.

Rotation is for a still-active credential. It allows at most 3,600 seconds of overlap with its replacement. It cannot undo revocation.

## Source of each fact

- `acr/internal/config/entitlement.go`
- `acr/internal/config/auth_workload.go`
- `acr/internal/config/auth_projection.go`
- `acr/internal/entitlements/credential.go`
- `acr/internal/authprojection/client.go`
- `internal/apiservice/acr_workload_source.go`
- `internal/apiservice/accessverify.go`
- `internal/apiservice/acr/workloadsubject.go`
- `internal/apiservice/acr/workloadauth.go`
- `internal/apiservice/acr/internalauth.go`
- `internal/apiservice/acr/handlers.go`
- `internal/apiservice/acr/llmconfig.go`
- `internal/apiservice/acr/access.go`
- `internal/apiservice/acr/credentials.go`
- `internal/pgmigrate/archive/sql/0157_add_internal_service_credential_audits_principal_id.sql`
- `internal/auth/contracts/workload_token.go`
- `internal/auth/authapi/acr_events.go`
- `internal/auth/authconfig/workload.go`
- `internal/auth/authruntime/acr_events.go`
- `internal/apiservice/admin/servicecredentials.go`
- `internal/adminops/servicecredentials.go`
- `cmd/auth-migrate/main.go`
- `internal/storage/postgres/authschema/publicreads.go`
- `internal/storage/postgres/authschema/apply.go`
- `internal/storage/postgres/authschema/defer_narrowing.go`
