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

## Retire the old credentials

The `svc_acr_` class is retired. The current build does not mint, list, rotate or revoke it: the `dho service-credentials` commands are removed and the go-api internal ACR routes refuse the class. After the checks pass:

1. Remove the old `svc_acr_<redacted>` bearer from the consumer's secret input and from any secret store that holds it.
2. Treat each remaining `svc_acr_` value as dead. It cannot reach the internal ACR routes.
3. Keep token values out of logs, tickets and chat.

## Rollback

Rollback changes which credential the `acr-api` consumer sends. On a server build that still accepts `svc_acr_` on the internal ACR routes, set `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=service_token` and point `ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE` at a credential that is still valid. Keep the internal go-api origin. Workload mode can leave this file mounted, but it never reads it.

On a server build that refuses `svc_acr_`, a `svc_acr_` credential gets the uniform `401` body `{"detail":"Unauthorized"}` whatever its state. Changing the consumer alone cannot restore that path, and the current build has no command that mints a new `svc_acr_` credential. To go back, roll the server to a build that accepts the class and use a credential that is still valid.

A revoked or expired credential is never reactivated. Do not clear `revoked_at`, extend an expiry or restore a deleted row to undo retirement.

If the workload path fails, keep `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=workload` and repair its cause. Use the refusal reasons in the check step to find it. A failed exchange sends no request, and acr-api never falls back to the service token.

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
- `cmd/auth-migrate/main.go`
- `internal/storage/postgres/authschema/publicreads.go`
- `internal/storage/postgres/authschema/apply.go`
- `internal/storage/postgres/authschema/defer_narrowing.go`
