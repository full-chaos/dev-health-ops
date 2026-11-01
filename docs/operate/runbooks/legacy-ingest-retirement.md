---
page_id: op-rb-legacy-ingest-retired
summary: The legacy ingest routes are retired. Read what changed, what the service does now, the one Helm render refusal, the prod default, and why there is no rollback.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Legacy ingest retirement

The six legacy ingest routes are retired. go-api does not mount them. No setting turns them on.

## What legacy ingest was

These routes took a `POST` with a JSON body:

- `/api/v1/ingest/commits`
- `/api/v1/ingest/pull-requests`
- `/api/v1/ingest/work-items`
- `/api/v1/ingest/deployments`
- `/api/v1/ingest/incidents`
- `/api/v1/ingest/telemetry`

They used one deployment-wide API key (`INGEST_API_KEYS`, header `X-API-Key`) or one HMAC secret (`INGEST_SIGNING_SECRET`, header `X-Signature-256`). Neither names an organization. The handler wrote the `org_id` from the body as given. Any holder of the key could write to any organization. This was a cross-tenant write path.

## Timeline

1. Deprecated and guarded (ops #4934). The routes mounted only when `DEV_HEALTH_LEGACY_INGEST_ENABLED` was true. The default was off.
2. Pinned off in prod (deploy #643). The prod values set `legacyIngest.enabled: false`.
3. Retired (ops #5234). go-api no longer mounts the routes. It no longer reads `DEV_HEALTH_LEGACY_INGEST_ENABLED`, `DEV_HEALTH_LEGACY_INGEST_SUNSET`, `INGEST_API_KEYS` or `INGEST_SIGNING_SECRET`.
4. Credential classes deleted (ops #5252). The classes `legacy_ingest_api_key` and `legacy_ingest_hmac_signature` are gone from the registry and from the stored-class check. See [Credential classes](../../reference/auth/credential-classes.md).

## What the service does now

- Each of the six paths answers `404`. This holds with every old setting set, in development and in production.
- The service never refuses to start because of these settings. It does not read them.
- The paths send no `Deprecation` or `Sunset` header.

## Fail-closed configuration: the Helm render refusal

The only refusal is in the Helm chart. It happens when you render the chart, not when the service starts. The render fails, whether `goApi.enabled` is true or false, with one of these texts:

- `legacyIngest is retired: the legacy /api/v1/ingest/* routes of go-api are gone (CHAOS-11813); only legacyIngest.enabled: false may remain`
- `legacyIngest.enabled is retired: the legacy /api/v1/ingest/* routes of go-api are gone (CHAOS-11813); set it false or remove it`
- `legacyIngest.sunset is retired: the legacy /api/v1/ingest/* routes of go-api are gone (CHAOS-11813); remove it`
- `goApi.extraEnv must not set <NAME>: the legacy ingest routes are retired (CHAOS-11813) and go-api reads no such setting`

To clear a refusal:

1. Remove `legacyIngest.sunset`, or set it empty.
2. Set `legacyIngest.enabled` to `false`, or remove it.
3. Remove `DEV_HEALTH_LEGACY_INGEST_ENABLED` and `DEV_HEALTH_LEGACY_INGEST_SUNSET` from `goApi.extraEnv`. The check ignores case.
4. Render again.

## Prod default

The prod values keep `legacyIngest.enabled: false` with no sunset. The chart still accepts this pin and renders it. It changes nothing in the render.

## Rollback

There is no rollback to legacy ingest.

- No flag or setting turns the routes on. Setting the old values opens nothing.
- The retirement is final by design. The routes used one key with no organization, which made a cross-tenant write path.
- To restore the routes, the owners must make a new design decision. This is not an operator action.

For a bad release, use the rollback criteria in [Upgrade](../maintain/upgrade.md). Do not assume application rollback reverses a data migration.

## Replacement path

This page names no replacement. The retirement change found no sender in prod and no client in the repositories that sent these credentials. If you have a sender that used these routes, tell the platform owners before you change it.
