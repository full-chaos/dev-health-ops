---
page_id: admin-incidents
summary: Connect supported incident-response sources, register PagerDuty OAuth, discover services, map operational scope, and verify canonical incident synchronization.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/architecture/pagerduty-contract.md
  - docs/providers/jira-service-management.md
  - current incident-source admin, credential, service-discovery, webhook, and sync implementation
applicability: current
lifecycle: active
---

# Connect incident-response sources

Incident-response integrations bring operational events, services, schedules, responders, and incident evidence into the canonical Dev Health operational model. A source is not ready merely because authentication succeeded: the administrator must verify provider identity, dataset permission, service discovery, product mappings, and a bounded synchronization.
{: .fc-page-lede }

## Current availability

| Source | Status | Supported boundary |
| --- | --- | --- |
| PagerDuty REST and Webhooks V3 | Current | Read-only organization-scoped synchronization plus verified signed event delivery |
| Jira Service Management incidents | Not release-ready | Code and unit contracts exist, but live tenant proof and release readiness remain blocked |
| JSM Ops Alerts or standalone Opsgenie | Not supported by this incident slice | Alerts are separate capabilities and are never inferred into incidents |

Do not infer a canonical incident from ordinary Jira issues, alert-like labels, timestamps, text similarity, or write-only associations.

## PagerDuty connection model

A Dev Health organization administrator connects PagerDuty once for the organization. Other users consume the synchronized operational data; they do not complete individual consent flows.

The consenting PagerDuty identity must be able to see every service, team, schedule, responder, and incident Dev Health should ingest.

### Choose authentication

| Mode | Use it when | Behavior |
| --- | --- | --- |
| Authorization code with PKCE | Hosted or interactive self-hosted Dev Health | An organization admin authorizes the registered app once. Dev Health stores and refreshes the organization-scoped OAuth credential. |
| Client credentials | Private automation without interactive consent | Dev Health validates a private scoped app with client ID, client secret, subdomain, and region. |
| REST API token | OAuth is unavailable | Explicit compatibility fallback; still requires live validation. |

## Register a PagerDuty OAuth app

You need PagerDuty Account Owner or Global Admin access.

1. Open **Integrations → Developer Tools → App Registration** in PagerDuty.
2. Create an environment-specific app, such as `Dev Health Production`.
3. Enable **OAuth 2.0** and choose **Scoped OAuth**.
4. Register the exact browser callback:

   ```text
   https://YOUR_HOST/org/admin/integrations/pagerduty/callback
   ```

   This is a Dev Health Web route. Do not register the ops API route as the browser callback.

5. Grant read access for the synchronized resources:

| Resource | Scope | Dev Health datasets |
| --- | --- | --- |
| Incidents | `incidents.read` | Incidents, alerts, timeline entries, and notes |
| Services and business services | `services.read` | Service discovery and mapping |
| Escalation policies | `escalation_policies.read` | Escalation context |
| Schedules | `schedules.read` | Schedule context |
| On-calls | `oncalls.read` | On-call assignments |
| Users | `users.read` | Responder identity |
| Teams | `teams.read` | Team context |

6. Register the app and store the client ID and client secret in the environment's secret manager.

Use separate apps per environment when separate blast radius and rate-limit accounting are important.

## Configure the Dev Health runtime

The API and every worker that can synchronize PagerDuty need the same app values:

```dotenv
PAGER_DUTY_CLIENT_ID="<pagerduty-client-id>"
PAGER_DUTY_SECRET="<pagerduty-client-secret>"
PAGER_DUTY_REDIRECT_URI="https://YOUR_HOST/org/admin/integrations/pagerduty/callback"
```

OAuth state and encrypted credentials are stored in PostgreSQL. API and workers also need the same stable encryption key:

```dotenv
POSTGRES_URI="postgresql+asyncpg://..."
SETTINGS_ENCRYPTION_KEY="<stable-production-encryption-key>"
```

Apply PostgreSQL migrations before connecting PagerDuty:

```bash
dev-hops migrate postgres upgrade
```

Restart or roll out API and workers after changing the app secrets. Never expose `PAGER_DUTY_SECRET` through browser-visible configuration.

## Authorize the organization

1. Sign in to Dev Health as an organization administrator.
2. Open **Admin → Integrations → PagerDuty**.
3. Choose **OAuth authorization**.
4. Review and authorize the read-only scopes.
5. Return to the PagerDuty integration page after the callback completes.
6. Load connection status and run permission preflight for every selected dataset.

A healthy connection identifies the intended account, region, authentication mode, and granted scopes. A completed OAuth exchange with missing dataset permission is not a ready connection.

## Discover and map services

After permission preflight:

1. Discover services visible to the authorized identity.
2. Select services that belong in this Dev Health organization.
3. Map each selected service to repositories or teams where that association is known.
4. Review selected incident and operational datasets.
5. Save the synchronization configuration.
6. Run a bounded initial backfill and monitor it before widening the window.

The canonical PagerDuty target includes services, business services, escalation policies, schedules, on-calls, users, teams, incidents, incident alerts, incident log entries, and incident notes. Child incident datasets depend on the parent incident dataset.

## PagerDuty Webhooks V3

Webhooks are separate from OAuth. OAuth authorizes REST reads; Webhooks V3 delivers supported incident and service events to a persisted opaque binding route.

The current receiver:

- verifies HMAC-SHA256 over the exact raw request body;
- accepts multiple `v1=` signatures during rotation;
- recognizes `pagey.ping` as a health event with no canonical write;
- fails closed for unknown events;
- deduplicates identical event replay durably;
- reports same-identity/different-body reuse as a conflict;
- resolves organization, source, credential, and signing authority from the persisted binding rather than payload fields.

Webhook delivery requires a publicly reachable HTTPS endpoint. A localhost OAuth callback does not make localhost a valid provider webhook destination.

## Verify the source

Before marking the source ready:

- connection status reports the intended account, region, mode, and scopes;
- permission preflight passes for every selected dataset;
- service discovery returns expected services;
- mappings reflect the intended Dev Health scope;
- one bounded backfill completes;
- latest successful synchronization advances;
- a known incident is visible in the supported product surface;
- when webhooks are enabled, the replacement binding passes the provider health event before activation.

## Troubleshooting

### App configuration is missing

Confirm client ID, client secret, and redirect URI are present on the API. Workers also need client ID and secret for token refresh.

### Redirect mismatch

PagerDuty registration and `PAGER_DUTY_REDIRECT_URI` must match exactly. The expected path is:

```text
/org/admin/integrations/pagerduty/callback
```

Start authorization again after correcting the value; OAuth state is short-lived and one-time.

### Missing scopes or services

Update PagerDuty read access or reconnect with an identity that can see the expected services, teams, schedules, and incidents. Then rerun preflight and discovery.

### Refresh fails later

Verify API and workers share the same client ID, client secret, and `SETTINGS_ENCRYPTION_KEY`. If the app secret or grant was revoked, disconnect and reconnect.

### Webhook verification fails

Verify the active binding route and provider subscription identity, preserve the exact raw request body for signature validation, allow multiple `v1=` signatures during rotation, and create/verify a replacement before revoking the old binding.

## Disconnect and rotate

Disconnecting removes active local authority and attempts remote revocation while preserving non-secret historical evidence. Rotation is additive: create and verify the replacement, switch synchronization or webhook delivery, then revoke the superseded credential or binding.

Continue with [Check synchronization status and freshness](../sync-and-coverage/status-and-freshness.md) and [Rotate or revoke provider credentials](credential-lifecycle.md).
