---
page_id: admin-source-pagerduty
summary: Register a read-only PagerDuty OAuth app, connect it once for the Dev Health organization, discover services, and verify incident synchronization.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/architecture/pagerduty-contract.md
  - current PagerDuty admin, OAuth, service-discovery, credential, and sync implementation
applicability: current
lifecycle: active
---

# Connect PagerDuty

PagerDuty is an organization-scoped operational data source. A Dev Health organization administrator connects PagerDuty once; other users read the incidents, services, schedules, on-call assignments, and related operational context synchronized into that Dev Health organization.
{: .fc-page-lede }

The PagerDuty identity granting consent must be able to see every PagerDuty service, team, schedule, responder, and incident that Dev Health is expected to ingest. A successful OAuth redirect alone does not prove that every selected dataset is visible.

## Choose the authentication mode

| Mode | Use it when | Behavior |
| --- | --- | --- |
| Authorization code with PKCE | Hosted or interactive self-hosted Dev Health | An organization admin authorizes the registered app once. Dev Health stores and refreshes the organization-scoped OAuth credential. |
| Client credentials | Private automation without interactive consent | Dev Health validates a private scoped app with its client ID, client secret, account subdomain, and region. |
| REST API token | OAuth is unavailable and compatibility is required | Use only as the explicit fallback. The token must still pass a live permission check. |

The recommended interactive mode is authorization code with PKCE. App visibility and OAuth mode are separate: a private scoped app can still support authorization inside the PagerDuty account that created it.

## Register the PagerDuty app

You need PagerDuty Account Owner or Global Admin access.

1. In PagerDuty, open **Integrations → Developer Tools → App Registration**.
2. Create an app named for the environment, such as `Dev Health Production`.
3. Enable **OAuth 2.0** and select **Scoped OAuth**.
4. Register the exact browser callback URL:

   ```text
   https://YOUR_HOST/org/admin/integrations/pagerduty/callback
   ```

   This is a Dev Health Web route. Do not register the ops API route as the browser callback.

5. Grant read access for the datasets the organization will synchronize:

| PagerDuty resource | Scope | Dev Health use |
| --- | --- | --- |
| Incidents | `incidents.read` | Incidents, alerts, timeline entries, and notes |
| Services and business services | `services.read` | Service discovery and mapping |
| Escalation policies | `escalation_policies.read` | Escalation context |
| Schedules | `schedules.read` | Schedule context |
| On-calls | `oncalls.read` | On-call assignments |
| Users | `users.read` | Responder identity |
| Teams | `teams.read` | Team context |

6. Register the app and store the client ID and client secret immediately in the environment's secret manager.

Use a distinct app per environment when separate blast radius and rate-limit accounting are important.

## Configure the Dev Health runtime

The API and every worker that can run PagerDuty synchronization need the same app configuration:

```dotenv
PAGER_DUTY_CLIENT_ID="<pagerduty-client-id>"
PAGER_DUTY_SECRET="<pagerduty-client-secret>"
PAGER_DUTY_REDIRECT_URI="https://YOUR_HOST/org/admin/integrations/pagerduty/callback"
```

PagerDuty OAuth state and encrypted credentials are stored in PostgreSQL. The API and workers must also share a stable encryption key:

```dotenv
POSTGRES_URI="postgresql+asyncpg://..."
SETTINGS_ENCRYPTION_KEY="<stable-production-encryption-key>"
```

Apply PostgreSQL migrations before connecting the provider:

```bash
dev-hops migrate postgres upgrade
```

Restart or roll out the API and workers after adding or rotating the app secrets. Never expose `PAGER_DUTY_SECRET` through browser-visible environment variables.

## Authorize the organization

1. Sign in to Dev Health as an organization administrator.
2. Open **Admin → Integrations → PagerDuty**.
3. Choose **OAuth authorization**.
4. Review and authorize the read-only PagerDuty scopes.
5. Return to the PagerDuty integration page after the callback completes.
6. Load the connection status and run permission preflight for every selected dataset.

A healthy connection reports that it is connected, identifies `oauth` as its authentication mode, records the PagerDuty region and account subdomain, and shows the granted scopes. Missing dataset permission is a configuration failure even when the OAuth exchange itself succeeded.

## Discover services and start synchronization

After permission preflight succeeds:

1. Discover the PagerDuty services visible to the authorized identity.
2. Select the services that belong in this Dev Health organization.
3. Map each selected service to one or more Dev Health repositories or teams where that association is known.
4. Review the incident and operational datasets selected for synchronization.
5. Save the synchronization configuration.
6. Start a bounded initial backfill and monitor its status before widening the time window.

The canonical PagerDuty target expands to services, business services, escalation policies, schedules, on-calls, users, teams, incidents, incident alerts, incident log entries, and incident notes. Incident child datasets depend on the parent incident dataset.

## Webhook delivery

PagerDuty Webhooks V3 is separate from the OAuth callback. OAuth authorizes REST reads; a V3 subscription delivers supported incident and service events to an opaque binding route managed by Dev Health.

The current receiver:

- verifies the HMAC-SHA256 signature against the exact raw body;
- accepts multiple `v1=` signatures during secret rotation;
- recognizes `pagey.ping` as a health check with no canonical write;
- fails closed for unknown events;
- deduplicates replayed event identities durably;
- does not trust organization, source, or credential authority from the URL query or JSON body.

A webhook subscription requires a publicly reachable HTTPS endpoint. Local OAuth testing can use a localhost callback, but webhook testing cannot rely on a private localhost route without a supported tunnel.

## Verify the connection

Before considering PagerDuty ready:

- connection status reports the intended account, region, mode, and scopes;
- permission preflight passes for every selected dataset;
- service discovery returns the expected services;
- one bounded backfill completes;
- the latest successful synchronization timestamp advances;
- a known incident is visible in the supported product surface;
- missing services or incidents are explained by scope, permission, mapping, or time-window evidence rather than assumed absent.

## Troubleshooting

### App configuration is missing

When Dev Health reports that `PAGER_DUTY_CLIENT_ID` is not configured, confirm the client ID, client secret, and redirect URI are present in the API environment, then restart the API. Workers also require the client ID and secret for token refresh during sync.

### Redirect URI mismatch

The PagerDuty registration and `PAGER_DUTY_REDIRECT_URI` must match exactly, including scheme, host, port, path, and trailing-slash behavior. The expected path is:

```text
/org/admin/integrations/pagerduty/callback
```

### Invalid or expired OAuth state

Start authorization again from Dev Health. OAuth state is short-lived, one-time, and bound to the Dev Health organization. Do not reuse an old callback URL.

### Missing scopes

Add the missing read access in PagerDuty, then reconnect so PagerDuty issues a credential with the new scope set. Do not treat a partially scoped connection as complete.

### No services or partial data

Check the PagerDuty account subdomain and region, then verify that the consenting PagerDuty identity can access the expected teams, services, schedules, and incidents. Confirm service selection, repository mapping, dataset selection, and the synchronization window.

### Token refresh fails after an initially successful sync

Verify that API and workers use the same client ID, client secret, and `SETTINGS_ENCRYPTION_KEY`. When the app secret or grant has been revoked, disconnect and reconnect the integration.

## Disconnect or rotate

Disconnecting removes the active local binding and attempts to revoke the remote OAuth grant. Rotation should create and verify the replacement first, update API and workers together, then retire the superseded credential or webhook binding only after the new path is healthy.

Continue with [Check synchronization status and freshness](../sync-and-coverage/status-and-freshness.md) and [Rotate or revoke provider credentials](credential-lifecycle.md).
