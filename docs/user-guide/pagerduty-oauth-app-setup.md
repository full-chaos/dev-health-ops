# PagerDuty OAuth app setup

This guide registers the PagerDuty OAuth 2.0 app used by Dev Health's
organization-scoped, read-only PagerDuty integration.

A Dev Health organization admin connects PagerDuty once for the organization.
Other Dev Health users do not complete separate PagerDuty consent flows; they use
the operational data synced into their Dev Health organization. The PagerDuty
identity that grants consent must be able to see every service, team, schedule,
and incident that Dev Health should ingest.

> Replace `YOUR_HOST` with the public host that serves Dev Health Web. The OAuth
> callback is a browser route on the web application, not the ops API callback
> endpoint.

## Choose a connection mode

| Deployment | Recommended mode | Behavior |
| --- | --- | --- |
| Hosted or interactive self-hosted Dev Health | Authorization code with PKCE | A Dev Health admin authorizes the registered app once for each Dev Health organization. Dev Health stores and refreshes the organization-scoped OAuth credential. |
| Private automation without interactive consent | Client credentials | Register a private scoped PagerDuty app and enter its client ID, client secret, account subdomain, and region in Dev Health. |
| Compatibility only | REST API token | Use the explicit API-token fallback only when OAuth is unavailable. |

PagerDuty app visibility does not determine whether browser authorization is
available. A private Scoped OAuth app may use PKCE user authorization within the
PagerDuty account that created it, or use client credentials without interactive
consent. Publishing is only relevant when distributing an eligible app to other
PagerDuty accounts.

The rest of this guide covers the recommended authorization-code flow.

## 1. Register the app in PagerDuty

You need PagerDuty Account Owner or Global Admin access to register a scoped OAuth
app.

1. In PagerDuty, open **Integrations → Developer Tools → App Registration**.
2. Select **New App**.
3. Enter a name such as `Dev Health` and a description such as
   `Read-only operational data sync for Dev Health`.
4. Enable **OAuth 2.0**, then continue to the OAuth configuration step.
5. Select **Scoped OAuth**.
6. Add the redirect URL for the environment:

   ```text
   https://YOUR_HOST/org/admin/integrations/pagerduty/callback
   ```

   The value must exactly match `PAGER_DUTY_REDIRECT_URI` in the ops runtime.
   Do not register `/api/v1/admin/integrations/pagerduty/callback` as the browser
   redirect. Dev Health Web receives the browser redirect, removes the OAuth
   parameters from browser history, and sends the authenticated callback request
   to the ops API.

7. Grant **Read Access** for the resources below. Dev Health does not request
   write scopes in the current PagerDuty integration.

| PagerDuty resource | OAuth scope | Dev Health datasets |
| --- | --- | --- |
| Incidents | `incidents.read` | Incidents, alerts, timeline entries, and notes |
| Services | `services.read` | Services and business services |
| Escalation policies | `escalation_policies.read` | Escalation policies |
| Schedules | `schedules.read` | Schedules |
| On-calls | `oncalls.read` | On-call assignments |
| Users | `users.read` | Users and responder identity |
| Teams | `teams.read` | Teams |

8. Register the app.
9. Copy or download the **Client ID** and **Client Secret** immediately and store
   them in your secret manager. PagerDuty may not display the client secret again.

For a lower blast radius and separate rate-limit accounting, register a distinct
private app for each environment, such as production and staging.

## 2. Configure the Dev Health ops runtime

Set the OAuth app credentials on the ops API and every sync worker that can run
PagerDuty jobs:

```dotenv
PAGER_DUTY_CLIENT_ID="<pagerduty-client-id>"
PAGER_DUTY_SECRET="<pagerduty-client-secret>"
PAGER_DUTY_REDIRECT_URI="https://YOUR_HOST/org/admin/integrations/pagerduty/callback"
```

The same values must be available to the API and workers. The API uses them to
start and complete authorization; workers use them to refresh short-lived access
tokens during sync.

Also configure a stable PostgreSQL connection and encryption key:

```dotenv
POSTGRES_URI="postgresql+asyncpg://..."
SETTINGS_ENCRYPTION_KEY="<stable-production-encryption-key>"
```

OAuth state, encrypted tokens, expiration metadata, granted scopes, and account
metadata are stored in PostgreSQL. Keep `SETTINGS_ENCRYPTION_KEY` stable across
restarts and identical anywhere encrypted credentials are read.

Apply the PostgreSQL migrations before connecting PagerDuty:

```bash
dev-hops migrate postgres upgrade
```

Then restart or roll out the ops API and workers so they receive the new secrets.
Do not expose `PAGER_DUTY_SECRET` through browser-visible environment variables.

## 3. Connect PagerDuty once for the organization

1. Sign in to Dev Health as an organization admin.
2. Open **Admin → Integrations → PagerDuty**.
3. Select **OAuth authorization**.
4. In PagerDuty, review the read-only permissions and authorize the app.
5. After Dev Health reports that the connection is complete, return to the
   PagerDuty integration page and load its status.
6. Run permission preflight for every selected dataset.

The callback stores an encrypted access token, refresh token when provided,
expiration, granted scopes, account identity, region, and subdomain. The normal
integration descriptor remains tokenless.

## 4. Validate the shared connection

A healthy OAuth connection reports:

```json
{
  "connected": true,
  "credential_name": "default",
  "auth_mode": "oauth",
  "region": "us",
  "subdomain": "acme",
  "granted_scopes": [
    "incidents.read",
    "services.read"
  ],
  "has_refresh_token": true
}
```

The exact `granted_scopes` list depends on the selected datasets. Preflight should
mark every selected dataset as granted. If a dataset is missing permission, update
the PagerDuty app's read access and reconnect so PagerDuty issues a token with the
new scope set.

After preflight succeeds, discover PagerDuty services, map them to one or more Dev
Health repositories, configure the sync, and start the initial backfill.

## Local testing

Authorization-code redirects can use localhost because PagerDuty redirects the
browser running on your machine:

```text
http://localhost:3000/org/admin/integrations/pagerduty/callback
```

Register that exact redirect URL in the PagerDuty app and set:

```dotenv
PAGER_DUTY_REDIRECT_URI="http://localhost:3000/org/admin/integrations/pagerduty/callback"
```

PagerDuty webhooks are separate from the OAuth callback and require a publicly
reachable HTTPS endpoint when webhook mode is enabled.

## Troubleshooting

### `PAGER_DUTY_CLIENT_ID is not configured`

The ops API did not receive the registered app configuration. Set
`PAGER_DUTY_CLIENT_ID`, `PAGER_DUTY_SECRET`, and `PAGER_DUTY_REDIRECT_URI`, then
restart the API.

### Redirect URI mismatch

Confirm that PagerDuty App Registration and `PAGER_DUTY_REDIRECT_URI` use the
same scheme, host, port, path, and trailing-slash behavior. The expected path is:

```text
/org/admin/integrations/pagerduty/callback
```

### Invalid or expired OAuth state

Start the connection again from Dev Health. Authorization state is short-lived,
one-time, and bound to the Dev Health organization. Reusing a callback or opening
an old callback URL is rejected.

### Missing required scopes

Dev Health rejects a completed authorization that lacks a scope required by the
selected datasets and attempts to revoke the issued token. Add the missing read
access in PagerDuty, then reconnect.

### Partial data or no accessible services

Check the account subdomain and region first. Then confirm that the PagerDuty user
who granted consent can access the required teams, services, schedules, and
incidents. Reconnect with an appropriately authorized PagerDuty identity when
necessary.

### Sync works initially but refresh later fails

Confirm that every worker has the same `PAGER_DUTY_CLIENT_ID` and
`PAGER_DUTY_SECRET` as the API, and that `SETTINGS_ENCRYPTION_KEY` has not changed.
If the PagerDuty client secret or grant was revoked, disconnect and reconnect the
integration.

## Disconnect and rotate

Disconnecting the PagerDuty integration removes the local OAuth binding, clears
the active credential descriptor, and attempts to revoke the remote token. To
rotate the PagerDuty app's client secret, update the API and workers together,
roll them out, then reconnect any authorization-code bindings that PagerDuty has
invalidated.

See PagerDuty's official [Apps documentation](https://support.pagerduty.com/main/docs/apps)
and [Scoped OAuth overview](https://www.pagerduty.com/blog/insights/build-sophisticated-apps-for-your-pagerduty-environment-using-oauth-2-0-and-api-scopes/)
for the provider-side concepts behind app registration and consent.
