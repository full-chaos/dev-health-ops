---
page_id: op-rb-auth
summary: Recover repeated provider authentication, OAuth refresh, scope, callback, discovery, or webhook-binding failures without exposing or repeatedly testing a compromised credential.
content_type: runbook
owner: platform-operations
source_of_truth:
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/architecture/pagerduty-contract.md
  - current provider credential validation and revocation implementation
applicability: current
lifecycle: active
---

# Provider authentication failure

Use this runbook when a configured provider repeatedly fails authorization, token refresh, live permission validation, source discovery, or signed webhook delivery. Stop automated retry when continuing can amplify provider lockout, rate-limit, or duplicate-delivery risk.
{: .fc-page-lede }

## Preserve the incident context

Record:

- Dev Health organization, provider, source, and connection name;
- provider account, host, region, subdomain, installation, or namespace identity;
- authentication mode;
- selected datasets and affected synchronization or webhook bindings;
- first and latest failure time;
- run, source, binding, subscription, or correlation identifiers;
- whether API, workers, or both are affected.

Do not record tokens, client secrets, signing secrets, refresh tokens, authorization headers, callback query parameters, or raw credential-bearing payloads.

## Classify the failure

| Failure | Evidence | Response |
| --- | --- | --- |
| Runtime app setting missing | API cannot start authorization or worker cannot refresh | Restore the setting on every owning process and roll out consistently |
| OAuth callback mismatch | Provider rejects redirect or Dev Health callback does not complete | Match scheme, host, port, path, and slash exactly, then start a new authorization |
| Expired or reused OAuth state | Callback is old, duplicated, or bound to another organization | Restart from the Dev Health administrator flow; do not reuse the URL |
| Missing scope or permission | Live preflight fails for selected datasets | Update provider app/identity access, reconnect, and rerun preflight |
| Wrong account, host, region, or subdomain | Connection succeeds but discovery returns the wrong boundary | Correct source identity and reconnect rather than inventing identifiers |
| Revoked app secret or grant | Initial setup worked; refresh later fails | Update API and workers together, disconnect, and reconnect |
| Encryption-key mismatch | Stored credentials cannot be decrypted after rollout | Restore the intended stable key or follow credential recovery and reconnect |
| Webhook binding/signature failure | Subscription cannot authenticate against active binding | Verify route, subscription identity, exact-body HMAC, and rotation state |
| Provider outage or rate limit | Valid credentials fail through provider availability/budget evidence | Pause amplification and retry with bounded provider-aware backoff |

## PagerDuty-specific checks

The API and sync workers must share:

```text
PAGER_DUTY_CLIENT_ID
PAGER_DUTY_SECRET
SETTINGS_ENCRYPTION_KEY
```

The callback must be:

```text
/org/admin/integrations/pagerduty/callback
```

After reconnecting, run permission preflight for every selected dataset and verify service discovery. OAuth success without `incidents.read`, `services.read`, or another selected scope is incomplete.

For PagerDuty Webhooks V3:

- resolve the active opaque binding;
- compare `x-webhook-subscription` with the persisted subscription identity;
- verify every accepted `v1=` signature against the exact raw body;
- do not require an invented timestamp header;
- use `pagey.ping` to verify a candidate binding without creating an incident;
- create and verify a replacement before revoking the old binding.

## Recover

1. Disable or pause repeated failing jobs or subscriptions when amplification is unsafe.
2. Correct the provider app, identity, callback, runtime secret, encryption key, or binding.
3. Roll out the corrected configuration to every owning API and worker process.
4. Reauthorize or reconnect through the supported administrator flow.
5. Run live permission preflight and source discovery.
6. Execute one bounded synchronization or provider test event.
7. Verify downstream canonical data and freshness.
8. Re-enable normal schedules or webhook delivery.
9. Revoke superseded credentials or bindings and record any coverage gap.

## Security escalation

Unexpected use, secret exposure, unexplained grant replacement, or same-identity/different-body webhook conflicts are security events—not routine authentication failures. Revoke authority, preserve non-secret evidence, and follow the security incident process.
