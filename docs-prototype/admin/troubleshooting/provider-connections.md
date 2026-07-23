---
page_id: admin-provider-fail
summary: Diagnose provider identity, OAuth callback, scope, discovery, mapping, refresh, and bounded synchronization failures.
content_type: troubleshooting
owner: platform-product
source_of_truth:
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - current provider credential-validation and discovery behavior
applicability: current
lifecycle: active
---

# Provider connection failures

Use this guide when authorization completes but the source is not ready, expected repositories or services are missing, token refresh fails, or a bounded synchronization cannot begin.
{: .fc-page-lede }

## Preserve the failing state

Record the Dev Health organization, provider, connection name, provider account or host, region where applicable, authentication mode, selected datasets, visible error, and approximate time. Do not include tokens, client secrets, signing secrets, authorization headers, callback query parameters, or customer-sensitive payloads.

## Identify the failing stage

| Stage | Expected evidence | Common failure boundary |
| --- | --- | --- |
| Runtime configuration | Required client or token settings are present on API and workers | Missing environment value, mismatched secret, stale deployment |
| Authorization | Provider redirects back and Dev Health reports a completed connection | Redirect mismatch, expired one-time state, revoked grant |
| Permission preflight | Every selected dataset reports sufficient access | Missing scope, provider role cannot see required resource |
| Discovery | Expected organizations, projects, repositories, services, or teams appear | Wrong account/host/region, limited identity visibility |
| Mapping | Discovered sources are associated with the intended Dev Health scope | Missing repository/team/service mapping |
| Bounded sync | One small run completes and freshness advances | Worker, queue, provider budget, storage, or schema failure |
| Refresh or webhook delivery | Later runs and signed events continue after initial setup | Client-secret mismatch, encryption-key change, revoked refresh grant or binding |

## OAuth callback failures

Verify that the registered callback and runtime configuration match exactly: scheme, host, port, path, and trailing-slash behavior. For PagerDuty the browser callback is:

```text
/org/admin/integrations/pagerduty/callback
```

It is not the ops API callback route. Start authorization again from Dev Health after correcting the value; provider state is short-lived and one-time.

## Connected but missing permission

A successful OAuth exchange does not prove dataset access. Run permission preflight for every selected dataset. When a scope is missing, update the provider app or identity, then reconnect so a new credential is issued with the required permission set.

For PagerDuty, confirm the consenting identity can see the selected services, teams, schedules, on-calls, users, and incidents. Check account subdomain and region before treating missing discovery as a product bug.

## Connected but no source is discoverable

1. Confirm provider account, host, installation, namespace, region, or subdomain identity.
2. Confirm the provider identity can see the expected source directly in the provider.
3. Refresh discovery rather than manually inventing a source identifier.
4. Verify selected source mappings and dataset families.
5. Run one bounded synchronization after discovery succeeds.

Do not use a broader Jira query, guessed service identity, or text similarity to infer unsupported incident sources. Jira Service Management incident ingestion remains unavailable as a supported setup path until live tenant proof is complete.

## Initial sync fails

If authentication and discovery pass but the first run fails:

- check the admin-visible synchronization or backfill status;
- confirm API and workers share the required provider credentials and encryption settings;
- inspect worker and queue health;
- inspect provider rate-limit or budget deferrals;
- verify PostgreSQL and ClickHouse migrations are current;
- preserve run, source, and integration identifiers for operations.

Escalate worker, queue, migration, or storage failures to [Recover from ingestion failure](../../operate/runbooks/ingestion-failure.md).

## Refresh later fails

For OAuth providers, compare API and worker runtime configuration. A changed `SETTINGS_ENCRYPTION_KEY`, missing client secret on workers, revoked app secret, or revoked grant can allow setup to succeed and later refresh to fail. Update the runtime consistently, then disconnect and reconnect when the grant can no longer be refreshed.

## Webhook validation fails

For PagerDuty V3:

- verify the subscription uses the active opaque binding route;
- verify the provider subscription identity matches the persisted binding;
- verify the HMAC signature against the exact raw body;
- allow multiple `v1=` signatures during secret rotation;
- treat `pagey.ping` as a health event, not an incident;
- do not invent a timestamp header requirement;
- create and verify a replacement binding before revoking the old one.

Unknown events and malformed authority must fail closed.
