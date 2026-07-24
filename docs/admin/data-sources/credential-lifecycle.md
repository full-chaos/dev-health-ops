---
page_id: admin-credentials
summary: Replace, rotate, revoke, or disconnect provider credentials while preserving source identity, coverage, and recovery evidence.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/security/credential-encryption-rotation.md
  - current provider credential and revocation implementation
applicability: current
lifecycle: active
---

# Rotate or revoke provider credentials

A credential change affects more than authentication. It can change which organizations, repositories, services, teams, and datasets Dev Health can see. Preserve the current source identity and coverage before replacing a credential, then prove the replacement with a bounded live check before revoking the old authority.
{: .fc-page-lede }

## Before changing the credential

Record:

- Dev Health organization and provider;
- integration source and connection name;
- provider account, host, region, subdomain, installation, or namespace identity;
- selected datasets and source mappings;
- latest successful synchronization time;
- active backfills, scheduled runs, or webhook subscriptions;
- current credential owner and rotation reason.

Do not record the secret value in screenshots, issue comments, logs, or rotation notes.

## Routine replacement

1. Create the replacement with the minimum required access.
2. Add it through the supported Dev Health connection or secret field.
3. Run live authentication or permission preflight for the selected datasets.
4. Confirm the provider identity returned by the replacement matches the existing integration source.
5. Run one bounded synchronization or backfill.
6. Verify that expected namespaces, repositories, services, or teams remain discoverable and that freshness advances.
7. Switch scheduled or webhook delivery to the replacement where required.
8. Revoke the old credential only after the replacement path is healthy.
9. Record the rotation time, verifier, affected source, and any observed coverage gap.

## OAuth connections

An OAuth app credential and an organization authorization are separate lifecycle objects:

- the app client ID and secret configure the Dev Health runtime;
- the organization authorization stores the provider grant and refresh state;
- the API and sync workers must share the app configuration and the stable credential-encryption key;
- changing the app secret can invalidate existing grants and may require reconnecting the organization.

For PagerDuty, update the API and workers together. If the grant or client secret was revoked, disconnect and reconnect the organization after the new runtime configuration is deployed.

## Webhook secret or binding rotation

Webhook rotation must be additive:

1. Create a replacement binding and signing secret.
2. Point the provider subscription at the replacement route.
3. Verify the replacement with the provider's authenticated health event.
4. Activate the replacement.
5. Revoke the superseded binding only after the switch is confirmed.

Do not mutate an old secret in place and assume queued events or replay protection now belong to the replacement. PagerDuty V3 can send more than one `v1=` signature during rotation; Dev Health verifies accepted signatures against the exact raw request body.

## Disconnect

Disconnecting a provider should:

- stop new authorized synchronization and webhook writes;
- revoke or queue revocation of remote grants where supported;
- remove active local bindings and credential descriptors;
- preserve non-secret audit and historical source identity needed to explain prior data;
- leave already persisted canonical evidence unchanged unless a separate retention or deletion workflow applies.

After disconnecting, verify that new jobs fail closed rather than silently using an environment fallback or stale credential.

## Suspected compromise

For a suspected compromise:

1. disable or revoke the credential immediately through the provider and Dev Health control planes;
2. stop affected schedules or webhook bindings when continuing delivery would be unsafe;
3. preserve audit identifiers and timing without copying secret-bearing payloads;
4. rotate the app secret, token, signing secret, and encryption material according to the incident scope;
5. reconnect and validate through the same bounded checks as a routine replacement;
6. follow the security incident process for exposure assessment and notification.

Continue with [Check synchronization status and freshness](../sync-and-coverage/status-and-freshness.md) after any credential change.
