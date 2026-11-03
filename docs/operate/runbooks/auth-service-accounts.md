---
page_id: op-rb-auth-service-accounts
summary: Create, disable, enable and change the owner of an organization's service accounts through auth-service, and what each refusal and audit row means.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Service accounts

A service account is a non-human principal that belongs to one organization and has one named owner. auth-service is
the only writer of service accounts. This page covers the lifecycle: create, disable, enable and change of owner. It
covers no credential: a service account holds none yet, so it cannot sign in or call anything.

## Rules

- Every account has an owner. The owner is an enabled user and an active member of the account's organization, at
  create and at every change of owner.
- Losing the owner does not disable the account. If the owner is disabled or leaves the organization, the account
  keeps its state, and Enable refuses until the account has an owner that passes the same check. Name a new owner
  first.
- Names are unique inside an organization. A name is 1 to 63 characters of `a-z`, `0-9`, `.`, `_` and `-`, and starts
  with a letter or a digit.
- Nothing deletes a service account. Disable it.
- Create, enable and change of owner need auth-service to own users (`AUTH_USERS_OWNED`). Disable works either way.
- A suspended organization refuses create, enable and change of owner.

## Routes

The routes are internal to auth-service. They sit behind `AUTH_MEMBERSHIP_ROUTES` with the other provisioning routes,
take the session issuer service credential (`svc_authsess_` with `memberships:write`) or the go-api workload token,
admitted as the other provisioning routes admit it, and have no go-api caller yet.

| Route | Body | Answer |
| --- | --- | --- |
| `POST /internal/v1/service-accounts/create` | `organization_id`, `actor_principal_id`, `owner_principal_id`, `name` | `service_account_id`, `organization_id`, `name`, `status`, `owner_principal_id` |
| `POST /internal/v1/service-accounts/disable` | `service_account_id`, `actor_principal_id` | `service_account_id`, `status` |
| `POST /internal/v1/service-accounts/enable` | `service_account_id`, `actor_principal_id` | `service_account_id`, `status` |
| `POST /internal/v1/service-accounts/owner` | `service_account_id`, `actor_principal_id`, `owner_principal_id` | `service_account_id`, `owner_principal_id` |

The actor is an active superuser, or an owner or admin member of the account's organization. The rule is read on the
live users and membership rows before the verb and again inside it, after its locks. A non-superuser actor may not
name a superuser as an owner, nor move an account off a superuser owner. An account that does not exist is named
(404) only to an active superuser; any other actor gets the refusal it would get for an account of another organization
(`admin_required`), so the reason never shows whether the account exists.

## Keys

A key is `fcsak_` and 32 random bytes. The registry keeps its SHA-256 digest, never the value. An account holds one live key, or two during a rotation's overlap.

A key authenticates nowhere until CHAOS-12362 lands: no route accepts one yet.

Issue and rotate need auth-service to own users (`AUTH_USERS_OWNED`). Revoke works either way.

| Route | Body | Answer |
| --- | --- | --- |
| `POST /internal/v1/service-accounts/keys/issue` | `service_account_id`, `actor_principal_id`, optional `expires_in_seconds` | `credential-receipt.v1` kind `issue`, with the value once |
| `POST /internal/v1/service-accounts/keys/rotate` | `service_account_id`, `actor_principal_id`, `overlap_seconds`, optional `expires_in_seconds` | `credential-receipt.v1` kind `rotate`, with the value once |
| `POST /internal/v1/service-accounts/keys/revoke` | `credential_id`, `actor_principal_id`, `reason` | `credential-entry.v1`, also on a repeat |
| `POST /internal/v1/service-accounts/keys/list` | `service_account_id`, `actor_principal_id` | `credential-list.v1`, the live keys, no value and no whole digest |

`expires_in_seconds` is 1 to 31536000. If it is absent, the expiry is 90 days. `null` is a malformed body. `overlap_seconds` is 0 to 86400 and it is required.

The actor rule is the rule above. Issue and rotate also refuse a non-superuser on a superuser-owned account, before the verb and again after its locks. Revoke has no such check, so a kill stays open on a disabled account, a suspended organization and a lost owner. List runs the rule once and writes nothing. An unknown key answers as an unknown account does: 404 `key_not_found` to an active superuser, the actor refusal to any other actor.

A route answer lost on a timeout leaves a live key with no holder. List the keys and revoke that key.

A blind margin restore cannot undo a revocation or a shortened expiry made after the target, so such a key is live again until its expiry, up to 365 days. The apply names the count of live keys in `rotate_keys_after_blind_margin`: rotate or revoke every key.

## Refusals

| Status | `reason_code` | Meaning |
| --- | --- | --- |
| 400 | `malformed` | The body is not one strict JSON object of the route's members. |
| 400 | `name_invalid` | The name is outside the name pattern. |
| 403 | `principal_deactivated`, `admin_required`, `superuser_required` | The actor rule refused. |
| 404 | `organization_not_found`, `service_account_not_found` | No such organization or account. |
| 409 | `organization_suspended` | The organization is suspended. |
| 409 | `name_taken` | The organization has an account with that name. |
| 409 | `owner_invalid` | The owner is not a user, or is disabled. |
| 409 | `owner_not_member` | The owner is not an active member of the organization. |
| 409 | `already_in_state` | Disable of a disabled account, enable of an active one, or the current owner named again. |
| 400 | `expiry_invalid`, `overlap_invalid`, `reason_invalid` | A key route's value is outside its bounds. |
| 404 | `key_not_found` | No such key. |
| 409 | `service_account_disabled` | Issue or rotate on a disabled account. |
| 409 | `key_live` | Issue on an account with a live key. |
| 409 | `key_none` | Rotate on an account with no live key. |
| 409 | `key_rotating` | Rotate during an overlap. |
| 409 | `state_changed` | The owner or organization changed while the verb ran. Disable reads again once before it answers this. |
| 409 | `users_not_owned` | auth-service does not own users. |
| 503 | `store_unavailable` | The store failed. Nothing was written. |

## Audit and events

Each verb writes one audit row and one provisioning event, in one transaction with the change.

| Verb | Audit event | Reason code | Provisioning event |
| --- | --- | --- | --- |
| Create | `auth.service_account.created` | `principal_provisioned` | `service_account.created` |
| Disable | `auth.service_account.disabled` | `principal_disabled` | `service_account.disabled` |
| Enable | `auth.service_account.enabled` | `principal_enabled` | `service_account.enabled` |
| Change of owner | `auth.service_account.owner_changed` | `service_account_owner_changed` | `service_account.owner_changed` |

The row's actor is the human the call acts for, `target_id` is the account, `principal_id` is the owner the row
concerns (for a change of owner, the new owner; the previous owner is the `principal_id` of the earlier rows), and the
caller credential is named as on the other provisioning rows.
