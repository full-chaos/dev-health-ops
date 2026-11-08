---
page_id: admin-service-accounts
summary: What an organization service account is, who may manage it, and the lifecycle an administrator runs on it; links the operator runbook for routes, refusals and audit rows.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/auth/serviceaccount/serviceaccount.go
  - internal/auth/serviceaccount/keys.go
  - internal/auth/authapi/provisioning.go
applicability: current
lifecycle: active
---

# Service accounts

A service account is a non-human principal for one organization. It has one named owner, who is a person. This page
tells an administrator what to decide. The operator page,
[Service accounts runbook](../../operate/runbooks/auth-service-accounts.md), holds the routes, the bodies, every
refusal and the audit rows.

A service account is auth-native data. It has no row in the public user tables. The person who owns it is a
user, and `public.users` is the record of that user.

## Know before you start

- The owner must be an enabled user and an active member of the account's organization. This is checked at create, at every change of owner, and again when you enable the account.
- A name is unique inside the organization. Use 1 to 63 characters of `a-z`, `0-9`, `.`, `_` and `-`, starting with a letter or a digit.
- Nothing deletes a service account. Disable it.
- If the owner is disabled or leaves the organization, the account keeps its state. Name a new owner before you enable it again.
- You are an owner or admin member of the organization, or an active superuser. Only a superuser may name a superuser as owner.
- A suspended organization refuses create, enable and change of owner.
- Create, enable, change of owner and the issue and rotate of a key need auth-service to own users (`AUTH_USERS_OWNED`).

## Create

1. Choose an owner who is an active member of the organization.
2. Choose a name.
3. An operator calls `service-accounts/create` with `organization_id`, `actor_principal_id`, `owner_principal_id` and `name`. The routes are internal to auth-service and have no go-api caller yet.
4. A name that is taken is refused (`name_taken`). An owner who is not a member is refused (`owner_not_member`).

## Change owner, disable, enable

| Goal | Route | Refusal to expect |
| --- | --- | --- |
| Move the account to another owner | `service-accounts/owner` | `owner_invalid`, `owner_not_member`, `already_in_state` |
| Stop the account | `service-accounts/disable` | `already_in_state` |
| Start it again | `service-accounts/enable` | `owner_invalid`, `owner_not_member` |

Disable works whether auth-service owns users or not.

## Keys

A key is a secret that proves the account. An account holds one live key, or two during a rotation overlap. The registry
keeps only a digest. The value is shown once, in the answer to issue or rotate.

A key authenticates nowhere yet. No route accepts it yet. Issue a key only when you need to
prepare for that work, and treat the value as a secret from the first moment.

| Goal | Route |
| --- | --- |
| First key | `service-accounts/keys/issue` (optional `expires_in_seconds`, 1 to 31536000; default 90 days) |
| New key with an overlap | `service-accounts/keys/rotate` (`overlap_seconds`, 0 to 86400, required) |
| Kill a key | `service-accounts/keys/revoke` (works on a disabled account too) |
| See the live keys | `service-accounts/keys/list` (no value, no whole digest) |

If an answer is lost on a timeout, the key is live and nobody holds it. List the keys and revoke that one.

## Check the result

Each verb writes one audit row and one provisioning event in the same commit as the change. The runbook lists them.
A refusal writes nothing. Do not share a key value in a ticket or a chat.
