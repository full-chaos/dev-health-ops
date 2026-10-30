---
page_id: ref-auth-identity-linking
summary: How a sign-in provider identity is bound to an account, when a sign-in links, creates or refuses, what just-in-time provisioning does, and what an administrator sees.
content_type: reference
owner: engineering
source_of_truth:
  - internal/auth/identity/identities.go
  - internal/auth/identity/externalkey.go
  - internal/auth/identity/social/service.go
  - internal/auth/identity/oidc/service.go
  - internal/auth/identity/saml/service.go
  - internal/api/session/social.go
  - internal/api/sso/provision.go
applicability: current
lifecycle: active
---

# Identity linking and just-in-time provisioning

This page describes which account a federated sign-in reaches, when an account
is created, and which outcomes an administrator sees. Federated sign-in means
a social provider (GitHub, GitLab, Google) or an organization's enterprise SSO
provider (OIDC, OAuth or SAML).

SSO sign-in is served by go-api today. auth-service also carries an SSO
implementation that is mounted but not in use; a new path is planned. The
behaviour below holds for the path in use unless a line says otherwise.

## How a sign-in finds its account

A sign-in is matched to an account by the identity that the provider asserts for
the person. The email address never selects an account.

## Sign-in outcomes

| Situation | Outcome |
| --- | --- |
| The key is bound to an account | Signed in to that account. The email is not read. |
| The key is new, and an account already holds the email | Refused with a conflict. Nothing is written or linked. For a social sign-in the answer names the existing sign-in method, so the user can sign in that way and then link. |
| The key is new, no account holds the email, and creation is allowed | An account is created (see below). |
| The key is new and creation is not allowed | Refused. Nothing is written. |
| The key is new and the provider cannot show a verified email (social, OAuth) | Refused. No account is created or linked. A social key that is already bound signs in without this check; an SSO OAuth sign-in is checked every time. |

## Explicit link and unlink

A signed-in user adds or removes a social identity with a request that carries
the user's bearer session (cookies are not read). These routes are mounted only
when `AUTH_SOCIAL_LINK_ROUTES` is `true` (see
[configuration](configuration.md)).

- The session must have been signed in within the last 10 minutes; an older
  session is asked to sign in again.
- The provider token must have been issued to the configured client of that
  provider.
- An account holds at most one identity per provider. Linking an identity the
  account already holds changes nothing.
- An identity held by another account is refused with a conflict and is never
  moved.
- An account that already holds a different identity at the same provider is
  refused.
- Unlinking an identity that is not the account's answers not found.
- The account's last sign-in method cannot be unlinked while the account has no
  password.
- Link and unlink are each written with an audit record in one transaction.

## Account creation

**Social sign-in.** A new key with a verified provider email, held by no
account, creates a user with a verified email and no organization. The user
finishes onboarding (create an organization, or join with an invite) before
using the product. Which email counts as verified is the provider's: GitHub's
primary verified address, else its first verified one; GitLab's confirmed
address; Google's verified address.

**Enterprise SSO (just-in-time provisioning).** Each SSO provider row has an
`auto_provision_users` switch (on by default), an optional `allowed_domains` list and a
default role (`member` when unset). With `auto_provision_users` on:

1. The response must carry an email. When `allowed_domains` is set, the email
   domain must be in the list (compared exactly). An email with no domain is
   refused when the list is set. These checks run on every sign-in, also for a
   key that is already linked: when its domain leaves the list, or the provider
   sends no email, the next sign-in is refused. To let the user in again, an
   organization admin adds the domain to `allowed_domains`.
2. A new key whose email is held by another account is refused with an
   identity conflict.
3. Otherwise the user, the identity and the membership in the provider's
   organization, with the provider's default role, are created in one
   transaction. A failure writes nothing.
4. A known account with no membership in any organization joins the provider's
   organization with the default role. A member of another organization only is
   refused.

With `auto_provision_users` off, a new key is refused and nothing is written, and
a known account with no membership in the provider's organization is refused
with `membership_required`. With it on, a known account joins as in step 4;
`membership_required` then applies to an account that is a member of another
organization only. An SSO sign-in never issues a session outside the provider's
organization.

For OAuth providers the provider must report the email as verified. For OIDC and
SAML providers the identity provider's response is the source of the email.

## What is trusted

- **OIDC.** The ID token must be signed with an asymmetric algorithm and must
  carry the expected issuer, audience, nonce, subject and valid times. The
  login is bound to the browser that started it.
- **SAML.** The response or the assertion must be signed with an allowed
  algorithm, address this service's entity, ACS URL and recipient, be within
  its validity window, and answer an open request. A used assertion is not
  accepted again. Encrypted assertions and transient name identifiers are
  refused.
- **Social providers.** The provider token must have been issued to the
  configured client.

## What an administrator sees

**go-api (SSO and social sign-in in use).** A refusal answers with an HTTP
status and a message. `identity_conflict`, `membership_required` and
`oauth_domain_not_allowed` are also carried as response codes. The message of an
OIDC or SAML domain refusal names the email domain. Each SSO sign-in attempt
writes an `SSO_LOGIN` audit row, with status success or failure.

**auth-service.** Social exchange, link and unlink outcomes are counted by
`auth.social.outcomes` and logged with a reason code; OIDC outcomes are counted
by `auth.oidc.outcomes`. An OAuth sign-in refusal that go-api audits also writes
the `sso_login` failure row go-api writes, with go-api's stage and message,
before it answers: `state_auth` (the state expired or does not match, or the
login cookie is missing or wrong), `provider_profile` (the profile has no user
id), `email_verification` (no verified email), `provisioning` (an unusable email
or subject, auto-provisioning off, an identity conflict or an email another
account holds, or no membership) and
`domain_check` (the domain is off the list; the message names it). When that
row cannot be written, the sign-in answers 500. A provider error go-api records (the OAuth
token exchange failing, the OAuth profile request failing in transport or with
a non-2xx status, or a SAML assertion without an email) is recorded as go-api
records it: the provider row's `last_error`, `last_error_at`
and `updated_at` (its status is left as it was) and an `sso_login` failure row
with no stage, in one transaction, before the sign-in answers; the SAML answer
is 400 `SAML authentication failed`. When the record cannot be written, the
sign-in answers 500. No other OIDC or SAML refusal writes a row. The reason
codes are:

| Reason | Meaning |
| --- | --- |
| `email_collision` | An account holds the email; sign in with the existing method, then link. |
| `identity_conflict` | The key or subject belongs to another account. |
| `provider_already_linked` | The account already holds another identity at this provider. |
| `email_unverified` | The provider did not report a verified email. |
| `membership_required` | The account is not a member of the provider's organization. |
| `provisioning_disabled` | The key is new and `auto_provision_users` is off. |
| `domain_not_allowed` | The email domain is not in `allowed_domains`. |
| `signup_unavailable` | Account creation is not available on this path. |
| `principal_deactivated` | The account is disabled. |
| `replayed` | A SAML assertion or flow was already used. |

Provider text never reaches an auth-service response.

Link, unlink and creation through auth-service write audit records
(`auth.identity.linked`, `auth.identity.unlinked`, `auth.principal.provisioned`)
and provisioning events (`external_identity.linked`, `external_identity.unlinked`,
`principal.created`, `user.created`, `user.email_verified`).
