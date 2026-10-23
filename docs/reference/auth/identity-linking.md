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

## The identity key

An external identity is the triple **issuer, provider, subject**.

- The three values are compared exactly, byte for byte. Case and white space are
  not folded; two spellings are two identities.
- A social provider has a fixed issuer. An SSO subject is scoped to one
  configured provider row, never to a protocol name, so the same subject at two
  provider rows is two identities.
- One account holds at most one identity per provider.
- A sign-in is matched by this key. The email address never selects an account.

## Sign-in outcomes

| Situation | Outcome |
| --- | --- |
| The key is bound to an account | Signed in to that account. The email is not read. |
| The key is new, and an account already holds the email | Refused with a conflict. Nothing is written or linked. For a social sign-in the answer names the existing sign-in method, so the user can sign in that way and then link. |
| The key is new, no account holds the email, and creation is allowed | An account is created (see below). |
| The key is new and creation is not allowed | Refused. Nothing is written. |
| The provider cannot show a verified email (social, OAuth) | Refused. No account is created or linked. |

## Explicit link and unlink

A signed-in user adds or removes a social identity with a request that carries
the user's bearer session (cookies are not read). These routes are mounted only
when `AUTH_SOCIAL_LINK_ROUTES` is `true` (see
[configuration](configuration.md)).

- The session must have been signed in within the last 10 minutes; an older
  session is asked to sign in again.
- The provider token must have been issued to the configured client of that
  provider.
- Linking an identity the account already holds changes nothing.
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
   refused when the list is set.
2. A new key whose email is held by another account is refused with an
   identity conflict.
3. Otherwise the user, the identity and the membership in the provider's
   organization, with the provider's default role, are created in one
   transaction. A failure writes nothing.
4. A known account with no membership in any organization joins the provider's
   organization with the default role. A member of another organization only is
   refused.

With `auto_provision_users` off, a new key is refused and nothing is written. An SSO
sign-in always ends in the provider's organization: an account without a
membership there is refused with `membership_required`.

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

The codes below are the auth-service outcome codes; they appear in logs and in
the `auth.social.outcomes` metric. go-api answers with an HTTP status and a
fixed message, and carries `identity_conflict`, `membership_required` and
`oauth_domain_not_allowed` as response codes. Provider text never reaches the
response.

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
| `principal_deactivated` | The account is disabled (auth-service). |
| `replayed` | A SAML assertion or flow was already used. |

Link, unlink and creation write audit records (`auth.identity.linked`,
`auth.identity.unlinked`, `auth.principal.provisioned`) and provisioning events
(`external_identity.linked`, `external_identity.unlinked`, `principal.created`,
`user.created`, `user.email_verified`).
