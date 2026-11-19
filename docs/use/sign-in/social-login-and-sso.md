---
page_id: use-sign-in-methods
summary: Compare email-and-password, social sign-in, and single sign-on, and see who controls each method.
content_type: concept
owner: documentation
source_of_truth:
  - internal/api/session/social.go
  - internal/api/sso/sso.go
  - internal/api/sso/gate.go
applicability: current
lifecycle: active
---

# Social sign-in and single sign-on

Dev Health has three ways to sign in. They differ in who holds the credential and who decides whether the method is available to you.

| Method | Who holds the credential | Who decides it is available |
| --- | --- | --- |
| Email and password | Dev Health | Always available for accounts created with a password |
| Social sign-in | GitHub, GitLab, or Google | The deployment: a provider button appears only when the deployment has configured it |
| Single sign-on (SSO) | Your organization's identity provider | The deployment turns it on, and your administrator shares a sign-in link |

## Social sign-in

- The providers are GitHub, GitLab, and Google. No other provider is supported.
- You sign in at the provider, and Dev Health receives your profile from it. Dev Health never sees your provider password.
- The first sign-in creates a Dev Health account that is already email-verified and has no Dev Health password. You then continue to onboarding to create or join an organization.
- If a provider is not configured for the deployment, its button is absent. Asking for it directly is refused with "Social login not configured".
- A social account has no Dev Health password, so password reset and password change do not apply. See [Recover access](recover-access.md).
- If you see "Social login failed", try again or use email and password if your account has one.

Which providers appear is decided by whoever runs your deployment, not per organization. Ask your administrator if a provider you need is missing.

## Single sign-on

Single sign-on uses your organization's identity provider. It supports OpenID Connect and SAML.

The sign-in page has no single sign-on form and no list of providers. Single sign-on starts from a link that your organization administrator shares.

1. Open the link your administrator shared.
2. Sign in at your identity provider.

Expected result: you return to Dev Health signed in. If the link led to a specific page, you return to that page.

If SSO is off for the deployment, or the link is not valid, you land on the sign-in page with a message.

| Message after you return | What it means |
| --- | --- |
| Single sign-on is not available for this account: it is not a member of the organization. | Your account is not a member of the organization that owns the provider. Ask an administrator to add you. |
| Single sign-on could not link this account to an existing sign-in. Contact your administrator. | The identity matches a sign-in that exists already. |
| Single sign-on is not included in the plan of this organization. Contact your administrator. | The plan has no SSO. |
| Single sign-on is temporarily unavailable. Please try again later. | A temporary fault. |
| Single sign-on failed. Please try again or use another sign-in method. | Any other refusal. |

Administrators diagnosing SSO problems: see [Authentication and role problems](../../admin/troubleshooting/authentication-and-roles.md).

## If it does not work

See [Sign-in problems](../troubleshooting/sign-in.md).
