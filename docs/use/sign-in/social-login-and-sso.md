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
| Single sign-on (SSO) | Your organization's identity provider | Your organization, on a plan that includes SSO |

## Social sign-in

- The providers are GitHub, GitLab, and Google. No other provider is supported.
- You sign in at the provider, and Dev Health receives your profile from it. Dev Health never sees your provider password.
- The first sign-in creates a Dev Health account that is already email-verified and has no Dev Health password. You then continue to onboarding to create or join an organization.
- If a provider is not configured for the deployment, its button is absent. Asking for it directly is refused with "Social login not configured".
- If the email address of your provider account is already registered with a different provider, sign-in is refused and names the provider to use. Sign in with that provider instead.
- A social account has no Dev Health password, so password reset and password change do not apply. See [Recover access](recover-access.md).
- If you see "Social login failed", try again or use email and password if your account has one.

Which providers appear is decided by whoever runs your deployment, not per organization. Ask your administrator if a provider you need is missing.

## Single sign-on

- SSO connects Dev Health to your organization's identity provider using OpenID Connect, OAuth, or SAML.
- SSO is an enterprise feature. Without an entitlement for your organization, SSO requests are refused with a message that the feature is not licensed.
- Your organization's administrator sets up and activates an SSO provider. To use SSO, or to find out whether your organization has it, ask your organization admin.
- The Dev Health sign-in page does not show an SSO button. SSO sign-in starts from your organization's identity provider or from a link your administrator gives you.
- A person's first SSO sign-in creates an account and membership only if the administrator turned automatic account creation on for that provider. Otherwise the sign-in is refused with "User not found and auto-provisioning disabled", and an administrator must add you first.
- An account managed by SSO has no Dev Health password to change.

Administrators diagnosing SSO problems: see [Authentication and role problems](../../admin/troubleshooting/authentication-and-roles.md).

## If it does not work

See [Sign-in problems](../troubleshooting/sign-in.md).
