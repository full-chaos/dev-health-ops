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
| Single sign-on (SSO) | Your organization's identity provider | Not available yet |

## Social sign-in

- The providers are GitHub, GitLab, and Google. No other provider is supported.
- You sign in at the provider, and Dev Health receives your profile from it. Dev Health never sees your provider password.
- The first sign-in creates a Dev Health account that is already email-verified and has no Dev Health password. You then continue to onboarding to create or join an organization.
- If a provider is not configured for the deployment, its button is absent. Asking for it directly is refused with "Social login not configured".
- A social account has no Dev Health password, so password reset and password change do not apply. See [Recover access](recover-access.md).
- If you see "Social login failed", try again or use email and password if your account has one.

Which providers appear is decided by whoever runs your deployment, not per organization. Ask your administrator if a provider you need is missing.

## Single sign-on

Single sign-on is not available yet through the Dev Health sign-in page, and the sign-in page has no SSO button. To find out whether or when your organization can use it, ask your organization admin.

Administrators diagnosing SSO problems: see [Authentication and role problems](../../admin/troubleshooting/authentication-and-roles.md).

## If it does not work

See [Sign-in problems](../troubleshooting/sign-in.md).
