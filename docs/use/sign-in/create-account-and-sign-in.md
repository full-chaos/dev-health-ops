---
page_id: use-sign-in-account
summary: Create an account with an email address and password, verify the address, sign in, and sign out.
content_type: task-guide
owner: documentation
source_of_truth:
  - internal/api/session/register.go
  - internal/api/session/login.go
  - internal/api/session/verify.go
  - internal/auth/passwordpolicy/passwordpolicy.go
applicability: current
lifecycle: active
---

# Create an account and sign in

Use this procedure when you sign in with an email address and password. If your organization uses single sign-on, or you sign in with GitHub, GitLab, or Google, read [Choose social sign-in or single sign-on](social-login-and-sso.md) first.

## Before you begin

- A working email address that can receive a verification message.
- A password that meets the rules below.
- If you were invited to an organization, keep the invite code or link.

## Password rules

A password must:

- be 12 to 128 characters long;
- include at least one letter and at least one digit;
- not be a commonly used password.

If a password breaks a rule, sign-up is refused and the message lists every rule that failed.

## Create an account

1. Open the sign-up page and enter your email address and a password. A full name and an organization name are optional.
2. Accept the Terms of Service and Privacy Policy, then select **Create account**.
3. Open the verification message sent to your email address and follow its link.

Expected result: the page confirms that your email was verified and offers **Sign in**.

Things to know:

- An email address can be registered once. Registering it again is refused with "Email already registered". Sign in or [recover access](recover-access.md) instead.
- Depending on how your deployment is configured, sign-up can also create a workspace for you and make you its owner. If it does not, you join an organization with an invite code at your first sign-in.
- Sign-up attempts are limited. After too many in a short time you must wait before trying again.

## Sign in

1. Open the sign-in page, enter your email address and password, then select **Sign in**.
2. If you have no organization yet, you continue to onboarding. Choose to create an organization or to join one with an invite code.

Expected result: you land in the product, or in onboarding on your first sign-in.

Things to know:

- The sign-in page shows a button for GitHub, GitLab, or Google only when your deployment has that provider configured.
- A wrong password, an unknown email address, a disabled account, and an account that has no password all give the same refusal ("Invalid email or password" on the sign-in page). This is deliberate: it does not reveal whether an account exists.
- If your email address is not verified, you are not signed in. The page asks you to verify your email address first. Use the link in the original message, or [request a new one](recover-access.md#request-a-new-verification-email).
- After repeated failed attempts the account is locked for a short time and sign-in is refused, even with the correct password. Wait, then try again.
- A signed-in session lasts for a limited time. If it ends, you are asked to sign in again.

## Join an organization with an invite

Select the invite link, or enter the invite code during onboarding. The invite is accepted for the signed-in account. An invite that is used, expired, or issued for a different email address is refused.

## Switch organization

If you belong to more than one organization, switch between them from the product. You can only switch to an organization you are a member of.

## Sign out

Use the product's sign-out control. This ends the current session.

## If it does not work

See [Sign-in problems](../troubleshooting/sign-in.md).
