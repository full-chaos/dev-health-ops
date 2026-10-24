---
page_id: use-sign-in-recover
summary: Reset a forgotten password, request a new verification email, and change your password while signed in.
content_type: task-guide
owner: documentation
source_of_truth:
  - internal/api/session/verify.go
  - internal/api/session/changepassword.go
applicability: current
lifecycle: active
---

# Recover access

Use this procedure when you cannot sign in because you forgot your password or your email address is not verified. It applies to accounts that sign in with an email address and password.

## Reset a forgotten password

1. On the sign-in page, select **Forgot password?**.
2. Enter your email address and select **Send reset link**.
3. Open the message and follow the link. Choose a new password and select **Reset password**.
4. Sign in with the new password.

Expected result: the page confirms that the password was reset.

Things to know:

- The answer is the same whether or not an account exists for that address: "If the account exists, a password reset email has been sent". If no message arrives, check the spelling of the address and your spam folder.
- A reset link works once and expires. If it is refused with "Invalid or expired token", request a new link.
- Resetting a password signs out every existing session for the account.
- Reset requests are limited. After too many in a short time you must wait before asking again.
- Use a password of 12 or more characters with a letter and a digit. This is the rule for new accounts and for changing a password while signed in.
- An account that signs in with GitHub, GitLab, Google, or single sign-on has no Dev Health password to reset. Sign in with the same provider you used before.

## Request a new verification email

If the verification message expired or was lost:

1. Request a new verification email for your address.
2. Open the newest message and follow its link.

Only the newest link works. The answer is the same whether or not the address is registered or already verified: "If an account exists with that email, a verification link has been sent". Requests are limited.

## Change your password while signed in

1. Open **Settings**.
2. In the password section, enter your current password and the new password, then select **Change password**.

Expected result: the page confirms the change. Your other sessions are signed out; this session continues.

Things to know:

- The current password must be correct, or the change is refused.
- Accounts managed by a sign-in provider are refused with "Password is managed by your sign-in provider".
- Password changes are limited. After too many in a short time you must wait before trying again.

## If it does not work

See [Sign-in problems](../troubleshooting/sign-in.md).
