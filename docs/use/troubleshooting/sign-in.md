---
page_id: use-sign-in-problems
summary: Match a sign-in, sign-up, or recovery message to its cause and the next step.
content_type: troubleshooting
owner: documentation
applicability: current
lifecycle: active
---

# Sign-in problems

Find the message you see, then follow the next step. If none helps, ask your organization admin and give them the time of the attempt and the exact message, but never your password or a reset link.

| What you see | Likely cause | Next step |
| --- | --- | --- |
| "Invalid email or password" | Wrong password, unknown email address, disabled account, or an account that has no password. The message is the same for all four. | Check the address and password. If you forgot the password, [reset it](../sign-in/recover-access.md#reset-a-forgotten-password). If the account signs in with a provider, use that provider. If the account may be disabled, ask your organization admin. |
| "Please verify your email" | The account exists but the address was never verified. | Follow the link in the verification email, or [request a new one](../sign-in/recover-access.md#request-a-new-verification-email). |
| Sign-in is refused even with the correct password, after several wrong attempts | Repeated failed attempts locked the account for a short time. | Wait, then sign in with the correct password. |
| "Too many requests" or "Too many registration attempts" | A rate limit on sign-in, sign-up, reset, or verification requests. | Wait and try again later. Do not repeat the request quickly. |
| "Invalid or expired token" on a reset link | The link was used, replaced by a newer one, or expired. | [Request a new reset link](../sign-in/recover-access.md#reset-a-forgotten-password) and use only the newest message. |
| "Invalid or expired verification token" | The verification link expired or was replaced. | [Request a new verification email](../sign-in/recover-access.md#request-a-new-verification-email). |
| "Password validation failed" | The password broke a rule. The message lists which. | Use [12 to 128 characters with a letter and a digit](../sign-in/create-account-and-sign-in.md#password-rules) that is not a common password. |
| "Email already registered" | An account already exists for that address. | Sign in, or [reset the password](../sign-in/recover-access.md). |
| No reset or verification message arrives | The address is mistyped, the account does not exist (the answer is the same either way), or the message is in spam. | Check spelling and spam, then request again once. |
| A provider button is missing | The deployment has not configured that provider. | Ask your administrator. See [Social sign-in and single sign-on](../sign-in/social-login-and-sso.md). |
| "This email is already registered with a different sign-in method" | The address belongs to an account created with another method. | Sign in with the original method. |
| "Social login failed", "Access was denied" | The provider refused or the exchange failed. | Try again. If it repeats, use another method on the same account or ask your administrator. |
| "Password is managed by your sign-in provider" | The account uses a provider or SSO, so it has no Dev Health password. | Change the password at the provider. |
| SSO request refused as "not licensed" | The organization has no SSO entitlement. | Ask your organization admin. |
| "User not found and auto-provisioning disabled" | Your SSO sign-in succeeded but no Dev Health account exists, and automatic account creation is off. | Ask your organization admin to add you. |
| "Invalid or expired invite" or an invite refused for your account | The invite was used, expired, or issued for a different email address. | Ask the sender for a new invite to the address you sign in with. |
| Sign-up refused with a 403 from a custom front end | The request did not come from an allowed site. | Use the standard sign-up page, or ask the platform operator to allow the site. |

For role and membership problems after you are signed in, see [Missing permission or unavailable view](permissions-and-availability.md).
