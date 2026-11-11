---
page_id: use-sign-in-onboarding
summary: Create your organization and add a first tool the first time you sign in.
content_type: task-guide
owner: documentation
source_of_truth:
  - internal/api/session/invites.go
applicability: current
lifecycle: active
---

# Set up your workspace

Onboarding runs the first time you sign in with an account that has no organization. It creates your organization and, in the guided flow, helps you add a first tool.

## Create the organization

1. Sign in. Dev Health opens onboarding when your account has no organization.
2. Enter an **Organization Name**, then select **Create Workspace**.

Expected result: you are the owner of the new organization and you move on to the next step.

Things to know:

- Too many attempts in a short time are refused with "Too many requests". Wait, then try again.
- A name that fails validation is refused with the reason shown.
- If you sign in again before you finish, Dev Health sends you back to the step you stopped at.

## Guided setup

Depending on how your deployment is configured, onboarding is a guided flow of three steps: **workspace**, **integration**, and **complete**. Without it, onboarding is the single workspace page and you go to the dashboard after you create the organization.

### Add a tool

The integration step is titled **Connect your tools**. Until you add a tool, delivery and review views stay empty.

- **App install.** The first choice. Select it to install the app and return to setup.
- **Other providers.** Choose GitLab, Jira, or Linear to continue in the integrations page of the organization.
- **Personal access token.** Use a token instead of the app.
- **Skip for now.** Continue without a tool. You can add one later from the integrations page.

When a tool is connected, the step shows "Your first integration is connected" and a **Continue** button.

### Finish

The last step shows "You're all set" and a button to go to the dashboard.

## After setup

Until the first sync finishes, the dashboard shows a setup prompt that names the next action:

| Prompt | What to do |
| --- | --- |
| Connect a source to begin | Start the app install. |
| Integration setup skipped | Connect a tool when you are ready. |
| First sync pending | Choose repositories and start the first sync. |
| Sync could not complete | Read the message and retry. Your setup is kept. |

The prompt goes away when the first sync is complete.

## If it does not work

If onboarding shows "We couldn't load your onboarding", select **Retry**. Your progress is kept. See also [Sign-in problems](../troubleshooting/sign-in.md).
