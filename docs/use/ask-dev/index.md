---
page_id: use-ask-dev
summary: Open Ask Dev, ask an engineering question, read the answer with its evidence and limits, and manage your conversations.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - docs/admin/ask-dev/index.md
applicability: current
lifecycle: active
---

# Ask Dev: ask a question and read the answer

Ask Dev takes an engineering question in plain words, investigates it across the data Dev Health already holds, and returns an explanation with its evidence, its limits, and what is missing.
{: .fc-page-lede }

!!! warning "Availability"
    Ask Dev is built but is **not turned on in production today**. It appears only in a deployment where an administrator has enabled it for your organization, and only when the Dev Health web app is configured to reach the Ask Dev service. If you do not see it, it is not enabled for you. See [Turn on Ask Dev](../../admin/ask-dev/index.md) for what an administrator needs. This page describes the behavior that is built; it makes no promise about when it will be available to you.

## Before you begin

- You are signed in, and your organization has Ask Dev enabled.
- You are not impersonating another user. Ask Dev is hidden while you impersonate.
- Ask Dev is not offered on sign-in, marketing, or platform-validation pages.
- Your access is checked on every question. Ask Dev answers only from data you are allowed to see.

## 1. Open Ask Dev

Use any of these:

- the **Ask Dev** button in the top bar, which opens a side panel;
- the shortcut Ctrl+J (Cmd+J on macOS);
- the command palette;
- the **Ask Dev** row in the left navigation, which opens the full page at `/dev`;
- **Expand** in the side panel, which moves the conversation to the full page.

## 2. Ask a question

Type in the box that reads "Ask about a project, a repository, a team…". A question can be up to 8,000 characters.

Ask Dev handles questions of these kinds today: status, completion forecast, investment, comparison, cohort ranking, quadrant, sankey and sunburst views. It does not rank people against each other.

### Page context

On some pages Ask Dev offers a removable chip for what the page shows, such as one repository or one team. Chips appear only where an administrator has also enabled contextual entry points. A chip is a hint, not a limit: nothing is sent when you open the panel, only the entity identifiers go with your next question, and you can remove the chip first. Filters, the time window, your role, and people are never sent.

## 3. Read the answer

An answer can contain:

- an explanation in plain words, with the main drivers and findings;
- **evidence** you can open in a drawer, so you can check the claim against the source records;
- **Coverage** and **Limitations**, which say what the answer could and could not use;
- one **Missing data** sentence when data is absent. Missing data is not the same as zero, and Ask Dev does not treat it as healthy;
- an **AI-written** badge whenever a model wrote part of the analysis;
- a chart, table or other view, only when the question calls for one.

Ask Dev words its explanations as what the evidence *appears* or *suggests*. Check the evidence before you act on it.

## 4. Follow up, retry and stop

- Ask a follow-up in the same conversation.
- **Try again** repeats a question that failed.
- Stopping a running question is available only in deployments that have turned on stopping. If you do not see a stop control, your deployment has not.
- Rate **Correct?** and **Useful?** on an answer with yes or no.

## 5. Manage conversations

Your history lists your conversations. You can reopen one or delete it. Deleting cannot be undone, and it does not stop a question that is still running. Renaming a conversation is not available.

Retention is a per-conversation tier of 0 days or 30 days. A conversation from the web app uses the service default, which is 0 days: the content is meant to be removed 24 hours after its last turn. An expired conversation is not served on any read; the stored rows are removed only where the operator has turned on the Ask Dev service's retention cleanup job, and the web app does not request the 30-day tier. Do not rely on a conversation being kept.

## If it does not work

| You see | What it means |
| --- | --- |
| No Ask Dev button | Ask Dev is not enabled for your organization, you are impersonating, or the page is excluded. |
| "Too many questions in a short time" | You hit the per-user limit. Wait a minute and ask again. |
| "Your organization has used this month's Ask Dev allowance" | The monthly allowance is spent. An administrator can ask the platform owner about it. |
| "No repository is available to you here" | Your organization has no repository you may use. Ask Dev needs one in this version. |
| The panel says Ask Dev is not available | The service is not reachable or streaming is off in this deployment. Tell an administrator. |

## Related information

- [Investment](../investment/index.md)
- [Turn on Ask Dev](../../admin/ask-dev/index.md)
- [Privacy and telemetry](../../admin/security-and-audit/privacy.md)
