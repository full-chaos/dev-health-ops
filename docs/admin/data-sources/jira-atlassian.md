---
page_id: admin-jira
summary: Connect a supported Jira source, verify project visibility, and understand the current Jira Service Management availability boundary.
content_type: task-guide
owner: platform-product
source_of_truth:
  - current Jira provider implementation
applicability: current
lifecycle: active
---

# Connect Jira or Atlassian

Connect Jira through the workspace administration flow exposed by your Dev
Health deployment. Verify the account and project scope before relying on Jira
work items in product results.
{: .fc-page-lede }

## Before you begin

Confirm:

- the Atlassian site or Jira host belongs to the intended organization;
- the credential is owned and rotated through the approved secret-management
  process;
- the account can read the projects and issue fields required by the selected
  synchronization;
- the Dev Health workspace has permission to create and operate the connection.

Do not place reusable Jira credentials in screenshots, issue comments, or
ordinary logs.

## Connect and verify Jira

1. Open the workspace data-source or connection settings.
2. Choose Jira or Atlassian when that provider is available in the deployment.
3. Complete the supported credential flow for the intended site.
4. Select or configure the bounded project scope.
5. Start a bounded synchronization.
6. Confirm that the expected projects and issues are visible.
7. Check synchronization status, coverage, and freshness before using the data
   in reports or analysis.

Authentication success does not prove project coverage. Verify a known issue
from each required project and confirm that excluded projects remain outside
the workspace boundary.

## Optional webhook delivery

If the deployment uses Jira webhooks, configure them only after the provider
connection works. A webhook supplements discovery and reconciliation; it does
not replace the initial synchronization or historical backfill.

Continue with [Configure a provider webhook](../../integrate/webhooks/configure.md)
and [Verify a webhook signature](../../integrate/webhooks/verify-signatures.md).

## Jira Service Management incidents

Jira Service Management incident ingestion is not yet a supported
administrator workflow. The provider implementation and unit contracts exist,
but live tenant proof and release readiness remain blocked.

Do not broaden an ordinary Jira query, reinterpret alerts, or infer incidents
from labels, timestamps, text similarity, or issue-key prefixes as a
substitute. Use the supported
[incident-response source](incident-response.md) for current canonical incident
ingestion.

## Troubleshoot the connection

- If authentication fails, verify the site identity, credential owner, and
  current permission boundary.
- If projects are missing, check account access and the configured project
  scope separately.
- If synchronization succeeds but data is incomplete, inspect the bounded
  window, selected datasets, coverage, and latest successful freshness time.
- If only webhook delivery fails, keep the connection intact and troubleshoot
  webhook authentication and routing independently.

See [Provider connection failures](../troubleshooting/provider-connections.md)
for the diagnostic sequence.

