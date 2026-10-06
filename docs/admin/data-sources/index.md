---
page_id: admin-sources
summary: Connect supported providers, understand their authentication boundaries, and verify that source discovery and synchronization cover the intended organization.
content_type: landing
owner: platform-product
applicability: current
lifecycle: active
---

# Connect data sources

A provider connection gives one Dev Health organization permission to discover and synchronize a bounded set of source data. Authentication is only the first stage: the administrator must also verify provider identity, visible namespaces or services, selected datasets, repository or team mappings, and the first successful synchronization.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<div class="fc-topic-card" markdown>

### [GitHub](github.md)

Connect the supported GitHub App or token path, verify installation scope, and select the repositories that belong in the workspace.

</div>

<div class="fc-topic-card" markdown>

### [GitLab](gitlab.md)

Connect a GitLab namespace or instance, verify project visibility, and select the groups and projects that Dev Health should synchronize.

</div>

<div class="fc-topic-card" markdown>

### [Incident-response sources](incident-response.md)

Register PagerDuty OAuth, authorize the organization, discover services, map operational scope, and verify canonical incident synchronization.

</div>

<div class="fc-topic-card" markdown>

### [Credential lifecycle](credential-lifecycle.md)

Rotate, replace, revoke, or disconnect provider credentials without losing the evidence needed to verify recovery.

</div>

</div>

## A connection is ready when

- the provider account, host, region, or installation identity is the intended one;
- required scopes or permissions pass live validation;
- expected organizations, groups, projects, repositories, services, or teams are discoverable;
- selected datasets and mappings match the workspace boundary;
- a bounded initial synchronization or backfill completes;
- the latest successful synchronization and product freshness advance.

## Dataset selection

Every dataset a connection can produce — commits, pull requests, deployments, security alerts, and the rest — is opt-in. A dataset synchronizes only after an administrator selects it for that connection; no dataset is enabled as a side effect of connecting a provider or of a scheduled synchronization running.

GitHub and GitLab connections created before this rule was enforced may already have security-alert synchronization enabled without it ever being explicitly selected. That existing setting is not changed automatically — synchronization continues so no organization loses security-alert data without acting — but it remains visible and disableable like any other dataset from the connection's dataset list. Disable it there if the workspace should not collect security-alert data.

### What the synchronization settings show and what a save changes

Each connection keeps one on/off setting per dataset. The checkboxes in a connection's synchronization settings show that setting: a box is checked when at least one dataset of its data type is on for the connection, including a dataset that was switched on through the API. A data type made of several datasets (pull requests, work items) shows checked when only some of them are on; uncheck it and check it again to switch all of them on.

A save changes only what you changed. Checking a box switches on every dataset of that data type; unchecking it switches them off. A box you did not touch keeps its datasets exactly as they are, so saving a schedule or another setting never switches a dataset on or off. If another administrator, another browser tab, or the API changed a dataset while your form was open, your save keeps that change unless you changed the same box yourself.

Two cases differ:

- **PagerDuty.** The platform manages PagerDuty datasets as one set; they cannot be switched off one at a time. Unchecking the only box ("operational") stops the whole PagerDuty configuration at its next run.
- **Incident data without the incident feature.** When incident data is on for a connection but the organization does not have canonical incident ingestion, you can still save other settings. A save is refused only when it switches incident data on.

A backfill started with `dho backfill run` covers the datasets that are on for the connection. It does not switch a dataset on.

## Availability boundaries

PagerDuty canonical incident ingestion is a supported current path. Jira Service Management incident ingestion is not yet a supported administrator workflow: its code and unit contracts exist, but live tenant proof and release readiness remain blocked. Do not configure broad Jira queries or infer incidents from ordinary issues, alerts, labels, timestamps, or text similarity as a substitute.
