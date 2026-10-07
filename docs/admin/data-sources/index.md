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

Each connection keeps one on/off setting per dataset. A checkbox in a connection's synchronization settings is checked when at least one dataset of that data type is on for the connection, including a dataset that was switched on through the API. It is unchecked when all datasets of that data type are off, also when an earlier save selected the data type. A data type that has no dataset setting for the provider shows as it was saved. A data type made of several datasets (pull requests, work items) shows checked when only some of them are on; uncheck it, save, check it again and save to switch all of them on.

A save changes only what you changed. Checking a box switches on every dataset of that data type; unchecking it switches them off. A box you did not touch keeps its datasets exactly as they are, so saving a schedule or another setting never switches a dataset on or off. When all datasets of a data type were switched off after the data type was selected (through the API, or by an uncheck in another configuration of the same connection), its box shows unchecked. A save of the form keeps those datasets off and removes the data type from the saved list. Check the box and save to switch the datasets on.

The save compares your form with the settings at the moment of the save. If another administrator, another browser tab, or the API changed a data type while your form was open, your save applies your form to that data type: reload the form before you save when someone else can have changed the connection.

Two cases differ:

- **PagerDuty.** The platform manages PagerDuty datasets as one set; they cannot be switched off one at a time. Unchecking the only box ("operational") stops the whole PagerDuty configuration at its next run.
- **Incident data without the incident feature.** When incident data is on for a connection but the organization does not have canonical incident ingestion, you can still save other settings. A save is refused only when it selects incident data, or when incident data was selected in an earlier save and its box is still checked. When incident data is in the saved list and its datasets are off, the box is unchecked: a save of the form is accepted and removes incident data from the saved list, and the requests that were refused because of it are accepted after that save. In every other case a save of the list the form shows changes nothing else for the connection: "Sync now", a backfill, a change of the repository selection and the scheduled runs are accepted or refused exactly as before the save. This is also true for an older configuration that covers one repository or project of the connection: such a save gives it the data types that were selected for the connection, never a data type that shows only because its datasets are on. The incident datasets stay on after such a save, so the scheduled runs of the whole connection are not planned until you switch them off or the organization gets the feature. Incident data itself is not fetched while the organization does not have the feature.

A backfill started with `dho backfill run` covers the datasets that are on for the connection. It does not switch a dataset on. A PagerDuty backfill always covers the PagerDuty set.

**API clients.** `GET` of a sync configuration that covers a whole connection returns in `sync_targets` the data types that have a dataset on: first the ones in the stored list (the data types requests selected), in the stored order, then the other data types the settings form offers. A data type the form does not offer (`blame`, `security`) is returned only when it is in the stored list and its dataset is on. A stored data type whose datasets are all off, or that has no dataset row, is not returned; a stored data type the provider has no dataset for is returned as stored. A `PATCH` with `sync_targets` is compared with that list as it is at the save: a data type the request adds is switched on and stored (also `blame` and `security`), a data type the request leaves out is switched off and removed from the stored list (`blame` and `security` are only removed from the stored list: their datasets stay on), and every other data type keeps its datasets. A stored data type that `GET` does not return is removed from the stored list by a `PATCH` that does not name it, and its datasets stay off. The stored list never gets a data type only because its datasets are on. The request field `sync_targets_base` has no meaning and is ignored.

## Availability boundaries

PagerDuty canonical incident ingestion is a supported current path. Jira Service Management incident ingestion is not yet a supported administrator workflow: its code and unit contracts exist, but live tenant proof and release readiness remain blocked. Do not configure broad Jira queries or infer incidents from ordinary issues, alerts, labels, timestamps, or text similarity as a substitute.
