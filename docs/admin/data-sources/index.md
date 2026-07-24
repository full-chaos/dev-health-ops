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

## Availability boundaries

PagerDuty canonical incident ingestion is a supported current path. Jira Service Management incident ingestion is not yet a supported administrator workflow: its code and unit contracts exist, but live tenant proof and release readiness remain blocked. Do not configure broad Jira queries or infer incidents from ordinary issues, alerts, labels, timestamps, or text similarity as a substitute.
