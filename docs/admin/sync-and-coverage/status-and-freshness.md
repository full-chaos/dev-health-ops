---
page_id: admin-sync-status
summary: Verify source identity, permission, discovery, mapping, run state, and freshness before interpreting a missing or delayed product result.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - current provider connection and synchronization surfaces
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/ops/workers.md
applicability: current
lifecycle: active
---

# Check synchronization status and freshness

Use this procedure after a product user has preserved the failing workspace, scope, period, filters, workflow, and visible state. The goal is to determine whether the source is absent, unauthorized, unmapped, waiting, failed, stale, or successfully synchronized but not relevant to the selected product question.
{: .fc-page-lede }

## Check the source boundary

1. Confirm the expected provider connection exists for the Dev Health organization.
2. Confirm the provider account, host, region, subdomain, installation, or namespace identity is the intended one.
3. Confirm the credential is active and permission preflight passes for the selected datasets.
4. Refresh source discovery and verify that the expected repositories, projects, services, or teams are visible.
5. Confirm each discovered source is mapped to the intended Dev Health repository, team, or workspace scope.

For PagerDuty, verify the expected services are discoverable before diagnosing incident data. A connection with valid OAuth but no access to the relevant service cannot produce complete incident coverage.

## Check the run boundary

Read the administrative activity and execution records together:

- latest planned, dispatched, running, retrying, completed, or failed synchronization;
- active bounded backfill and its selected time window;
- source and dataset family;
- queue or worker handling the run;
- provider-budget or rate-limit deferral;
- terminal error or completion evidence;
- latest successful source and processing timestamps.

Manual, scheduled, and backfill synchronization share the same canonical run model. The timing trigger differs; the execution truth should still identify the planned units and final outcome.

## Check freshness against the product question

Compare:

- the product time window;
- the source record's updated or event time;
- the latest successful provider read;
- the latest successful processing or materialization time;
- the current time and any known provider delay.

A successful connection or recent worker heartbeat is not proof that the selected product period is covered. A completed run outside the selected time window may be healthy but irrelevant.

## Provider-specific checks

### PagerDuty

A healthy current path includes:

- connected status with the intended account, region, authentication mode, and granted scopes;
- permission preflight for every selected dataset;
- service discovery and repository/team mapping;
- a bounded initial incident backfill;
- current REST synchronization and, when configured, a verified V3 webhook binding;
- advancing incident freshness in the product.

The canonical operational target includes services, business services, escalation policies, schedules, on-calls, users, teams, incidents, incident alerts, incident timeline entries, and incident notes. Missing child data can reflect missing parent incidents or missing dataset permission.

### Jira Service Management

Do not interpret a generic Jira issue sync as JSM incident coverage. The JSM incident producer is not a supported release-ready workflow until live tenant proof exists. Ordinary Jira issues, alert-like text, labels, timestamps, or Opsgenie relationships must not be promoted to canonical incidents by inference.

## Result

- If source identity, permission, discovery, mapping, run completion, and freshness are healthy, return to the product workflow and reproduce with the same context.
- If the run is active or deferred, communicate the visible waiting state and expected ownership rather than representing it as zero.
- If the provider connection or mapping is incomplete, correct the administrator boundary and run one bounded verification.
- If workers, queues, migrations, or storage are failing, escalate to [Recover from ingestion failure](../../operate/runbooks/ingestion-failure.md) with run, source, dataset, and timestamps—but no credentials.
