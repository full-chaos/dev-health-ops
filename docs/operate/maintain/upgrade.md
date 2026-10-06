---
page_id: op-upgrade
summary: Upgrade an immutable reviewed revision with backups, migration control, health checks, and rollback criteria.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Upgrade Dev Health

1. Review release, configuration, schema, queue, and compatibility changes.
2. Back up required stores and capture current configuration references.
3. Define health, data-progress, and rollback criteria.
4. Pause or bound high-volume backfills if the release requires it.
5. Apply migrations through the supported release path.
6. Deploy the immutable revision.
7. Verify API, workers, queues, stores, product freshness, and one source path.
8. Roll back only according to schema compatibility and retained evidence.

Do not assume application rollback also reverses an irreversible data migration.

## Ask Dev sequential-tool-call wire contract compatibility

The OpenAI-compatible provider's readiness certification fingerprint folds in
a `READINESS_VERSION` constant. This deploy bumps it (v2 -> v3) because the
outbound wire contract changed: native tool requests now send
`parallel_tool_calls` (gated by model family). A v2 certification never
demonstrated that its endpoint accepts the new parameter.

**Expected state immediately after deploy**: every previously stored
readiness certification -- platform and BYO -- reads as not-current. Ask Dev
capability endpoints report degraded/not-ready readiness until preflight
re-runs and re-certifies each configured provider connection. This is
expected, one-time, self-healing behavior, not an incident: no data migration
or manual database change is required.

**Remediation**: run Ask Dev preflight/readiness certification for the
platform connection and every configured workspace BYO connection after this
deploy. Do not treat the transient not-ready state as a provider outage or
attempt to restore the prior fingerprint.

## Work items stopped after migration 0108

Migration 0108 turned off every dataset that a sync configuration's target
list did not name. If a configuration had work-item datasets enabled but its
targets did not include `work-items`, those datasets are now off. Work items
(and the data that depends on them, such as issue-based AI attribution;
pull-request attribution comes from the `prs` dataset) then stop with no
error: the configuration still syncs its other datasets and reports success.

**How to see it**: the scheduler writes one `sync.plan.work_item_family_stopped`
WARN log entry each time it plans a configuration of a work-item provider
(GitHub, GitLab, Jira, Linear) that has no enabled work-item dataset, while the
integration's newest successful work-items unit is **14 days old or newer**. The
entry carries `provider`, `org_id`, `integration_id`, `family`,
`last_success_age_days` (whole days since that unit), and `warn_window_days`.
After 14 days without a successful work-items unit the WARN stops, so a
configuration whose work items were turned off on purpose does not warn for
ever. Every such plan, whatever its history, also increments
`sync_plan_gate_total{provider="<provider>",dataset="work-items",outcome="family_not_enabled"}`
on the scheduler metrics endpoint, and that count does not expire. A
configuration that never ran work items does not write the WARN, because work
items are opt-in for a new integration.

**How to turn work items on again**: tick "Work Items" in the targets of the
sync configuration, or send an API `PATCH` of the configuration with
`sync_targets` that includes `work-items`. The next scheduled run plans the
work-items unit again.
