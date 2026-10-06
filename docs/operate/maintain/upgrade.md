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
work-items unit again. That save switches on only the work-item datasets: a
save changes the datasets of the targets you added or removed and leaves every
other dataset as it is. The targets a configuration shows are read from its
enabled datasets, so "Work Items" shows unchecked while those datasets are off.

**How to see what a save changed**: the API metrics endpoint counts the dataset
rows that saves of sync configurations switched, in
`sync_config_dataset_rows_changed_total{provider="<provider>",direction="enabled"|"disabled"}`,
and writes one `sync_config_dataset_rows_changed` INFO log entry per save that
switched a row (`org_id`, `integration_id`, `provider`, `enabled_dataset_keys`,
`disabled_dataset_keys`). `sync_config_save_stale_base_total{provider="<provider>"}`
counts the saves whose `sync_targets_base` (the list the form was shown) was
not the list the datasets showed at the save: another save, the dataset API or
a backfill changed a dataset while the form was open, and the save kept that
change. These two families replace `sync_target_dataset_drift_repaired_total`,
which is no longer written: a save no longer rewrites datasets it did not
change.
