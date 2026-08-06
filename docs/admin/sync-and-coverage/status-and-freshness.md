---
page_id: admin-sync-status
summary: Verify source identity, permission, discovery, mapping, run state, and freshness before interpreting a missing or delayed product result.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - current provider connection and synchronization surfaces
  - docs/admin/data-sources/incident-response.md
  - docs/operate/run/workers-and-jobs.md
applicability: current
lifecycle: active
---

# Check synchronization status and freshness

Use this procedure after a product user has preserved the failing workspace, scope, period, filters, workflow, and visible state. The goal is to determine whether the source is absent, unauthorized, unmapped, waiting, failed, stale, or successfully synchronized but not relevant to the selected product question.
{: .fc-page-lede }

## Check the source boundary

1. Confirm the expected provider connection exists for the Dev Health organization.
2. Confirm the provider account, host, region, subdomain, installation, or namespace identity is the intended one.
3. Confirm the missing dataset is selected for this connection. An unselected dataset produces no data — that is expected behavior, not a failure, and datasets are opt-in for every provider.
4. Confirm the credential is active and permission preflight passes for the selected datasets.
5. Refresh source discovery and verify that the expected repositories, projects, services, or teams are visible.
6. Confirm each discovered source is mapped to the intended Dev Health repository, team, or workspace scope.

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

### Budget-deferred and budget-exhausted units

A unit whose estimated provider cost does not fit its budget bucket is
*deferred*, not failed: it returns to `retrying` with a later availability
time and the reason `budget_deferred`. This is normal when a bucket is
temporarily full. It is **not** normal for the same unit to stay there.

Read the two states separately:

- **Blocked.** The run's unit rollup reports a budget-blocked count, and each
  unit reports how many times it has been deferred. A dataset that is enabled,
  shows as enabled, and produces nothing is visible here rather than looking
  idle.
- **Exhausted.** A unit that cannot ever fit its bucket — typically the first
  incremental synchronization of a high-cost dataset over a wide initial
  depth — stops being deferred once its deferral count or its elapsed
  deferral time passes the configured caps. It then fails with the reason
  `budget_deferral_exhausted`, and the failure text names the bucket, the
  estimate, the cap it could not fit, and the window span that produced the
  estimate.

An exhausted unit is a configuration outcome, not a provider fault. The two
remedies are to synchronize a narrower window with a bounded backfill, or to
raise that bucket's cap. Raising per-synchronization ingest caps is not a
remedy — it changes what is fetched, not whether the unit is admitted.

### Datasets still catching up

A successful run does not mean a dataset has reached the current time. High-cost
record families synchronize through capped incremental windows, one window per
scheduled tick, and each capped tick finalizes as an ordinary successful run.
Run status alone therefore reads "complete" for a dataset whose coverage may
still be weeks behind.

The run's unit rollup reports, for every (source, dataset) pair it planned that
carries a watermark:

- the stored watermark, and how far behind the current time it is;
- whether that pair is **catching up** — a high-cost dataset trailing by
  strictly more than the configured window cap; and
- roughly how many further scheduled ticks the pair needs to reach the current
  time, at one capped window per tick.

Read these separately from run status:

- **Catching up** is a healthy in-progress state, not a fault. The remedy, if
  the pace is unacceptable, is a bounded backfill over the outstanding period —
  catch-up itself does not accelerate.
- A pair trailing by no more than one window is the steady state of a healthy
  ratchet mid-flight and is not flagged.
- Only high-cost families are capped, so only they are flagged. A lag is still
  reported for every other dataset; interpret a large one there as a different
  problem — a stalled schedule, a failing unit, or a provider gap — and diagnose
  it through the run boundary above.
- A dataset with no watermark at all has never recorded a successful read. It
  reports no lag, because there is no coverage to measure from; that is a
  cold-start or a never-succeeding dataset, not a dataset that is behind.
- A collapsed record family reports one entry per child dataset, not one for the
  family as a whole. Each child carries its own watermark, so a family that
  looks current overall can still hold a badly stale child.
- The estimate of remaining ticks accounts for the configured watermark overlap.
  Each window re-reads that overlap, so one tick's forward progress is the
  window span minus the overlap; a large overlap means many more ticks than the
  window span alone would suggest.

This report is scoped to the run you are reading, and declares that scope
explicitly. It covers only the source-and-dataset pairs that run planned. A run
restricted to one source, or to a subset of datasets, that reports nothing
catching up is not telling you the workspace is current — it is telling you
nothing was behind *among the pairs it touched*. To ask the workspace-wide
question, read freshness per dataset across every configured source rather than
inferring it from a single run.

## Check freshness against the product question

Compare:

- the product time window;
- the source record's updated or event time;
- the latest successful provider read;
- the latest successful processing or materialization time;
- the current time and any known provider delay.

A successful connection or recent worker heartbeat is not proof that the selected product period is covered. A completed run outside the selected time window may be healthy but irrelevant.

### Ask Dev metric freshness

Ask Dev daily metrics use coverage watermarks, not a universal elapsed-hours
threshold. A daily source is fresh when its latest valid materialized day covers
the latest completed UTC day required by the selected window and the required
upstream source is configured and available. The open UTC day is partial and is
not itself evidence that completed data is stale.

Treat the visible states separately:

- `UNCONFIGURED`: no active synchronization configuration covers the required
  upstream dataset;
- `UNAVAILABLE`: the configured source or metric store could not provide a
  usable result;
- `STALE`: the source is available, but its materialized-day watermark does not
  cover the required completed day;
- `PARTIAL`: the selected window includes the open UTC day;
- `NO_MATCH`: the source is available and current, but no rows match the
  authorized scope and window;
- `INSUFFICIENT_EVIDENCE`: rows exist but a required denominator or persisted
  input is absent;
- `ZERO`: matching measured rows explicitly produce zero.

Do not convert any of these states to zero, and do not use a connection's last
successful sync timestamp as a substitute for table-specific coverage.

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
- If workers, queues, migrations, or storage are failing, escalate to [Recover from ingestion failure](../../operate/runbooks/ingestion-failure.md) with run, source, dataset, and timestamps but no credentials.
