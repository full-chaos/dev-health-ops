---
page_id: op-ask-dev-rollout
summary: Run the one-time Ask Dev ephemeral-expiry repair and arm the isolated question-understanding shadow, with the verification signal for each.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - src/dev_health_ops/cli.py
  - src/dev_health_ops/api/dev/persistence/service.py
  - src/dev_health_ops/workers/ask_dev_retention.py
  - src/dev_health_ops/llm/qua_shadow_budget.py
  - src/dev_health_ops/api/dev/production_runtime.py
applicability: current
lifecycle: active
---

# Roll out Ask Dev runtime changes

Two operator actions accompany an Ask Dev deployment: a one-time repair of conversations that the 0-day retention tier could never delete, and an optional, separately budgeted question-understanding shadow used to gather evaluation evidence. Neither is required to serve Ask Dev traffic, and neither changes an answer a user receives.
{: .fc-page-lede }

## Before you begin

- Required role: platform operator with shell access to an API or worker container and the deployment's environment configuration.
- Required environment: a reachable PostgreSQL semantic database. Both procedures read `POSTGRES_URI` (or `DATABASE_URI`); the CLI also accepts `--db`.
- Neither procedure touches ClickHouse, provider credentials, or organization entitlements.

## Stamp stranded 0-day conversations

Organizations on the 0-day (ephemeral) retention tier expect a conversation to be deleted as soon as its work finishes. Before the retention fix existed, a 0-day conversation could reach a state where every one of its runs was terminal but no expiry was ever recorded — leaving it invisible to the purge sweep and retained indefinitely, which is the opposite of what the tier promises. The same was true of a 0-day conversation abandoned before its first message, or one holding a run that a crash left non-terminal.

The repair finds those rows and stamps an expiry on them. The ordinary retention sweep then collects them on its next pass, exactly as it collects any other now-due row.

### The repair usually runs itself

The scheduled sweep performs this repair before every purge pass, so an ordinary deployment needs no operator action at all:

| Fact | Value |
| --- | --- |
| Scheduled entry | `ask-dev-retention-sweep` |
| Task | `dev_health_ops.workers.tasks.run_ask_dev_retention_cleanup` |
| Schedule | Daily at 05:30 |
| Queue | `default` |
| Batch size | 500 rows |
| Batch cap per run | 20 batches for the repair, then 20 for the purge |

Run the command below only when you need the repair to happen sooner than the next scheduled pass — for example immediately after deploying to an environment that accumulated stranded rows, or while the beat schedule is stopped.

!!! note "Prerequisite"
    A deployment with no running Celery beat has no scheduled sweep, so the repair *and* the purge are both operator-driven until beat is restored. Confirm the worker and beat state first: [Workers, jobs, retries, and schedules](workers-and-jobs.md).

### Run the repair manually

```bash
dev-hops maintenance backfill-ask-dev-ephemeral-expiry
```

The command takes no arguments of its own. It drains the full backlog in one invocation, committing each batch of up to 500 rows and stopping when a batch comes back short. It is idempotent and resumable: re-running it after a partial drain continues where it stopped, and re-running it once the backlog is empty stamps nothing.

Only conversations idle for at least one hour are eligible. A conversation created or touched inside that window is never stamped, so an in-flight turn cannot have its expiry set out from under it.

### Verify the result

- The command logs `Ask Dev ephemeral-expiry backfill complete: stamped=N` and exits `0`.
- A second immediate run reports `stamped=0`. That, not the first run's count, is the proof the backlog is drained.
- Purging is a separate step. The rows this stamps are deleted by the retention sweep, not by this command; watch `devhealth_ask_dev_retention_sweep_purged_total` rise on the next sweep.

### If it does not work

- A `PostgreSQL semantic database` prerequisite error means neither `--db` nor `POSTGRES_URI`/`DATABASE_URI` resolved. See [Environment and secrets](../configure/environment-and-secrets.md).
- A stamped count that never reaches `0` across repeated runs means rows are being stranded faster than they are repaired — that is a defect in the retention path, not a backlog. Escalate rather than looping the command.
- For sweep failures rather than repair failures, see [Worker or queue failure](../runbooks/worker-or-queue-failure.md).

## Arm the question-understanding shadow

The question-understanding shadow evaluates an additional model call alongside the deterministic subject resolution, records what it would have proposed, and — by default — never influences the run. It exists to gather evaluation evidence before any decision to let model-proposed subjects take effect.

!!! caution "This spends real provider money"
    For an organization on its own LLM provider, the shadow call is constructed from that organization's own credentials. The isolated quota below bounds what this platform *records*; it cannot bound the customer's own provider-side spend, rate limits, or credits. Enabling the shadow for a BYO organization spends that customer's provider credits on platform evaluation work. Volume is structurally capped at one shadow call per live run — never fire-and-forget, never unbounded — but the cost is real and is the customer's.

### Controls

| Variable | Effect | Default when unset |
| --- | --- | --- |
| `ASK_DEV_QUA_SHADOW_ENABLED` | `1` constructs the shadow provider and evaluates. Any other value, including unset, leaves the seam byte-identical to not existing | Off |
| `ASK_DEV_QUA_COMMIT_ENABLED` | `1` **and** the shadow flag set allows a verified proposal to become the run's subject | Off |
| `ASK_DEV_QUA_SHADOW_MAX_BUDGET_MICRO_USD` | Operator-wide ceiling per organization per calendar month, in micro-USD | `500000` (≈ USD 0.50) |

These are process environment variables on the Ops API, not organization settings and not entries in the feature registry. They apply to every organization the runtime serves; there is no per-organization override and no admin surface for them. Changing one requires an API restart.

Two prerequisites gate the seam regardless of the variables:

- the organization must have the `ask_dev_wave_3_1` feature decision allowed — with it off, no shadow is constructed at all;
- the organization must have a usable provider. Without one the shadow resolves to a typed skip and records nothing.

There is no value of `ASK_DEV_QUA_SHADOW_MAX_BUDGET_MICRO_USD` that disables enforcement. An unparseable value resolves to `0`, which stops every shadow call rather than allowing them.

### Procedure

1. Confirm the organizations in scope have `ask_dev_wave_3_1` allowed. Without it the flag is inert for them.
2. Confirm the model in use is priced. The shadow quota fails closed on an unpriced provider/model pair — it cannot bound what it cannot cost, so it skips instead of guessing. Check `devhealth_ask_dev_platform_model_unpriced_total`; a non-zero value is a configuration defect to fix before arming, not a provider fault.
3. Decide the ceiling. Leave `ASK_DEV_QUA_SHADOW_MAX_BUDGET_MICRO_USD` unset unless you have a reason to move it; the default is deliberately conservative and independent of any organization's own live LLM budget.
4. Set `ASK_DEV_QUA_SHADOW_ENABLED=1` in the Ops API environment and restart the API.
5. Leave `ASK_DEV_QUA_COMMIT_ENABLED` unset. Promoting a proposal to a real subject is a separate decision that should follow shadow evidence, not accompany it.

### Verify the result

Watch `devhealth_ask_dev_qua_shadow_total`, labelled by `status` and by the deterministic decision it ran alongside:

| `status` | Meaning |
| --- | --- |
| `evaluated` | The shadow ran and recorded a proposal |
| `skipped_disabled` | The flag is not `1` — the seam is off |
| `skipped_no_provider` | No shadow provider could be constructed for the organization |
| `skipped_budget_exhausted` | The isolated quota is spent, or the provider/model pair is unpriced |
| `skipped_no_mentions`, `skipped_catalog_unavailable`, `skipped_timeout`, `skipped_provider_error`, `skipped_unexpected_decision`, `skipped_invalid_output` | The shadow declined or failed for the stated reason |

Arming succeeded when `status="evaluated"` appears. A flag set to `1` that only ever produces `skipped_disabled` means the restart did not pick up the variable; only ever producing `skipped_budget_exhausted` means step 2 was not satisfied.

Also watch:

- `devhealth_ask_dev_qua_shadow_fault_total` — every increment is a defect in the shadow path. It can never fail or roll back the live run it shadows, but a rising count is a bug, not noise.
- `devhealth_ask_dev_qua_shadow_cardinality_uncorroborated_total` — organization-wide proposals the deterministic interpreter did not independently reach. Recorded, never acted on.
- `devhealth_ask_dev_qua_commit_total` — must stay at zero while `ASK_DEV_QUA_COMMIT_ENABLED` is unset.

### Roll back

Unset `ASK_DEV_QUA_SHADOW_ENABLED` (or set it to any value other than `1`) and restart the API. The seam then constructs nothing and evaluates nothing. Because the shadow never influenced a live answer, no user-visible behavior changes on the way out, and no conversation, evidence, or retention state needs repair.

## Related information

- [Workers, jobs, retries, and schedules](workers-and-jobs.md)
- [Safe operational controls](operational-controls.md)
- [Feature flags and availability](../../reference/configuration/feature-flags.md)
- [Ask Dev web proxy or browser failure](../runbooks/ask-dev-web-proxy-or-browser-failure.md)
