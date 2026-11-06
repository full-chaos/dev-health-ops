---
page_id: op-ask-dev-rollout
summary: Repair Ask Dev conversations that the 0-day retention tier could never delete, with the verification signal.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - internal/maintenancecli/maintenance.go
  - internal/jobs/system/retention_postgres.go
  - internal/scheduler/fixed/ask_dev_admission.go
applicability: current
lifecycle: active
---

# Repair stranded Ask Dev conversations

One operator action accompanies an Ask Dev deployment: a repair of Ask Dev conversations that the 0-day retention tier could never delete. It is not required to serve Ask Dev traffic.
{: .fc-page-lede }

> **Note:** Ask Dev is served by ACR, not by the Go api or the removed Python `/api/v1/dev` surface. This repair applies only to Ask Dev conversation rows that are still in PostgreSQL (`public.dev_conversations`).

## Before you begin

- Required role: platform operator with shell access to an API or worker container and the deployment's environment configuration.
- Required environment: a reachable PostgreSQL semantic database. The procedure reads `POSTGRES_URI` (or `DATABASE_URI`); the CLI also accepts `--db`.
- The procedure does not touch ClickHouse, provider credentials, or organization entitlements.

## Stamp stranded 0-day conversations

Organizations on the 0-day (ephemeral) retention tier expect a conversation to be deleted as soon as its work finishes. Before the retention fix existed, a 0-day conversation could reach a state where every one of its runs was terminal but no expiry was ever recorded — leaving it invisible to the purge sweep and retained indefinitely, which is the opposite of what the tier promises. The same was true of a 0-day conversation abandoned before its first message, or one holding a run that a crash left non-terminal.

The repair finds those rows and stamps an expiry on them. The ordinary retention sweep then collects them on its next pass, exactly as it collects any other now-due row.

### Run the repair manually

```bash
dho maintenance backfill-ask-dev-ephemeral-expiry
```

The command takes no arguments of its own. It drains the full backlog in one invocation, committing each batch of up to 500 rows and stopping when a batch comes back short. It is idempotent and resumable: re-running it after a partial drain continues where it stopped, and re-running it once the backlog is empty stamps nothing.

Only conversations idle for at least one hour are eligible. A conversation created or touched inside that window is never stamped, so an in-flight turn cannot have its expiry set out from under it.

### Verify the result

- The command logs `Ask Dev ephemeral-expiry backfill complete: stamped=N` and exits `0`.
- A second immediate run reports `stamped=0`. That, not the first run's count, is the proof the backlog is drained.
- Purging is a separate step. The retention sweep deletes the rows this stamps; confirm by rerunning the command (`stamped=0`) and checking that the rows are gone from `public.dev_conversations`.

### If it does not work

- A `PostgreSQL semantic database` prerequisite error means neither `--db` nor `POSTGRES_URI`/`DATABASE_URI` resolved. See [Environment and secrets](../configure/environment-and-secrets.md).
- A stamped count that never reaches `0` across repeated runs means rows are being stranded faster than they are repaired — that is a defect in the retention path, not a backlog. Escalate rather than looping the command.
- For sweep failures rather than repair failures, see [Worker or queue failure](../runbooks/worker-or-queue-failure.md).

## Related information

- [Safe operational controls](operational-controls.md)
- [Feature flags and availability](../../reference/configuration/feature-flags.md)
