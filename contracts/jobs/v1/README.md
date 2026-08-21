# Dev Health job contracts v1

This directory is the language-neutral source of truth for bounded worker job
arguments. River owns the job kind and queue columns; `encoded_args` contains
only the envelope defined by `envelope.schema.json`.

## Stability policy

- Kinds are lowercase dotted identifiers and are never renamed or reused.
- Contract versions are positive integers. A decoder rejects an unknown
  version instead of guessing.
- The only compatible in-place schema edit is adding an optional field. A
  removal, rename, new required field, type/constraint change, or semantic
  change requires a new version.
- Decoders remain strict: an implementation must explicitly add that optional
  field before advertising the new schema digest, and producers must leave it
  unset until every old digest has drained. “Optional” does not mean arbitrary
  unknown fields are ignored.
- A kind at version N supports both N and N-1 while a rolling deployment may
  contain either binary. Version 1 is the only exception because N-1 does not
  exist.
- A producer may emit a version only after every target worker group reports that
  exact kind/version and schema digest in a capability report. The digest
  covers both the envelope and kind payload schemas, preventing an older
  strict decoder from falsely qualifying after an additive optional-field
  revision within the same version.
- The complete encoded envelope is limited to 16 KiB. Values contain bounded
  identifiers and safe options only. Credentials, DSNs, headers, SQL, raw
  provider/webhook payloads, rendered reports, and tenant data are forbidden.

`registry.json` defines the supported kind/version pairs, their canonical
queues, and execution policy. `migration-state.json` records producer routing
and promotion state without making queue state authoritative for product state.
The validated [`deploy/go-workers/deployment.json`](../../../deploy/go-workers/deployment.json)
deployment manifest selects registered queues for independently scalable worker
groups and declares each group's per-queue concurrency, replica range, resource
limits, and connection footprint.

The application mapping from job kind to queue is canonical. Deployment can
place groups on disjoint sets, such as `sync,sync_provider` and
`investment,metrics`, or intentionally overlap a queue such as `metrics`.
River distributes claims among all consumers of an overlapping queue. A worker
process constructs one River client for its selected queue set; a separate
process is the boundary for a separate group. Deployment changes do not create
new queues or remap a job kind.

`system.retention_cleanup` version 3 adds only the table-scoped
`ask_dev_conversations` policy. Versions 1 and 2 keep their originally
published policy enums; accepting v3 must never make an older-version decoder
accept the new deletion capability.

The registry and Go consumer accept v3 before production emission is enabled.
`migration-state.json` therefore held `producer_version` at 2 until capability
reports from every live worker group proved the v3 schema digest. The fixed
scheduler treats that producer version as an admission boundary and cannot
construct a v3 envelope while the route remains below v3.

**Rollout proof (CHAOS-3481, 2026-08-21):** collected against live fleet
revision `4a39bcf0efc4a948dca9b335e6b1184fa6113a25` -- every production
go-worker-{ops,heavy,sync,sync-provider}, go-scheduler, and go-reconciler
container reported that exact `org.opencontainers.image.revision` (the
`retention` queue is consumed only by go-worker-ops). A capability report
collected from that revision's image
(`ghcr.io/full-chaos/dev-health-go-contractcheck:latest`, same revision label)
declared `system.retention_cleanup` versions `[1, 2, 3]` with v3 digest
`sha256:7a50e5aca532c7e3fbdce68bdd8d5441a4f22393f7cbbf7619dc550f664c21c6`,
matching this tree's compiled digest exactly. `worker-contractcheck rollout
--queues retention --report <that report>` against a candidate tree with
`producer_version=3` returned `all live capability reports support producer
versions` (rc=0). `producer_version` is now 3.

## Validation

From the repository root:

```text
go run ./cmd/worker-contractcheck validate
go run ./cmd/worker-contractcheck capabilities --queues metrics
go run ./cmd/worker-contractcheck compare --base <old-v1-dir> --candidate contracts/jobs/v1
```

`compare` exits non-zero for a breaking in-place edit and is designed to be
wired to CI with the merge-base contract directory as `--base`. `validate`
also fails when the registry and deployment groups drift, a long-running
process receives the migration DSN, or the rendered maximum connection budget
exceeds its checked-in PostgreSQL/PgBouncer ceilings.
