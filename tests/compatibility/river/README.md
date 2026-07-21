# River compatibility harness

This directory contains the isolated Phase 0 compatibility proof for River
v0.40.0. It exercises River against direct PostgreSQL and against PgBouncer in
transaction mode with River `PollOnly`. The Python path is a compatibility
probe for the rejected `riverqueue` production option; it is not a production
enqueue adapter.

The runner never accepts an external database URL. It creates a unique Docker
Compose project from the pinned
[`compose.compatibility.yml`](compose.compatibility.yml), uses Docker-assigned
ports bound only to `127.0.0.1`, and removes the containers and volumes with
`docker compose down -v` on success, failure, or interruption. It does not
touch the repository's development PostgreSQL or ClickHouse data.

## Prerequisites

- Docker with Compose v2;
- a Go installation with automatic toolchain selection enabled; the runner
  builds both CLIs with and asserts the exact candidate runtime Go 1.25.9;
- `jq`;
- the repository virtual environment synchronized with the locked dev
  dependencies on Python 3.13.14, including `riverqueue`, SQLAlchemy, and
  `asyncpg`.

Prepare that exact interpreter and frozen environment with:

```bash
uv python install 3.13.14
uv sync --python 3.13.14 --frozen --all-extras --dev
```

The runner fails closed if the Python patch version or the pinned
`riverqueue`, SQLAlchemy, or `asyncpg` versions drift.

The default Python executable is `.venv/bin/python`. Set
`RIVER_COMPAT_PYTHON` to another fully prepared Python executable when the
virtual environment lives elsewhere. The runner does not install or download
Python dependencies.

## Run

From the `dev-health-ops` repository root:

```bash
tests/compatibility/river/run.sh > "${TMPDIR:-/tmp}/river-compat-result.json"
```

Stdout contains exactly one combined, sanitized JSON document after every
required assertion passes and the Compose project has been removed. Progress
uses stderr. A failed run exits nonzero without printing a partial result.

Do not run the harness with shell xtrace (`set -x` or `bash -x`). The runner
itself never enables xtrace and never prints database URLs or credentials.

To atomically regenerate the committed evidence from that exact sanitized
stdout schema, run:

```bash
tests/compatibility/river/record.sh
```

The recorder validates the redaction contract before replacing
`docs/architecture/evidence/go-worker-migration/v1-river-spike/local-harness-results.json`.
It leaves the existing evidence untouched when any harness assertion fails.

## What the runner proves

The runner first builds the River v0.40 and nested v0.39 CLIs once each into a
private temporary directory and runs the measured matrices near service
startup, ahead of the next steady-state Compose health check where possible so
`pg_isready` sessions do not contaminate the load deltas. The required N/N-1
migration-prefix steps remain ordered around the direct v0.40 matrix.

For each of `direct` and `poll-only`, it:

1. runs a 20-sample execute/cancel/retry/scheduled-job matrix;
2. asserts every Boolean emitted under `gates` against the mode-specific truth
   table and requires the expected job outcomes;
3. runs Python immediate and future-scheduled commits, rollback, and the
   known-incompatible unique insertion;
4. has the Go worker consume the committed Python payload and verifies the
   version, source, queue policy, and first-attempt completion;
5. starts a blocking crash candidate, waits for its first-attempt start event,
   sends that process a real `SIGKILL`, and requires a fresh worker to rescue
   and complete the same job as attempt 2.

The Python commit and rollback paths also verify the domain-row/River-job
transaction boundary. The unique case must report the selected River v0.40
schema incompatibility; an unexpected successful unique insert fails the run.

Direct mode requires every emitted gate to pass. With River v0.40,
`riverpgxv5`, and `PollOnly`, neither cross-client nor same-client
`JobCancel` propagates cancellation to an already running worker context. The
harness bounds both observations, records both gates as `false`, and then uses
an explicit test-only worker release so retry, scheduling, Python interop, and
crash recovery can continue without presenting the release as River
cancellation. This measured limitation remains an architecture blocker; the
combined run is not labeled a pass.

The nested River v0.39 N-1 integration is deliberately isolated in
`run_nested_n_minus_one` in [`run.sh`](run.sh). The hook runs first on the fresh
database: v0.39 migrates through schema 6, then inserts and completes
old-version work. After the direct v0.40 matrix upgrades through schema 7, the
hook runs again in both directions: v0.39 inserts a versioned job for v0.40 to
consume, then v0.40 inserts the same contract for a v0.39 worker to consume.
Both phase outputs are sanitized before they enter the combined result, and any
migration, insert, work, or consume mismatch fails closed.

## Output and redaction

The combined document has this shape:

```json
{
  "schema_version": 1,
  "status": "complete_with_architecture_blocker",
  "architecture_blocker": "poll_only_running_cancel_not_propagated",
  "evidence_scope": "local_ephemeral_compatibility_harness",
  "samples_per_mode": 20,
  "profiles": [
    {"mode": "direct"},
    {"mode": "poll-only"}
  ],
  "nested_n_minus_1": {"status": "pass"},
  "redaction": {
    "contains_raw_logs": false,
    "contains_credentials_or_dsns": false,
    "contains_job_payloads": false,
    "contains_dynamic_ports": false,
    "contains_container_or_project_ids": false
  }
}
```

The real profiles include the sanitized Go measurements, Python transaction
contracts, cross-language consumer results, and crash-recovery results. They do
not include database URLs, credentials, ports, container or Compose project
IDs, markers, River job IDs, or job payloads.

Tool stdout and stderr, including failure details that might contain driver
connection strings, remain in a mode-`0700` temporary directory and are
deleted by the exit trap. The runner writes no raw logs or evidence files into
the repository. Review the combined JSON before copying it into any committed
evidence artifact.

## Static validation

These checks do not start the compatibility services:

```bash
bash -n tests/compatibility/river/run.sh
shellcheck tests/compatibility/river/run.sh tests/compatibility/river/record.sh
docker compose \
  --project-name rivercompat-static-check \
  --file tests/compatibility/river/compose.compatibility.yml \
  config --quiet
```

The live runner is intentionally separate from ordinary unit tests because it
starts pinned PostgreSQL and PgBouncer containers and performs a real process
kill.
