# River compatibility (frozen)

This directory held the Phase 0 compatibility proof for River v0.40.0: a runner (`run.sh`) and a recorder
(`record.sh`) that drove the Go probe and a Python `riverqueue` client against direct PostgreSQL and PgBouncer in
session and transaction mode. Python is gone from this repository, so both scripts are deleted. Nothing in CI ran
them.

What stays:

- `go/` and `nminus1/`: the Go probe, the worker and the nested River v0.39 module, with their Go tests.
- `cmd/river-compat`: the probe's command.
- `compose.compatibility.yml`: the pinned services (the test-image mirror reads it).
- `fixtures/`: the job payloads of both versions.
- `ci/evidence/go-worker-migration/v1-river-spike/local-harness-results.json`: the last recorded run, frozen.

`ci/check_river_compat_static.sh` (go-quality) still checks, without starting services, that the compose file
loads, that the PgBouncer session pool covers the probe's two pools, and that the frozen evidence holds the
recorded truth table and versions.
