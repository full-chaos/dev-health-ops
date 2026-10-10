# Go integration shard manifest

This directory is the manifest `ci/check_go.sh` plans the Go integration shards from (it replaces the single file `ci/go_integration_shards.tsv`, CHAOS-6926).

Layout: one file per package, `<package key with "/" written "__">.tsv`, holding exactly one data row `<package key><TAB><estimated seconds>` (comments allowed), plus `_shards.tsv` with the one `shards<TAB><count>` row. A PR that adds a package or re-times one touches only that package's file, so two open PRs cannot conflict on neighbouring lines of one sorted file. The loader (`load_integration_shard_manifest`) reads every `*.tsv` here in C-locale order and fails when a file holds more or fewer than one package row, when a file name is not its key's spelling, or when a key repeats.

The rationale that used to head the single file follows.

Deterministic package weights for Go integration CI sharding.

The `shards` row (in `_shards.tsv`) declares the shard count. Every other row is
<repository-relative package key><TAB><estimated seconds>. The package set is
checked against ci/check_go.sh's live integration-tag discovery before any
shard runs, so this file cannot silently omit or duplicate a package.

Every weight below is the observed wall time of that package's own
`go test -tags=integration -count=1` run, read directly from the "ok
<package> <N>s" line the Go test binary prints in the hosted CI shard job's
own log, taken from a green `Go` workflow run against main that already
includes the once-per-process ClickHouse migration chain (earlier runs time
ClickHouse-backed packages much slower and would reintroduce stale weights),
rounded up (ceil) to the next whole second. A prior revision of this file mixed
local runs, an arm64 compute host's runs (roughly 2x local wall-clock), and
ad hoc "rounded up for headroom" local numbers per package as each package
gained tests -- accurate on the day each number was written, but a stale
proxy for what the SAME package costs on the actual hosted CI runner today,
which is the only venue that decides whether a shard blows its job budget.
This revision replaces every one of those local/estimated numbers with a
real, repeatable hosted measurement, all sourced the same way, so a reader
no longer has to guess which convention produced which row.

internal/providersync does not run under this manifest's "packages" target
at all -- its own top-level tests are shard-balanced separately, by
ci/go_providersync_test_shards.tsv, across a fixed four-way split unrelated
to the shard count declared below. Its weight here exists ONLY to control
where the longest-processing-time-first (LPT) bin-packing below places it:
a big enough weight keeps it alone in the lowest-numbered shard, so that
shard's "packages" job never actually runs (see
INTEGRATION_SHARD_NON_PROVIDER_COUNTS in ci/check_go.sh) and every other
package balances across the REMAINING shards instead. Its weight is the
SUM of its own four provider-test-shard jobs' reported wall times, from the
same hosted run -- the closest hosted equivalent to "the whole package, run once" available, since CI never runs
it as a single `go test ./internal/providersync` invocation.

internal/jobs/metrics/daily is treated the same way (CHAOS-8813 follow-up): one `go test` of it took 22 of the 25 minutes of a job, so it runs only as the name-partitioned `daily` test shards of ci/go_daily_test_shards.tsv, and its weight here (2400) is a placement weight that keeps it alone in a shard whose "packages" job never runs. Raising `shards` by one keeps the number of running packages jobs unchanged.

This isolation is a property of the WEIGHTS, not a guarantee: it holds only
while this weight is at least as large as the other packages' balanced
per-shard share once split across the remaining shards. The regression
suite that reads this file's own planner output (tests/tooling/
test_go_integration_sharding.py) asserts that inequality directly from the
planner's printed weights, not from a hardcoded shard membership list --
so a future package addition that erodes this margin fails that assertion
loudly, with the actual numbers, instead of a human silently recounting a
new shard-1 membership as fine.

`internal/workerservice` and `internal/queryapi/server` are isolated by name (`INTEGRATION_ISOLATED_PACKAGE_KEYS` in `ci/check_go.sh`, CHAOS-8935): the planner gives it a shard of its own at its true weight and puts no other package there. One `go test` starts a shard's packages in alphabetical order, so a long package that sorts last started minutes late and set the job's wall time. Each isolated package takes its own shard, so `_shards.tsv` must count one shard per isolated package on top of the shards the other packages need. A name in that list that matches no discovered package fails the plan.

`internal/workerservice` is also split by top-level test name (CHAOS-9060): one `go test` of it took 24 of the 25 minutes of its job. It runs only as the name-partitioned `workerservice` test shards of `ci/go_workerservice_test_shards.tsv` (the same mechanism as the `daily` shards), never in the `packages` target. Its shard here (its isolated shard, at its weight) therefore has no `packages` job; `_shards.tsv` is unchanged.

`internal/queryapi/server` is split by top-level test name the same way (CHAOS-9162): one `go test` of it takes 1,492 s (24.9 minutes, 751 tests) alone, against the 25-minute job cap. It runs only as the name-partitioned `queryapiserver` test shards of `ci/go_queryapi_server_test_shards.tsv`, never in the `packages` target; its isolated shard here has no `packages` job.
