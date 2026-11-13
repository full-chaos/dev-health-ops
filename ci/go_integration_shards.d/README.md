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
ci/go_providersync_test_shards.tsv, across a fixed seven-way split (CHAOS-9201: was four; shards ran 20-24 min of the 25 min cap) unrelated
to the shard count declared below. Its weight here exists ONLY to control
where the longest-processing-time-first (LPT) bin-packing below places it:
a big enough weight keeps it alone in the lowest-numbered shard, so that
shard's "packages" job never actually runs (see
INTEGRATION_SHARD_NON_PROVIDER_COUNTS in ci/check_go.sh) and every other
package balances across the REMAINING shards instead. Its weight is the
SUM of its own provider-test-shard jobs' reported wall times, from the
same hosted run -- the closest hosted equivalent to "the whole package, run once" available, since CI never runs
it as a single `go test ./internal/providersync` invocation.

internal/jobs/metrics/daily is treated the same way (CHAOS-8813 follow-up): one `go test` of it took 22 of the 25 minutes of a job, so it runs only as the name-partitioned `daily` test shards of ci/go_daily_test_shards.tsv, and its weight here (2400) is a placement weight that keeps it alone in a shard whose "packages" job never runs. Raising `shards` by one keeps the number of running packages jobs unchanged.

This isolation is a property of the WEIGHTS, not a guarantee: it holds only
while this weight is at least as large as the other packages' balanced
per-shard share once split across the remaining shards. The regression
checks that read this file's own planner output
assert that inequality directly from the
planner's printed weights, not from a hardcoded shard membership list --
so a future package addition that erodes this margin fails that assertion
loudly, with the actual numbers, instead of a human silently recounting a
new shard-1 membership as fine.

`internal/workerservice` and `internal/queryapi/server` are isolated by name (`INTEGRATION_ISOLATED_PACKAGE_KEYS` in `ci/check_go.sh`, CHAOS-8935): the planner gives it a shard of its own at its true weight and puts no other package there. When this rule was written, one `go test` started a shard's packages in alphabetical order, so a long package that sorted last started minutes late and set the job's wall time (since CHAOS-11411 a shard's packages start longest weight first, ties by key). Each isolated package takes its own shard, so `_shards.tsv` must count one shard per isolated package on top of the shards the other packages need. A name in that list that matches no discovered package fails the plan.

`internal/workerservice` is also split by top-level test name (CHAOS-9060): one `go test` of it took 24 of the 25 minutes of its job. It runs only as the name-partitioned `workerservice` test shards of `ci/go_workerservice_test_shards.tsv` (the same mechanism as the `daily` shards), never in the `packages` target. Its shard here (its isolated shard, at its weight) therefore has no `packages` job; `_shards.tsv` is unchanged.

`internal/queryapi/server` is split by top-level test name the same way (CHAOS-9162): one `go test` of it takes 1,492 s (24.9 minutes, 751 tests) alone, against the 25-minute job cap. It runs only as the name-partitioned `queryapiserver` test shards of `ci/go_queryapi_server_test_shards.tsv`, never in the `packages` target; its isolated shard here has no `packages` job.

`internal/jobs/metrics/remaining` is isolated by name the same way (CHAOS-9631, `INTEGRATION_ISOLATED_PACKAGE_KEYS`): its own `go test` took 1,053 to 1,164 s on the hosted runner, and in a shard with 20 other packages it started minutes late and set the job's wall time at 22 to 25 minutes of the 25. Alone in its shard it still took 1,122 s of test time and 20 min 21 s of job time, so it is also split by top-level test name (CHAOS-9639): it runs only as the name-partitioned `remaining` test shards of `ci/go_remaining_test_shards.tsv` (the same mechanism as `queryapiserver`), never in the `packages` target. Its isolated shard here (at its weight) therefore has no `packages` job; `_shards.tsv` is unchanged. Every discovered top-level test lands in exactly one `remaining` shard, so a new test cannot fall outside every shard; the Go integration-shard checks assert the shard union equals `go test -list` of the package.

CHAOS-9631 re-weighted every package with the slowest `ok <package> <N>s` seen over three hosted runs of the same job (runs 38481753205, 38481953906, 38482585925, plus job 115617536025 for shard 4). The old weights had drifted far from the hosted time (`internal/api/syncadmin` 6 s against 187 to 260 s, `internal/jobs/investment` 120 s against 479 to 582 s), so shard totals no longer predicted job wall time. The weights of `internal/workerservice` (1,281 s) and `internal/queryapi/server` (969 s) are the sum of their own test-shard jobs from run 38482585925; the old 1,090 s and 520 s were low enough that LPT filled every empty shard with ordinary packages before the isolated one was placed. `internal/providersync` (2,337 s) and `internal/jobs/metrics/daily` (2,400 s) keep their placement weights: the true providersync sum is 5,476 s, but a weight that large would make the balance guard in the Go integration-shard checks compare every shard against that one singleton and stop checking anything. Both stay above the ordinary shards' balanced share (about 1,990 s at 14 shards). A weight is a placement value, not a promise of wall time: `go test` runs four packages at a time, so a shard's wall time is set by its longest package and by the order in which packages start.

`internal/fixturescli` was isolated by name (CHAOS-11411) while one `go test` of it took 1,145 to 1,168 s. The shared servers (#5197) cut it to about 493 s, and CHAOS-11658 took it out of `INTEGRATION_ISOLATED_PACKAGE_KEYS`: an isolated package needs an empty shard when its turn comes in the longest-weight-first order, so at its true weight the plan found none and failed. Inside every `packages` shard the packages start longest weight first, ties by key (`check_integration_package_shard`), so a long package never starts last.

CHAOS-11658 re-weighted every measured package with the ceil of the median `ok <package> <N>s` over five merge_group runs of the `Go` workflow after #5197 (runs 41154835924, 41156120128, 41159152153, 41161275621, 41161426835; REST job logs only). The five split packages (`providersync`, `jobs/metrics/daily`, `jobs/metrics/remaining`, `queryapi/server`, `workerservice`) print no single `ok` line and keep their placement weights. A package `ok` time includes the three packages that run beside it, so a weight is a placement value. At 14 shards the longest planned shard sum fell from 2,287 s to 2,191 s; 13 and 12 shards gave 2,445 s and 2,717 s and let ordinary packages into the shards of `providersync` and `jobs/metrics/daily`, so `shards` stays 14. A shard run prints `::warning title=INTEGRATION WEIGHT DRIFT::` for a package whose measured time is more than 50% from its weight and at least 60 s on either side (`warn_integration_weight_drift`); it never changes the result of the run.

CHAOS-13520 (every job five minutes or less) adds split-by-test packages. A package file may hold a `split-by-test<TAB>N` row (N >= 2): the package then runs as N parts, each a set of the top-level names its shards select (the integration-only names; every name when a tag-only hook makes the package run whole). The names are cut into the N parts longest first (ties by name), and the plan places each part like a package, on the lightest shard; a shard runs a split package's names of that shard through `-run`, and counts its other names as skipped, so no package of the same go test group runs them again. A name weighs its measured `<TestName><TAB><seconds>` row of the same file, else an even share of what the measured rows leave of the package weight (at least 1 s), so a new test joins the plan with no edit here. The plan refuses a test row without the split row, a test row that names no test the shards run, more parts than names, and a split of an isolated package or of a package that already has its own test shards. CHAOS-13622 re-coarsened the legs to about ten minutes: at 50 shards every leg paid its setup and compile again and waited in the runner queue longer than it ran. N is ceil(weight / 540) for every package over 540 s (a package of 540 s or less runs whole), and `shards` is 22 (17 packages shards beside the five shards of `providersync`, `jobs/metrics/daily` and the three isolated packages), so a packages shard holds about 1,260 s of package time (about 570 s of go test wall at the measured 2.2 overlap). At 20 shards one packages shard ran 934 s of go test wall, over the fail line, because `internal/auth/authruntime`, `internal/auth/authapi` and `internal/auth/identity` ran 790 s, 274 s and 130 s against weights of 264 s, 64 s and 14 s; at those measured weights 20 shards plan about 650 s of wall per packages shard, so `shards` went to 22. A packages shard warns when its go test wall passes 600 s and fails past 900 s (`check_integration_shard_wall`). `internal/cigates/splitshards` (TestEverySplitByTestNameRunsInExactlyOnePackagesShard) checks, against `go test -tags=integration -list` of the current tree, that every name of every split package runs in exactly one packages shard of the emitted matrix and that the package sits in at most N shards.

CHAOS-13669 re-weighted every package of the `packages` shards with the ceil of the median `ok <package> <N>s` over the 17 packages shards of three main push runs of the `Go` workflow at 22 shards (runs 43398271620, 43394752490, 43394597857; REST job logs only; a split package's weight is the sum of its parts' `ok` times in a run). The five placement weights (`providersync`, `jobs/metrics/daily`, `workerservice`, `queryapi/server`, `jobs/metrics/remaining`) stay. In those runs every packages shard's go test wall was its longest package's `ok` time plus 32 to 112 s (the other packages run beside it), so the wall is set by the longest part, not by the shard sum: shard 6 ran 779 to 876 s with `internal/storage/postgres/authschema` at 739 to 822 s. A package whose median is over 450 s therefore runs as `split-by-test` parts, N = ceil(median / 450), so that a part plus the overlap tail stays under the 540 s goal. `internal/fixturescli` and `internal/storage/postgres` have a tag-only hook (a `func init` in an integration file), so each of their parts runs every name of its share, untagged ones too. `internal/storage/postgres/authschema` (755 s, 190 tests, no hook, none parallel), `internal/auth/restorefence` (611 s) and `internal/auth/session` (514 s) run as two parts each the same way.
