# Race weights, one file per package (CHAOS-10300)

Measured wall seconds of one package under `go test -race` (CHAOS-6690). ci/go_race_shard.awk balances the race shards by
these weights. A wrong weight costs balance, never coverage: the shards always partition the full package list.

- One file per package of the root module: `<package path with / as __>.tsv`, for example `internal/httpguard` is
  `internal__httpguard.tsv`. The file holds one row: `package<TAB>seconds`. Comment lines (`#`) and blank lines are ignored.
- A new package adds its file: its measured `go test -race` seconds from a hosted race leg (job id in the PR body), 1 when
  it has no test. It edits no other file, so two pull requests that add packages never conflict.
- Value rule (CHAOS-8135): the largest hosted race-leg time of the package; never a local or scaled number.
- `bash ci/check_go.sh race-weights-rows` holds the directory to the package list: a package with no file, a file of a
  package that is gone, a file with more or fewer than one row, a file not named after its package, and a package weighed
  twice (the message prints both weights) all fail. Row order does not matter.
- Two pull requests that weigh the same package edit the same file: Git reports a real conflict, resolved by keeping the
  newer measurement.
- Merge rule (D6122): a pull request whose only overlap with main is files under `ci/go_race_weights.d/` may merge once
  `git merge-tree` shows 0 conflicts and `bash ci/check_go.sh race-weights-rows` exits 0 on the merged tree; no main
  merge and no full CI rerun is needed for that overlap alone.
- Merge rule (D6124, extends D6122): an overlap only on files under `ci/go_race_weights.d/`, `.github/workflows/go.yml`,
  `ci/check_go.sh` and `*.md` merges without a main take when `git merge-tree --write-tree origin/main <head>` exits 0 and
  the merged tree passes `race-weights-rows`, the path-filter tests, `bash -n ci/check_go.sh`, the go.yml parse and `go vet ./...`.
