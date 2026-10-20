# Recorded Python/Go parity pairs

`parity_test.go` (`TestCLIOutputParityRecordedPair`) replays each case under `parity/` with `bigboyrender.Run` and
compares exit code, stdout, stderr and every file the verb wrote, byte for byte; it also fails when the verb leaves any
file the Python script did not. The Python side was recorded once by running the six scripts as they were at ops
`0731fdb51` (the parent of the commit that ported them, before their deletion) from `ci/bigboy/`:

- Python 3.12.3, git 2.43
- inputs: `parity/<case>/in/` (`*.go.fixture` are copies of Go source, renamed so gofmt and vet skip them;
  `authorization.go.fixture` of the `*-real` and `grants-ok` cases is `internal/storage/clickhouse/authorization.go`
  at that commit, the rest are synthetic)

Each case directory holds `args` (the Go arguments, one per line), `env` (optional `KEY=VALUE` lines), `in/`, `rc`,
`stdout`, `stderr` and `out/` (the files the script wrote or changed, relative to the work directory). Placeholders
in `args`, `env`, `stdout` and `stderr`: `{WORK}` is the case's work directory (the working directory of the run),
`{REPO}` a git repository built by the test, `{OLD}` and `{NEW}` its two commits, `{FILE:name}` the exact content of
`in/name` as one argument. A case with `stderr.traceback` has no `stderr`: Python stopped on an uncaught exception there and
printed a traceback. The Go verb prints exactly the one line in `stderr.traceback` on stderr instead; its exit code,
stdout and written files are compared with the Python run as in every other case.

A posture case (`pg-*`) holds `in/old.go.fixture` and `in/new.go.fixture`: the repository has one commit with each as
`internal/storage/postgres/{api,domain}_authorization.go`, and `in/expect.out` seeds the out file (`# seed`), so an
append and not an overwrite is proved.

The recorded commands, run in a work directory `$W` that holds the files of `in/` (the posture cases use a
repository `$R` with the two commits instead of the files `old.go`/`new.go`); `$A` is the Python script directory:

| cases | Python command |
| --- | --- |
| `render-*` | `API_CH_PASSWORD=<env> python3 $A/render-dho-api-ch-users.py $W/authorization.go $W/out.xml` (no password in `render-no-password`, `absent.go` in `render-absent-manifest`) |
| `grants-*` | `python3 $A/bigboy-grants-check-compare.py "<content of in/actual.tsv>" "<content of in/authorization.go.fixture>"` |
| `pg-api-*` | `BIGBOY_OPS_REPO=$R python3 $A/cut-contents-pg-api-posture.py $OLD $NEW $W/expect.out` |
| `pg-domain-*` | `BIGBOY_OPS_REPO=$R python3 $A/cut-contents-pg-domain-posture.py $OLD $NEW $W/expect.out` |
| `bump-*` | in `$W`: `python3 $A/bump-swap-rewrite-values.py <old> <new> <o7> <s7> <old-op> <new-op> <old-dho> <new-dho> <arm>` (arguments in `args`) |

`$R` is built with `git init`, then for each of the two texts: write the posture file, `git add -A`, `git commit
--allow-empty` (author and committer `t <t@t>`, dates 2026-01-01).

Not recorded, because the Go verb differs on purpose: the usage lines (they name the verb, not the `.py` file).
`guards_test.go` covers those. `grants-no-function` is recorded: Python stopped on an IndexError (traceback on
stderr, empty stdout, exit 1); Go prints the line in its `stderr.traceback` instead.
