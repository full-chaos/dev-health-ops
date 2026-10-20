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

Cases added with the refusal-parity round (same commands as above; Python 3.12.3, `LC_ALL=C.UTF-8`):

- `*-missing-args`, `bump-extra-args`: too few (or, for bump, too many) arguments. Python crashed unpacking
  `sys.argv` (rc 1, empty stdout); Go prints its usage line (`stderr.traceback`) with rc 1. `grants-extra-args` and
  `pg-api-extra-args` are accepted by Python (it slices `argv`) and by Go. `render-missing-args` is Python's own rc 2
  and empty stdout; only the usage text differs, pinned in `stderr.traceback`.
- `*unicode*`: Python's `\w`, `\s` and `\b` are Unicode (letters and numbers, and the str.isspace set: `\t\n\v\f\r`,
  `\x1c-\x1f`, space, `\x85`, `\xa0`, `\u1680`, `\u2000-\u200a`, `\u2028`, `\u2029`, `\u202f`, `\u205f`, `\u3000`).
- `*invalid-utf8*`: Python read the file in text mode, or `.decode()`d the `git show` bytes, and stopped on an
  uncaught `UnicodeDecodeError` (rc 1, empty stdout). `bump-local-invalid-utf8` shows the prod file already written;
  `bump-prod-invalid-utf8` shows nothing written. Go prints `UnicodeDecodeError: <path or sha:path>: invalid UTF-8`.
- `bump-local-lacks-*`: Python's bare `AssertionError`; Go names the missing text.

Not recorded, because the Go verb differs on purpose: the usage lines (they name the verb, not the `.py` file).
`guards_test.go` covers those. `grants-no-function` is recorded: Python stopped on an IndexError (traceback on
stderr, empty stdout, exit 1); Go prints the line in its `stderr.traceback` instead.
