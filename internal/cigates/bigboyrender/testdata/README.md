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
`in/name` as one argument. Every case records Python's stderr: `stderr` (compared exactly) or, where Python printed a traceback, `stderr.python`
(recorded as is, work and script paths replaced by `{WORK}` and `{A}`) next to `stderr.traceback`, the one line the Go
verb prints instead. The harness accepts `stderr.traceback` ONLY when `stderr.python` starts with `Traceback (most
recent call last):`. The one other declared difference is `usageExceptions` in `parity_test.go` (case -> reason): its
case holds `stderr.usage` (the verb's line) and `stderr.python` (Python's own plain line). Exit code, stdout and written
files are compared with the Python run as in every other case.

The harness runs the BUILT `cigate` binary (arguments and environment passed as bytes, so a non-UTF-8 argument or
variable reaches the verb as it reached Python) and compares the PROCESS stderr, so a stray write by a library
(for example the registry WARN of a short password, `render-short-password`) fails the case.

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

Not recorded, because the Go verb differs on purpose: the other usage lines (they name the verb, not the `.py` file).
`guards_test.go` covers those. `grants-no-function` is recorded: Python stopped on an IndexError (traceback on
stderr, empty stdout, exit 1); Go prints the line in its `stderr.traceback` instead.

Cases added with the review round (same commands; Python 3.12.3, `LC_ALL=C.UTF-8`, Unicode 15.0.0):

- `render-short-password`: a password under the registry minimum; the registry's WARN must not reach stderr.
- `render-password-invalid-utf8`, `render-password-invalid-utf8-no-grants`: the environment holds the byte `\xff`
  (surrogateescape in Python); `password.encode()` raised `UnicodeEncodeError` (rc 1, no file) unless the manifest
  yielded no grants first (Python's own stderr line, `stderr`).
- `bump-new-sha-invalid-utf8`, `bump-arm-hex-invalid-utf8`: a new-value argument with an invalid byte; Python had
  already truncated `values.prod.yaml` (`out/values.prod.yaml` is empty) and the write raised `UnicodeEncodeError`.
  `bump-old-sha-invalid-utf8`: an old-value argument; the `assert` failed first, nothing written.
- `grants-repr-nonprintable`, `grants-repr-invalid-byte`: repr of table names with U+00A0, U+200B, U+0080, U+00AD, U+088F,
  U+E0001, U+10FFFF (escaped `\xhh`, `\uhhhh`, `\Uhhhhhhhh`), and an argv byte `\xff` (`\udcff`, sorted as the code
  point U+DCFF, before U+E000).
- `render-unicode15-unassigned-table`, `grants-unicode15-unassigned-table`: U+088F is assigned in Unicode 17 (Go) and
  not in 15 (Python), so Python does not read it as a word character. `unicode15.go` pins the Unicode 15 tables.
