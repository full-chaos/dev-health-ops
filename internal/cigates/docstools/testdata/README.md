# Recorded Python/Go parity pairs

`parity_test.go` (`TestCLIOutputParityRecordedPair`) replays each case under `parity/` with `docstools.Run` in a temp work directory (the repository root of the run) and compares exit code, stdout, stderr and every written file byte for byte, and fails on any extra file. Case layout: `args` (one per line, verb first), `rc`, `stdout`, `stderr` (or empty `stderr.skip`), `in/`, `out/`; `{WORK}` is the work directory. The Python side was recorded once, locally, with the scripts as at the parent of the porting commit. Parts follow.

# Recorded Python/Go parity pairs, queue-map verbs

Cases `genqm-*` and `driftqm-*` under `parity/`. The Python side was recorded once from the scripts as they
were at ops `8a47d3e948d99450a614c3ac0fbae11dd2d137a0` (`git rev-parse HEAD`), Python 3.12.3. Each case is replayed by
`TestCLIOutputParityRecordedPair` (exit code, stdout, stderr, written files, no stray files).

Recording: for each case a work directory `$W` receives `in/`, the script is copied to `$W/scripts/` (so its `ROOT` is
`$W`; `check_queue_mapping_docs_drift.py` also needs `gen_queue_mapping_docs.py` beside it), and it runs with cwd `$W`:

| cases | Python command (cwd `$W`) | Go args |
| --- | --- | --- |
| `genqm-*` | `python3 $W/scripts/gen_queue_mapping_docs.py` | `gen-queue-mapping` |
| `driftqm-*` | `python3 $W/scripts/check_queue_mapping_docs_drift.py` | `check-queue-mapping-drift` |


`$W` is replaced by `{WORK}` in `stdout` and `stderr`. `out/` holds each file the script created or changed.
A case with `stderr.skip` ended in a Python traceback (`genqm-missing-doc`, `genqm-bad-json`,
`genqm-job-kind-not-in-registry`); exit code, stdout and files are compared, the traceback is not.

Deliberate message edits made to the recordings after the run (the Go verb names itself, not the dead script):
`gen_queue_mapping_docs:` -> `gen-queue-mapping:`, `CELERY_CORRESPONDENCE` -> `qmCorrespondence`,
`scripts/gen_queue_mapping_docs.py` -> `internal/cigates/docstools/queuemap.go`, and in the stale-block line
`Run 'python scripts/gen_queue_mapping_docs.py' and commit` -> `Run 'cigate docs gen-queue-mapping' and commit`.

Inputs are synthetic: the nine curated process names with a handful of job kinds.

# Part f1: gen-taxonomy, check-investment-drift

Cases `parity/gen-taxonomy-*` and `parity/check-investment-drift-*`. Recorded once with Python 3.12.3 (`python3 -I -B`) from
the scripts at ops `8a47d3e948d99450a614c3ac0fbae11dd2d137a0^` (`scripts/gen_taxonomy_docs.py`) and `bbc071d61`
(`scripts/check_investment_docs_drift.py`); both read the same inputs, so no case depends on the commit.

Record commands, per case, in a fresh work directory `$W` (cwd `$W`, so the script's `ROOT` is `$W`):

1. Write the files of the case's `in/` into `$W`. They are Markdown pages only (`docs/reference/taxonomies/investment.md`,
   other `docs/**/*.md` for the JSON-example cases).
2. Copy the script to `$W/scripts/` (`git show <commit>:scripts/<script>`). Write `$W/src/dev_health_ops/investment_taxonomy.py`
   as a stub of two set literals, `THEMES = {...}` and `SUBCATEGORIES = {...}`, holding the 5 themes and 15 subcategories in
   the sorted order of `units.SortedThemes` / `units.SortedSubcategories`. Neither script file nor stub is stored here.
3. Run `python3 -I -B $W/scripts/gen_taxonomy_docs.py` (cases `gen-taxonomy-*`, Go args `gen-taxonomy`) or
   `python3 -I -B $W/scripts/check_investment_docs_drift.py` (cases `check-investment-drift-*`, Go args `check-investment-drift`).
4. Store rc, stdout, stderr with `$W` replaced by `{WORK}`, and under `out/` every file the run created or changed
   (`gen-taxonomy` rewrites the page).

Case pages: a base page with the 5 theme rows, the 15 subcategory bullets, the placeholder `theme.subcategory`, a
`src/dev_health_ops/investment_taxonomy.py` mention and a generated block in sorted order. `gen-taxonomy-ok` starts from a
block in reverse order, so the run reorders it. The drift cases delete or add one line (theme, subcategory, theme row,
subcategory bullet), add JSON examples (nested `investment`, invalid JSON, list payload, list-valued field) and order several
`docs/**/*.md` files (`B.md`, `a.md`, `b/z.md`, `b.md`).

Substitutions in recorded text: none. No output of these two verbs names the `.py` file or a Python command.

Cases with `stderr.skip` (Python died on a traceback; the Go verb prints a short `Traceback ...` stub and exits 1): `gen-taxonomy-missing-doc`,
`check-investment-drift-missing-doc`, `check-investment-drift-no-markers`, `check-investment-drift-bad-mapping-line`.

Differences on purpose:

- The Go verbs read the live registry (`internal/jobs/workgraph/units`), which is sorted; Python read source-literal order
  (the page, and `gen_taxonomy_docs.py`, used the order of the set literal). The stub above has the sorted order, so the
  recording equals the Go output. The real page was regenerated in the sorted order, so the block test compares it exactly.
- "THEMES not found / not a set literal / non-string literal / lacks theme prefix" ValueErrors have no Go equivalent.
- `generated subcategory mapping drift` is not recorded: Python printed the expected dict in set-iteration order (changes with
  `PYTHONHASHSEED`). Go prints it in sorted-key order; `invtaxonomy_guard_test.go` asserts it.
- `\s` in the JSON-fence regex is ASCII in Go and Unicode in Python; JSON with NaN/Infinity is skipped by Go (Python accepts it).
