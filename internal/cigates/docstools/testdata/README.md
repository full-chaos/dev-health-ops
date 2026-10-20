# Recorded Python/Go parity pairs

`parity_test.go` (`TestCLIOutputParityRecordedPair`) replays each case under `parity/` with `docstools.Run` in a temp work directory (the repository root of the run) and compares exit code, stdout, stderr and every written file byte for byte, and fails on any extra file. Case layout: `args` (one per line, verb first), `rc`, `stdout`, `stderr`, `in/`, `out/`; `{WORK}` is the work directory. The Python side was recorded once, locally, with the scripts as at the parent of the porting commit. Parts follow.

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
A case with `stderr.traceback` has no `stderr`: Python stopped on an uncaught exception there and printed a traceback.
The Go verb prints exactly the one line in `stderr.traceback` instead (the exception line, `Kind: message`; `{WORK}` is
substituted); the harness compares Go stderr to it exactly and requires exactly one line. Its exit code, stdout and
written files are compared with the Python run as in every other case. No case skips stderr.

`genqm-crlf-doc` and `driftqm-crlf-doc`: the page has CRLF line ends. Python reads text mode (universal newlines) and
writes LF: the drift check passes, the generator writes an LF page. `*-invalid-utf8-*` cases: the file is not UTF-8,
Python raises UnicodeDecodeError (traceback, rc 1, nothing written); Go prints `UnicodeDecodeError: <path>: invalid UTF-8`.

Not a parity pair, by decision: a kind in `registry.json` and `migration-state.json` that no `deployment.json` process
lists in `job_kinds`. Python iterated only the process kinds and passed (rc 0, the kind absent from the table). Go
refuses (rc 1, the kind named) because a guard must not pass a kind it leaves out; `queuemap_guard_test.go` covers it.

Also not a parity pair, by decision (D6129): a kind a process lists in `job_kinds` whose `registry.json` queue is not one
of that process's `queues`. Python generated the page without the kind and passed (rc 0, "Queue mapping docs drift
check passed"). Go refuses (rc 1; `gen-queue-mapping` stderr names the kind, process and queue, stdout empty;
`check-queue-mapping-drift` prints `ERROR: generator failed to render ...` on stdout); the expectation is hand-written in
`TestQueuemapRefusesKindQueueMismatch`, since a recording from Python would say rc 0.

Unicode rows (D6129 amendment): every ported regex and character test, with the Python source it ports.

| Python | Go | Same Unicode semantics | Planted rows |
| --- | --- | --- | --- |
| `KEY_RE` `` `([a-z][a-z_]*(?:\.[a-z][a-z_]*)?)` `` (literal ASCII classes) | `invTxKeyRe`, same pattern | yes, ASCII-only in both | `check-investment-drift-key-word-nonascii` (cafe with e-acute, u-umlaut), `check-investment-drift-key-digit-nonascii` (U+0663), `check-investment-drift-key-nbsp-padded` (U+00A0) |
| `JSON_BLOCK_RE` ```` ```json\s*(.*?)\s*``` ```` (DOTALL) | `invTxJSONBlockRe`, `\s` as `a1PySpace` | yes | `check-investment-drift-json-nbsp-fence` |
| `str.strip()` | `a1Strip` (`a1IsPySpace`) | yes | `check-investment-drift-block-unicode-space` |
| `str.splitlines()` | `invTxSplitLines` | yes (the Python separator set) | `check-investment-drift-block-unicode-space` |
| `sorted()` of str | `sort.Strings` | yes (UTF-8 byte order is code point order) | `genqm-nonascii-names`, `driftqm-nonascii-names` (kind with e-acute) |
| `repr()` of str | `a1Repr`, `a1ListRepr` (`qmList` quotes only the curated ASCII queue names) | yes | none needed (the kind is not repr-ed) |

`queuemap.go` has no regex or character test. `*-nonascii-names`: the kind `sync.cafe` carries an e-acute in all three
JSON files; Python recorded rc 0 and the page row, Go matches.

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

Cases with `stderr.traceback` (Python died on a traceback; the Go verb prints the one exception line and exits 1):
`gen-taxonomy-missing-doc`, `check-investment-drift-missing-doc`, `check-investment-drift-no-markers`,
`check-investment-drift-bad-mapping-line`, `check-investment-drift-invalid-utf8-page`. `check-investment-drift-json-nan-example`
holds JSON examples with NaN, Infinity and -Infinity (accepted by Python's json.loads, so the keys are checked) and one
with a constant as a key (rejected, the block is skipped).

Differences on purpose:

- The Go verbs read the live registry (`internal/jobs/workgraph/units`), which is sorted; Python read source-literal order
  (the page, and `gen_taxonomy_docs.py`, used the order of the set literal). The stub above has the sorted order, so the
  recording equals the Go output. The real page was regenerated in the sorted order, so the block test compares it exactly.
- "THEMES not found / not a set literal / non-string literal / lacks theme prefix" ValueErrors have no Go equivalent.
- `generated subcategory mapping drift` is not recorded: Python printed the expected dict in set-iteration order (changes with
  `PYTHONHASHSEED`). Go prints it in sorted-key order; `invtaxonomy_guard_test.go` asserts it.
- Python's `str` regex and string classes are Unicode: `\s` (and `str.strip()`) match exactly the code points where
  `str.isspace()` is true (`\t`-`\r`, `\x1c`-`\x1f`, space, `\x85`, `\xa0`, `\u1680`, `\u2000`-`\u200a`, `\u2028`, `\u2029`,
  `\u202f`, `\u205f`, `\u3000`; checked over all code points). The Go port uses the same set (`a1PySpace`, `a1Strip`),
  not Go's `\s` or `unicode.IsSpace` (which lacks `\x1c`-`\x1f`). Cases: `check-investment-drift-json-nbsp-fence` (NBSP,
  U+3000, `\x1c`, `\x1f` around the JSON fence body hide no unknown key) and `check-investment-drift-block-unicode-space`
  (generated-block lines padded with `\x1f`, U+3000, U+2003 U+00A0, U+1680, U+0085 still parse). The other regex in the
  package, the key regex, has only `[a-z_.]` classes (ASCII in Python too); no `\w`, `\d` or `\b` is ported.
- JSON examples decode as in Python: NaN, Infinity and
  -Infinity outside strings are accepted (rewritten to null before Go's decoder); other input Python rejects, Go rejects.
