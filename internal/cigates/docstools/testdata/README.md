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

# Part r3: symlinked pages and producer records with a missing field

Recorded with Python 3.12.3 (`python3 -I -B`) from the scripts at ops `28adf8cd` (`check_investment_docs_drift.py`,
`gen_queue_mapping_docs.py`, `check_queue_mapping_docs_drift.py`), same work-directory recipe as the parts above.

`check-investment-drift-symlink-*`, `-dir-named-md`: Python walks `docs` with `Path.rglob("*.md")`, then `read_text()`.
A case with a `symlinks` file (lines `<link> -> <target>`, relative to the link's directory) gets those links made in the
work directory before the run (git keeps a dangling or looping link badly, so they are text). Python 3.12 results:
a symlink to a page is read (`symlink-page-unknown-key`: rc 1, the unknown key reported under the link's name); a symlink
to a directory is not descended (`symlink-dir`: rc 0, the page inside is never read); a dangling `*.md` link is listed
and read (`FileNotFoundError`, rc 1); a directory named `*.md`, real or by link, is listed and read
(`IsADirectoryError`, rc 1, `dir-named-md`, `symlink-to-dir-named-md`); a link loop named `*.md` gives
`OSError: [Errno 40]` (rc 1, `symlink-loop`). The tracebacks are pinned as one line each (`stderr.traceback`).

`genqm-missing-*`, `driftqm-missing-*`: one producer key removed from the first record of the list (or the list itself,
`*-top-*`) in `genqm-ok` / `driftqm-ok` inputs. The Python generator indexes every one of these with `[]`: in
`deployment.json` `processes`, `name`, `queues`, `job_kinds`, `binary`; in `registry.json` `jobs`, `kind`, `queue`,
`timeout_seconds`, `max_attempts`; in `migration-state.json` `jobs`, `kind`, `state`, `route`, `rollback_route`. It reads
none with `.get()`. Each gives `KeyError: '<key>'` (traceback), rc 1, stdout empty, for both verbs (the drift check
catches only `SystemExit`, so the `KeyError` is not turned into an `ERROR:` line).

## Stricter than Python: producer values (RISK-NOTES)

Recorded with the scripts at 28adf8cd, Python 3.12.3, on the `genqm-ok` inputs with the `coverage.c` record changed.
Go refuses each of these (rc 1, stdout empty, one stderr line, no page written); Python generated the page (rc 0)
except where marked. Hand-written rows: `TestQueuemapRefusesNullAndNonIntegerProducerValues`.

| input | Python | Go |
| --- | --- | --- |
| `timeout_seconds` 30.0 | rc 0, cell `30.0` | rc 1 |
| `max_attempts` 30.0 | rc 0, cell `30.0` | rc 1 |
| `timeout_seconds` / `max_attempts` true | rc 0, cell `True` | rc 1 |
| `timeout_seconds` / `max_attempts` "30" | rc 0, cell `30` | rc 1 |
| `timeout_seconds` / `max_attempts` NaN | rc 0, cell `nan` | rc 1 (the JSON parser refuses NaN anywhere in the file) |
| `timeout_seconds` null | rc 0, cell `None` | rc 1 |
| `max_attempts` null | rc 0, cell `None` | rc 1 |
| `state` null | rc 0, `state=`None` ⚠` | rc 1 |
| `route` null | rc 0, `route=`None`` | rc 1 |
| `rollback_route` null | rc 0, `rollback_route=`None`` | rc 1 |
| registry `queue` null | rc 0, the kind left out of its row (cells `—`) | rc 1 |
| registry `kind` null | rc 1, `TypeError: '<' not supported between instances of 'str' and 'NoneType'` | rc 1, other message |

Same as Python (recorded pairs): an absent key (`genqm-missing-registry-queue`, `genqm-missing-state`), a case-folded
key (`genqm-key-case-folded-kind`, `genqm-key-case-folded-queue`) and a Unicode-folded key
(`genqm-key-unicode-folded-state`, U+017F): `KeyError` naming the key, rc 1.

# validate-inventory-review

Cases `parity/val-*`. Recorded with Python 3.12.3 (`python3 -I`) from `scripts/validate_docs_inventory_review.py` as at
ops `8f2e19e47` (frozen as `refusals/validate_docs_inventory_review.py.frozen`), the script itself unmodified.

Per case, in a fresh work directory `$W`: write the inputs (below), make the links of `symlinks`, pad (below), then run, with cwd `$W`,
`python3 -I pyrun.py validate-inventory-review <script> <args after the verb>` with `PYTHONHASHSEED=0`. `pyrun.py`
sets `sys.argv[0]` to the verb (so argparse names the verb, not the `.py` file) and `exec`s the script as `__main__`.
Store rc and stdout, every file the run created or changed under `out/`, and stderr with `$W`, the script directory and
the recorder directory replaced by `{WORK}`, `{SCRIPTS}` and `{RECORDER}`. When stderr is a traceback it is kept whole
as `stderr.python`, and `stderr.traceback` holds the exception text that ends it (it can be several lines: the
`Inventory review failed:` message is); the Go verb prints exactly that text. Cases `val-x-*` were built from `val-ok`'s
inputs by one edit each; for an accepted edit of the factual JSON the committed TSV and summary were written by the
script's own `_write_factual_tsv` and `_summary`.

Inputs: a case with a `base` file (`val-ok`) starts from that case's `in/`; its own `in/` holds only the files that
differ and is written over it; its `drop` file lists base inputs it does not have (removed). A `.fixture` suffix is
dropped from a name: it marks an input that is not valid JSON. `placeInputs` in `parity_test.go` does this for every
case; the recording did the same, so the recorded bytes are those of the full inputs.

Padding: the script checks fixed sizes (439 disposition rows: 303 ops, 136 web). A case holds a few rows; its `counts`
file says how many ("disposition ops web"). Before the run, Python's and Go's alike (`ivPadCase` in
`validate_inventory_review_cases_test.go`), the disposition matrix gets `303-ops` ops and `136-web` web filler rows
appended (`full-chaos/dev-health-ops pad/ops-NNNN.md ...`, `full-chaos/dev-health-web pad/web-NNNN.tsx ...`, valid and
non-public), the web snapshot the same web rows, and `docs-data/inventory/ops-~pad.tsv` the ops rows under the standard
header. A file that is missing or not a regular file is not padded. So the recorded script and the Go verb check the
real constants.

`validate_inventory_argparse.golden.json`: Python 3.12 argparse with the script's three options over 3371 argument
vectors (recipe inside); `TestValidateInventoryArgparseMatchesPython` compares exit code, help, usage error and the
`str(Path)` of each value.

The refusal inventory (`refusals/validate-inventory-review.tsv`) maps every line of the frozen script that can refuse
to the cases (run live through the built binary by `TestValidateInventoryRefusalRowsRunLiveThroughTheCLI`) or Go tests
that answer it.

Not recordable, hand-written in `validate_inventory_review_refusals_test.go` with what Python 3.12.3 printed:
mode 000 inputs (`PermissionError`; an unreadable IA directory is an empty glob in Python, so every target URL is
non-IA).

Python's order is not fixed in two places: `REQUIRED_DISPOSITION_FIELDS` is a set (the "missing <field>" lines of one
row come in hash order; Go uses a fixed order, the cases were recorded with `PYTHONHASHSEED=0` and have one missing
field per row) and `LIST_FIELDS` (which list field's `TypeError` comes first when two are bad).

## Stricter than Python (RISK-NOTES)

The Go verb refuses these (exit 1, no stdout, one stderr line `cigate: refused (stricter than the Python checker): ...`);
`TestValidateInventoryStricterThanPython` pins each.

| input | Python 3.12.3 |
| --- | --- |
| a JSON string with a lone surrogate (`\ud800`) in a field the TSV writes | rc 1, `UnicodeEncodeError` |
| the same in a key nothing writes | rc 0 |
| JSON nested deeper than 200 | rc 0 (its limit is higher) |
| `publication_classification` / `artifact_type` values of mixed types (None next to str) | rc 1, `TypeError: '<' not supported ...` |
| two or more numeric (bool/int/float) classification or artifact-type values | sorts them; rc 0 when the snapshots match |
| `rows` a str, an empty str, or an object; a row given as a list of pairs | iterates it (`dict()` of the items); rc 0 or rc 1 by shape |
| a non-UTF-8 file name matched by `ops-*.tsv` or `*.tsv` | rc 0 |
