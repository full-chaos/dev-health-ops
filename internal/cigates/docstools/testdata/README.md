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

# Part c1: check-built-site-links, check-docs-search, check-docs-facts

Cases `parity/links-*`, `parity/search-*`, `parity/facts-*` and the helper corpora under `helpers/`. Recorded once with
Python 3.14.7 (`python3.14 -I -B`, Unicode 16.0.0: the Python the CI jobs run, set up as `python-version: '3.14'`) from `scripts/check_built_site_links.py`,
`scripts/check_docs_candidate_search.py` and `scripts/check_docs_candidate_facts.py` at ops `a64af6106a44`
(`git show a64af6106a44:scripts/<script>`). The three scripts, their tests under `tests/docs` and the `.py` registry the facts
check read are not part of the recording.

Record recipe, per case, in a fresh work directory `$W` (cwd `$W`, so the facts script's `ROOT` is `$W`): write the files of the
case's `in/` into `$W` (a `.fixture` suffix is dropped; `symlinks` lines `<link> -> <target>` are made after), copy the script to
`$W/scripts/`, run `python3 -I -B $W/scripts/<script> <args>` (`{WORK}` in `args` is `$W`), store rc, stdout and stderr with `$W`
replaced by `{WORK}`, and under `out/` every file the run created or changed (the search report). The facts cases also carry a
stub `src/dev_health_ops/investment_taxonomy.py` (the 5 themes and 15 subcategories as two set literals) in `in/`; the Go verb does not read it.

Cases with `stderr.traceback`: Python died on an uncaught exception (a traceback); the Go verb prints the one exception line
instead. Three of those lines are edited on purpose: for `UnicodeDecodeError` Go names the file, Python the byte position
(`links-invalid-utf8-page`, `facts-page-invalid-utf8`).

| Go verb | Python | cases |
| --- | --- | --- |
| `check-built-site-links` | `check_built_site_links.py` | `links-*`: Unicode `\b` before `href=`/`src=`/`id=` (the `\w` of a non-ASCII letter, digit or underscore blocks the match; a rejected match restarts one character on), same-page anchors skipped when `--site-dir` is relative (`relative_to` of a relative path on a resolved one raises), `Path.resolve()` of symlinks, loops and NUL, `urlsplit` ValueErrors, `rglob` order and symlinked directories, unreadable pages |
| `check-docs-search` | `check_docs_candidate_search.py` | `search-*`: ranking, stemming, double `html.unescape`, tag stripping, `str.lower` with final sigma and U+0130, URL normalisation, every `max_rank` form, the TSV report bytes (tab dialect, `\r\n`), error order |
| `check-docs-facts` | `check_docs_candidate_facts.py` | `facts-*`: key regex, missing and unknown keys (a dotted placeholder is unknown), extra arguments ignored |

Helper corpora (`helpers/*.jsonl`, rows `{"in", "out"}` or `{"in", "err"}`), each replayed by `dc_helpers_test.go`:
`urlsplit` (4784 strings of URL pieces, control characters, brackets, fullwidth characters), `unquote` (4116, invalid UTF-8
escapes), `unescape` (11402, with the 4300-digit integer limit, every HTML5 entity name with and without `;` and with a suffix, numeric references), `lower` (6625,
final-sigma contexts), `floatrepr` (523), `csvrow` (3000 rows, `csv.writer(delimiter="\t")`), `normalize` (34, `_normalize_url`), `jsonloads` (83: number forms, the 4300-digit limit with and without a sign, repeated keys, escapes, depth), `realpath` (one tree of 16 links, 60 queries relative and absolute: chains, loops, dangling, `..` after a link).
The tables below and the corpora were recorded again with 3.14.7 after the first review round found that 3.12.3 differs from it in `Path.suffix` (a trailing dot is a suffix), `Path.resolve()` (a symlink loop is no error), `Path.exists()`/`is_dir()`/`is_file()` (any error is false) and the `os.lstat` NUL message. `pytables/lower.tsv` (every code point whose `str.lower()` differs from itself), `pytables/charref.tsv` (every numeric character
reference whose `html.unescape` result differs from `chr(n)`) and `pytables/final_sigma.tsv` (Case_Ignorable and Cased ranges, probed
through `str.lower()` of U+03A3) are the Python tables the Go helpers use directly; `TestHelperLowerTableCoversEveryCodePoint` and the corpora check them.

Also run, not stored (the built site needs mkdocs): on the real built site (212 pages, 1352 search documents) the Go verbs gave the same
exit code, stdout and report bytes as Python for `check-built-site-links` (relative and absolute `--site-dir`), the 28 queries of
`docs-data/search-acceptance.json`, and 600 generated queries (538 failing, 62 passing: every rank and top result equal).

## Stricter than Python, or different on purpose (RISK-NOTES)

Each row is in the refusal table `cmd/cigate/docs_checks_refusals_test.go` (103 rows, pinned) unless marked.

- Arguments: no abbreviation of an option, no `-h`/`--help` (exit 0 in Python, exit 2 here), no `--`, no value that starts with `-` (a
  negative number, `"-a b"`, `--site-dir=-x`), no empty value (`Path("")` is `.` in Python). Exit code 2 and a usage line; the text differs from argparse's.
- A bracketed netloc (`//[::1]/x`): Python validates the host as an IP address and accepts a valid one; Go refuses every bracketed netloc
  (an unclosed or stray bracket is Python's own `Invalid IPv6 URL`).
- JSON: a lone surrogate escape (`"\ud800"`) is refused (the Python str keeps it); nesting deeper than 500 is refused (Python raises
  `RecursionError` near 990); the error text of a malformed file is Go's, not `JSONDecodeError`'s, with the same exit code. NaN, Infinity and -Infinity are accepted as Python does.
- Search fields: `location`, `title`, `text`, `query`, `expected` must be strings (Python took `str()` of a truthy number, list or object);
  `max_rank` as a non-empty string is refused (Python parsed it with `int()`). Falsy values still mean the default as in Python.
- Page names that are not UTF-8 and a named pipe named `*.html` are refused (Python listed the first and blocked on the second).
- Paths: every path stays a string as written (pathlib keeps `..`; the kernel resolves `link/../site` through the link, `filepath.Join`/`Dir` would clean it first): the tree walk below `--site-dir`, the report's parent (`new/../report.tsv` makes `new`), and `Path.resolve()` of link targets follow Python (cases `links-site-dir-through-a-link-and-dotdot`, `links-absolute-site-dir-through-a-link-and-dotdot`, `links-site-dir-with-a-dotdot-component`, `search-report-path-with-dotdot-makes-the-parent`, `search-paths-through-a-link-and-dotdot`). The search report is opened and truncated, and its header written, before anything is ranked: a ranking error leaves the header and the rows so far, as in Python (`search-ranking-error-leaves-the-header-in-the-report`, `search-ranking-error-leaves-the-rows-so-far-in-the-report`).
- Working directory: the facts script took its root from its own path (`parents[1]`), so it ran from any directory; the Go verb takes the nearest directory at or above the working directory that holds `go.mod` (cases `facts-from-the-taxonomy-directory`, `facts-from-a-new-nested-directory`) and refuses a working directory outside any module (STRICTER; refusal row `facts outside any module`). The links and search scripts have no root: their paths, and the default report `.build/docs-search-acceptance.tsv`, are relative to the working directory in both (cases `links-from-a-nested-directory`, `search-from-a-nested-directory`, `search-default-report-from-a-nested-directory`; a case may carry a `cwd` file naming the directory, below the work directory, the run starts in).
- `check-docs-facts` reads the Go registry (`internal/jobs/workgraph/units`, the source `check-investment-drift` reads) and not
  `src/dev_health_ops/investment_taxonomy.py`: the Python refusals "registry file missing", "not a set literal", "non-string literal",
  "not found" and a `SyntaxError` in that file have no Go counterpart (the vocabulary cannot be missing or malformed). Named gap; Go never refuses what Python accepted here.
- A decimal character reference of more than 4300 digits is refused in the search text (the integer string limit, `sys.int_info.default_max_str_digits`, raises `ValueError`; it counts digits and leading zeros, not a sign; JSON integers of more than 4300 digits are refused the same way).
- Drift that no test can pin: `repr()` of a string prints a code point as itself when Go's `unicode.IsPrint` (Unicode 17) says printable,
  Python 3.14 (Unicode 16) escapes the characters Unicode 17 added, which only shows in the text of a failure line; `str.lower()`,
  `html.unescape` numeric references and the final-sigma rule use the Python 3.14.7 tables recorded here; named entities
  come from Go's table plus `nGt;` and `nLt;`, which it lacks (the corpus runs every Python entity name through both); `\w` before `href`, `src` and `id` is
  letters, numbers and `_` of Go's Unicode (17).
- A file name or an input path with a non-UTF-8 byte is not compared for ordering with Python's surrogate escape (refused for page names; other paths are used as the OS gives them).
- The built-site fixtures of the recorded cases sit in directories named `site`, `site2` and `.build`, which `.gitignore` ignores (`**/site**`, `.build/`);
  the first commit lost 119 of them and the tests passed on the working tree that wrote them. `.gitignore` now ends with `!` rules for this testdata, and
  `TestEveryFileOnDiskUnderARootIsKeptByGit` (internal/testsupport/recordedfiles) fails for any file under a testdata root that git does not keep.
