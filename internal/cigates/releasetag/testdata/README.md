# Recorded Python/Go parity pairs of `cigate release next-tag`

`parity/<case>/` is one run of the former `scripts/auto_tag.py` (CPython 3.14.7, `python3.14 -I -B`, `LC_ALL=C.UTF-8`) in a
temporary git repository. `parity_test.go` builds `cmd/cigate`, rebuilds the repository with real git, runs
`cigate release next-tag` in it and compares exit code, stdout and the file `GITHUB_OUTPUT` names byte for byte. No Python
runs in a test. `refusals/auto_tag.py.frozen` is a byte copy of the script at the parent of the porting commit.

Case layout (`{WORK}` stands for the work directory in every recorded text):

| file | meaning |
| --- | --- |
| `repo` | `none` (no repository), or one line per step: `commit`, or `tag NAME` (a commit, then a lightweight tag on it) |
| `args` | a JSON array: the arguments after `release next-tag` |
| `in/` | files copied into the work directory, without the `.fixture` suffix (a file of the cases is often not valid JSON, and the manifest tool reads a `.json` file as a golden) |
| `no-github-output`, `github-output-path` | `GITHUB_OUTPUT` unset, or set to this path under the work directory (empty: set to the empty string); the default is `github_output` |
| `rc`, `stdout` | what Python printed and exited with |
| `github_output` | what the file `GITHUB_OUTPUT` names held after the run (absent: the run left no file) |
| `stderr` | Python's stderr, byte for byte (it is git's own complaint, which both programs pass through) |
| `stderr.traceback` | Python stopped on an uncaught exception. Go prints git's complaint, then one `Kind: message` line (`python-exception` holds the Python exception class); the test compares Go's stderr with this file exactly |
| `stderr.argparse` | Python exited 2 with an argparse error. The message after `error: ` is the same text; the usage line before it is Go's own |
| `stricter` | not a Python recording: see below |

Recording, for each case: a temporary directory `$W` with `git init`, the `repo` steps (user name and mail set, signing off,
`GIT_CONFIG_GLOBAL` and `GIT_CONFIG_SYSTEM` at `/dev/null`, `GIT_CEILING_DIRECTORIES` at the parent of `$W`), `in/` copied,
then `python3.14 -I -B auto_tag.py <args>` with cwd `$W` and `GITHUB_OUTPUT=$W/github_output`.

## Stricter than Python (D6129)

A case with a `stricter` file records what Go decides, not Python. Each one is a row of kind `stricter` in
`refusals/auto_tag.tsv`. Go refuses with exit 1, an empty stdout and the reason on stderr, before it prints a line:

- a `package.json` version string that carries a control character (category Cc, `\n` included) or a lone surrogate. Python
  matched `1.3.0\n` through `$`, printed it and wrote `new_tag=v1.3.0\n` plus a blank line to `GITHUB_OUTPUT`; a lone
  surrogate stopped its `print` after the first line (and only under a locale whose stdout is strict);
- a `package.json` version that is not a string and not falsy (`true`, a number other than zero, `NaN`, `Infinity`, a
  non-empty list or object): Python stopped in `re.match` after it had printed two lines;
- nesting of more than 10000 levels: Python's C scanner stops at an 8 MB stack, between 50000 and 100000 levels.

A falsy version (`null`, `false`, `0`, `-0.0`, `""`, `[]`, `{}`) is not stricter: Python skipped it and printed its Python
text (`None`, `False`, `0`, `-0.0`, nothing, `[]`, `{}`), and so does Go.

## Go owns

The usage line and the `-h` text (Python's prog name is `auto_tag.py`), and the text of a traceback line. The exit codes, the
argparse error messages, the order of the errors (`-h` exits 0 when it is reached, an ambiguous abbreviation is an error only
when it is reached) and the case classes are Python's.

## Unicode

`semver_test.go` holds the Unicode rows of the port: `\d` of `re.match` is the Nd category of Python's Unicode
(`pyunicodedata`), `[a-zA-Z]` is ASCII, `$` also matches before one final `\n`, `int()` takes any Nd digit (`pythonparity.ParseInt`, 4300 digits at most), `lstrip("v")` strips every leading `v`,
`str.strip()` is `pythonparity.Strip`, and argparse's negative-number test (`-\.?\d`) takes a Unicode digit. The rows are
the output of Python 3.14.7 for the same strings.
