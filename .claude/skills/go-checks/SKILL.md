---
name: go-checks
description: Run the Go gate (ci/check_go.sh) correctly — required for any Go change, since ci/local_validate.sh does NOT cover Go
disable-model-invocation: true
---

# Go checks

`ci/local_validate.sh` does **not** validate Go. Its Go stage is commented out
(`ci/local_validate.sh:1189`, `# run_stage "go: format + vet + test" go_fast gate_go_fast`),
so a `GATE PASSED. [8/8 ...]` line says nothing about a Go diff. Any change under
`internal/`, `cmd/` or `contracts/` needs this gate as well.

## Invocation

```bash
cd <your ops worktree root>
uv sync --all-extras --dev                       # once per worktree
PYTHON="$PWD/.venv/bin/python" bash ci/check_go.sh all
```

Expect `rc=0` and ~118 `ok` package lines. Verbs: `all` (full), `fast`
(format + vet + unit).

## Why both the venv and `PYTHON` are required

Many Go tests are **live-Python oracles**: they shell out to a Python
interpreter that imports real `dev_health_ops` modules and compare its output
against the Go implementation byte-for-byte. That interpreter is resolved from
the *environment*, not from the worktree, and it is resolved two different ways
in two different places:

| Call site | Resolution |
|---|---|
| synccoverage / fernet oracles | `<root>/.venv/bin/python`, falling back to `python3` only when the venv is **absent** |
| `pythonExecutable` (`internal/providersync/capabilities_test.go`) | `$PYTHON`, else bare `python3` from PATH |

So a worktree needs **both**: a `.venv` for the first group, and `PYTHON`
exported for the second. Missing either produces a failure that reads exactly
like a code defect in a file you never touched.

Observed failure modes, both environment and neither a real regression:

```
# no .venv in the worktree
ModuleNotFoundError: No module named 'pydantic'

# .venv present, PYTHON unset -> bare python3 from PATH
ModuleNotFoundError: No module named 'radon'
```

A missing module that IS declared in `pyproject.toml` is the signature. Fix the
environment before reading the diff. This is the same PATH-resolution class as
CHAOS-3913 (lefthook resolving a global `mypy`/`ruff`), in a third place.

## Hard rules

- **Never build the worktree venv with pyenv.** Use `uv sync --all-extras --dev`.
  It is the sanctioned path precisely because creating the venv under pyenv is
  awkward; this is settled, do not re-derive a pip alternative.
- **`ok` does not mean "ran".** Env-gated suites skip and the package still
  prints `ok`. Count before believing a green:
  ```bash
  go test ./internal/<pkg>/... -v 2>&1 | grep -cE '^\s*--- (PASS|SKIP|FAIL)'
  ```
  A suite that skipped everything is not evidence.
- **Integration tests are behind a build tag** and are opt-out, not optional:
  ```bash
  go test -tags=integration ./internal/<pkg>/... -count=1
  ```
- **A red check means no merge.** No diagnosed exclusions.

## Cross-language parity oracles

Changing anything that exists in **both** Python and Go — the sync coverage
payload builder is the live example — means the parity oracle must pass, and it
sits behind its own gate:

```bash
PYTHON="$PWD/.venv/bin/python" \
DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR=/tmp/oracle-proof \
  go test ./internal/synccoverage/ -run TestPayloadMatchesLivePythonProduction -count=1
```

Without the env vars it **skips**, and the package still reports `ok` — see the
`ok` rule above. Run it explicitly when you touch either side.

Confirm such an oracle is actually comparing what you changed rather than
passing vacuously: perturb the Go side (e.g. bump a shared constant), watch it
FAIL, and check the diff it prints names the Python value you expect. A parity
test you have not seen fail is not yet evidence.

## Constants that must move in lockstep

`internal/synccoverage/types.go`'s `projectionVersion` and
`SYNC_COVERAGE_PROJECTION_VERSION` in
`src/dev_health_ops/api/services/sync_coverage.py` are read by different
processes against the same table. The API filters projections on its value, so
a mismatch makes every projection unreadable and coverage returns 503
indefinitely. Bump both in one changeset, and remember the Python side is
bind-mounted into the API container (live on save) while the Go side ships in a
built image — they do **not** go live at the same moment locally.
