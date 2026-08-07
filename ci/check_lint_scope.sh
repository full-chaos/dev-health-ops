#!/usr/bin/env bash
# Assert that `ruff check .` still has something to check.
#
# CHAOS-3513 Codex round 2. ruff honours .gitignore and .ignore for file
# discovery, and over an empty file set `ruff check .` exits 0 -- so the
# required `lint` check reports green having inspected nothing. Measured on
# this repo: appending `src/` to .gitignore takes ruff's file set from 1045
# source files to 0.
#
# This runs INSIDE the lint job, immediately before the ruff steps whose exit
# status is the gate, because that is the only place ruff is installed --
# lint.yml does `pip install ruff` and ruff appears in no requirements file, so
# a guard living in the pytest suite would have skipped in CI on every run
# while reading as coverage. That is the failure this repo keeps finding, and
# writing it into the fix would have been the third instance in one ticket.
set -euo pipefail

MIN_FILES="${LINT_SCOPE_MIN_FILES:-500}"

if ! command -v ruff >/dev/null 2>&1; then
  echo "check_lint_scope: ruff is not on PATH -- this check cannot measure the lint scope, and a lint gate whose scope was never measured is not evidence" >&2
  exit 1
fi

files="$(ruff check --show-files .)"
count="$(printf '%s\n' "${files}" | grep -c . || true)"

printf 'ruff would check %s files\n' "${count}"

failed=""

# Membership is tested with bash pattern matching, NOT `... | grep -q`. Under
# `set -o pipefail` (set above), `grep -q` closes the pipe as soon as it
# matches, the writing `printf` takes SIGPIPE and exits 141, and pipefail makes
# the whole pipeline report failure -- so a SUCCESSFUL match reads as "no
# match". The first version of this file did exactly that and would have failed
# the lint gate on every run. Same pipe-lifetime class as CHAOS-3362/3468.
if [[ "${files}" != *"/src/"* ]]; then
  failed="${failed}  - no files under src/: the lint gate would pass having inspected none of the source\n"
fi

if [[ "${files}" != *"/tests/"* ]]; then
  failed="${failed}  - no files under tests/: same failure, one tree over\n"
fi

if [[ "${count}" -lt "${MIN_FILES}" ]]; then
  failed="${failed}  - only ${count} files in scope, below the floor of ${MIN_FILES} -- a partial exclusion, not a clean tree\n"
fi

if [[ -n "${failed}" ]]; then
  echo "check_lint_scope FAILED -- ruff's file set collapsed:" >&2
  printf "%b" "${failed}" >&2
  echo "Check .gitignore, .ignore, and any ruff config for an exclusion that swallowed the tree." >&2
  exit 1
fi

echo "check_lint_scope OK"
