#!/usr/bin/env bash
#
# Run a Python dev tool (mypy, ruff, ...) from the PROJECT environment, never
# from whatever happens to be first on PATH.
#
# Usage: scripts/run_py_tool.sh <tool> [args...]
#
# Why this exists (CHAOS-3913): the lefthook gates used to invoke bare `mypy`
# and bare `ruff`.
# Outside an activated project environment -- which is every git worktree, since
# worktrees do not carry the repo root's untracked direnv setup -- that resolved
# a global interpreter whose site-packages hold a SHADOW copy of this project's
# dependencies. mypy then type-checked our source against the wrong library
# versions and reported errors in files the author never touched, while CI's
# typecheck job stayed green. A gate that is usually wrong trains people to
# bypass it, which is worse than no gate at all.
#
# Concretely: Homebrew's python3.14 site-packages carried openai 2.37.0
# (`http_client: httpx.AsyncClient`) while pyproject declares openai>=3.0.0,
# whose signature is `http_client: httpx2.AsyncClient`. Four phantom arg-type
# errors, zero real defects. The same PATH lookup later stopped resolving at
# all in a worktree ("pyenv: ruff: command not found"), blocking commits
# outright -- the same root cause with a louder symptom.
#
# Resolution order:
#   1. $VIRTUAL_ENV          -- an explicitly activated env is always intentional.
#   2. <main worktree>/.venv -- the repo-local env. Resolved via
#      `git rev-parse --git-common-dir` so this works from a linked worktree,
#      which has no .venv of its own.
#   3. PATH                  -- CI installs the project's requirements into the
#      job interpreter, so bare mypy is correct there.
#
# In case 3 we cannot prove the environment is the project's, so a FAILURE is
# annotated with the interpreter and its site-packages. That turns the next
# occurrence of this bug into a one-line diagnosis instead of an afternoon.
#
# Kept POSIX-ish and free of bash 4 builtins (no mapfile/associative arrays):
# macOS still ships /bin/bash 3.2, and this must not depend on a Homebrew bash.
set -euo pipefail

TOOL_BIN=""
TOOL_ORIGIN=""

resolve_tool() {
    local tool="$1"
    if [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/${tool}" ]; then
        TOOL_BIN="${VIRTUAL_ENV}/bin/${tool}"
        TOOL_ORIGIN="virtualenv"
        return 0
    fi

    # --git-common-dir points at the MAIN worktree's .git even when we are
    # inside a linked worktree, which is exactly how we find the shared .venv.
    local common_dir
    local repo_root
    if common_dir=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null); then
        repo_root=${common_dir%/.git}
        repo_root=${repo_root%/}
        if [ -x "${repo_root}/.venv/bin/${tool}" ]; then
            TOOL_BIN="${repo_root}/.venv/bin/${tool}"
            TOOL_ORIGIN="repo venv"
            return 0
        fi
    fi

    local from_path
    if from_path=$(command -v "${tool}" 2>/dev/null); then
        TOOL_BIN="${from_path}"
        TOOL_ORIGIN="PATH"
        return 0
    fi

    return 1
}

if [ "$#" -eq 0 ]; then
    echo "run_py_tool: usage: run_py_tool.sh <tool> [args...]" >&2
    exit 2
fi
TOOL="$1"
shift

if ! resolve_tool "${TOOL}"; then
    echo "run_py_tool: no ${TOOL} in \$VIRTUAL_ENV, the repo .venv, or PATH." >&2
    echo "run_py_tool: install the project's dev dependencies, then retry." >&2
    exit 1
fi

rc=0
"${TOOL_BIN}" "$@" || rc=$?

if [ "${rc}" -ne 0 ] && [ "${TOOL_ORIGIN}" = "PATH" ]; then
    # Only annotate on failure: a green run needs no explanation, and a noisy
    # header on every commit is how people learn to stop reading hook output.
    site_packages=$("$(dirname "${TOOL_BIN}")/python" -c \
        'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null) || site_packages="unknown"
    cat >&2 <<EOF

run_py_tool: ${TOOL} came from PATH, not from a project environment.
run_py_tool:   ${TOOL} ${TOOL_BIN}
run_py_tool:   site-packages ${site_packages}
run_py_tool: If these errors are in files you did not touch, that interpreter's
run_py_tool: packages may not match pyproject.toml -- confirm against CI
run_py_tool: before treating them as real. See CHAOS-3913.
EOF
fi

exit "${rc}"
