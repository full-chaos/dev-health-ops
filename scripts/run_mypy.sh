#!/usr/bin/env bash
#
# Run mypy against the PROJECT environment, never against whatever `mypy`
# happens to be first on PATH.
#
# Why this exists (CHAOS-3913): the lefthook gate used to invoke bare `mypy`.
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
# errors, zero real defects.
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

MYPY_BIN=""
MYPY_ORIGIN=""

resolve_mypy() {
    if [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/mypy" ]; then
        MYPY_BIN="${VIRTUAL_ENV}/bin/mypy"
        MYPY_ORIGIN="virtualenv"
        return 0
    fi

    # --git-common-dir points at the MAIN worktree's .git even when we are
    # inside a linked worktree, which is exactly how we find the shared .venv.
    local common_dir
    local repo_root
    if common_dir=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null); then
        repo_root=${common_dir%/.git}
        repo_root=${repo_root%/}
        if [ -x "${repo_root}/.venv/bin/mypy" ]; then
            MYPY_BIN="${repo_root}/.venv/bin/mypy"
            MYPY_ORIGIN="repo venv"
            return 0
        fi
    fi

    local from_path
    if from_path=$(command -v mypy 2>/dev/null); then
        MYPY_BIN="${from_path}"
        MYPY_ORIGIN="PATH"
        return 0
    fi

    return 1
}

if ! resolve_mypy; then
    echo "run_mypy: no mypy found in \$VIRTUAL_ENV, the repo .venv, or PATH." >&2
    echo "run_mypy: install the project's dev dependencies, then retry." >&2
    exit 1
fi

rc=0
"${MYPY_BIN}" "$@" || rc=$?

if [ "${rc}" -ne 0 ] && [ "${MYPY_ORIGIN}" = "PATH" ]; then
    # Only annotate on failure: a green run needs no explanation, and a noisy
    # header on every commit is how people learn to stop reading hook output.
    site_packages=$("${MYPY_BIN%/mypy}/python" -c \
        'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null) || site_packages="unknown"
    cat >&2 <<EOF

run_mypy: mypy came from PATH, not from a project environment.
run_mypy:   mypy          ${MYPY_BIN}
run_mypy:   site-packages ${site_packages}
run_mypy: If these errors are in files you did not touch, that interpreter's
run_mypy: packages may not match pyproject.toml -- confirm against CI's
run_mypy: typecheck job before treating them as real. See CHAOS-3913.
EOF
fi

exit "${rc}"
