#!/usr/bin/env bash
# CHAOS-3219 D3 / Codex finding (HIGH, 2026-08-05): resolve an ABSOLUTE,
# validated parent-repo compose.yml path for the optional ACR overlay
# (tests/acceptance/compose.ask-dev-acr.yml), instead of trusting that
# overlay's own relative `${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE:-../compose.yml}`
# default, which resolves against --project-directory (ops_root) and is
# silently WRONG from a worktree layout (`<checkout>/ops-worktrees/<branch>`
# -- `../compose.yml` resolves to `ops-worktrees/compose.yml`, which does
# not exist; reproduced live, `docker compose config --quiet` failed
# outright). Extracted into its own sourceable script (rather than left
# inline in run_ask_dev_compose.sh) so it is independently testable without
# needing --web-root or a live Compose boot -- see
# tests/acceptance/test_ask_dev_compose.py's
# test_resolve_acr_parent_compose_* tests, which exercise this against real
# canonical- and worktree-shaped git layouts.
#
# Usage: source this file, then call `resolve_acr_parent_compose "<ops_root>"`.
# On success it exports ASK_DEV_ACCEPTANCE_PARENT_COMPOSE and
# ASK_DEV_ACCEPTANCE_PARENT_ACR_DEV_DIR as absolute paths. On failure it
# prints a diagnostic to stderr and returns 64 (the caller's `set -e`
# propagates that as the launcher's own exit code) -- never falls back to a
# relative guess that could silently point at the wrong (or a nonexistent)
# parent repo.
resolve_acr_parent_compose() {
  local ops_root="$1"

  if [[ -n "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE:-}" ]]; then
    if [[ ! -f "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE}" ]]; then
      echo "ASK_DEV_ACCEPTANCE_PARENT_COMPOSE=${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE} does not exist" >&2
      return 64
    fi
    local parent_compose_dir
    parent_compose_dir="$(cd -- "$(dirname -- "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE}")" && pwd)"
    export ASK_DEV_ACCEPTANCE_PARENT_COMPOSE="${parent_compose_dir}/$(basename -- "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE}")"
  else
    # git itself always knows the ONE real checkout, shared by every
    # worktree, via --git-common-dir -- use that instead of guessing from
    # ops_root's own path shape.
    local git_common_dir canonical_ops_root candidate
    git_common_dir="$(cd -- "${ops_root}" && git rev-parse --git-common-dir)"
    canonical_ops_root="$(cd -- "${ops_root}" && cd -- "${git_common_dir}/.." && pwd)"
    candidate="${canonical_ops_root}/../compose.yml"
    if [[ ! -f "${candidate}" ]]; then
      echo "ASK_DEV_ACCEPTANCE_ACR=1 requires a sibling dev-health checkout with compose.yml one level above the canonical ops checkout (looked for ${candidate}); set ASK_DEV_ACCEPTANCE_PARENT_COMPOSE explicitly to override." >&2
      return 64
    fi
    export ASK_DEV_ACCEPTANCE_PARENT_COMPOSE="$(cd -- "$(dirname -- "${candidate}")" && pwd)/compose.yml"
  fi
  # Same class of bug, same fix: default alongside the just-resolved parent
  # compose file rather than a second independent relative guess.
  export ASK_DEV_ACCEPTANCE_PARENT_ACR_DEV_DIR="${ASK_DEV_ACCEPTANCE_PARENT_ACR_DEV_DIR:-$(dirname -- "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE}")/.acr-dev}"
}

# Allow direct invocation for the conformance tests (and manual debugging)
# without forcing every caller to `source` it: `bash resolve_acr_parent_compose.sh
# <ops_root>` prints the two resolved values, one per line.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  set -euo pipefail
  resolve_acr_parent_compose "$1"
  echo "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE}"
  echo "${ASK_DEV_ACCEPTANCE_PARENT_ACR_DEV_DIR}"
fi
