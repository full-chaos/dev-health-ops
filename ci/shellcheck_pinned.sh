#!/usr/bin/env bash
# Single source of truth for the shellcheck version AND the set of files linted.
#
# WHY THIS EXISTS (CHAOS-4915)
# ----------------------------
# Before this script, `ci/check_river_compat_static.sh` asserted only
# `command -v shellcheck` and then ran whatever version the runner happened to
# ship, while `lefthook.yml` linted no shell at all. Two consequences, both
# measured rather than assumed:
#
#   1. NONDETERMINISM. A runner-side shellcheck upgrade can red a build with no
#      diff to blame -- the worst debugging shape there is, because the first
#      thing everyone does is bisect a commit range that contains no cause.
#   2. NO LOCAL EQUIVALENT. Nothing checked shell before push, so the only way
#      to learn a script was unclean was to fail CI.
#
# NOT the rationale: an earlier framing claimed shellcheck 0.10.0 emits SC2015
# where 0.11.0 does not. That was measured and is FALSE for that version pair --
# both emit SC2015 on identical lines with identical counts, and both produce
# byte-identical output on this repo's linted files. The pin is justified by
# determinism alone. (The original incident on another repo is unexplained, not
# disproved; it is tracked separately and is not a reason for this pin.)
#
# WHY 0.11.0: it is what the fleet already runs, so pinning it changes no
# verdict today. Deliberately NOT pinning an older version -- the version that
# reproduces the fewest surprises is the one already in use.
set -euo pipefail

SHELLCHECK_REQUIRED_VERSION="0.11.0"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"

if ! command -v shellcheck >/dev/null 2>&1; then
  printf 'ERROR: shellcheck is required and not on PATH (pinned version %s)\n' \
    "${SHELLCHECK_REQUIRED_VERSION}" >&2
  printf '  install: https://github.com/koalaman/shellcheck/releases/tag/v%s\n' \
    "${SHELLCHECK_REQUIRED_VERSION}" >&2
  exit 2
fi

# IN BAND, always -- not only on mismatch. A pin that reports nothing when it
# agrees cannot be distinguished from a pin that never ran.
actual_version="$(shellcheck --version | awk '/^version:/ {print $2}')"
printf 'shellcheck: pinned=%s found=%s\n' "${SHELLCHECK_REQUIRED_VERSION}" "${actual_version}"

if [ "${actual_version}" != "${SHELLCHECK_REQUIRED_VERSION}" ]; then
  printf 'ERROR: shellcheck version mismatch -- pinned %s, found %s\n' \
    "${SHELLCHECK_REQUIRED_VERSION}" "${actual_version}" >&2
  printf '  A different version may enforce different rules, so a pass here would\n' >&2
  printf '  not mean what CI means by a pass. Refusing to lint rather than report\n' >&2
  printf '  a verdict from an unknown ruleset.\n' >&2
  printf '  install: https://github.com/koalaman/shellcheck/releases/tag/v%s\n' \
    "${SHELLCHECK_REQUIRED_VERSION}" >&2
  exit 2
fi

# Explicit list, not a glob. A glob would silently enrol every new .sh file and
# red the build on a commit that merely ADDED an unclean script -- and 8 tracked
# scripts are unclean under this pin today (ci/check_go_containers.sh,
# ci/local_validate.sh, ci/run_live_backend_e2e.sh,
# ci/run_metrics_executed_proof.sh, and four under scripts/acceptance/).
# Cleaning those is separate work; enrolling them here would widen the rules,
# which this change deliberately does not do.
FILES=(
  # the set CI already linted before this change
  "${ROOT}/tests/compatibility/river/run.sh"
  "${ROOT}/tests/compatibility/river/record.sh"
  "${ROOT}/ci/check_go.sh"
  "${ROOT}/ci/check_river_compat_static.sh"
  # this script itself
  "${ROOT}/ci/shellcheck_pinned.sh"
  # tracked scripts CI never linted, added because they are already clean
  # under the pin -- verified, not assumed
  "${ROOT}/ci/aggregate_gate_results.sh"
  "${ROOT}/ci/check_lint_scope.sh"
  "${ROOT}/ci/run_tests.sh"
  "${ROOT}/docker/init-extra-dbs.sh"
  "${ROOT}/scripts/acceptance/armed_corpus_run.sh"
  "${ROOT}/scripts/acceptance/container_source_guard.sh"
  "${ROOT}/scripts/acceptance/mint_ask_dev_world_snapshot.sh"
  "${ROOT}/scripts/acceptance/run_wave4_corpus.sh"
  "${ROOT}/scripts/backup-standing.sh"
  "${ROOT}/scripts/build-images.sh"
  "${ROOT}/scripts/run_py_tool.sh"
  "${ROOT}/tests/compatibility/provider/run.sh"
)

# Arguments override the canonical set (used by the red-first fixture test).
if [ "$#" -gt 0 ]; then
  FILES=("$@")
fi

shellcheck "${FILES[@]}"
