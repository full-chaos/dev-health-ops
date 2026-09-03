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
# THE DRIFT IS REAL, AND SHAPE-SPECIFIC. 0.11.0 stopped emitting SC2015 when the
# command immediately before `||` is a TEST command. Measured on both binaries:
#
#                                          0.9.0  0.10.0  0.11.0
#   [ A ] && echo x || echo y                no      no      no
#   [ A ] && { echo x; } || { echo y; }     yes     yes     yes
#   [ A ] && true || false                  yes     yes     yes
#   [ A ] && [ B ] || { C; }                YES     YES      no   <-- differs
#   [ A ] && [ B ] && [ C ] || { D; }       YES     YES      no   <-- differs
#   grep -q x f && [ B ] || { C; }          YES     YES      no   <-- differs
#   [ A ] && f || { f; }                    yes     yes     yes
#
# Every cell measured against a real binary of that version -- the 0.9.0 column
# was completed by lane-4441 and re-measured here. 0.9.0 and 0.10.0 agree
# everywhere; only 0.11.0 differs, and only where a TEST command precedes `||`.
#
# 0.9.0 matters because it is what the runner ACTUALLY ships: the step never
# printed a version, and the ubuntu-24.04 image manifest lists 0.9.0. The
# "0.10.0" in the first report of this was a reproduction binary, not the
# runner -- which is its own lesson about version claims nobody printed.
#
# So a script using `... && [ test ] || { ... }` -- the ordinary argument-count
# idiom -- is clean on 0.11.0 and fails on 0.9.0 and 0.10.0. That is exactly how an
# unpinned linter reds a build nobody changed.
#
# (An earlier revision of this comment claimed the drift was false. That was
# wrong: every shape probed had a group, `true`, or a function before `||` --
# never a test command, the one shape where the versions differ. A negative from
# a sample that excludes the case in question is not evidence about the case.)
#
# WHY 0.11.0: it is what every DEVELOPER host already runs, so nobody has to
# downgrade locally to match CI. (The runner is the exception and ships 0.9.0 --
# which is why the CI install step above exists. An earlier draft of this line
# said "every host", contradicting the runner version stated a few lines up;
# codex round 3 caught it.) And measured, not assumed: all 17 files below produce IDENTICAL
# results under 0.9.0 (today's runner) and 0.11.0 (this pin), so adopting the
# pin changes no verdict on anything it lints. It does ACCEPT upstream's narrowing on
# the shape above -- a deliberate upstream decision, since with a test before
# `||` the idiom really is the intended one. Team-lead ruled for this trade-off
# explicitly, and the two scripts that pass only because of it are excluded
# below rather than enrolled.
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
# red the build on a commit that merely ADDED an unclean script.
#
# EXCLUDED, unclean under this pin (cleaning them is separate work):
#   ci/check_go_containers.sh, ci/local_validate.sh, ci/run_live_backend_e2e.sh,
#   ci/run_metrics_executed_proof.sh, and four under scripts/acceptance/
#   (armed_corpus_boot.sh, resolve_acr_parent_compose.sh, run_ask_dev_compose.sh,
#   run_ask_dev_provider_profile.sh).
#
# ALSO EXCLUDED, and for a different reason worth stating -- these are clean
# under 0.11.0 but DIRTY under 0.9.0 and 0.10.0, i.e. they pass only because of the
# narrowing described above:
#   scripts/acceptance/armed_corpus_run.sh   (SC2015 under 0.9.0 AND 0.10.0)
#   scripts/backup-standing.sh               (SC2015 under 0.9.0 AND 0.10.0)
# 0.9.0 is named first deliberately: it is the version the runner actually
# ships, so it is the one that makes these exclusions load-bearing today.
# Citing only 0.10.0 -- a reproduction binary nobody runs in CI -- would leave a
# future reader re-deriving the exclusion under 0.9.0 and finding it justified,
# but not for the reason given. That is the same confusion this rewrite fixed.
# Enrolling a file that passes only under the relaxed rule would quietly bank
# the relaxation as coverage. Every file below is clean under BOTH versions.
FILES=(
  # the set CI already linted before this change
  "${ROOT}/tests/compatibility/river/run.sh"
  "${ROOT}/tests/compatibility/river/record.sh"
  "${ROOT}/ci/check_go.sh"
  "${ROOT}/ci/check_river_compat_static.sh"
  # this script itself, AND the self-test that enforces it -- lane-4441's read:
  # the script enforcing the lint was the one script the lint did not check,
  # which is the guard-file-is-not-guarded shape one level up.
  "${ROOT}/ci/shellcheck_pinned.sh"
  "${ROOT}/ci/check_shellcheck_pin.sh"
  # tracked scripts CI never linted, added because they are already clean
  # under the pin -- verified, not assumed
  "${ROOT}/ci/aggregate_gate_results.sh"
  "${ROOT}/ci/check_lint_scope.sh"
  "${ROOT}/ci/run_tests.sh"
  "${ROOT}/docker/init-extra-dbs.sh"
  "${ROOT}/scripts/acceptance/container_source_guard.sh"
  "${ROOT}/scripts/acceptance/mint_ask_dev_world_snapshot.sh"
  "${ROOT}/scripts/acceptance/run_wave4_corpus.sh"
  "${ROOT}/scripts/build-images.sh"
  "${ROOT}/scripts/run_py_tool.sh"
  "${ROOT}/tests/compatibility/provider/run.sh"
  "${ROOT}/.github/docs-legacy/examples/customer-push/generic-runner.sh"
)

# Arguments override the canonical set (used by the red-first fixture test).
if [ "$#" -gt 0 ]; then
  FILES=("$@")
fi

shellcheck "${FILES[@]}"
