#!/usr/bin/env bash
# Self-test for the shellcheck pin (CHAOS-4915). No docker, no services.
#
# Two assertions, and the SECOND is the one that matters:
#   1. the canonical file set is clean under the pinned version
#   2. the deliberately-unclean fixture FAILS under it
#
# (2) exists because (1) alone cannot distinguish "everything is clean" from
# "the linter never ran". A lint step that silently stops linting reports
# exactly what a clean tree reports: success. This asserts the rules still bite.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
PINNED="${SCRIPT_DIR}/shellcheck_pinned.sh"
FIXTURE="${SCRIPT_DIR}/fixtures/shellcheck_unclean_fixture.sh"

printf '== 1/2: canonical set must be CLEAN ==\n'
if ! bash "${PINNED}"; then
  printf 'FAIL: the canonical shellcheck set is not clean under the pinned version\n' >&2
  exit 1
fi
printf 'ok: canonical set clean\n\n'

printf '== 2/2: unclean fixture must FAIL (proves the pin actually lints) ==\n'
# `if ! cmd` rather than `cmd || handler` on purpose: under `set -e` we need the
# non-zero exit to be an expected outcome here, not an abort.
if bash "${PINNED}" "${FIXTURE}" >/dev/null 2>&1; then
  printf 'FAIL: the unclean fixture PASSED shellcheck.\n' >&2
  printf '  Either the pin is not running, the fixture was cleaned up by mistake,\n' >&2
  printf '  or the pinned version no longer reports on it. A linter that passes\n' >&2
  printf '  its own negative control is not checking anything -- fix this before\n' >&2
  printf '  trusting any green from assertion 1 above.\n' >&2
  exit 1
fi
printf 'ok: fixture correctly rejected\n'
