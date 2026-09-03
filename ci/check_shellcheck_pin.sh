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

# THE FIXTURE MUST EXIST AND BE READABLE, CHECKED SEPARATELY.
#
# codex round 1 finding: this assertion used to accept ANY non-zero exit as
# proof. Delete the fixture and `shellcheck_pinned.sh` exits 2 ("does not
# exist") -- which the old check happily reported as "ok: fixture correctly
# rejected", exit 0. Reproduced: with the fixture moved away, the whole
# self-test passed.
#
# So the NEGATIVE CONTROL HAD THE EXACT DEFECT IT EXISTS TO CATCH: it could not
# tell "the linter rejected bad input" from "the linter never saw any input".
# Non-zero is not a reason; it is only an outcome.
if [ ! -r "${FIXTURE}" ]; then
  printf 'FAIL: the fixture is missing or unreadable: %s\n' "${FIXTURE}" >&2
  printf '  Without it there is no negative control, and assertion 1 alone\n' >&2
  printf '  cannot distinguish a clean tree from a linter that never ran.\n' >&2
  exit 1
fi

# Capture the output and require the SPECIFIC diagnostic, not merely failure.
set +e
fixture_output="$(bash "${PINNED}" "${FIXTURE}" 2>&1)"
fixture_status=$?
set -e

if [ "${fixture_status}" -eq 0 ]; then
  printf 'FAIL: the unclean fixture PASSED shellcheck.\n' >&2
  printf '  Either the pin is not running, the fixture was cleaned up by\n' >&2
  printf '  mistake, or the pinned version no longer reports on it. A linter\n' >&2
  printf '  that passes its own negative control is not checking anything --\n' >&2
  printf '  fix this before trusting any green from assertion 1 above.\n' >&2
  exit 1
fi

case "${fixture_output}" in
  *SC2015*) : ;;
  *)
    printf 'FAIL: the fixture failed, but NOT with SC2015 (exit %s).\n' "${fixture_status}" >&2
    printf '  A non-zero exit for some other reason -- an unreadable file, a\n' >&2
    printf '  version mismatch, a parse error -- proves nothing about whether\n' >&2
    printf '  the rules still fire. Output was:\n' >&2
    printf '%s\n' "${fixture_output}" >&2
    exit 1
    ;;
esac
printf 'ok: fixture correctly rejected, with SC2015\n'
