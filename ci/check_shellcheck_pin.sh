#!/usr/bin/env bash
# Self-test for the shellcheck pin (CHAOS-4915). No docker, no services.
#
# Three assertions, and only the FIRST is about the code being clean:
#   1. the canonical file set is clean under the pinned version
#   2. the deliberately-unclean fixture FAILS under it, with SC2015
#   3. the lefthook pre-push hook still invokes this self-test
#
# (2) exists because (1) alone cannot distinguish "everything is clean" from
# "the linter never ran". A lint step that silently stops linting reports
# exactly what a clean tree reports: success. This asserts the rules still bite.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
PINNED="${SCRIPT_DIR}/shellcheck_pinned.sh"
FIXTURE="${SCRIPT_DIR}/fixtures/shellcheck_unclean_fixture.sh"

printf '== 1/3: canonical set must be CLEAN ==\n'
if ! bash "${PINNED}"; then
  printf 'FAIL: the canonical shellcheck set is not clean under the pinned version\n' >&2
  exit 1
fi
printf 'ok: canonical set clean\n\n'

printf '== 2/3: unclean fixture must FAIL (proves the pin actually lints) ==\n'

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
printf 'ok: fixture correctly rejected, with SC2015\n\n'

# 3/3 -- THE LOCAL HALF MUST STILL BE WIRED.
#
# codex round 2 (P2): nothing asserted that lefthook.yml still invokes this
# script. Delete the pre-push hook and CI stays green -- the pin keeps working
# in CI while the local equivalent, which is half the point of CHAOS-4915,
# quietly stops existing. Nobody would notice until someone pushed an unclean
# script and CI caught what their machine no longer did.
#
# Deliberately a grep of the pre-push SECTION rather than a YAML parse: this
# also runs from the lefthook hook itself on developer machines, where PyYAML
# is not guaranteed. It is a wiring assertion, not a schema check.
printf '== 3/3: the lefthook pre-push hook must still be wired ==\n'
LEFTHOOK="${SCRIPT_DIR}/../lefthook.yml"
if [ ! -r "${LEFTHOOK}" ]; then
  printf 'FAIL: lefthook.yml is missing or unreadable: %s\n' "${LEFTHOOK}" >&2
  exit 1
fi
# TWO FALSE PASSES lane-4441 found in the first version of this scan -- a
# wiring assertion certifying "still wired" when it is not, which is the exact
# failure it exists to prevent. Both reproduced here before fixing:
#
#   A1: the terminator was `/^[a-zA-Z0-9_-]+:/` and the match was a substring
#       search for the script name anywhere in the section. Comment out the
#       `run:` line -- which is how anyone actually disables a hook -- and the
#       name still appears, in the comment. PASS.
#   A4: that terminator does not match a key beginning with a dot, and lefthook
#       really uses `.templates:`. The section never closed, the scan ran to
#       EOF, and any later mention of the script anywhere satisfied it. PASS.
#
# Both halves are now tighter: ANY column-0 token closes the section, and the
# match must be a `run:` LINE. A mention is not a wiring.
#
# An indented or quoted `pre-push:` still fails this scan. Left deliberately:
# top-level lefthook keys are unquoted at column 0 by convention, and a text
# scan that rejects a restructure fails SAFE -- someone reformats, this
# complains, they look. For a wiring assertion that is the right direction to
# be wrong in, and it keeps the reason for not parsing YAML intact.
pre_push_section="$(awk '/^pre-push:/{f=1;next} /^[^[:space:]]/{f=0} f' "${LEFTHOOK}")"
if printf '%s\n' "${pre_push_section}" | grep -qE '^[[:space:]]*run:.*check_shellcheck_pin\.sh'; then
  printf 'ok: pre-push hook still invokes this self-test\n'
else
  printf 'FAIL: lefthook.yml pre-push has no run: line invoking ci/check_shellcheck_pin.sh\n' >&2
  printf '  CI would stay green while the LOCAL half of the pin silently\n' >&2
  printf '  disappeared. Restore the pre-push command, or if the hook was\n' >&2
  printf '  removed deliberately, remove this assertion in the same commit so\n' >&2
  printf '  the decision is visible in the diff rather than implied by a gap.\n' >&2
  exit 1
fi
