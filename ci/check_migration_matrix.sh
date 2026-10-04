#!/usr/bin/env bash
# Guard docs/go-migration-matrix.md against the two ways it can lie.
#
# WHY THIS EXISTS
# ---------------
# The page has one machine-checked column -- which language executes each
# family -- and that column read 100% NATIVE throughout 2026-07-22 → 2026-09-07,
# a stretch in which the two governing tickets were set Done twenty times and
# nineteen of those were reversed. Nothing on the page tracked output
# correctness, deployment at main, or a proof that the deployed build had ever
# run. The STATUS (v2) section adds those columns; this script is what stops
# them going the way of the "Last verified" stamp, which no test, script or
# workflow read at all until today.
#
# CHAOS-8620: THE AGE BUDGET IS GONE
# ---------------------------------
# The page tracked the Python -> Go migration of the api. The Python api is
# deleted, so the page is a record, not a living plan, and a refresh needs a
# live Postgres and a fleet file that CI does not have. A budget on the age of
# the stamps would turn every PR and main red once a week for a record nobody
# is editing. So no stamp age is checked anywhere. What stays is STRUCTURAL:
#
#   stamp     Pure bash + git. The "Last verified" sha and the ops_sha in
#             last-render.json must be real 40-hex commits that are ancestors
#             of HEAD, and rendered_at must parse. Runs on every PR.
#   contract  Delegates to `go run ./cmd/dev-health-migration-matrix -check`,
#             which re-renders from COMMITTED sources only and fails when the
#             page disagrees with its generator. Needs Go.
#
# ops_sha is the MERGE-BASE with main at render time, written by the tool. It
# is the merge-base and NOT the render commit because a squash merge makes a
# branch commit unreachable from main forever. `render_commit` records the
# actual commit for the audit trail and is never checked.
#
# `all` runs both. Default is `all` locally.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="${MATRIX_ROOT:-$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)}"

DOC="${ROOT}/docs/go-migration-matrix.md"
RENDER_JSON="${ROOT}/contracts/migration-status/v1/last-render.json"

# The ONE command that re-verifies this page. Every staleness failure prints
# it verbatim (team-lead ruling, 2026-09-09): a gate that fails weekly is only
# tolerable if the fix is a command you can paste, not a procedure you have to
# reconstruct from the doc. Anything that makes this longer than one line plus
# a commit is a bug in the tool, not in the operator.
REVERIFY_COMMAND="go run ./cmd/dev-health-migration-matrix -render -root ."

# printf BUILTIN, not `cat <<EOF`. Bash writes a here-document into a pipe it
# also holds the read end of, so a payload at or above the measured ~400-byte
# budget hangs the script forever on a host with a small pipe buffer
# (CHAOS-3362) -- and `cat >file <<EOF` is the same pipe, so it is not a fix.
# tests/tooling/test_local_validate_heredocs.py enforces this over all of ci/.
usage() {
  printf '%s\n' \
    'usage: ci/check_migration_matrix.sh [stamp|contract|all]' \
    '' \
    '  stamp      bash+git only; asserts the "Last verified" sha AND the' \
    '             tool-written ops_sha in last-render.json are each 40-hex' \
    '             ancestors of HEAD. No age is checked (CHAOS-8620).' \
    '  contract   go run ./cmd/dev-health-migration-matrix -check' \
    '  all        both (default)' \
    '' \
    'env:' \
    '  MATRIX_ROOT          repository root (default: this script'"'"'s parent)'
}

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

check_stamp() {
  [ -f "${DOC}" ] || die "missing ${DOC}"

  # The stamp line looks like:
  #   **Last verified:** `<40 hex>` (ops main, YYYY-MM-DD) -- ...
  local line sha
  line="$(grep -m1 -E '^\*\*Last verified:\*\*' "${DOC}" || true)"
  [ -n "${line}" ] || die "check_migration_matrix: ${DOC} has no '**Last verified:**' line.
The stamp is what tells a reader when the hand-curated rows were last read
against real code. Removing it does not make the page fresher."

  # Deliberately extracts whatever sits in the backticks rather than matching
  # 40-hex directly: an ABBREVIATED sha must produce a loud, specific failure,
  # not a silent "no match" that reads like a missing line.
  # shellcheck disable=SC2016  # the backticks are markdown delimiters in the
  # sed pattern, not a command substitution; the quotes must stay single.
  sha="$(printf '%s' "${line}" | sed -n 's/.*`\([0-9a-fA-F]*\)`.*/\1/p')"
  [ -n "${sha}" ] || die "check_migration_matrix: could not read a sha out of the 'Last verified' line:
  ${line}"

  case "${sha}" in
    *[!0-9a-f]* | "")
      die "check_migration_matrix: 'Last verified' sha '${sha}' is not lower-case hex."
      ;;
  esac
  [ "${#sha}" -eq 40 ] || die "check_migration_matrix: 'Last verified' sha '${sha}' is ${#sha} characters, not 40.
An abbreviated sha cannot be checked for ancestry, so it cannot be checked at all."

  git -C "${ROOT}" cat-file -e "${sha}^{commit}" 2>/dev/null || die \
    "check_migration_matrix: commit ${sha} is not in this repository.
If CI made a shallow checkout, this job needs fetch-depth: 0."

  git -C "${ROOT}" merge-base --is-ancestor "${sha}" HEAD || die \
    "check_migration_matrix: ${sha} is NOT an ancestor of HEAD.
The page claims to have been verified against a commit this branch does not
contain, so nothing on it was verified against what is about to merge."

  [ -f "${RENDER_JSON}" ] || die "missing ${RENDER_JSON}
Run: go run ./cmd/dev-health-migration-matrix -render -root ."

  local rendered_at rendered_epoch
  rendered_at="$(sed -n 's/.*"rendered_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${RENDER_JSON}" | head -n1)"
  [ -n "${rendered_at}" ] || die "check_migration_matrix: ${RENDER_JSON} has no rendered_at"

  # `date -d` is GNU; BSD date needs -j -f. Try GNU first, fall back, and if
  # neither parses, FAIL rather than skip -- a freshness check that silently
  # opts out on an unfamiliar host is the stage-manifest bug (CHAOS-3571) in
  # miniature.
  rendered_epoch="$(date -u -d "${rendered_at}" +%s 2>/dev/null \
    || date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "${rendered_at}" +%s 2>/dev/null \
    || true)"
  [ -n "${rendered_epoch}" ] || die "check_migration_matrix: could not parse rendered_at '${rendered_at}'"

  # The sha `-render` ACTUALLY read from, written by the tool via
  # `git rev-parse HEAD` and never typed by hand (team-lead ruling,
  # 2026-09-09). The markdown stamp above is a human's claim about the
  # hand-curated rows; THIS is a machine's record of what the generated cells
  # were produced from, and it is the one a person cannot forge by editing a
  # line. Both are checked, for different halves of the page.
  local ops_sha
  ops_sha="$(sed -n 's/.*"ops_sha"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${RENDER_JSON}" | head -n1)"
  [ -n "${ops_sha}" ] || die "check_migration_matrix: ${RENDER_JSON} has no ops_sha.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"

  case "${ops_sha}" in
    *[!0-9a-f]* | "") die "check_migration_matrix: ops_sha '${ops_sha}' is not lower-case hex" ;;
  esac
  [ "${#ops_sha}" -eq 40 ] || die "check_migration_matrix: ops_sha '${ops_sha}' is ${#ops_sha} characters, not 40"

  git -C "${ROOT}" cat-file -e "${ops_sha}^{commit}" 2>/dev/null || die \
    "check_migration_matrix: ops_sha ${ops_sha} is not a commit in this repository.

The likeliest cause is a render that recorded its own BRANCH tip. A squash
merge replaces a branch's commits with one new commit, so the original is
never reachable from main again and this check fails on main forever -- which
is exactly what #2389 did. ops_sha must be the MERGE-BASE with main (the last
main commit the render observed), which survives a squash; the render commit
itself is recorded separately as render_commit and is deliberately NOT checked.
A shallow CI checkout (needs fetch-depth: 0) is the other, rarer cause.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"

  git -C "${ROOT}" merge-base --is-ancestor "${ops_sha}" HEAD || die \
    "check_migration_matrix: ops_sha ${ops_sha} is NOT an ancestor of HEAD.

Same cause as above: ops_sha must be the merge-base with main, not the commit
the render ran on. A branch tip recorded here disappears at squash-merge and
strands this check on main. If ops_sha IS a merge-base and still is not an
ancestor, the render came from an unrelated history.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"

  printf 'migration matrix stamp OK: last verified %s, rendered %s at %s (no age budget)\n' \
    "${sha:0:12}" "${rendered_at}" "${ops_sha:0:12}"
}

check_contract() {
  command -v go >/dev/null 2>&1 || die "check_migration_matrix: contract mode needs the Go toolchain on PATH"
  ( cd "${ROOT}" && go run ./cmd/dev-health-migration-matrix -check -root . )
}

main() {
  case "${1:-all}" in
    stamp) check_stamp ;;
    contract) check_contract ;;
    all)
      check_stamp
      check_contract
      ;;
    -h | --help | help)
      usage
      ;;
    *)
      usage >&2
      die "unknown mode: $1"
      ;;
  esac
}

main "$@"
