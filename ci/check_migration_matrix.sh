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
# TWO MODES, DELIBERATELY SPLIT BY WHAT THEY NEED
# -----------------------------------------------
# THE TWO SHAS, AND WHY BOTH ARE CHECKED
# --------------------------------------
#   "Last verified" (markdown)  a HUMAN's claim about the hand-curated citation
#                               and CLI-verb rows. Forgeable by editing a line,
#                               which is why the failure message says so out loud.
#   ops_sha (last-render.json)  the sha `-render` actually read from, written by
#                               the tool via `git rev-parse HEAD`. Not typeable
#                               by hand, so it is the half of the freshness claim
#                               that cannot be faked.
#
#   freshness  Pure bash + git. No Go toolchain, no venv, no network. It runs
#              UNCONDITIONALLY on every PR -- and that is the entire point: a
#              document rots when NOBODY touches it, so a path-filtered or
#              relevance-gated check is structurally incapable of catching the
#              failure it exists to catch. This is the mode that would have
#              fired on 2026-09-05, not on 2026-09-09.
#
#   contract   Delegates to `go run ./cmd/dev-health-migration-matrix -check`,
#              which re-renders from COMMITTED sources only and fails when a
#              row claims a status its evidence column cannot support. Needs
#              Go; runs on the Go-relevant leg.
#
# `all` runs both. Default is `all` locally, so a human typing the bare command
# gets the whole gate.
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

# The staleness budget, in days, for BOTH the hand-curated "Last verified"
# stamp and the live-source render. Seven is not arbitrary: the stamp that
# prompted this work was five days and roughly forty merges behind main and
# still read as current.
MAX_AGE_DAYS="${MATRIX_MAX_AGE_DAYS:-7}"

# printf BUILTIN, not `cat <<EOF`. Bash writes a here-document into a pipe it
# also holds the read end of, so a payload at or above the measured ~400-byte
# budget hangs the script forever on a host with a small pipe buffer
# (CHAOS-3362) -- and `cat >file <<EOF` is the same pipe, so it is not a fix.
# tests/tooling/test_local_validate_heredocs.py enforces this over all of ci/.
usage() {
  printf '%s\n' \
    'usage: ci/check_migration_matrix.sh [freshness|contract|all]' \
    '' \
    '  freshness  bash+git only; asserts the "Last verified" sha AND the' \
    '             tool-written ops_sha in last-render.json are each 40-hex,' \
    '             ancestors of HEAD, and no older than MATRIX_MAX_AGE_DAYS' \
    '             (default 7).' \
    '  contract   go run ./cmd/dev-health-migration-matrix -check' \
    '  all        both (default)' \
    '' \
    'env:' \
    '  MATRIX_MAX_AGE_DAYS  staleness budget in days (default 7)' \
    '  MATRIX_ROOT          repository root (default: this script'"'"'s parent)'
}

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

# epoch_now and epoch_of are separated so a test can freeze the clock without
# reaching into git.
epoch_now() {
  printf '%s' "${MATRIX_NOW_EPOCH:-$(date -u +%s)}"
}

check_freshness() {
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

  local verified_epoch now age_days
  verified_epoch="$(git -C "${ROOT}" show -s --format=%ct "${sha}")"
  now="$(epoch_now)"
  age_days=$(( (now - verified_epoch) / 86400 ))

  if [ "${age_days}" -gt "${MAX_AGE_DAYS}" ]; then
    die "check_migration_matrix: 'Last verified' ${sha} is ${age_days} days old (budget ${MAX_AGE_DAYS} days).

Re-verify, then commit:
  ${REVERIFY_COMMAND}

That refreshes every machine-read cell and writes the sha it read from into
${RENDER_JSON#"${ROOT}/"}. The 'Last verified' stamp is the OTHER half -- the
hand-curated citation and CLI-verb rows -- so moving it means re-reading those
rows against the current tree first and fixing what has changed. Bumping the
stamp WITHOUT re-reading them is the exact failure this gate exists to prevent,
and it is not detectable from here; that part is on you."
  fi

  [ -f "${RENDER_JSON}" ] || die "missing ${RENDER_JSON}
Run: go run ./cmd/dev-health-migration-matrix -render -root ."

  local rendered_at rendered_epoch render_age
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
    "check_migration_matrix: the render claims to have been made at ${ops_sha}, which is not a
commit in this repository. If CI made a shallow checkout, this job needs fetch-depth: 0.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"

  git -C "${ROOT}" merge-base --is-ancestor "${ops_sha}" HEAD || die \
    "check_migration_matrix: the render was made at ${ops_sha}, which is NOT an ancestor of HEAD.
The generated cells on this page were produced from a tree this branch does not
contain, so they describe something other than what is about to merge.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"

  render_age=$(( ($(epoch_now) - rendered_epoch) / 86400 ))
  if [ "${render_age}" -gt "${MAX_AGE_DAYS}" ]; then
    die "check_migration_matrix: the live sources behind the STATUS (v2) tables were last read
${render_age} days ago (budget ${MAX_AGE_DAYS} days). The page is describing a fleet and a
routing table that may no longer exist.

Re-verify, then commit:
  ${REVERIFY_COMMAND}"
  fi

  printf 'migration matrix freshness OK: last verified %s (%d days), rendered %s at %s (%d days), budget %d days\n' \
    "${sha:0:12}" "${age_days}" "${rendered_at}" "${ops_sha:0:12}" "${render_age}" "${MAX_AGE_DAYS}"
}

check_contract() {
  command -v go >/dev/null 2>&1 || die "check_migration_matrix: contract mode needs the Go toolchain on PATH"
  ( cd "${ROOT}" && go run ./cmd/dev-health-migration-matrix -check -root . )
}

main() {
  case "${1:-all}" in
    freshness) check_freshness ;;
    contract) check_contract ;;
    all)
      check_freshness
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
