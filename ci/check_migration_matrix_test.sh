#!/usr/bin/env bash
# RED/GREEN for ci/check_migration_matrix.sh's `freshness` mode.
#
# Freshness is the half of the gate that runs with no Go toolchain, on every
# PR, including PRs that touch nothing near the matrix -- which is exactly the
# case the gate exists for, and exactly the case no other check covers. So it
# gets its own executed proof rather than an argued one.
#
# Each case builds a throwaway git repository, plants a specific lie in the
# document, and asserts the script rejects it. The final case plants no lie and
# asserts it passes, so a script that failed unconditionally could not pass this
# file either.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
UNDER_TEST="${SCRIPT_DIR}/check_migration_matrix.sh"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/matrix-freshness-test.XXXXXX")"
cleanup() { rm -rf "${WORK}"; }
# trapped_rc is captured as the FIRST statement in the trap body: anything that
# runs before it clobbers $? with its own status, and the trap then reports a
# failed run as rc=0.
trapped_rc=0
trap 'trapped_rc=$?; cleanup; exit "${trapped_rc}"' EXIT

PASSED=0
FAILED=0

# fixture <name> <last-verified-cell> <commit-age-days> <render-age-days>
# Builds a repo whose HEAD contains a matrix doc and a render snapshot, with
# the "Last verified" commit backdated by <commit-age-days>. Echoes the path.
fixture() {
  local name="$1" cell="$2" commit_age="$3" render_age="$4"
  local repo="${WORK}/${name}"
  mkdir -p "${repo}/docs" "${repo}/contracts/migration-status/v1"
  git -C "${repo}" init -q
  git -C "${repo}" config user.email test@example.com
  git -C "${repo}" config user.name test
  git -C "${repo}" config commit.gpgsign false

  local commit_date
  commit_date="$(date -u -d "@$(( $(date -u +%s) - commit_age * 86400 ))" +%Y-%m-%dT%H:%M:%SZ)"

  printf 'seed\n' > "${repo}/docs/go-migration-matrix.md"
  git -C "${repo}" add -A
  GIT_AUTHOR_DATE="${commit_date}" GIT_COMMITTER_DATE="${commit_date}" \
    git -C "${repo}" commit -qm seed

  local verified_sha
  verified_sha="$(git -C "${repo}" rev-parse HEAD)"

  # `${x-default}`, NOT `${x:-default}`: a case that sets FIXTURE_OPS_SHA to
  # the EMPTY string is testing the missing-ops_sha path, and the colon form
  # would silently substitute the default and test nothing. Caught by this
  # file's own render_sha_missing case reporting rc=0.
  local rendered_at ops_sha="${FIXTURE_OPS_SHA-SENTINEL}"
  rendered_at="$(date -u -d "@$(( $(date -u +%s) - render_age * 86400 ))" +%Y-%m-%dT%H:%M:%SZ)"
  if [ "${ops_sha}" = "SENTINEL" ]; then
    ops_sha="${verified_sha}"
  fi
  printf '{\n  "rendered_at": "%s",\n  "ops_sha": "%s"\n}\n' "${rendered_at}" "${ops_sha}" \
    > "${repo}/contracts/migration-status/v1/last-render.json"

  # The caller may want a literal cell (an abbreviation, a foreign sha, or
  # nothing at all); SENTINEL means "use the real seed commit".
  local rendered_cell="${cell}"
  if [ "${cell}" = "SENTINEL" ]; then
    rendered_cell="${verified_sha}"
  fi
  if [ "${cell}" = "NOLINE" ]; then
    printf '# matrix\n\nno stamp here\n' > "${repo}/docs/go-migration-matrix.md"
  else
    # shellcheck disable=SC2016  # markdown backticks in a printf FORMAT
    # string; expansion here would corrupt the fixture.
    printf '# matrix\n\n**Last verified:** `%s` (ops main, 2026-09-04) -- read against.\n' \
      "${rendered_cell}" > "${repo}/docs/go-migration-matrix.md"
  fi

  git -C "${repo}" add -A
  GIT_AUTHOR_DATE="${commit_date}" GIT_COMMITTER_DATE="${commit_date}" \
    git -C "${repo}" commit -qm stamp
  printf '%s' "${repo}"
}

# expect <fail|pass> <name> <description> <fixture-args...>
expect() {
  local want="$1" name="$2" description="$3"
  shift 3
  local repo output rc=0
  repo="$(fixture "${name}" "$@")"
  # Cleared here rather than in fixture(), so a case that sets it affects
  # exactly one run and cannot leak into the next.
  unset FIXTURE_OPS_SHA
  output="$(MATRIX_ROOT="${repo}" MATRIX_MAX_AGE_DAYS=7 bash "${UNDER_TEST}" freshness 2>&1)" || rc=$?

  if { [ "${want}" = "fail" ] && [ "${rc}" -ne 0 ]; } || { [ "${want}" = "pass" ] && [ "${rc}" -eq 0 ]; }; then
    printf 'ok   %s (%s)\n' "${name}" "${description}"
    PASSED=$((PASSED + 1))
  else
    printf 'FAIL %s (%s): wanted %s, rc=%d\n%s\n' "${name}" "${description}" "${want}" "${rc}" "${output}"
    FAILED=$((FAILED + 1))
  fi
}

expect fail no_stamp "a page with the stamp deleted is not a fresh page" NOLINE 1 0
expect fail abbreviated "an abbreviated sha cannot be checked for ancestry" e3e2e77c 1 0
expect fail not_hex "a non-hex stamp is not a commit" not-a-sha-at-all 1 0
expect fail unknown_commit "a 40-hex sha this repo does not contain" "$(printf 'a%.0s' {1..40})" 1 0
expect fail stale_stamp "a stamp older than the budget" SENTINEL 9 0
expect fail stale_render "live sources last read outside the budget" SENTINEL 1 9
expect pass fresh "a real, recent, ancestor stamp with a recent render" SENTINEL 1 0

# The ops_sha half: the sha `-render` actually read from. A human cannot type
# it, so these cases cover the ways it can be WRONG rather than forged.
FIXTURE_OPS_SHA="$(printf 'b%.0s' {1..40})" \
  expect fail render_from_unknown_commit "the render claims a commit this repo does not have" SENTINEL 1 0
FIXTURE_OPS_SHA="deadbeef" \
  expect fail render_sha_abbreviated "an abbreviated ops_sha cannot be checked for ancestry" SENTINEL 1 0
FIXTURE_OPS_SHA="" \
  expect fail render_sha_missing "a render with no ops_sha cannot say what it read" SENTINEL 1 0

# Every staleness failure must hand the operator the exact re-verify command.
# A weekly gate whose message says "re-verify" without saying HOW is a gate
# people learn to route around (team-lead ruling, 2026-09-09).
check_message_names_the_command() {
  local repo output
  repo="$(fixture message_check SENTINEL 9 0)"
  unset FIXTURE_OPS_SHA
  output="$(MATRIX_ROOT="${repo}" MATRIX_MAX_AGE_DAYS=7 bash "${UNDER_TEST}" freshness 2>&1)" || true

  if printf '%s' "${output}" | grep -qF 'go run ./cmd/dev-health-migration-matrix -render -root .' \
    && printf '%s' "${output}" | grep -qE '[0-9]+ days old \(budget 7 days\)'; then
    printf 'ok   message_check (a stale failure prints the exact command and the age in days)\n'
    PASSED=$((PASSED + 1))
  else
    printf 'FAIL message_check: the failure text must carry the re-verify command AND the age in days:\n%s\n' "${output}"
    FAILED=$((FAILED + 1))
  fi
}
check_message_names_the_command

printf '\n%d passed, %d failed\n' "${PASSED}" "${FAILED}"
[ "${FAILED}" -eq 0 ]
