#!/usr/bin/env bash
# Usage: auth_queue_relevance.sh <target-branch-tip-sha> <merge-group-head-sha>
# Prints exactly one line, relevant=true or relevant=false, and exits 0.
#
# The auth gates' relevance for a merge queue entry. It judges the WHOLE group ahead of the entry, never the entry's
# own PR alone: merge_group.base_sha is the parent ENTRY, so the diff runs from merge-base(target branch tip, entry
# head) to the entry head, with the same rule a pull request uses (ci/check_go.sh auth relevant). It fails closed:
# bad input, a git error, a non-zero exit of the relevance check (a failing go list included) or a missing verdict
# all print relevant=true, so the auth gates run.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || {
  echo relevant=true
  exit 0
}
run_gates() {
  printf 'auth relevance (merge_group): %s; the auth gates run\n' "$1" >&2
  echo relevant=true
  exit 0
}
[ "$#" -eq 2 ] || run_gates "usage: auth_queue_relevance.sh <target-branch-tip-sha> <merge-group-head-sha>"
tip=$1
head=$2
[[ $tip =~ ^[0-9a-f]{40}$ && $head =~ ^[0-9a-f]{40}$ ]] || run_gates "both arguments must be full commit shas"
mb="$(git -C "${root}" merge-base "${tip}" "${head}")" || run_gates "no merge base of ${tip} and ${head}"
printf 'auth relevance (merge_group): judging %s..%s (the whole group)\n' "${mb}" "${head}" >&2
out="$(bash "${root}/ci/check_go.sh" auth relevant "${mb}" "${head}")" || run_gates "ci/check_go.sh auth relevant exited non-zero"
verdict="$(grep -xE 'relevant=(true|false)' <<<"${out}")" || run_gates "the relevance check printed no verdict"
[ "$(grep -c . <<<"${verdict}")" -eq 1 ] || run_gates "the relevance check printed more than one verdict"
echo "${verdict}"
