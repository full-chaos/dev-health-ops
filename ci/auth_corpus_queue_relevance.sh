#!/usr/bin/env bash
# Usage: auth_corpus_queue_relevance.sh <target-branch-tip-sha> <merge-group-head-sha>
# Prints exactly one line, corpus=true or corpus=false, and exits 0.
#
# Whether a merge queue entry must run the external auth corpus (CHAOS-11596). It judges the WHOLE group ahead of the
# entry: the diff runs from merge-base(target branch tip, entry head) to the entry head, against the globs of
# ci/auth_corpus_paths.txt. It fails closed: bad input, a git error or a missing or empty path list all print corpus=true.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || {
  echo corpus=true
  exit 0
}
run_corpus() {
  printf 'auth corpus relevance (merge_group): %s; the corpus runs\n' "$1" >&2
  echo corpus=true
  exit 0
}
[ "$#" -eq 2 ] || run_corpus "usage: auth_corpus_queue_relevance.sh <target-branch-tip-sha> <merge-group-head-sha>"
tip=$1
head=$2
[[ $tip =~ ^[0-9a-f]{40}$ && $head =~ ^[0-9a-f]{40}$ ]] || run_corpus "both arguments must be full commit shas"
globs=()
while IFS= read -r line; do
  [ -n "${line}" ] && [[ ${line} != \#* ]] && globs+=("${line}")
done <"${root}/ci/auth_corpus_paths.txt" 2>/dev/null
[ "${#globs[@]}" -gt 0 ] || run_corpus "ci/auth_corpus_paths.txt is missing or has no rows"
mb="$(git -C "${root}" merge-base "${tip}" "${head}")" || run_corpus "no merge base of ${tip} and ${head}"
changed="$(git -C "${root}" diff --name-only --no-renames "${mb}" "${head}")" || run_corpus "git diff failed"
printf 'auth corpus relevance (merge_group): judging %s..%s (the whole group)\n' "${mb}" "${head}" >&2
while IFS= read -r file; do
  [ -n "${file}" ] || continue
  for glob in "${globs[@]}"; do
    # shellcheck disable=SC2254 # the unquoted glob is the point
    case "${file}" in
      ${glob})
        printf 'auth corpus relevance: %s matches %s\n' "${file}" "${glob}" >&2
        echo corpus=true
        exit 0
        ;;
    esac
  done
done <<<"${changed}"
echo corpus=false
