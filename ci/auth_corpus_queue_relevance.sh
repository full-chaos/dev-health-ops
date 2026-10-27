#!/usr/bin/env bash
# Usage: auth_corpus_queue_relevance.sh <target-branch-tip-sha> <merge-group-head-sha>
# Prints exactly one line, corpus=true or corpus=false, and exits 0.
#
# Whether a merge queue entry must run the external auth corpus (CHAOS-11596). It judges the WHOLE group ahead of the
# entry: the diff runs from merge-base(target branch tip, entry head) to the entry head, against the rows of
# ci/auth_corpus_paths.txt.
#   Row form 1: `dir/*` is a bash glob, its star crosses slashes. Row form 2: `=dir/*.go` matches one directory only.
#   A malformed row prints corpus=true: a CR, leading or trailing whitespace, only `=`, a `=` after the first character, or an `=` row
#   with no slash, an empty directory part or last element, a glob character in the directory part, or `//`.
# Changed paths are read with git diff -z and core.quotePath=false, so any file name is matched as written.
# It fails closed: bad input, a git error or a missing or empty path list all print corpus=true.
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
# row_problem ROW: prints why a row of the path list is malformed (it would match nothing, so the corpus would be skipped).
row_problem() {
  local row=$1 pat dir
  [[ ${row} == *$'\r'* ]] && { echo "carriage return"; return; }
  [[ ${row} == [[:blank:]]* || ${row} == *[[:blank:]] ]] && { echo "leading or trailing whitespace"; return; }
  [[ ${row} == "=" ]] && { echo "only the = prefix"; return; }
  [[ ${row:1} == *=* ]] && { echo "= after the first character"; return; }
  [[ ${row} == =* ]] || return 0
  pat=${row#=}
  [[ ${pat} == */* ]] || { echo "= row without a slash"; return; }
  dir=${pat%/*}
  [ -n "${dir}" ] || { echo "= row with an empty directory part"; return; }
  case ${dir} in *[\*\?\[]*) echo "= row with a glob character in its directory part"; return ;; esac
  [[ ${pat} == *//* ]] && { echo "= row with an empty path element"; return; }
  [ -n "${pat##*/}" ] || { echo "= row whose last element is empty"; return; }
  return 0
}
globs=()
# "|| [ -n ${line} ]": a last row without a newline is still a row.
while IFS= read -r line || [ -n "${line}" ]; do
  { [ -n "${line}" ] && [[ ${line} != \#* ]]; } || continue
  problem="$(row_problem "${line}")"
  [ -z "${problem}" ] || run_corpus "malformed row in ci/auth_corpus_paths.txt (${problem})"
  globs+=("${line}")
done <"${root}/ci/auth_corpus_paths.txt" 2>/dev/null
[ "${#globs[@]}" -gt 0 ] || run_corpus "ci/auth_corpus_paths.txt is missing or has no rows"
mb="$(git -C "${root}" merge-base "${tip}" "${head}")" || run_corpus "no merge base of ${tip} and ${head}"
# -z and core.quotePath=false: a path with a non-ASCII byte, a tab, a quote or a backslash is read as written, never quoted.
changed_file="$(mktemp)" || run_corpus "no temporary file"
trap 'rm -f "${changed_file}"' EXIT
git -C "${root}" -c core.quotePath=false diff -z --name-only --no-renames "${mb}" "${head}" >"${changed_file}" || run_corpus "git diff failed"
printf 'auth corpus relevance (merge_group): judging %s..%s (the whole group)\n' "${mb}" "${head}" >&2
while IFS= read -r -d '' file; do
  [ -n "${file}" ] || continue
  for glob in "${globs[@]}"; do
    flat=0
    pat=${glob}
    if [[ ${glob} == =* ]]; then
      flat=1
      pat=${glob#=}
    fi
    # shellcheck disable=SC2254 # the unquoted glob is the point
    case "${file}" in
      ${pat})
        if [ "${flat}" = 1 ] && [ "${file%/*}" != "${pat%/*}" ]; then
          continue
        fi
        printf 'auth corpus relevance: %s matches %s\n' "${file}" "${glob}" >&2
        echo corpus=true
        exit 0
        ;;
    esac
  done
done <"${changed_file}"
echo corpus=false
