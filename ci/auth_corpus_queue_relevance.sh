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
# CHAOS-11670: the entry is ALSO relevant when a changed file sits in a directory of the LIVE `go list -deps` closure of the
#   roots below (the auth route packages and both binaries): the repo module's packages, plus the directory of every file those
#   packages embed. The script runs go itself, in the checkout, with GOFLAGS=-mod=readonly, GOTOOLCHAIN=local and LC_ALL=C, once,
#   only when the floor did not already match. It fails closed (one named line, corpus=true): no go, go list failing or timing
#   out, an empty list, a root that does not exist, or a directory that is not a plain relative path of [A-Za-z0-9._/-] segments.
# Changed paths are read with git diff -z and core.quotePath=false, so any file name is matched as written.
# It fails closed: bad input, a git error or a missing or empty path list all print corpus=true.
set -uo pipefail
# CHAOS-11670: byte semantics for every pattern below (ranges, ?, [..]): a table row with a non-ASCII letter must not pass in any locale.
export LC_ALL=C
export GOFLAGS=-mod=readonly GOTOOLCHAIN=local
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

# The floor did not match: judge the live closure. Two sets, one source (ci/auth_corpus_set.sh): the roots, and the package the
# corpus runs `go test -tags integration` on, with its tests and their dependencies (internal/testsupport/venueoracle, ...).
# shellcheck source=ci/auth_corpus_set.sh
source "${root}/ci/auth_corpus_set.sh" 2>/dev/null || run_corpus "closure unavailable: ci/auth_corpus_set.sh cannot be read"
[ "${#AUTH_CORPUS_ROOTS[@]}" -gt 0 ] && [ -n "${AUTH_CORPUS_TEST_PACKAGE:-}" ] || run_corpus "closure unavailable: ci/auth_corpus_set.sh sets no roots or no tested package"
for r in "${AUTH_CORPUS_ROOTS[@]}" "${AUTH_CORPUS_TEST_PACKAGE}"; do
  [ -d "${root}/${r#./}" ] || run_corpus "closure root ${r} does not exist"
done
command -v go >/dev/null 2>&1 || run_corpus "closure unavailable: go is not on PATH"
module="$(cd "${root}" && go list -m 2>/dev/null)" || run_corpus "closure unavailable: go list -m failed"
case "${module}" in '' | *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-]*) run_corpus "closure unavailable: odd module path" ;; esac
golist_file="$(mktemp)" || run_corpus "no temporary file"
golist_test_file="$(mktemp)" || run_corpus "no temporary file"
trap 'rm -f "${changed_file}" "${golist_file}" "${golist_test_file}"' EXIT
# P: a package of the repo module in the closure (a test variant prints as `PATH [PATH.test]`, the test main as `PATH.test`).
# E: a file such a package embeds (go:embed reaches into subdirectories), in the package or in its tests.
golist_template='{{if not .Standard}}{{with .Module}}{{if eq .Path "'"${module}"'"}}P {{$.ImportPath}}{{"\n"}}{{range $.EmbedFiles}}E {{$.ImportPath}}/{{.}}{{"\n"}}{{end}}{{range $.TestEmbedFiles}}E {{$.ImportPath}}/{{.}}{{"\n"}}{{end}}{{range $.XTestEmbedFiles}}E {{$.ImportPath}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}{{end}}'
(cd "${root}" && timeout 300 go list -deps -f "${golist_template}" "${AUTH_CORPUS_ROOTS[@]}") >"${golist_file}" 2>/dev/null \
  || run_corpus "closure unavailable: go list -deps of the roots failed"
(cd "${root}" && timeout 300 go list -deps -test -tags integration -f "${golist_template}" "${AUTH_CORPUS_TEST_PACKAGE}") >"${golist_test_file}" 2>/dev/null \
  || run_corpus "closure unavailable: go list -deps -test of the tested package failed"
declare -A closure_dirs=()
add_closure_rows() {
  local list_file="$1" line kind path rest
  rows_added=0
  while IFS= read -r line || [ -n "${line}" ]; do
    [ -n "${line}" ] || continue
    kind=${line%% *}
    path=${line#* }
    case "${kind}" in P | E) ;; *) run_corpus "closure unavailable: unexpected go list line" ;; esac
    case "${path}" in "${module}"/*) ;; *) run_corpus "closure unavailable: a package outside the module" ;; esac
    path=${path#"${module}"/}
    case "${path}" in
      *" ["*"]/"*) rest=${path#*"]/"}; path="${path%% \[*}/${rest}" ;;
      *" ["*"]") path=${path%% \[*} ;;
    esac
    [ "${kind}" = P ] && path=${path%.test}
    [ "${kind}" = E ] && path=${path%/*}
    case "${path}" in '' | *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-]*) run_corpus "closure unavailable: malformed directory" ;; esac
    case "/${path}/" in */./* | */../* | *//*) run_corpus "closure unavailable: malformed directory" ;; esac
    closure_dirs["${path}"]=1
    rows_added=$((rows_added + 1))
  done <"${list_file}"
}
rows_added=0
add_closure_rows "${golist_file}"
[ "${rows_added}" -gt 0 ] || run_corpus "closure unavailable: go list printed no package"
add_closure_rows "${golist_test_file}"
[ "${rows_added}" -gt 0 ] || run_corpus "closure unavailable: go list -test printed no package"
while IFS= read -r -d '' file; do
  case "${file}" in
    */*)
      dir=${file%/*}
      if [ -n "${dir}" ] && [ -n "${closure_dirs["${dir}"]+x}" ]; then
        printf 'auth corpus relevance: %s is in the closure directory %s\n' "${file}" "${dir}" >&2
        echo corpus=true
        exit 0
      fi
      ;;
  esac
done <"${changed_file}"
echo corpus=false
