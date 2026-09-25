#!/usr/bin/env bash
# merge-tree-vet.sh -- build main + a PR's merge-tree and vet it BEFORE the merge (Trap #401).
#
#   ci/merge-tree-vet.sh <pr-number>
#   ci/merge-tree-vet.sh --head <rev> [--base <rev>]     (no network; used by the tests)
#
# WHY. Two PRs that each pass CI on their own base can conflict SEMANTICALLY
# while merging textually clean: #3044 added `Peek` to httpapi.CounterStore and
# #3041's test stub `failingCounters` had none; both merged back to back and
# main's go-quality went red for every fresh merge ref until a one-method fix
# landed (Trap #401). `git merge-tree` cleanliness is not evidence that the
# result builds. This script computes the merge result of <base> (default
# origin/main, freshly fetched) and the PR head, checks out ONLY that tree into
# a scratch directory, and runs, in it:
#
#   vet              bash ci/check_go.sh vet              (go vet ./... in every Go module)
#   integration-vet  bash ci/check_go.sh integration-vet  (the -tags=integration build, whole tree)
#   registry         bash ci/check_venue_oracle_registry.sh (every venue test is registered)
#
# It runs EVERY stage even after one fails, so one run reports all the breaks,
# and prints one final line:  merge-tree-vet: OK|FAIL pr=<n|-> head=<sha> base=<sha> tree=<oid>
# Exit 0 = OK, 1 = FAIL (a stage failed, or the merge conflicts textually),
# 2 = usage / could not measure. A measurement that did not happen is never OK:
# a missing stage script fails, it does not skip.
#
# It never touches the invoking checkout's working tree, index or config: the
# tree is read with `git archive` into `mktemp -d` (its own throwaway git repo) under
# ${DEV_HEALTH_SCRATCH:-${TMPDIR:-/tmp}} and removed on exit. Run it from a lane worktree (never from a
# shared checkout).
#
# Where the bytes go (a /tmp Go cache once reached 33.5 GB on a shared host):
#   scratch tree   ${DEV_HEALTH_SCRATCH:-${TMPDIR:-/tmp}}   (throwaway, removed on exit)
#   Go build cache DEV_HEALTH_GO_CACHE, else ${GOCACHE}, else `go env GOCACHE` (the user's own
#                  warm cache, off /tmp), else <scratch root>/merge-tree-vet-gocache only when Go
#                  cannot name one. It is never placed under /tmp while any of the first three exists.
set -euo pipefail

usage() {
  printf 'usage: ci/merge-tree-vet.sh <pr-number> | --head <rev> [--base <rev>]\n' >&2
  exit 2
}

pr="" head="" base=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --head) [ "$#" -ge 2 ] || usage; head="$2"; shift 2 ;;
    --base) [ "$#" -ge 2 ] || usage; base="$2"; shift 2 ;;
    -h|--help) usage ;;
    -*) usage ;;
    *) [ -z "${pr}" ] || usage; pr="$1"; shift ;;
  esac
done
if [ -n "${pr}" ]; then
  case "${pr}" in *[!0-9]*) usage ;; esac
  [ -z "${head}" ] || usage
elif [ -z "${head}" ]; then
  usage
fi

command -v git >/dev/null 2>&1 || { echo "merge-tree-vet: git is required" >&2; exit 2; }
git rev-parse --git-dir >/dev/null 2>&1 || { echo "merge-tree-vet: not inside a git checkout" >&2; exit 2; }

if [ -n "${pr}" ]; then
  git fetch -q origin "pull/${pr}/head" || { echo "merge-tree-vet: cannot fetch pull/${pr}/head" >&2; exit 2; }
  head="$(git rev-parse FETCH_HEAD)"
fi
if [ -z "${base}" ]; then
  git fetch -q origin main || { echo "merge-tree-vet: cannot fetch origin/main" >&2; exit 2; }
  base="origin/main"
fi
head_sha="$(git rev-parse --verify "${head}^{commit}")" || { echo "merge-tree-vet: unknown head ${head}" >&2; exit 2; }
base_sha="$(git rev-parse --verify "${base}^{commit}")" || { echo "merge-tree-vet: unknown base ${base}" >&2; exit 2; }
label="${pr:--}"

final() { # final <OK|FAIL> <tree>
  printf 'merge-tree-vet: %s pr=%s head=%s base=%s tree=%s\n' "$1" "${label}" "${head_sha:0:12}" "${base_sha:0:12}" "${2:--}"
}

# `git merge-tree --write-tree` exits 1 on a textual conflict, 0 on a clean merge
# (first output line = the merged tree object).
set +e
merged="$(git merge-tree --write-tree --no-messages "${base_sha}" "${head_sha}" 2>/dev/null)"
mt_rc=$?
set -e
if [ "${mt_rc}" -eq 1 ]; then
  echo "merge-tree-vet: FAIL merge: ${head_sha:0:12} does not merge cleanly into ${base_sha:0:12} (textual conflict)" >&2
  final FAIL
  exit 1
elif [ "${mt_rc}" -ne 0 ]; then
  echo "merge-tree-vet: could not compute the merge tree (git merge-tree exit ${mt_rc})" >&2
  exit 2
fi
tree="$(printf '%s\n' "${merged}" | head -n1)"
[ -n "${tree}" ] || { echo "merge-tree-vet: git merge-tree printed no tree" >&2; exit 2; }

scratch_root="${DEV_HEALTH_SCRATCH:-${TMPDIR:-/tmp}}"
mkdir -p "${scratch_root}"
scratch="$(mktemp -d "${scratch_root}/merge-tree-vet.XXXXXX")"
cleanup() { rm -rf -- "${scratch}"; }
trap cleanup EXIT
git archive "${tree}" | tar -x -C "${scratch}"
# check_go.sh enumerates files with `git ls-files` and `git status`, so the
# scratch tree is its OWN throwaway repository (never the invoker's): the merged
# tree committed once, with no config read or written.
(
  cd "${scratch}"
  export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
  git init -q
  git add -A
  git -c user.name=merge-tree-vet -c user.email=merge-tree-vet@invalid -c commit.gpgsign=false \
    commit -q -m "merge-tree of ${base_sha} and ${head_sha}"
)

# One warm Go build cache across runs on this host; check_go.sh honours it. Default to the
# invoker's own Go cache, not a fresh copy under the scratch root (see the header).
default_go_cache="${GOCACHE:-}"
[ -n "${default_go_cache}" ] || default_go_cache="$(go env GOCACHE 2>/dev/null || true)"
[ -n "${default_go_cache}" ] || default_go_cache="${scratch_root}/merge-tree-vet-gocache"
export DEV_HEALTH_GO_CACHE="${DEV_HEALTH_GO_CACHE:-${default_go_cache}}"

failed=0
run_stage() { # run_stage <name> <script relative to the tree> [args...]
  local name="$1" script="$2"
  shift 2
  if [ ! -f "${scratch}/${script}" ]; then
    printf 'merge-tree-vet: FAIL %s: %s is missing from the merged tree (a stage that cannot run is not a pass)\n' "${name}" "${script}" >&2
    failed=1
    return
  fi
  printf 'merge-tree-vet: stage %s ...\n' "${name}"
  if (cd "${scratch}" && bash "${script}" "$@") >"${scratch}/.stage-${name}.log" 2>&1; then
    printf 'merge-tree-vet: PASS %s\n' "${name}"
  else
    printf 'merge-tree-vet: FAIL %s (last lines of its output follow)\n' "${name}" >&2
    tail -n 25 "${scratch}/.stage-${name}.log" >&2
    failed=1
  fi
}

run_stage vet ci/check_go.sh vet
run_stage integration-vet ci/check_go.sh integration-vet
run_stage registry ci/check_venue_oracle_registry.sh

if [ "${failed}" -ne 0 ]; then
  final FAIL "${tree}"
  exit 1
fi
final OK "${tree}"
