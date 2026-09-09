#!/usr/bin/env bash
# Assert that the checked-in gqlgen output differs from a fresh generation by
# EXACTLY the documented set of deliberate hand-edits, and nothing else
# (CHAOS-5489).
#
# WHY THIS EXISTS. cmd/query-api's generated GraphQL layer carries hand-edits
# that regeneration reverts -- nullability rulings (CHAOS-4650, 4657, 4658,
# 4701, 4703) and, until the SDL half of CHAOS-5483 lands, three SankeyCoverage
# fields that exist in the model but not yet in the schema. Until now nothing
# recorded that, and the only thing preventing an accidental revert was that
# `gqlgen generate` could not run at all: its CLI dependencies were missing
# from go.sum. That is an accident protecting a contract, and CHAOS-5489
# removes it. This check is what replaces it.
#
# The guard is EQUALITY, not containment. New drift fails, and so does a
# hand-edit QUIETLY DISAPPEARING -- the second is the direction an allowlist
# usually misses, and it is the one that silently reverts a chris ruling.
#
# Generation runs in a COPY, never in the working tree, so this check cannot
# damage anything even though the generator it drives deletes files before
# writing them.
#
#   ci/check_gqlgen_drift.sh            verify (CI)
#   ci/check_gqlgen_drift.sh --update   rewrite the allowlist deliberately
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
ALLOWLIST="${ROOT}/contracts/gqlgen/v1/expected-drift.allowlist"

GENERATED_FILES=(
  "cmd/query-api/internal/graph/generated.go"
  "cmd/query-api/internal/graph/model/models_gen.go"
  "cmd/query-api/internal/graph/schema.resolvers.go"
)

UPDATE=0
[ "${1:-}" = "--update" ] && UPDATE=1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/gqlgen-drift-XXXXXX")"
# shellcheck disable=SC2317  # invoked by the EXIT trap below
cleanup() { chmod -R u+w "${WORK}" 2>/dev/null || true; rm -rf "${WORK}"; }
trap cleanup EXIT

# Copy the tree rather than generating in place. `git worktree` is not usable
# here: it would materialise HEAD and miss uncommitted work, so a developer
# checking their own change would be told about someone else's tree.
tar -C "${ROOT}" \
  --exclude=./.git --exclude=./.venv --exclude=./.uv-cache \
  --exclude=./node_modules --exclude=./.uv --exclude=./site \
  -cf - . | tar -C "${WORK}" -xf -

set +e
( cd "${WORK}/cmd/query-api" && go run github.com/99designs/gqlgen generate --config gqlgen.yml ) >"${WORK}/.gen.log" 2>&1
rc=$?
set -e
if [ "${rc}" -ne 0 ]; then
  echo "check_gqlgen_drift: generation FAILED in the temp copy (exit ${rc}). Output:" >&2
  cat "${WORK}/.gen.log" >&2
  echo "check_gqlgen_drift: an EMPTY log above is gqlgen's own known failure mode, not a lost message." >&2
  exit 1
fi

# Normalised changed lines: side marker plus content, line numbers dropped so
# an unrelated insertion elsewhere in a generated file does not churn every
# entry. `diff` is used directly rather than `git diff` -- the copy is not a
# repository, and more importantly a git-based comparison would silently treat
# an untracked file as clean, which is the exact trap this repo has hit.
actual="${WORK}/.actual"
: >"${actual}"
for rel in "${GENERATED_FILES[@]}"; do
  if [ ! -f "${WORK}/${rel}" ]; then
    echo "check_gqlgen_drift: generation left ${rel} MISSING in the copy -- treat any success as false." >&2
    exit 1
  fi
  diff -u "${ROOT}/${rel}" "${WORK}/${rel}" \
    | grep -E '^[+-][^+-]' \
    | sed -e "s|^|${rel}\t|" >>"${actual}" || true
done
sort -o "${actual}" "${actual}"

if [ "${UPDATE}" -eq 1 ]; then
  mkdir -p "$(dirname "${ALLOWLIST}")"
  {
    echo "# gqlgen expected-drift allowlist (CHAOS-5489). GENERATED -- rewrite with:"
    echo "#     ci/check_gqlgen_drift.sh --update"
    echo "#"
    echo "# Every line below is a difference between the CHECKED-IN generated files"
    echo "# and a fresh 'gqlgen generate'. Each one is a deliberate hand-edit that"
    echo "# regeneration would revert. The guard asserts this set EXACTLY: a new"
    echo "# entry means unreviewed drift, and a MISSING entry means a hand-edit was"
    echo "# silently lost -- which is how a chris nullability ruling would quietly"
    echo "# revert. Neither direction is allowed to pass."
    echo "#"
    echo "# Tickets represented here: CHAOS-4650, CHAOS-4657, CHAOS-4658,"
    echo "# CHAOS-4701, CHAOS-4703 (nullable value fields and their marshalers),"
    echo "# and CHAOS-5483 (three SankeyCoverage fields that exist in the model but"
    echo "# not yet in the SDL -- those entries DISAPPEAR when the SDL half lands,"
    echo "# and this file must shrink accordingly rather than be re-blessed)."
    echo "#"
    printf '# Format: <file><TAB><+|-><line content>. %s\n' "'-' is in the checked-in file and"
    echo "# not in a fresh generation; '+' is the reverse."
    cat "${actual}"
  } >"${ALLOWLIST}"
  echo "check_gqlgen_drift: allowlist rewritten with $(wc -l <"${actual}") entries. REVIEW THE DIFF -- it is a contract."
  exit 0
fi

if [ ! -f "${ALLOWLIST}" ]; then
  echo "check_gqlgen_drift: ${ALLOWLIST} is missing. Create it with --update." >&2
  exit 1
fi

expected="${WORK}/.expected"
grep -v '^#' "${ALLOWLIST}" | grep -v '^[[:space:]]*$' | sort >"${expected}"

if diff -q "${expected}" "${actual}" >/dev/null; then
  echo "check_gqlgen_drift: OK. $(wc -l <"${actual}") drift lines, all documented."
  exit 0
fi

echo "check_gqlgen_drift: the gqlgen output drift does not match the allowlist." >&2
echo >&2
echo "NEW drift (present now, not documented) -- a hand-edit or generator change nobody recorded:" >&2
comm -13 "${expected}" "${actual}" | sed 's/^/  /' >&2
echo >&2
echo "LOST drift (documented, no longer present) -- a hand-edit that has been REVERTED:" >&2
comm -23 "${expected}" "${actual}" | sed 's/^/  /' >&2
echo >&2
echo "If the change is deliberate: ci/check_gqlgen_drift.sh --update, and say which ticket in the commit." >&2
echo "If it is not: something regenerated over a hand-edit. See cmd/query-api/README.md." >&2
exit 1
