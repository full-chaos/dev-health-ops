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
# The exact half of the contract: sha256 of the raw unified diff, positions
# included. See the note beside the awk below for why the allowlist alone
# cannot be exact.
DIGEST="${ROOT}/contracts/gqlgen/v1/expected-drift.sha256"

GENERATED_FILES=(
  "cmd/query-api/internal/graph/generated.go"
  "cmd/query-api/internal/graph/model/models_gen.go"
  "cmd/query-api/internal/graph/schema.resolvers.go"
)

UPDATE=0
[ "${1:-}" = "--update" ] && UPDATE=1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/gqlgen-drift-XXXXXX")"
# shellcheck disable=SC2317,SC2329  # invoked by the EXIT trap below
# SC2329 is 0.11.0's rename of the never-invoked check; the pin is 0.11.0,
# so silencing only SC2317 left this failing under the version CI runs.
cleanup() { chmod -R u+w "${WORK}" 2>/dev/null || true; rm -rf "${WORK}"; }
trap cleanup EXIT

# Copy the tree rather than generating in place. `git worktree` is not usable
# here: it would materialise HEAD and miss uncommitted work, so a developer
# checking their own change would be told about someone else's tree.
tar -C "${ROOT}" \
  --exclude=./.git --exclude=./.venv --exclude=./.uv-cache \
  --exclude=./node_modules --exclude=./.uv --exclude=./site \
  -cf - . | tar -C "${WORK}" -xf -

# The generator is injectable ONLY so this script's own behaviour can be
# tested. Review round r5 showed every assertion here was textual: mutations
# that added an early exit, skipped the digest comparison, or replaced the
# wrapper with a no-op passed all ten wiring tests, because nothing ever ran
# the guard and checked what it DID. A fake generator makes that testable in
# milliseconds instead of a 10s gqlgen run per case. The wiring test asserts
# CI never sets this, which is the other half of the safety.
# GENERATING IN A COPY IS NOT ISOLATION IF THE CONFIG NAMES ABSOLUTE PATHS.
#
# This script believed the temp copy made it safe. Review round r8 set
# exec.filename to an absolute path and the generator wrote straight through
# the copy into a real file on disk -- copying a tree does not relocate an
# absolute path, and the after-the-fact scan only ever looked inside the copy.
# The wrapper already refused such a config; the guard did not, which is
# exactly the asymmetry that made it exploitable. Same check, same function,
# both scripts, run BEFORE anything is generated.
# shellcheck source=ci/gqlgen_output_scope.sh
# shellcheck disable=SC1091  # resolved at runtime from SCRIPT_DIR; the
# canonical lint runs without -x, so following the source is not available
# to it. The path is asserted by the wiring test instead.
. "${SCRIPT_DIR}/gqlgen_output_scope.sh"
if ! gqlgen_output_scope_check "${WORK}/cmd/query-api" "${WORK}/cmd/query-api/internal/graph"; then
  echo "check_gqlgen_drift: REFUSING -- gqlgen.yml writes outside the generated root (above)." >&2
  echo "  An absolute or escaping output path is not contained by the temp copy," >&2
  echo "  so this refuses to generate rather than discover the damage afterwards." >&2
  exit 2
fi

GENERATE_CMD="${GQLGEN_DRIFT_GENERATE_CMD:-go run github.com/99designs/gqlgen generate --config gqlgen.yml}"
# Marker for detecting writes ANYWHERE in the copy, not only under the graph
# root: a stanza pointed at internal/foo/ must be a finding, not an escape,
# and checking only where output is EXPECTED cannot see output that is not.
# `git status --porcelain` is unavailable here -- the copy deliberately
# excludes .git -- so a timestamp marker plus `find -newer` gives the same
# answer for one stat pass.
#
# `.outside` is excluded because the shell creates a redirect's target BEFORE
# running the command that writes it, so the results file is always newer than
# the marker and the check reports itself. Found by it failing on a clean tree.
touch "${WORK}/.gen-marker"
set +e
( cd "${WORK}/cmd/query-api" && eval "${GENERATE_CMD}" ) >"${WORK}/.gen.log" 2>&1
rc=$?
set -e
if [ "${rc}" -ne 0 ]; then
  echo "check_gqlgen_drift: generation FAILED in the temp copy (exit ${rc}). Output:" >&2
  cat "${WORK}/.gen.log" >&2
  echo "check_gqlgen_drift: an EMPTY log above is gqlgen's own known failure mode, not a lost message." >&2
  exit 1
fi

# Each entry is <file> <declaration @ after:context> <+|-><content>.
#
# Line numbers are dropped so an unrelated insertion elsewhere in a generated
# file does not churn every entry. Something must take their place, because
# content alone is ambiguous: 139 of these entries are comment lines, and two
# genuinely different changes -- the same comment text removed from two
# different types -- normalise to one identical entry, so the guard would see
# one where there are two and a swap between them as no change at all.
#
# The DECLARATION comes from `diff -F`'s hunk header, which is stable under
# edits elsewhere in the file in a way a line number is not. Note it is the
# PRECEDING declaration, not the enclosing one: `diff -F` names the last
# matching line STRICTLY BEFORE the hunk start, so a hunk that opens on a
# struct header is labelled with the struct before it -- deleting the three
# parked SankeyCoverage fields yields `type ReworkThemeAllocation struct {`
# even though the hunk opens on `type SankeyCoverage struct {`. That is
# correct output, not a stale entry, and must not be "fixed": expected and
# actual are computed the same way and compared for equality, so the anchor
# only has to be reproducible, not semantic.
#
# The declaration ALONE is still not enough, which the review round proved by
# execution. Every change inside one hunk shares its declaration, so where a
# duplicated line appears twice in the same hunk -- schema.resolvers.go's
# repeated `attribute` import, under `(file scope)` -- documenting occurrence
# 1 while the generator removes occurrence 2 leaves the multiset unchanged
# and the guard reports OK. The `after:` context is the nearest unchanged
# line above the change within its hunk, and it separates exactly that case
# while still surviving unrelated edits elsewhere.
#
# `diff` is used directly rather than `git diff`: the copy is not a
# repository, and a git-based comparison silently treats an untracked file as
# clean, which is a trap this repo has hit.
# go.mod/go.sum are EXEMPT from the outside-root check and diffed instead.
# Running the generator runs the go tool, which tidies the module files as a
# matter of course -- review round r7 measured gqlgen removing 92 checksum
# lines on a pristine tree, which made this check fail the build before any
# drift was compared. That is a real generator side effect, so it is drift to
# be reported, not an out-of-scope write to be refused.
find "${WORK}" -type f -newer "${WORK}/.gen-marker" \
  -not -path "${WORK}/cmd/query-api/internal/graph/*" \
  -not -name '.gen-marker' -not -name '.gen.log' -not -name '.outside' \
  -not -path "${WORK}/.git/*" \
  -not -path "${WORK}/go.mod" -not -path "${WORK}/go.sum" \
  -print | sed "s|^${WORK}/||" | sort >"${WORK}/.outside"
if [ -s "${WORK}/.outside" ]; then
  echo "check_gqlgen_drift: generation wrote OUTSIDE the generated root:" >&2
  sed 's|^|    |' "${WORK}/.outside" >&2
  echo "  Only cmd/query-api/internal/graph is snapshotted, restored and diffed." >&2
  echo "  A generated file anywhere else is unprotected by every check here." >&2
  exit 1
fi

# THE UNIT OF COMPARISON IS THE AREA, NOT THE FILE LIST.
#
# Comparing only the three listed files is not the same as comparing the
# generated output. Round r5 pointed a resolver stanza at a new path and the
# guard still said "all documented"; round r6 went further and showed a file
# SET comparison is not enough either -- generation that OVERWRITES an
# existing, hand-written file (internal/graph/resolver.go) leaves the set
# unchanged, and a nested output escaped the maxdepth-1 scan entirely.
#
# So every .go file under the generated root is compared, recursively, by
# content. The three tracked files produce allowlist entries as before; ANY
# other file differing means generation wrote somewhere it must not, and that
# is a hard failure rather than a drift line -- there is no such thing as a
# documented hand-edit in a file gqlgen is not supposed to write.
GEN_ROOT="cmd/query-api/internal/graph"
( cd "${ROOT}/${GEN_ROOT}" 2>/dev/null && find . -type f -name '*.go' -print ) | sort >"${WORK}/.set-root"
( cd "${WORK}/${GEN_ROOT}" 2>/dev/null && find . -type f -name '*.go' -print ) | sort >"${WORK}/.set-gen"
if ! diff -q "${WORK}/.set-root" "${WORK}/.set-gen" >/dev/null; then
  echo "check_gqlgen_drift: regeneration changed WHICH FILES exist under ${GEN_ROOT}." >&2
  echo "  only in a fresh generation (untracked generator output):" >&2
  comm -13 "${WORK}/.set-root" "${WORK}/.set-gen" | sed 's|^\./|    |' >&2
  echo "  only in the checked-in tree (generation no longer produces it):" >&2
  comm -23 "${WORK}/.set-root" "${WORK}/.set-gen" | sed 's|^\./|    |' >&2
  echo "  A file the guard does not diff is a file whose contents are unprotected." >&2
  exit 1
fi

# The module files are compared like any other tracked output: a tidy that
# changes them is reported, so it is visible and deliberate rather than
# silently swallowed by the exemption above.
for modfile in go.mod go.sum; do
  if ! cmp -s "${ROOT}/${modfile}" "${WORK}/${modfile}"; then
    echo "check_gqlgen_drift: running the generator CHANGED ${modfile}:" >&2
    diff -u "${ROOT}/${modfile}" "${WORK}/${modfile}" | sed 's|^|    |' | head -20 >&2
    echo "  The go tool tidied the module files while generating. Commit the" >&2
    echo "  tidied ${modfile}, or pin the dependency that keeps moving." >&2
    exit 1
  fi
done

untracked_changed=0
while IFS= read -r rel; do
  [ -n "${rel}" ] || continue
  full="${GEN_ROOT}/${rel#./}"
  case " ${GENERATED_FILES[*]} " in *" ${full} "*) continue ;; esac
  if ! cmp -s "${ROOT}/${full}" "${WORK}/${full}"; then
    if [ "${untracked_changed}" -eq 0 ]; then
      echo "check_gqlgen_drift: regeneration MODIFIED files it does not generate:" >&2
    fi
    echo "    ${full}" >&2
    untracked_changed=1
  fi
done <"${WORK}/.set-root"
if [ "${untracked_changed}" -ne 0 ]; then
  echo "  These are hand-written. Generation overwriting one destroys it, and a" >&2
  echo "  file-set check cannot see it because the file still exists. Fix the" >&2
  echo "  gqlgen.yml stanza that points generation at them." >&2
  exit 1
fi

actual="${WORK}/.actual"
: >"${actual}"
for rel in "${GENERATED_FILES[@]}"; do
  if [ ! -f "${WORK}/${rel}" ]; then
    echo "check_gqlgen_drift: generation left ${rel} MISSING in the copy -- treat any success as false." >&2
    exit 1
  fi
  # diff exits 0 (same), 1 (differs) or >=2 (ERROR). Only the first two are
  # results; >=2 is a broken comparison and must not read as "no drift".
  # `diff` is run UNPIPED and its status read directly from `$?` on the very
  # next line, so nothing between the command and the read can overwrite it.
  # It deliberately does not end in `| ...` or `|| true`: a pipeline reports
  # its LAST command's status, and a `|| true` swallows the error entirely --
  # either way an empty diff and a failed diff become indistinguishable
  # downstream, which is how a guard like this passes vacuously.
  # `set +e` is REQUIRED, not defensive: diff exits 1 whenever the files
  # differ, which is the NORMAL case here, and under `set -e` that terminates
  # the script before the status is ever read. This is the same class the
  # round flagged, and it bit again while fixing it -- the guard aborted at
  # the first differing file and reported nothing.
  set +e
  diff -u -F '^\(type\|func\|var\|const\) ' \
    --label "checked-in/${rel}" --label "regenerated/${rel}" \
    "${ROOT}/${rel}" "${WORK}/${rel}" >"${WORK}/.raw" 2>"${WORK}/.differr"
  dstatus=$?
  set -e
  if [ "${dstatus}" -ge 2 ]; then
    echo "check_gqlgen_drift: diff FAILED on ${rel} (exit ${dstatus}); the comparison is broken, not empty." >&2
    cat "${WORK}/.differr" >&2
    exit 1
  fi
  # The middle field is the hunk's declaration PLUS the nearest unchanged
  # line above the change inside that hunk. The declaration alone is not
  # enough: two changes that share a declaration and have identical content
  # normalise to the same entry, so removing occurrence 2 of a duplicated
  # line while the allowlist documents occurrence 1 leaves the multiset
  # unchanged and the guard reports OK. That is a SILENT FALSE PASS, and it
  # is reachable on the real tree -- schema.resolvers.go's duplicated
  # `attribute` import sits under `(file scope)`, where every change in the
  # hunk shares one literal declaration. Reproduced before this line existed.
  # The context line restores just enough position to separate them while
  # still surviving unrelated edits elsewhere in the file, which is why a
  # line number is still not used.
  awk -v rel="${rel}" '
    /^@@/ {
      decl = $0; sub(/^@@[^@]*@@[[:space:]]*/, "", decl)
      if (decl == "") decl = "(file scope)"
      ctx = "(hunk start)"
      next
    }
    /^(---|\+\+\+)/ { next }
    /^ / { ctx = substr($0, 2); gsub(/^[ \t]+|[ \t]+$/, "", ctx); next }
    /^[+-]/ { printf "%s\t%s @ after:%s\t%s\n", rel, decl, ctx, $0 }
  ' "${WORK}/.raw" >>"${actual}"
  # The readable allowlist above is deliberately position-free, and position-free
  # keys CANNOT be unique: generated.go repeats itself heavily, and measuring the
  # real files shows even twenty lines of context still leaves colliding keys.
  # Two review rounds got past a context-only key. So the raw unified diff -- line
  # numbers and all -- is digested as well, and the digest is what makes the check
  # exact. A hand-edit MOVED to an identical-looking position changes the diff and
  # therefore the digest, even when every readable entry is unchanged.
  cat "${WORK}/.raw" >>"${WORK}/.rawall"
done
sort -o "${actual}" "${actual}"
: >>"${WORK}/.rawall"
if ! actual_digest="$(sha256sum <"${WORK}/.rawall" | cut -d" " -f1)"; then
  echo "check_gqlgen_drift: could not digest the raw diff; the comparison is broken, not empty." >&2
  exit 1
fi

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
    printf '# Format: <file><TAB><preceding decl @ after:context><TAB><+|-><line content>. %s\n' "'-' is in the checked-in file and"
    echo "# not in a fresh generation; '+' is the reverse."
    cat "${actual}"
  } >"${ALLOWLIST}"
  if ! written="$(wc -l <"${actual}")"; then
    echo "check_gqlgen_drift: rewrote the allowlist but could not count it; treat the count as unknown, not zero." >&2
    exit 1
  fi
  printf '%s\n' "${actual_digest}" >"${DIGEST}"
  echo "check_gqlgen_drift: allowlist rewritten with ${written} entries; digest ${actual_digest}."
  echo "check_gqlgen_drift: REVIEW THE DIFF -- it is a contract."
  exit 0
fi

if [ ! -f "${ALLOWLIST}" ]; then
  echo "check_gqlgen_drift: ${ALLOWLIST} is missing. Create it with --update." >&2
  exit 1
fi

expected="${WORK}/.expected"
grep -v '^#' "${ALLOWLIST}" | grep -v '^[[:space:]]*$' | sort >"${expected}"

if [ ! -f "${DIGEST}" ]; then
  echo "check_gqlgen_drift: ${DIGEST} is missing. Create it with --update." >&2
  exit 1
fi
expected_digest="$(tr -d "[:space:]" <"${DIGEST}")"
if [ "${expected_digest}" != "${actual_digest}" ] && diff -q "${expected}" "${actual}" >/dev/null; then
  echo "check_gqlgen_drift: the documented drift LINES are unchanged, but the raw diff is not." >&2
  echo "  expected digest ${expected_digest}" >&2
  echo "  actual   digest ${actual_digest}" >&2
  echo >&2
  echo "A hand-edit has MOVED: same content, same context, different position. The" >&2
  echo "readable allowlist cannot see that -- generated.go repeats itself, so a" >&2
  echo "position-free key is not unique -- which is why the digest exists." >&2
  echo "If the move is deliberate: ci/check_gqlgen_drift.sh --update." >&2
  exit 1
fi

if [ "${expected_digest}" = "${actual_digest}" ] && diff -q "${expected}" "${actual}" >/dev/null; then
  # Counted only to report it, but an unreadable count still means the file
  # this guard just blessed could not be read -- never print a blank one.
  if ! documented="$(wc -l <"${actual}")"; then
    echo "check_gqlgen_drift: the drift matched but the count could not be read; not reporting a blank total." >&2
    exit 1
  fi
  echo "check_gqlgen_drift: OK. ${documented} drift lines, all documented."
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
