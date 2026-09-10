#!/usr/bin/env bash
# Regenerate cmd/query-api's gqlgen layer WITHOUT the destructive failure mode
# (CHAOS-5489).
#
# `gqlgen generate` deletes its output files before regenerating them and does
# NOT put them back when the run fails. Measured on an unmodified tree: the
# command removed generated.go (95,609 lines) and models_gen.go (3,771), then
# exited 1 with an EMPTY error message -- no cause, no partial output, nothing
# to grep. Whoever ran it was left with a deleted Go GraphQL layer and no
# indication of why, and `git checkout --` was the only recovery.
#
# gqlgen's CLI offers nothing to avoid this: `generate` accepts only --verbose
# and --config, and codegen/config/config.go has no output-directory, dry-run
# or restore option. So the safety belongs here.
#
# This wrapper snapshots the generated files by CONTENT, runs the generator,
# and restores them on any failure. It is the command documented in
# cmd/query-api/README.md; the raw `go run` line it replaced is unsafe.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
QUERY_API="${ROOT}/cmd/query-api"

# Every file gqlgen owns, relative to ROOT. Kept in sync with gqlgen.yml's
# exec/model/resolver stanzas. An omission here used to pass silently: the
# claim that "the drift guard fails if generation writes anything outside
# this set" was FALSE -- the guard only ever diffed the three listed files,
# so pointing resolver.filename at a new path produced an unlisted file that
# neither script noticed, and the build broke on a redeclared Resolver. Both
# scripts now enumerate the generated directories and treat an unexpected
# file as a failure, so the claim is enforced instead of asserted.
GENERATED_FILES=(
  "cmd/query-api/internal/graph/generated.go"
  "cmd/query-api/internal/graph/model/models_gen.go"
  "cmd/query-api/internal/graph/schema.resolvers.go"
)

SNAPSHOT="$(mktemp -d "${TMPDIR:-/tmp}/gqlgen-snapshot-XXXXXX")"

restore() {
  # Restore is best-effort by necessity -- we are already on a failure path --
  # but it must never report success it did not achieve. Each copy is checked,
  # and a failure to restore is louder than the failure that caused it, since
  # it is the difference between "the run failed" and "your tree is broken".
  local failed=0 rel
  for rel in "${GENERATED_FILES[@]}"; do
    [ -f "${SNAPSHOT}/$(basename "${rel}")" ] || continue
    if ! cp -p "${SNAPSHOT}/$(basename "${rel}")" "${ROOT}/${rel}"; then
      echo "gqlgen_generate: RESTORE FAILED for ${rel} -- recover with: git checkout -- ${rel}" >&2
      failed=1
    fi
  done
  if [ "${failed}" -ne 0 ]; then
    echo "gqlgen_generate: the working tree may be INCOMPLETE. Do not commit until 'git status' is clean or deliberate." >&2
    return 1
  fi
  return 0
}

cleanup() { rm -rf "${SNAPSHOT}"; }

# Ctrl-C or a CI cancellation lands mid-generation, which is exactly when the
# output files are already deleted. With only an EXIT trap the snapshot was
# removed WITHOUT restoring first, so a cancelled run left the tree missing
# both generated files and the copies needed to put them back. Signals are
# trapped ahead of EXIT so recovery runs before the snapshot is discarded.
on_signal() {
  local sig="$1"
  echo "gqlgen_generate: ${sig} received mid-run; restoring before exit." >&2
  restore || true
  purge_unexpected || true
  cleanup
  trap - EXIT
  case "${sig}" in
    INT) exit 130 ;;
    TERM) exit 143 ;;
    *) exit 129 ;;
  esac
}
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM
trap 'on_signal HUP' HUP
trap cleanup EXIT

missing=0
for rel in "${GENERATED_FILES[@]}"; do
  if [ ! -f "${ROOT}/${rel}" ]; then
    echo "gqlgen_generate: REFUSING -- ${rel} is already missing before generation." >&2
    echo "  A previous failed run probably deleted it. Restore the tree first:" >&2
    echo "    git checkout -- ${rel}" >&2
    missing=1
  fi
done
[ "${missing}" -eq 0 ] || exit 2

for rel in "${GENERATED_FILES[@]}"; do
  cp -p "${ROOT}/${rel}" "${SNAPSHOT}/$(basename "${rel}")"
done

# gqlgen writes wherever its config points. A resolver/model stanza aimed at a
# new path creates a file this script never listed, and restoring the listed
# three then leaves that orphan behind -- a redeclared Resolver that breaks the
# build, from a command whose whole purpose is to be safe to run. Record the
# directory contents so anything new can be identified and removed.
GENERATED_DIRS=()
for rel in "${GENERATED_FILES[@]}"; do
  GENERATED_DIRS+=("$(dirname "${ROOT}/${rel}")")
done
list_generated_dirs() {
  local d
  for d in $(printf '%s\n' "${GENERATED_DIRS[@]}" | sort -u); do
    [ -d "${d}" ] || continue
    find "${d}" -maxdepth 1 -type f -name '*.go' -print
  done | sort
}
list_generated_dirs >"${SNAPSHOT}/.files-before"

purge_unexpected() {
  # Only ever removes files that did NOT exist before this run. A file that
  # predates the run is someone else's, and is left alone even if it is
  # unexpected -- deleting it would be a worse failure than the one being
  # recovered from.
  local f rc=0
  list_generated_dirs >"${SNAPSHOT}/.files-after" 2>/dev/null || return 0
  while IFS= read -r f; do
    [ -n "${f}" ] || continue
    echo "gqlgen_generate: removing ${f#"${ROOT}/"}, created by the failed run and not a tracked output." >&2
    rm -f "${f}" || rc=1
  done < <(comm -13 "${SNAPSHOT}/.files-before" "${SNAPSHOT}/.files-after")
  return "${rc}"
}

set +e
( cd "${QUERY_API}" && go run github.com/99designs/gqlgen generate --config gqlgen.yml )
rc=$?
set -e

# rc is read on the line immediately after the command. Reading it after an
# `if`/`&&`/`||` yields the STATUS OF THAT CONSTRUCT, not the generator's --
# a trap this repo has hit before and the reason a failing run once reported
# success.
if [ "${rc}" -ne 0 ]; then
  echo "gqlgen_generate: generation FAILED (exit ${rc})." >&2
  echo "  gqlgen deletes its output files before regenerating and does not" >&2
  echo "  restore them on failure, so they were reverted from a snapshot." >&2
  echo "  Note gqlgen can exit non-zero with NO message at all; an empty" >&2
  echo "  error above is its own known behaviour, not a lost log." >&2
  restore || exit 1
  purge_unexpected || exit 1
  echo "gqlgen_generate: generated files restored to their pre-run content." >&2
  exit "${rc}"
fi

# A zero exit that left a file missing is the shape that reads as a clean run
# and is not one -- exactly what the empty-error failure looks like from
# outside. It fails here rather than downstream in a build nobody connects
# back to this command.
for rel in "${GENERATED_FILES[@]}"; do
  if [ ! -f "${ROOT}/${rel}" ]; then
    echo "gqlgen_generate: generation reported success but ${rel} is MISSING." >&2
    restore || exit 1
    echo "gqlgen_generate: generated files restored; treat the success as false." >&2
    exit 1
  fi
done

# Generation succeeded, but if it wrote a file this script does not track then
# the config and this list have diverged, and every later run -- including the
# drift guard -- is blind to that file. Fail here, where the cause is obvious.
list_generated_dirs >"${SNAPSHOT}/.files-after"
if [ -s "$(comm -13 "${SNAPSHOT}/.files-before" "${SNAPSHOT}/.files-after" >"${SNAPSHOT}/.new"; echo "${SNAPSHOT}/.new")" ]; then
  echo "gqlgen_generate: generation wrote files this script does not track:" >&2
  sed "s|^${ROOT}/|  |" "${SNAPSHOT}/.new" >&2
  echo "  Add them to GENERATED_FILES here AND in ci/check_gqlgen_drift.sh, or" >&2
  echo "  fix the gqlgen.yml stanza that put them there. They are left in place" >&2
  echo "  because generation SUCCEEDED -- this is a scope mismatch, not a crash." >&2
  exit 1
fi

echo "gqlgen_generate: OK. Review the diff -- it is a contract, not a build artefact."
