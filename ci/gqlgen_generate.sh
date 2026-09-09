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
# exec/model/resolver stanzas; the drift guard fails if generation writes
# anything outside this set, so an omission here cannot pass silently.
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

echo "gqlgen_generate: OK. Review the diff -- it is a contract, not a build artefact."
