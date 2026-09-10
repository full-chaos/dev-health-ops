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
  restore_area || true
  restore_modules || true
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

# REFUSE, DO NOT RECOVER, when the config points outside the protected area.
#
# Round r7 pointed exec.filename at a pre-existing file at the repo root. The
# wrapper restored its area, announced "generated files restored to their
# pre-run content", and left that file overwritten -- a true statement about
# the area and a false one about the tree. Recovery cannot be stretched to
# cover the whole repository, so the answer is to not start: every output path
# the config names must resolve inside the generated root, checked BEFORE
# generation, or this exits without running the generator at all.
#
# Parsed with python3 + PyYAML rather than grep, because a key's value can be
# quoted, folded, or nested and a regex over YAML is its own defect class.
# The reader is written to a file with the printf BUILTIN, not fed in on a
# heredoc: bash writes a heredoc into a pipe whose read end it also holds, so
# a payload over the host's effective pipe budget hangs the script forever
# (CHAOS-3362). This repo has a test for exactly that, and it caught this.
OUTPUTS_PY="${SNAPSHOT}/read_outputs.py"
{
  printf '%s\n' 'import sys'
  printf '%s\n' 'try:'
  printf '%s\n' '    import yaml'
  printf '%s\n' 'except ImportError:'
  printf '%s\n' '    print("ERROR: PyYAML unavailable; cannot verify output paths")'
  printf '%s\n' '    sys.exit(3)'
  printf '%s\n' 'with open("gqlgen.yml", encoding="utf-8") as handle:'
  printf '%s\n' '    cfg = yaml.safe_load(handle) or {}'
  printf '%s\n' 'paths = []'
  printf '%s\n' 'for section in ("exec", "model", "federation"):'
  printf '%s\n' '    value = cfg.get(section) or {}'
  printf '%s\n' '    if isinstance(value, dict) and value.get("filename"):'
  printf '%s\n' '        paths.append(value["filename"])'
  printf '%s\n' 'resolver = cfg.get("resolver") or {}'
  printf '%s\n' 'if isinstance(resolver, dict):'
  printf '%s\n' '    if resolver.get("filename"):'
  printf '%s\n' '        paths.append(resolver["filename"])'
  printf '%s\n' '    if resolver.get("dir"):'
  printf '%s\n' '        paths.append(resolver["dir"].rstrip("/") + "/")'
  printf '%s\n' 'for path in paths:'
  printf '%s\n' '    print(path)'
} >"${OUTPUTS_PY}"
config_outputs="$(cd "${QUERY_API}" && python3 "${OUTPUTS_PY}")" || {
  echo "gqlgen_generate: could not read the generator's output paths from gqlgen.yml." >&2
  echo "${config_outputs}" >&2
  exit 2
}
if printf '%s\n' "${config_outputs}" | grep -q '^ERROR:'; then
  printf '%s\n' "${config_outputs}" >&2
  exit 2
fi
outside=0
while IFS= read -r out; do
  [ -n "${out}" ] || continue
  abs="$(cd "${QUERY_API}" && readlink -m "${out}")"
  case "${abs}/" in
    "${ROOT}/cmd/query-api/internal/graph/"*) ;;
    *)
      echo "gqlgen_generate: REFUSING -- gqlgen.yml points an output outside the protected area:" >&2
      echo "    ${out}  ->  ${abs#"${ROOT}/"}" >&2
      outside=1
      ;;
  esac
done <<EOF
${config_outputs}
EOF
if [ "${outside}" -ne 0 ]; then
  echo "  Only cmd/query-api/internal/graph is snapshotted and restored. Generation" >&2
  echo "  into any other path can destroy a hand-written file this script cannot" >&2
  echo "  put back, so it does not run at all rather than half-recover afterwards." >&2
  exit 2
fi

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
# THE UNIT OF PROTECTION IS THE AREA, NOT A FILE LIST.
#
# An earlier version of this script snapshotted three named files and compared
# directory file SETS. Review round r6 broke both halves at once: pointing
# exec.filename at the existing, hand-written internal/graph/resolver.go made
# gqlgen OVERWRITE it, and a set comparison cannot see a content change to a
# file that already existed -- so restore put the three listed files back,
# reported "restored to their pre-run content", and left resolver.go destroyed
# with the build broken. A nested output escaped entirely, because the scan was
# maxdepth 1.
#
# So everything under the generated root is snapshotted BY CONTENT, whether or
# not this script generates it. gqlgen can be pointed at any path in here;
# protecting only the paths it is pointed at today means the next config key
# re-opens the hole. resolver.go and telemetry.go are hand-written and are
# protected for exactly that reason.
GENERATED_ROOT="${ROOT}/cmd/query-api/internal/graph"
list_generated_area() {
  [ -d "${GENERATED_ROOT}" ] || return 0
  ( cd "${GENERATED_ROOT}" && find . -type f -name '*.go' -print ) | sort
}
# The module files are snapshotted with the area: running the generator runs
# the go tool, which tidies them. r7 measured 92 checksum lines removed on a
# pristine tree. A failed run must not leave that tidy behind either.
MODULE_FILES=(go.mod go.sum)
for m in "${MODULE_FILES[@]}"; do
  [ -f "${ROOT}/${m}" ] && cp -p "${ROOT}/${m}" "${SNAPSHOT}/${m}"
done
restore_modules() {
  local m rc=0
  for m in "${MODULE_FILES[@]}"; do
    [ -f "${SNAPSHOT}/${m}" ] || continue
    if ! cmp -s "${SNAPSHOT}/${m}" "${ROOT}/${m}"; then
      if cp -p "${SNAPSHOT}/${m}" "${ROOT}/${m}"; then
        echo "gqlgen_generate: restored ${m} (the go tool had tidied it)." >&2
      else
        echo "gqlgen_generate: RESTORE FAILED for ${m} -- recover with: git checkout -- ${m}" >&2
        rc=1
      fi
    fi
  done
  return "${rc}"
}

list_generated_area >"${SNAPSHOT}/.files-before"
mkdir -p "${SNAPSHOT}/area"
while IFS= read -r rel; do
  [ -n "${rel}" ] || continue
  mkdir -p "${SNAPSHOT}/area/$(dirname "${rel}")"
  cp -p "${GENERATED_ROOT}/${rel}" "${SNAPSHOT}/area/${rel}"
done <"${SNAPSHOT}/.files-before"

purge_unexpected() {
  # Files CREATED by the failed run are removed; files that predate it are
  # never removed, only restored. Deleting someone else's file would be a
  # worse failure than the one being recovered from.
  local f rc=0
  list_generated_area >"${SNAPSHOT}/.files-after" 2>/dev/null || return 0
  while IFS= read -r f; do
    [ -n "${f}" ] || continue
    echo "gqlgen_generate: removing ${f#./}, created by the failed run and not present before it." >&2
    rm -f "${GENERATED_ROOT}/${f}" || rc=1
  done < <(comm -13 "${SNAPSHOT}/.files-before" "${SNAPSHOT}/.files-after")
  return "${rc}"
}

restore_area() {
  # Restores CONTENT, not just presence. This is what a file-set comparison
  # could not do: an overwritten resolver.go still exists, so it looked fine.
  local rel rc=0
  while IFS= read -r rel; do
    [ -n "${rel}" ] || continue
    if ! cmp -s "${SNAPSHOT}/area/${rel}" "${GENERATED_ROOT}/${rel}" 2>/dev/null; then
      mkdir -p "$(dirname "${GENERATED_ROOT}/${rel}")"
      if cp -p "${SNAPSHOT}/area/${rel}" "${GENERATED_ROOT}/${rel}"; then
        echo "gqlgen_generate: restored ${rel#./} (the run had modified or removed it)." >&2
      else
        echo "gqlgen_generate: RESTORE FAILED for ${rel#./} -- recover with: git checkout -- cmd/query-api/internal/graph" >&2
        rc=1
      fi
    fi
  done <"${SNAPSHOT}/.files-before"
  return "${rc}"
}

# Injectable ONLY so this script's own recovery behaviour is testable. Round
# r6 found that nothing executed this wrapper at all, so an `exit 0` stub,
# a disabled restore, removed signal traps and a disabled missing-output check
# all survived the test suite. A fake generator makes each of those a
# millisecond test instead of a 10s real generation. The wiring test asserts
# CI sets neither this nor the guard's equivalent.
GENERATE_CMD="${GQLGEN_GENERATE_CMD:-go run github.com/99designs/gqlgen generate --config gqlgen.yml}"
set +e
( cd "${QUERY_API}" && eval "${GENERATE_CMD}" )
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
  restore_area || exit 1
  restore_modules || exit 1
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
    restore_area || exit 1
    purge_unexpected || exit 1
    echo "gqlgen_generate: generated files restored; treat the success as false." >&2
    exit 1
  fi
done

# Generation succeeded, but if it wrote a file this script does not track then
# the config and this list have diverged, and every later run -- including the
# drift guard -- is blind to that file. Fail here, where the cause is obvious.
list_generated_area >"${SNAPSHOT}/.files-after"
comm -13 "${SNAPSHOT}/.files-before" "${SNAPSHOT}/.files-after" >"${SNAPSHOT}/.new"
# A CHANGE to a file this script does not generate is the same defect as a new
# one: gqlgen was pointed somewhere it should not be. Checked by content, since
# an overwritten file is still present and a set comparison sees nothing.
: >"${SNAPSHOT}/.touched"
while IFS= read -r rel; do
  [ -n "${rel}" ] || continue
  case " ${GENERATED_FILES[*]} " in *" cmd/query-api/internal/graph/${rel#./} "*) continue ;; esac
  cmp -s "${SNAPSHOT}/area/${rel}" "${GENERATED_ROOT}/${rel}" || printf '%s\n' "${rel#./}" >>"${SNAPSHOT}/.touched"
done <"${SNAPSHOT}/.files-before"
if [ -s "${SNAPSHOT}/.new" ] || [ -s "${SNAPSHOT}/.touched" ]; then
  echo "gqlgen_generate: generation wrote outside the files this script tracks:" >&2
  sed 's|^\./|  created: |' "${SNAPSHOT}/.new" >&2
  sed 's|^|  overwritten: |' "${SNAPSHOT}/.touched" >&2
  echo "  Add them to GENERATED_FILES here AND in ci/check_gqlgen_drift.sh, or" >&2
  echo "  fix the gqlgen.yml stanza that put them there. They are left in place" >&2
  echo "  because generation SUCCEEDED -- this is a scope mismatch, not a crash." >&2
  exit 1
fi

echo "gqlgen_generate: OK. Review the diff -- it is a contract, not a build artefact."
