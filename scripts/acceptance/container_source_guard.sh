#!/usr/bin/env bash
# CHAOS-3572: the host-checkout <-> container-served-source identity guard,
# generalized from the mint-only version #1582 added
# (mint_ask_dev_world_snapshot.sh) so every ORDINARY acceptance-stack boot
# entrypoint gets the same protection, from ONE place.
#
# Mechanism (see CHAOS-3572 for the full writeup): compose is launched with
# `--project-directory <ops_root>`, and the api service bind-mounts that
# directory at /app. The container therefore reads whichever worktree
# BOOTED the stack, live -- not the checkout a later command happens to be
# invoked from. Several lanes each hold their own worktree on this machine
# (the normal state, not an edge case), so a stack booted by one lane keeps
# serving that lane's source after being handed to another. Nothing else
# reports this: `docker ps` shows a healthy stack, the API answers, tests
# pass -- the only symptom is that results describe code nobody is looking
# at.
#
# Usage: source this file, then call
#   container_source_guard_check <ops_root> <compose_arg>...
# where <compose_arg>... is the SAME base compose invocation array the
# caller uses for its other `docker compose` calls (e.g. `"${compose[@]}"`
# as built from `docker compose --project-name ... --project-directory ...
# -f ... -f ... --profile ...`), WITHOUT a subcommand -- this function
# appends `exec -T api ...` (and `ps -q api`) itself.
#
# Returns (does not exit -- the caller decides, same convention as any other
# boolean-ish shell function):
#   0  the container's source signature matches this checkout's.
#   70 mismatch -- REFUSING is printed with both paths and the remedy.
#      (Same code the mint guard, #1582, uses for the identical failure --
#      a caller distinguishing "the guard refused" from any other boot
#      failure does not need a second, different code to check.)
#
# macOS ships bash 3.2: no `${array[@]@Q}` (bash 5 only, the first version of
# the mint guard died on this with "bad substitution" -- a guard that cannot
# run is worse than none, because it reads as coverage) and no nameref
# locals (`declare -n`, bash 4.3+). The compose command is therefore passed
# POSITIONALLY ("$@"), never by name, and the file list is passed to the
# container-side python interpreter as a single newline-delimited argument,
# never interpolated through bash array quoting.

# The source signature: sha256 over a set of files, computed on the host and
# again inside the container, then hashed together. A signature rather than
# probing one symbol, so ANY drift between the checkouts is caught, not just
# drift in one already-known-interesting module. dev_health_ops/__init__.py
# anchors the set to the same installed package the container-side snippet
# resolves `dev_health_ops.__file__` against; the rest are the CHAOS-3544
# fixture-generation files the mint guard already covers.
CONTAINER_SOURCE_GUARD_FILES="dev_health_ops/__init__.py
dev_health_ops/fixtures/generators/interactions.py
dev_health_ops/fixtures/ttl_horizon.py
dev_health_ops/fixtures/world_snapshot.py
dev_health_ops/fixtures/world.py"

container_source_guard_check() {
  local ops_root="$1"
  shift
  local -a compose
  compose=("$@")

  echo "boot: verifying the api container is serving this checkout" >&2

  local host_signature
  host_signature="$(
    for rel in ${CONTAINER_SOURCE_GUARD_FILES}; do
      shasum -a 256 "${ops_root}/src/${rel}"
    done | awk '{print $1}' | shasum -a 256 | awk '{print $1}'
  )"

  local container_signature
  container_signature="$(
    "${compose[@]}" exec -T api python -c "
import hashlib, pathlib, sys
import dev_health_ops
root = pathlib.Path(dev_health_ops.__file__).resolve().parent.parent
parts = [
    hashlib.sha256((root / rel).read_bytes()).hexdigest()
    for rel in sys.argv[1].split()
]
print(hashlib.sha256(('\n'.join(parts) + '\n').encode()).hexdigest())
" "${CONTAINER_SOURCE_GUARD_FILES}" | tr -d '\r'
  )"

  if [[ "${host_signature}" == "${container_signature}" ]]; then
    echo "boot: api container matches this checkout (${host_signature:0:12})" >&2
    return 0
  fi

  # Mismatch: name BOTH paths, not just the signatures -- an operator hitting
  # this needs to know which worktree the container is actually bound to, the
  # same fact CHAOS-3572's own reproduction used (`docker inspect ...
  # --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{end}}'`).
  # Best-effort: if the container is gone or `docker inspect` fails, the
  # refusal still fires on the signature mismatch alone.
  local container_id served_from
  container_id="$("${compose[@]}" ps -q api 2>/dev/null || true)"
  served_from=""
  if [[ -n "${container_id}" ]]; then
    served_from="$(
      docker inspect "${container_id}" \
        --format '{{range .Mounts}}{{if eq .Destination "/app"}}{{.Source}}{{end}}{{end}}' \
        2>/dev/null || true
    )"
  fi

  echo "boot: REFUSING -- the api container is not serving this checkout." >&2
  echo "boot:   host      ${host_signature}" >&2
  echo "boot:   container ${container_signature}" >&2
  echo "boot:   this checkout   ${ops_root}" >&2
  echo "boot:   container /app  ${served_from:-<could not determine -- see docker inspect above>}" >&2
  echo "boot: compose bind-mounts --project-directory at /app, so a stack booted from" >&2
  echo "boot: a DIFFERENT worktree keeps serving that worktree's source even after" >&2
  echo "boot: being handed to this one. Tear it down and boot fresh from this checkout." >&2
  return 70
}

# Test-only direct-invocation entry point, mirroring ci/local_validate.sh's
# `--ch-probe-only` precedent: `bash container_source_guard.sh <ops_root>
# <compose_arg>...` drives the real function above through a stubbed
# `docker` on PATH, with no caller script (and no docker daemon) required.
# Never reached when this file is merely sourced by a boot entrypoint.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  container_source_guard_check "$@"
  exit $?
fi
