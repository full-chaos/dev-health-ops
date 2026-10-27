#!/usr/bin/env bash
# Remove, by id, every container that carries ONE testcontainers session label.
# Usage: ci/reap-testcontainers-session.sh SESSION_ID   (or TESTCONTAINERS_SESSION_ID)
# Never a prune and never another session: the label filter is the only selector.
set -euo pipefail
session="${1:-${TESTCONTAINERS_SESSION_ID:-}}"
if ! [[ "${session}" =~ ^[A-Za-z0-9._-]{8,}$ ]]; then
  echo "reap: a session id of 8+ chars [A-Za-z0-9._-] is required" >&2
  exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "reap: no docker on PATH; nothing to reap for session ${session}"
  exit 0
fi
if ! ids="$(docker ps -aq --filter "label=org.testcontainers.sessionId=${session}")"; then
  echo "reap: docker ps failed for session ${session}" >&2
  exit 1
fi
left=0 removed=0 failed=0
for id in ${ids}; do
  left=$((left + 1))
  if docker rm -f "${id}" >/dev/null; then
    removed=$((removed + 1))
  else
    failed=$((failed + 1))
  fi
done
printf 'reap: session=%s left=%d removed=%d failed=%d\n' "${session}" "${left}" "${removed}" "${failed}"
[ "${failed}" -eq 0 ]
