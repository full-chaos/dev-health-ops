#!/usr/bin/env bash
# Remove this job's testcontainers by session label (the pool runs with Ryuk off).
set -uo pipefail
session="${TESTCONTAINERS_SESSION_ID:-}"
if [ -z "${session}" ]; then
  echo "reap: TESTCONTAINERS_SESSION_ID is empty; nothing to remove"
  exit 0
fi
ids="$(docker ps -aq --filter "label=org.testcontainers.sessionId=${session}" 2>/dev/null || true)"
if [ -z "${ids}" ]; then
  echo "reap: no containers for session ${session}"
  exit 0
fi
echo "reap: removing $(printf '%s\n' "${ids}" | wc -l) container(s) for session ${session}"
# shellcheck disable=SC2086
docker rm -f -v ${ids} >/dev/null 2>&1 || true
