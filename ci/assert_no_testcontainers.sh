#!/usr/bin/env bash
# Fail loudly when this job's docker daemon already holds testcontainers.
set -uo pipefail
if ! ids="$(docker ps -aq --filter label=org.testcontainers=true)"; then
  echo "assert: docker ps failed; cannot prove an empty daemon" >&2
  exit 1
fi
if [ -n "${ids}" ]; then
  echo "assert: testcontainers already running at job start: $(printf '%s' "${ids}" | tr '\n' ' ')" >&2
  exit 1
fi
echo "assert: no testcontainers at job start"
