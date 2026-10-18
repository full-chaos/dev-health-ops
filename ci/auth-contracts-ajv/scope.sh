#!/usr/bin/env bash
# Decide whether the ajv corpus check must run. Writes run=true|false to
# $GITHUB_OUTPUT. Every command failure fails the step: a failed `git diff`
# must never read as "nothing changed", because that would skip the whole check
# and report a green job.
set -euo pipefail

event="${EVENT:?EVENT is required}"
run=true
if [ "$event" = "pull_request" ]; then
  base="${BASE_SHA:?BASE_SHA is required on pull_request}"
  head="${HEAD_SHA:?HEAD_SHA is required on pull_request}"
  changed="$(git diff --name-only "$base...$head")"
  if ! grep -Eq '^(contracts/auth/v1/|ci/auth-contracts-ajv/|\.github/workflows/auth-contracts-ajv\.yml$)' <<<"$changed"; then
    run=false
  fi
fi
echo "run=$run" >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
echo "ajv corpus check required: $run"
