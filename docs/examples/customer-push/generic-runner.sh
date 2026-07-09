#!/usr/bin/env sh
# FullChaos customer-push from ANY runner (portable POSIX sh, raw REST).
#
# No GitHub/GitLab assumptions -- works in a bare Docker container, a Jenkins
# shell step, a Buildkite job, or a laptop. Only `curl` and `jq` are required.
#
# Provide the batch envelope as $1 (a JSON file) and set:
#   FULLCHAOS_API_URL       e.g. https://app.fullchaos.example
#   FULLCHAOS_INGEST_TOKEN  fcpush_... token: schema:read + ingest:write + ingest:status
# The token belongs in the runner's secret store / an env file with mode 0600 --
# never commit it, and never pass a provider credential to FullChaos.
#
# Usage: FULLCHAOS_API_URL=... FULLCHAOS_INGEST_TOKEN=... ./generic-runner.sh batch.json
set -eu

batch="${1:?usage: generic-runner.sh <batch-envelope.json>}"
: "${FULLCHAOS_API_URL:?set FULLCHAOS_API_URL}"
: "${FULLCHAOS_INGEST_TOKEN:?set FULLCHAOS_INGEST_TOKEN}"
auth="Authorization: Bearer ${FULLCHAOS_INGEST_TOKEN}"
base="${FULLCHAOS_API_URL}/api/v1/external-ingest"

# 1. Validate (scope schema:read) -- no enqueue; abort on valid:false.
resp=$(curl -sS -X POST "${base}/validate" -H "${auth}" \
  -H "Content-Type: application/json" --data @"${batch}")
echo "validate: ${resp}"
echo "${resp}" | jq -e '.valid == true' >/dev/null

# 2. Submit (scope ingest:write) -- 202 Accepted returns ingestionId.
resp=$(curl -sS -X POST "${base}/batches" -H "${auth}" \
  -H "Content-Type: application/json" --data @"${batch}")
echo "submit: ${resp}"
id=$(echo "${resp}" | jq -r '.ingestionId')

# 3. Poll status (scope ingest:status) until terminal.
i=0
while [ "${i}" -lt 60 ]; do
  body=$(curl -sS "${base}/batches/${id}" -H "${auth}")
  status=$(echo "${body}" | jq -r '.status')
  echo "status: ${status}"
  case "${status}" in
    completed) echo "${body}" | jq .; exit 0 ;;
    partial)
      # Batch processed but some records were REJECTED. Fail by default so CI
      # surfaces the dropped data (mirrors `dev-hops push`, which is non-zero
      # unless completed with zero rejections); set ALLOW_PARTIAL=1 to accept.
      echo "${body}" | jq .
      [ "${ALLOW_PARTIAL:-0}" = "1" ] && exit 0
      echo "batch 'partial': records were rejected (see errors above)" >&2
      exit 1 ;;
    failed) echo "${body}" | jq .; exit 1 ;;
  esac
  i=$((i + 1))
  sleep 5
done
echo "timed out waiting for terminal status" >&2
exit 1
