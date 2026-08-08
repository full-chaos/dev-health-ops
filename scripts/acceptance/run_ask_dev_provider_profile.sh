#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: run_ask_dev_provider_profile.sh --profile lmstudio-local|ollama-local|ollama-cloud

Optional environment:
  ASK_DEV_PROVIDER_MODEL      Exact loaded/cloud model identifier.
  ASK_DEV_PROVIDER_BASE_URL   OpenAI-compatible /v1 base URL as seen by Compose.
  ASK_DEV_ACCEPTANCE_PYTHON   Python with this checkout installed (default: .venv).
  OLLAMA_API_KEY              Required only for ollama-cloud; supply from a secret.
EOF
}

if [[ "${1:-}" != "--profile" || -z "${2:-}" || "$#" -ne 2 ]]; then
  usage
  exit 64
fi

profile="$2"
case "${profile}" in
  lmstudio-local)
    provider="local"
    model="${ASK_DEV_PROVIDER_MODEL:-google/gemma-4-e4b}"
    base_url="${ASK_DEV_PROVIDER_BASE_URL:-http://host.docker.internal:1234/v1}"
    local_base_url="${base_url}"
    local_model="${model}"
    ollama_base_url=""
    ollama_model=""
    ollama_api_key=""
    ;;
  ollama-local)
    provider="ollama"
    model="${ASK_DEV_PROVIDER_MODEL:-}"
    base_url="${ASK_DEV_PROVIDER_BASE_URL:-http://host.docker.internal:11434/v1}"
    local_base_url=""
    local_model=""
    ollama_base_url="${base_url}"
    ollama_model="${model}"
    ollama_api_key=""
    ;;
  ollama-cloud)
    provider="ollama"
    model="${ASK_DEV_PROVIDER_MODEL:-}"
    base_url="${ASK_DEV_PROVIDER_BASE_URL:-https://ollama.com/v1}"
    local_base_url=""
    local_model=""
    ollama_base_url="${base_url}"
    ollama_model="${model}"
    ollama_api_key="${OLLAMA_API_KEY:-}"
    if [[ -z "${ollama_api_key}" ]]; then
      echo "OLLAMA_API_KEY is required for the opt-in ollama-cloud profile" >&2
      exit 64
    fi
    ;;
  *)
    usage
    exit 64
    ;;
esac

if [[ -z "${model}" ]]; then
  echo "ASK_DEV_PROVIDER_MODEL is required for ${profile}" >&2
  exit 64
fi
if [[ "${base_url}" != */v1 && "${base_url}" != */v1/ ]]; then
  echo "ASK_DEV_PROVIDER_BASE_URL must identify an OpenAI-compatible /v1 endpoint" >&2
  exit 64
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ops_root="$(cd -- "${script_dir}/../.." && pwd)"
python_bin="${ASK_DEV_ACCEPTANCE_PYTHON:-${ops_root}/.venv/bin/python}"
if [[ ! -x "${python_bin}" ]]; then
  echo "ASK_DEV_ACCEPTANCE_PYTHON must identify an executable project Python" >&2
  exit 64
fi
compose_file="${ops_root}/compose.yml"
profile_compose_file="${ops_root}/tests/acceptance/compose.ask-dev-provider-profile.yml"
project_name="dev-health-ask-dev-${profile}"
fixture_org_id="0a155cab-8833-42ac-a4ef-0d121725a7b0"

export ASK_DEV_PROFILE_PROVIDER="${provider}"
export ASK_DEV_PROFILE_MODEL="${model}"
export ASK_DEV_PROFILE_BASE_URL="${base_url}"
export ASK_DEV_PROFILE_LOCAL_BASE_URL="${local_base_url}"
export ASK_DEV_PROFILE_LOCAL_MODEL="${local_model}"
export ASK_DEV_PROFILE_OLLAMA_BASE_URL="${ollama_base_url}"
export ASK_DEV_PROFILE_OLLAMA_MODEL="${ollama_model}"
export ASK_DEV_PROFILE_OLLAMA_API_KEY="${ollama_api_key}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-provider-profile-unused}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${compose_file}"
  -f "${profile_compose_file}"
)

report_failure() {
  status=$?
  if [[ "${status}" -ne 0 ]]; then
    echo "Ask Dev ${profile} acceptance failed; retained containers and recent API logs follow." >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=200 api >&2 || true
  fi
  exit "${status}"
}
trap report_failure EXIT

"${compose[@]}" config --quiet
"${compose[@]}" down --volumes --remove-orphans
"${compose[@]}" up -d --build --wait \
  postgres pgbouncer clickhouse valkey migrate api

# CHAOS-3572: same wrong-worktree guard run_ask_dev_compose.sh runs -- see
# container_source_guard.sh. compose bind-mounts --project-directory at
# /app, so this stack could just as easily be serving a different checkout's
# source as the main acceptance launcher's stack can; nothing about being a
# provider-profile boot instead exempts it.
# shellcheck source=container_source_guard.sh
source "${script_dir}/container_source_guard.sh"
container_source_guard_check "${ops_root}" "${compose[@]}"

"${compose[@]}" exec -T api dev-hops fixtures generate \
  --sink clickhouse://ch:ch@clickhouse:8123/default \
  --org "${fixture_org_id}" \
  --repo-name meridian/web-app \
  --repo-count 1 \
  --days 28 \
  --commits-per-day 2 \
  --pr-count 10 \
  --team-count 3 \
  --seed 3200 \
  --with-metrics \
  --with-work-graph

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18081 \
ASK_DEV_ACCEPTANCE_EXPECTED_MODEL="${model}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
PYTHONPATH="${ops_root}/src:${ops_root}" \
  "${python_bin}" \
  "${ops_root}/scripts/acceptance/prepare_ask_dev_acceptance.py"

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18081 \
ASK_DEV_ACCEPTANCE_EXPECTED_PROVIDER="${provider}" \
ASK_DEV_ACCEPTANCE_EXPECTED_MODEL="${model}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
PYTHONPATH="${ops_root}/src:${ops_root}" \
  "${python_bin}" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_provider_profile.py"

"${compose[@]}" down --volumes --remove-orphans
trap - EXIT
echo "Ask Dev ${profile} provider-profile acceptance completed successfully."
