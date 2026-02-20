#!/usr/bin/env bash
set -euo pipefail

EXIT_MISSING_DEP=3
EXIT_FAILURE=10

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}" || exit "${EXIT_FAILURE}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: Required command '${cmd}' is not available."
    exit "${EXIT_MISSING_DEP}"
  fi
}

run_dev_hops() {
  if command -v dev-hops >/dev/null 2>&1; then
    dev-hops "$@"
    return
  fi
  if command -v poetry >/dev/null 2>&1; then
    poetry run dev-hops "$@"
    return
  fi
  if command -v uv >/dev/null 2>&1; then
    uv run dev-hops "$@"
    return
  fi
  python3 -m dev_health_ops.cli "$@"
}

exec_dev_hops() {
  if command -v dev-hops >/dev/null 2>&1; then
    exec dev-hops "$@"
  fi
  if command -v poetry >/dev/null 2>&1; then
    exec poetry run dev-hops "$@"
  fi
  if command -v uv >/dev/null 2>&1; then
    exec uv run dev-hops "$@"
  fi
  exec python3 -m dev_health_ops.cli "$@"
}

run_python() {
  if command -v python3 >/dev/null 2>&1; then
    python3 "$@"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    python "$@"
    return
  fi
  echo "ERROR: Python runtime not found."
  exit "${EXIT_MISSING_DEP}"
}

require_cmd curl

CLICKHOUSE_URI_DEFAULT="clickhouse://ch:ch@127.0.0.1:8123/default"
POSTGRES_URI_DEFAULT="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/test_db"

CLICKHOUSE_URI="${CLICKHOUSE_URI:-${CLICKHOUSE_URI_DEFAULT}}"
POSTGRES_URI="${POSTGRES_URI:-${POSTGRES_URI_DEFAULT}}"
DATABASE_URI="${POSTGRES_URI}"

API_HOST="${LIVE_E2E_API_HOST:-127.0.0.1}"
API_PORT="${LIVE_E2E_API_PORT:-18080}"
BASE_URL="http://${API_HOST}:${API_PORT}"

FIXTURE_SEED="${LIVE_E2E_FIXTURE_SEED:-20260219}"
FIXTURE_DAYS="${LIVE_E2E_FIXTURE_DAYS:-14}"
FIXTURE_REPO_NAME="${LIVE_E2E_FIXTURE_REPO_NAME:-acme/live-e2e}"
FIXTURE_COMMITS_PER_DAY="${LIVE_E2E_COMMITS_PER_DAY:-6}"
FIXTURE_PR_COUNT="${LIVE_E2E_PR_COUNT:-24}"

READINESS_ATTEMPTS="${LIVE_E2E_READINESS_ATTEMPTS:-90}"
READINESS_SLEEP_SECS="${LIVE_E2E_READINESS_SLEEP_SECS:-2}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/live-backend-e2e.XXXXXX")"
API_LOG_FILE="${LIVE_E2E_API_LOG_FILE:-${TMP_DIR}/api.log}"
API_PID=""

cleanup() {
  local rc=$?
  if [ -n "${API_PID}" ] && kill -0 "${API_PID}" >/dev/null 2>&1; then
    kill "${API_PID}" >/dev/null 2>&1 || true
    wait "${API_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
  return "${rc}"
}

trap cleanup EXIT INT TERM

generate_auth_token() {
  run_python - <<'PY'
import hashlib, jwt, uuid, os
from datetime import datetime, timedelta, timezone

user_id = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
org_id = uuid.UUID("11111111-2222-3333-4444-555555555555")

pg_uri = os.getenv("POSTGRES_URI", os.getenv("DATABASE_URI", ""))
if pg_uri:
    sync_uri = pg_uri.replace("+asyncpg", "", 1)
    from sqlalchemy import create_engine, text
    engine = create_engine(sync_uri)
    from dev_health_ops.models.git import Base
    import dev_health_ops.models.users  # register models
    Base.metadata.create_all(engine, checkfirst=True)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO organizations (id, slug, name, tier, is_active)"
            " VALUES (:id, :slug, :name, :tier, true)"
            " ON CONFLICT (id) DO NOTHING"
        ), {"id": str(org_id), "slug": "e2e-org", "name": "E2E Org", "tier": "enterprise"})
        conn.execute(text(
            "INSERT INTO users (id, email, is_active, is_superuser, auth_provider)"
            " VALUES (:id, :email, true, false, 'local')"
            " ON CONFLICT (id) DO NOTHING"
        ), {"id": str(user_id), "email": "e2e@test.local"})
        conn.execute(text(
            "INSERT INTO memberships (user_id, org_id, role)"
            " VALUES (:uid, :oid, :role)"
            " ON CONFLICT DO NOTHING"
        ), {"uid": str(user_id), "oid": str(org_id), "role": "admin"})
    engine.dispose()

enc_key = os.getenv("SETTINGS_ENCRYPTION_KEY", "dev-key-not-for-prod")
secret = hashlib.sha256(enc_key.encode()).hexdigest()
payload = {
    "sub": str(user_id),
    "email": "e2e@test.local",
    "org_id": str(org_id),
    "role": "admin",
    "is_superuser": False,
    "type": "access",
    "exp": datetime.now(timezone.utc) + timedelta(hours=1),
    "iat": datetime.now(timezone.utc),
    "jti": str(uuid.uuid4()),
}
print(jwt.encode(payload, secret, algorithm="HS256"))
PY
}

fetch_json() {
  local path="$1"
  local out_file="$2"
  local expected_status="$3"
  local extra_headers=()
  if [ -n "${AUTH_TOKEN:-}" ]; then
    extra_headers+=(-H "Authorization: Bearer ${AUTH_TOKEN}")
  fi
  local status
  status="$(
    curl -sS -o "${out_file}" -w "%{http_code}" \
      -H "Accept: application/json" \
      "${extra_headers[@]}" \
      "${BASE_URL}${path}"
  )"
  if [ "${status}" != "${expected_status}" ]; then
    echo "ERROR: ${path} returned HTTP ${status}, expected ${expected_status}."
    cat "${out_file}" || true
    return 1
  fi
}

wait_for_ready() {
  local readiness_file="${TMP_DIR}/health_ready.json"
  local i
  for ((i = 1; i <= READINESS_ATTEMPTS; i++)); do
    local status
    status="$(
      curl -sS -o "${readiness_file}" -w "%{http_code}" \
        -H "Accept: application/json" \
        "${BASE_URL}/health" || true
    )"
    if [ "${status}" = "200" ] && run_python - "${readiness_file}" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
assert payload.get("status") == "ok", payload
services = payload.get("services", {})
assert services.get("clickhouse") == "ok", services
assert services.get("postgres") == "ok", services
PY
    then
      echo "API ready after ${i} attempt(s)."
      return 0
    fi

    if [ -n "${API_PID}" ] && ! kill -0 "${API_PID}" >/dev/null 2>&1; then
      echo "ERROR: API process exited before becoming ready."
      tail -n 200 "${API_LOG_FILE}" || true
      return 1
    fi
    sleep "${READINESS_SLEEP_SECS}"
  done

  echo "ERROR: Timed out waiting for API readiness."
  tail -n 200 "${API_LOG_FILE}" || true
  return 1
}

echo "==> generating deterministic ClickHouse fixtures (metrics + work graph)"
(
  export DISABLE_DOTENV=1
  unset POSTGRES_URI
  unset DATABASE_URI
  unset DATABASE_URL
  run_dev_hops fixtures generate \
    --sink "${CLICKHOUSE_URI}" \
    --db-type clickhouse \
    --repo-name "${FIXTURE_REPO_NAME}" \
    --days "${FIXTURE_DAYS}" \
    --commits-per-day "${FIXTURE_COMMITS_PER_DAY}" \
    --pr-count "${FIXTURE_PR_COUNT}" \
    --seed "${FIXTURE_SEED}" \
    --with-metrics \
    --with-work-graph
)

echo "==> starting API at ${BASE_URL}"
(
  export DISABLE_DOTENV=1
  export DATABASE_URI="${DATABASE_URI}"
  export CLICKHOUSE_URI="${CLICKHOUSE_URI}"
  export POSTGRES_URI="${POSTGRES_URI}"
  exec_dev_hops \
    --db "${POSTGRES_URI}" \
    --analytics-db "${CLICKHOUSE_URI}" \
    api --host "${API_HOST}" --port "${API_PORT}"
) >"${API_LOG_FILE}" 2>&1 &
API_PID="$!"

echo "==> waiting for readiness"
wait_for_ready

echo "==> generating auth token for authenticated endpoints"
AUTH_TOKEN="$(generate_auth_token)"

echo "==> validating /health"
HEALTH_FILE="${TMP_DIR}/health.json"
fetch_json "/health" "${HEALTH_FILE}" "200"
run_python - "${HEALTH_FILE}" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload["status"] == "ok", payload
services = payload.get("services", {})
assert services.get("clickhouse") == "ok", services
assert services.get("postgres") == "ok", services
PY

echo "==> validating /api/v1/meta"
META_FILE="${TMP_DIR}/meta.json"
fetch_json "/api/v1/meta" "${META_FILE}" "200"
run_python - "${META_FILE}" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("backend") == "postgres", payload
assert payload.get("limits", {}).get("max_days") == 365, payload
assert payload.get("limits", {}).get("max_repos") == 1000, payload
supported = payload.get("supported_endpoints", [])
assert "/api/v1/home" in supported, supported
PY

echo "==> validating /api/v1/home"
HOME_FILE="${TMP_DIR}/home.json"
fetch_json "/api/v1/home" "${HOME_FILE}" "200"
run_python - "${HOME_FILE}" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
freshness = payload.get("freshness", {})
assert "last_ingested_at" in freshness, freshness
sources = freshness.get("sources", {})
allowed_states = {"ok", "down", "stale", "unknown", "not_configured", "error"}
for key in ("github", "gitlab", "jira", "ci"):
    assert key in sources, sources
    assert str(sources.get(key, "")).lower() in allowed_states, sources
coverage = freshness.get("coverage", {})
assert float(coverage.get("repos_covered_pct", 0.0)) > 0.0, coverage
deltas = payload.get("deltas", [])
assert len(deltas) >= 1, len(deltas)
for row in deltas:
    assert "metric" in row, row
    assert "value" in row, row
tiles = payload.get("tiles", {})
for key in ("understand", "measure", "align", "execute"):
    assert key in tiles, tiles
constraint = payload.get("constraint", {})
assert constraint.get("title"), constraint
assert constraint.get("evidence"), constraint
PY

echo "Live backend e2e checks passed."
