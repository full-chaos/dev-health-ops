# shellcheck shell=bash
# go-api-prove end to end, on the bearer path the tools pod uses.
#
# SOURCED by ci/run_live_backend_e2e.sh, never executed. Functions only, in
# the same caller-set-globals style as ci/lib/go_worker_fixture.sh.
#
# What it proves. go-api-prove runs against a real query-api and the real
# Python edge the caller already started, with NOTHING hand-minted: the
# proof envelope comes from mint-envelope and the edge access token from
# mint-edge-token, both signed locally from keys in their own environment,
# the edge token for the dedicated proof service principal. Then the edge's
# own per-request checks run against that principal: a bumped token_version
# refuses the old token and a fresh mint is accepted, and an inactive
# principal refuses the token while the minter refuses to mint.
#
# Reads: ROOT_DIR, BIN_DIR, TMP_DIR, BASE_URL, POSTGRES_URI,
# CLICKHOUSE_URI_NATIVE, E2E_ORG_ID, JWT_SECRET_KEY, QUERY_API_PORT,
# READINESS_ATTEMPTS, READINESS_SLEEP_SECS, EXIT_FAILURE. Sets QUERY_API_PID
# (the caller declares it and stops it with stop_service). Requires
# run_python and wait_for_http_ready to be defined.

GO_API_PROVE_E2E_OPERATION="featureFlags"
# Must match internal/edgetokenmint.ProvePrincipalID.
GO_API_PROVE_E2E_PRINCIPAL_ID="00000000-0000-4000-8000-00000000e0e1"

# The fixture program that provisions and then mutates the proof service
# principal row. A shell string written to a file, not a here-document, for
# the pipe-buffer reason run_live_backend_e2e.sh documents.
GO_API_PROVE_E2E_PRINCIPAL_PROGRAM='import os, sys, uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

action, principal_id = sys.argv[1], sys.argv[2]
engine = create_engine(os.environ["POSTGRES_URI"].replace("+asyncpg", "", 1))
now = datetime.now(timezone.utc)
with engine.begin() as conn:
    if action == "seed":
        conn.execute(text(
            "INSERT INTO users (id, email, is_active, is_verified, is_superuser, auth_provider, token_version, created_at, updated_at)"
            " VALUES (:id, :email, true, false, false, :provider, 0, :now, :now) ON CONFLICT (id) DO NOTHING"
        ), {"id": principal_id, "email": "go-api-prove@service.dev-health.invalid", "provider": "service", "now": now})
        conn.execute(text(
            "INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at)"
            " VALUES (:mid, :uid, :oid, :role, :now, :now) ON CONFLICT DO NOTHING"
        ), {"mid": str(uuid.uuid4()), "uid": principal_id, "oid": os.environ["E2E_ORG_ID"], "role": "viewer", "now": now})
    elif action == "bump-token-version":
        conn.execute(text("UPDATE users SET token_version = token_version + 1 WHERE id = :id"), {"id": principal_id})
    elif action == "deactivate":
        conn.execute(text("UPDATE users SET is_active = false WHERE id = :id"), {"id": principal_id})
    else:
        sys.exit("unknown action " + action)
engine.dispose()
'

GO_API_PROVE_E2E_JWKS_PROGRAM='import json
from dev_health_ops.api.graphql.principal_envelope import build_envelope_jwks
print(json.dumps(build_envelope_jwks()))
'

go_api_prove_e2e_fail() {
  echo "ERROR: go-api-prove e2e: $1"
  exit "${EXIT_FAILURE}"
}

# The Go binaries read POSTGRES_URI with pgx, which does not know the
# SQLAlchemy driver suffix the Python side uses.
go_api_prove_e2e_pgx_uri() {
  printf '%s' "${POSTGRES_URI/+asyncpg/}"
}

# E2E_ORG_ID is a plain shell variable in the caller, not an exported one,
# so it is handed to the program explicitly.
go_api_prove_e2e_principal() {
  E2E_ORG_ID="${E2E_ORG_ID}" run_python "${GO_API_PROVE_E2E_DIR}/principal.py" "$1" "${GO_API_PROVE_E2E_PRINCIPAL_ID}"
}

# go_api_prove_e2e_edge_status TOKEN_FILE -- prints the HTTP status the edge
# answers an authenticated GraphQL request with. The token reaches curl in a
# header FILE, never on its command line.
go_api_prove_e2e_edge_status() {
  local token_file="$1" header_file="${GO_API_PROVE_E2E_DIR}/edge-header"
  { printf 'Authorization: Bearer '; tr -d '\r\n' < "${token_file}"; printf '\n'; } > "${header_file}"
  curl -sS -o "${GO_API_PROVE_E2E_DIR}/edge-response.json" -w '%{http_code}' \
    -H @"${header_file}" -H 'Content-Type: application/json' \
    --data '{"query":"{ __typename }"}' "${BASE_URL}/graphql"
  rm -f "${header_file}"
}

go_api_prove_e2e_mint_edge_token() {
  POSTGRES_URI="$(go_api_prove_e2e_pgx_uri)" "${BIN_DIR}/mint-edge-token" -org "${E2E_ORG_ID}"
}

run_go_api_prove_e2e() {
  local dir commit prove_log rc status pgx_uri
  dir="${TMP_DIR}/go-api-prove"
  GO_API_PROVE_E2E_DIR="${dir}"
  mkdir -p "${dir}/artifacts"
  chmod 700 "${dir}"
  pgx_uri="$(go_api_prove_e2e_pgx_uri)"
  printf '%s' "${GO_API_PROVE_E2E_PRINCIPAL_PROGRAM}" > "${dir}/principal.py"
  printf '%s' "${GO_API_PROVE_E2E_JWKS_PROGRAM}" > "${dir}/jwks.py"

  echo "==> [go-api-prove e2e] building query-api, go-api-routing, go-api-prove, mint-envelope, mint-edge-token"
  # query-api refuses to identify an unstamped or modified build, so the
  # commit is stamped the way the image build stamps it: -buildvcs=false and
  # the commit through -ldflags.
  commit="${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
  go build -buildvcs=false -ldflags "-X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${commit}" \
    -o "${BIN_DIR}/query-api" ./cmd/query-api
  go build -o "${BIN_DIR}/go-api-routing" ./cmd/go-api-routing
  go build -o "${BIN_DIR}/go-api-prove" ./cmd/go-api-prove
  go build -o "${BIN_DIR}/mint-envelope" ./cmd/mint-envelope
  go build -o "${BIN_DIR}/mint-edge-token" ./cmd/mint-edge-token
  go run ./cmd/query-api/tools/registrydump -file cmd/query-api/query_route.go > "${dir}/documents.json"

  echo "==> [go-api-prove e2e] generating a throwaway envelope key pair"
  (umask 077 && openssl genpkey -algorithm ed25519 -out "${dir}/envelope.pem")
  GO_API_ENVELOPE_PRIVATE_KEY="$(cat "${dir}/envelope.pem")" run_python "${dir}/jwks.py" > "${dir}/jwks.json"

  echo "==> [go-api-prove e2e] starting query-api on :${QUERY_API_PORT} with the measurement route"
  set -m
  (
    export CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"
    export GO_API_REGISTRY_POSTGRES_URI="${pgx_uri}"
    export GO_API_ENVELOPE_JWKS_PATH="${dir}/jwks.json"
    export GO_API_ENVELOPE_ISSUER="dev-health-ops-edge"
    export GO_API_ENVELOPE_AUDIENCE="query-api"
    export QUERY_API_ADDR="127.0.0.1:${QUERY_API_PORT}"
    export DEV_HEALTH_ENV="ci"
    export GO_API_PROOF_ROUTE_ENABLED="true"
    exec "${BIN_DIR}/query-api"
  ) > "${dir}/query-api.log" 2>&1 &
  # Read by the caller's cleanup, which stops it with stop_service.
  # shellcheck disable=SC2034
  QUERY_API_PID="$!"
  set +m
  wait_for_http_ready "query-api" "http://127.0.0.1:${QUERY_API_PORT}/readyz" "${dir}/query-api.log" QUERY_API_PID

  echo "==> [go-api-prove e2e] routing ${GO_API_PROVE_E2E_OPERATION} to shadow at the running build"
  local query_api="http://127.0.0.1:${QUERY_API_PORT}"
  (
    local bearer
    cd "${ROOT_DIR}"
    bearer="$("${BIN_DIR}/mint-envelope" -org "${E2E_ORG_ID}" -key-file "${dir}/envelope.pem")" &&
    POSTGRES_URI="${pgx_uri}" GO_API_ROUTING_BEARER="${bearer}" "${BIN_DIR}/go-api-routing" enable -operations "${GO_API_PROVE_E2E_OPERATION}" -mode canary \
      -acknowledge-unproven -registry-url "${query_api}/registry" -buildinfo-url "${query_api}/buildinfo" \
      -recorded-by live-e2e -review-evidence "live-e2e: row for the go-api-prove bearer-path run" &&
    POSTGRES_URI="${pgx_uri}" GO_API_ROUTING_BEARER="${bearer}" "${BIN_DIR}/go-api-routing" disable -operations "${GO_API_PROVE_E2E_OPERATION}" -mode shadow -apply \
      -recorded-by live-e2e -review-evidence "live-e2e: measure through the proof route, not the edge dispatcher"
  ) || go_api_prove_e2e_fail "could not route ${GO_API_PROVE_E2E_OPERATION} to shadow"

  echo "==> [go-api-prove e2e] provisioning the proof service principal"
  go_api_prove_e2e_principal seed || go_api_prove_e2e_fail "could not provision the proof service principal"

  echo "==> [go-api-prove e2e] running go-api-prove with both bearers minted in-process"
  prove_log="${dir}/go-api-prove.log"
  set +e
  (
    cd "${ROOT_DIR}"
    unset GO_API_PROVE_BEARER GO_API_PROVE_PROOF_BEARER
    POSTGRES_URI="${pgx_uri}" "${BIN_DIR}/go-api-prove" \
      -registry-url "${query_api}/registry" \
      -buildinfo-url "${query_api}/buildinfo" \
      -proof-url "${query_api}/query/proof" \
      -edge-url "${BASE_URL}/graphql" \
      -documents "${dir}/documents.json" \
      -org "${E2E_ORG_ID}" \
      -artifact-dir "${dir}/artifacts" \
      -recorded-by live-e2e \
      -review-evidence "live-e2e: go-api-prove with the envelope and the edge access token minted by the tools-image helpers" \
      -proof-bearer-exec "[\"${BIN_DIR}/mint-envelope\",\"-org\",\"${E2E_ORG_ID}\",\"-key-file\",\"${dir}/envelope.pem\"]" \
      -edge-bearer-exec "[\"${BIN_DIR}/mint-edge-token\",\"-org\",\"${E2E_ORG_ID}\"]"
  ) > "${prove_log}" 2>&1
  rc=$?
  set -e
  cat "${prove_log}"
  [ "${rc}" -eq 0 ] || go_api_prove_e2e_fail "go-api-prove exited ${rc}"
  grep -Eq "^go-api-prove:   ${GO_API_PROVE_E2E_OPERATION} +mode=shadow +route=proof " "${prove_log}" \
    || go_api_prove_e2e_fail "${GO_API_PROVE_E2E_OPERATION} was not executed through the proof route"
  grep -Eq '^go-api-prove: attempted=[0-9]+ admitted=[0-9]+ executed=[1-9][0-9]* ' "${prove_log}" \
    || go_api_prove_e2e_fail "no operation was executed"
  grep -Eq '^go-api-prove:   edge access token mints = [1-9][0-9]*$' "${prove_log}" \
    || go_api_prove_e2e_fail "the edge access token was not minted by -edge-bearer-exec"
  grep -Eq '^go-api-prove:   envelope mints = [1-9][0-9]*$' "${prove_log}" \
    || go_api_prove_e2e_fail "the envelope was not minted by -proof-bearer-exec"

  echo "==> [go-api-prove e2e] the edge re-checks the principal row on every request"
  (umask 077 && go_api_prove_e2e_mint_edge_token > "${dir}/token-v0") || go_api_prove_e2e_fail "mint-edge-token refused a provisioned principal"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v0")"
  [ "${status}" = "200" ] || go_api_prove_e2e_fail "the edge answered ${status} to a fresh minted token, want 200"

  go_api_prove_e2e_principal bump-token-version || go_api_prove_e2e_fail "could not bump token_version"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v0")"
  [ "${status}" = "401" ] || go_api_prove_e2e_fail "the edge answered ${status} to a token minted before a token_version bump, want 401"
  (umask 077 && go_api_prove_e2e_mint_edge_token > "${dir}/token-v1") || go_api_prove_e2e_fail "mint-edge-token refused after a token_version bump"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v1")"
  [ "${status}" = "200" ] || go_api_prove_e2e_fail "the edge answered ${status} to a token minted after the bump, want 200"

  go_api_prove_e2e_principal deactivate || go_api_prove_e2e_fail "could not deactivate the principal"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v1")"
  [ "${status}" = "401" ] || go_api_prove_e2e_fail "the edge answered ${status} for an inactive principal, want 401"
  set +e
  go_api_prove_e2e_mint_edge_token > "${dir}/token-inactive" 2> "${dir}/mint-inactive.err"
  rc=$?
  set -e
  if [ "${rc}" -eq 0 ] || [ -s "${dir}/token-inactive" ]; then
    go_api_prove_e2e_fail "mint-edge-token minted for an inactive principal"
  fi
  grep -q "not active" "${dir}/mint-inactive.err" || go_api_prove_e2e_fail "mint-edge-token did not refuse the inactive principal by name"
  rm -f "${dir}"/token-* "${dir}/envelope.pem"
  echo "go-api-prove e2e checks passed."
}
