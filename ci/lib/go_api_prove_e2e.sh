# shellcheck shell=bash
# go-api-prove end to end, on the bearer path the tools pod uses.
#
# SOURCED by ci/run_live_backend_e2e.sh, never executed. Functions only, in
# the same caller-set-globals style as ci/lib/go_worker_fixture.sh.
#
# What it proves. go-api-prove runs against a real query-api, whose own
# /graphql is the product edge (CHAOS-6263: no Python process is involved),
# with NOTHING hand-minted: the proof envelope comes from mint-envelope and
# the edge access token from mint-edge-token, both signed locally from keys
# in their own environment, the edge token for the dedicated proof service
# principal. The prover runs in Go-edge mode (-go-edge), measuring through
# /graphql itself (query-api serves every registered operation: no routing row
# decides it, CHAOS-8702). Then the edge's own per-request checks
# run against that principal, with the registered document: a bumped
# token_version refuses the old token and a fresh mint is accepted, and an
# inactive principal refuses the token while the minter refuses to mint.
#
# Reads: ROOT_DIR, BIN_DIR, TMP_DIR, POSTGRES_URI, CLICKHOUSE_URI_NATIVE,
# E2E_ORG_ID, JWT_SECRET_KEY, QUERY_API_PORT, READINESS_ATTEMPTS,
# READINESS_SLEEP_SECS, EXIT_FAILURE. Sets QUERY_API_PID (the caller declares
# it and stops it with stop_service). Requires run_python and
# wait_for_http_ready to be defined.
#
# CHAOS-6241: query_api_e2e_start/query_api_e2e_mint_envelope_token are also
# called directly by the caller (ci/run_live_backend_e2e.sh) BEFORE
# run_go_api_prove_e2e, to validate the two REST routes (/api/v1/meta,
# /api/v1/home) query-api now serves against the SAME query-api process
# run_go_api_prove_e2e's GraphQL proof reuses -- one build, one process, one
# port.

GO_API_PROVE_E2E_OPERATION="featureFlags"
# Must match internal/edgetokenmint.ProvePrincipalID.
GO_API_PROVE_E2E_PRINCIPAL_ID="00000000-0000-4000-8000-00000000e0e1"

# Set by query_api_e2e_start; read by it, by run_go_api_prove_e2e, and by
# query_api_e2e_mint_envelope_token.
GO_API_PROVE_E2E_DIR=""
GO_API_PROVE_E2E_ENVELOPE_PEM=""
GO_API_PROVE_E2E_JWKS=""

# The fixture program that checks the migrated proof service principal row,
# grants it an org membership, and then mutates it. The users row itself comes
# from the application schema migration, never from this fixture. A shell string written to a file, not a here-document, for
# the pipe-buffer reason run_live_backend_e2e.sh documents.
GO_API_PROVE_E2E_PRINCIPAL_PROGRAM='import os, sys, uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

action, principal_id = sys.argv[1], sys.argv[2]
engine = create_engine(os.environ["POSTGRES_URI"].replace("+asyncpg", "", 1))
now = datetime.now(timezone.utc)
with engine.begin() as conn:
    if action == "assert-migrated":
        row = conn.execute(text(
            "SELECT auth_provider, password_hash IS NULL, is_active, is_superuser, token_version,"
            " (SELECT count(*) FROM memberships WHERE user_id = users.id)"
            " FROM users WHERE id = :id"
        ), {"id": principal_id}).one_or_none()
        if row is None:
            sys.exit("the migration did not create the proof service principal row")
        if tuple(row) != ("service", True, True, False, 0, 0):
            sys.exit("the migrated proof service principal row has an unexpected shape: " + repr(tuple(row)))
    elif action == "grant-membership":
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

# The request the edge checks send: the measured operation's REGISTERED
# document, from the same documents.json the prover reads, with the variables
# its proof request carries (internal/goapiproof/operations.go). query-api's
# /graphql serves registered documents only, so any other text (the old
# `{ __typename }` probe) is a 404 whatever the token is.
GO_API_PROVE_E2E_EDGE_REQUEST_PROGRAM='import json, sys

documents, operation, org = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
texts = [entry["document"] for entry in documents if entry["operation"] == operation]
if len(texts) != 1:
    sys.exit("documents.json does not carry exactly one document for " + operation)
variables = {"featureFlags": {"orgId": org, "provider": None, "project": None, "includeArchived": False, "limit": 100}}
if operation not in variables:
    sys.exit("no request variables for " + operation)
json.dump({"query": texts[0], "variables": variables[operation]}, sys.stdout)
'

# go_api_prove_e2e_edge_status TOKEN_FILE -- prints the HTTP status query-api's
# /graphql answers the registered document with, and leaves the response body
# (edge-response.json) and headers (edge-response.headers) for the caller. The
# token reaches curl in a header FILE, never on its command line.
go_api_prove_e2e_edge_status() {
  local token_file="$1" header_file="${GO_API_PROVE_E2E_DIR}/edge-header"
  { printf 'Authorization: Bearer '; tr -d '\r\n' < "${token_file}"; printf '\n'; } > "${header_file}"
  curl -sS -o "${GO_API_PROVE_E2E_DIR}/edge-response.json" -D "${GO_API_PROVE_E2E_DIR}/edge-response.headers" -w '%{http_code}' \
    -H @"${header_file}" -H 'Content-Type: application/json' \
    --data @"${GO_API_PROVE_E2E_DIR}/edge-request.json" "http://127.0.0.1:${QUERY_API_PORT}/graphql"
  rm -f "${header_file}"
}

# go_api_prove_e2e_edge_served -- the last edge answer is the operation SERVED
# by query-api: its own root in the data, no GraphQL error, and the plane
# header naming go. A 200 that carries an error (an operation not enabled, a
# refused variable) is not a served answer.
go_api_prove_e2e_edge_served() {
  grep -q "\"data\":{\"${GO_API_PROVE_E2E_OPERATION}\":" "${GO_API_PROVE_E2E_DIR}/edge-response.json" \
    && ! grep -q '"errors"' "${GO_API_PROVE_E2E_DIR}/edge-response.json" \
    && grep -qi '^x-dev-health-plane: go' "${GO_API_PROVE_E2E_DIR}/edge-response.headers"
}

go_api_prove_e2e_mint_edge_token() {
  POSTGRES_URI="$(go_api_prove_e2e_pgx_uri)" "${BIN_DIR}/dho" mint edge-token -org "${E2E_ORG_ID}"
}

# query_api_e2e_start builds and boots the single query-api process both the
# CHAOS-6241 REST checks (below) and run_go_api_prove_e2e's own GraphQL proof
# share -- one build, one process, one port, rather than each starting its
# own. Sets the caller-declared QUERY_API_PID global (read by cleanup(),
# stopped with stop_service) and the module globals GO_API_PROVE_E2E_DIR /
# GO_API_PROVE_E2E_ENVELOPE_PEM other functions in this file and the caller
# reuse.
#
# GO_API_HOME_ENABLED/GO_API_META_ENABLED (both default OFF in production --
# see internal/queryapi/server/home_route.go / meta_route.go) are turned on here: these
# two REST routes are Go-served in prod, and CHAOS-6241 deleted their Python
# bodies (main.py's home()/meta() now raise GoServedRouteUnavailableError
# unconditionally) -- the live-e2e pipeline-proof for them has to run
# against query-api, or it silently stops measuring anything for these two
# paths the moment the Python body is gone.
query_api_e2e_start() {
  local dir commit pgx_uri
  dir="${TMP_DIR}/go-api-prove"
  GO_API_PROVE_E2E_DIR="${dir}"
  mkdir -p "${dir}/artifacts"
  chmod 700 "${dir}"
  GO_API_PROVE_E2E_ENVELOPE_PEM="${dir}/envelope-keys/private/envelope-private-key.pem"
  GO_API_PROVE_E2E_JWKS="${dir}/envelope-keys/public/envelope-jwks.json"

  echo "==> [query-api] building dho (query-api, goapi prove, mint envelope, mint edge-token)"
  # query-api refuses to identify an unstamped or modified build, and
  # `dho goapi prove` refuses to measure a candidate built from another
  # commit than its own, so dho is stamped the way the image build stamps
  # it: -buildvcs=false and the commit through -ldflags. The one dho build
  # serves `dho query-api` and covers mint-envelope (needed immediately
  # below), go-api-prove and mint-edge-token too, so the candidate and the
  # prover are the same build and run_go_api_prove_e2e (below) builds
  # nothing of its own.
  commit="${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
  go build -buildvcs=false -ldflags "-X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${commit}" \
    -o "${BIN_DIR}/dho" ./cmd/dho
  go run ./cmd/registrydump -file internal/queryapi/server/query_route.go > "${dir}/documents.json"

  echo "==> [query-api] generating a throwaway envelope key pair"
  "${BIN_DIR}/dho" mint envelope-keys -dir "${dir}/envelope-keys" > /dev/null

  echo "==> [query-api] starting query-api on :${QUERY_API_PORT} (REST + measurement route)"
  set -m
  (
    export CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"
    export GO_API_REGISTRY_POSTGRES_URI="${pgx_uri}"
    export GO_API_ENVELOPE_JWKS_PATH="${GO_API_PROVE_E2E_JWKS}"
    export GO_API_ENVELOPE_ISSUER="dev-health-ops-edge"
    export GO_API_ENVELOPE_AUDIENCE="query-api"
    export QUERY_API_ADDR="127.0.0.1:${QUERY_API_PORT}"
    # /graphql is mounted only with the edge access-token key: the same key
    # mint-edge-token signs with, as in the deployed query-api.
    export GO_API_EDGE_JWT_SECRET="${JWT_SECRET_KEY}"
    export DEV_HEALTH_ENV="ci"
    export GO_API_PROOF_ROUTE_ENABLED="true"
    export GO_API_HOME_ENABLED="true"
    export GO_API_META_ENABLED="true"
    exec "${BIN_DIR}/dho" query-api
  ) > "${dir}/query-api.log" 2>&1 &
  # Read by the caller's cleanup, which stops it with stop_service.
  # shellcheck disable=SC2034
  QUERY_API_PID="$!"
  set +m
  wait_for_http_ready "query-api" "http://127.0.0.1:${QUERY_API_PORT}/readyz" "${dir}/query-api.log" QUERY_API_PID
}

# query_api_e2e_mint_envelope_token ORG_ID -- prints an envelope bearer
# token minted with query_api_e2e_start's throwaway key, the same auth
# query-api's REST routes (home_route.go's doc comment: "the same
# bearer-envelope verifier every other REST route in this binary uses")
# and run_go_api_prove_e2e's own dho goapi prove invocation mints
# in process, from the same key.
query_api_e2e_mint_envelope_token() {
  "${BIN_DIR}/dho" mint envelope -org "$1" -key-file "${GO_API_PROVE_E2E_ENVELOPE_PEM}"
}

# go_api_prove_e2e_prove LOG -- runs dho goapi prove in Go-edge mode against
# the running query-api (its /registry, /buildinfo, /query/proof and its own
# /graphql as the edge), writes the output to LOG and prints it. Returns the
# command's exit status.
go_api_prove_e2e_prove() {
  local log="$1" rc query_api="http://127.0.0.1:${QUERY_API_PORT}"
  set +e
  (
    cd "${ROOT_DIR}"
    unset GO_API_PROVE_PROOF_BEARER
    POSTGRES_URI="$(go_api_prove_e2e_pgx_uri)" GO_API_ENVELOPE_PRIVATE_KEY="$(cat "${GO_API_PROVE_E2E_ENVELOPE_PEM}")" \
      "${BIN_DIR}/dho" goapi prove \
      -go-edge \
      -registry-url "${query_api}/registry" \
      -buildinfo-url "${query_api}/buildinfo" \
      -proof-url "${query_api}/query/proof" \
      -edge-url "${query_api}/graphql" \
      -documents "${GO_API_PROVE_E2E_DIR}/documents.json" \
      -org "${E2E_ORG_ID}" \
      -artifact-dir "${GO_API_PROVE_E2E_DIR}/artifacts" \
      -recorded-by live-e2e \
      -review-evidence "live-e2e: go-api-prove -go-edge with the envelope and the edge access token minted in process"
  ) > "${log}" 2>&1
  rc=$?
  set -e
  cat "${log}"
  return "${rc}"
}

# go_api_prove_e2e_assert_run LOG COMMIT -- what every prove run here must say.
go_api_prove_e2e_assert_run() {
  local log="$1" commit="$2"
  grep -Fq "go-api-prove: edge_mode=go (Go-edge:" "${log}" \
    || go_api_prove_e2e_fail "the prover did not report Go-edge mode"
  grep -Fq "go-api-prove: prover_build=${commit} prover_build_modified=false candidate_build=${commit} prover_build_skew=false prover_build_skew_allowed=false" "${log}" \
    || go_api_prove_e2e_fail "the prover did not report its own build equal to the candidate's"
  grep -Eq '^go-api-prove: attempted=[0-9]+ admitted=[0-9]+ executed=[1-9][0-9]* ' "${log}" \
    || go_api_prove_e2e_fail "no operation was executed"
  grep -Eq '^go-api-prove:   edge access token mints = [1-9][0-9]*$' "${log}" \
    || go_api_prove_e2e_fail "the edge access token was not minted in process"
  grep -Eq '^go-api-prove:   envelope mints = [1-9][0-9]*$' "${log}" \
    || go_api_prove_e2e_fail "the envelope was not minted in process"
}

run_go_api_prove_e2e() {
  local dir commit prove_log rc status
  dir="${GO_API_PROVE_E2E_DIR}"
  printf '%s' "${GO_API_PROVE_E2E_PRINCIPAL_PROGRAM}" > "${dir}/principal.py"
  printf '%s' "${GO_API_PROVE_E2E_EDGE_REQUEST_PROGRAM}" > "${dir}/edge-request.py"

  # dho (goapi prove, mint edge-token) is already built, by
  # query_api_e2e_start -- nothing to build here.
  commit="${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
  go run ./cmd/registrydump -file internal/queryapi/server/query_route.go > "${dir}/documents.json"

  run_python "${dir}/edge-request.py" "${dir}/documents.json" "${GO_API_PROVE_E2E_OPERATION}" "${E2E_ORG_ID}" > "${dir}/edge-request.json" \
    || go_api_prove_e2e_fail "could not build the registered ${GO_API_PROVE_E2E_OPERATION} request"

  echo "==> [go-api-prove e2e] the migration created the proof service principal, with no membership"
  go_api_prove_e2e_principal assert-migrated || go_api_prove_e2e_fail "the proof service principal row is not the migrated one"
  set +e
  go_api_prove_e2e_mint_edge_token > "${dir}/token-no-membership" 2> "${dir}/mint-no-membership.err"
  rc=$?
  set -e
  if [ "${rc}" -eq 0 ] || [ -s "${dir}/token-no-membership" ]; then
    go_api_prove_e2e_fail "mint-edge-token minted for a principal with no membership in the org"
  fi
  grep -q "no membership in this org" "${dir}/mint-no-membership.err" \
    || go_api_prove_e2e_fail "mint-edge-token did not refuse the unmembered principal by name"

  echo "==> [go-api-prove e2e] granting the proof service principal a viewer membership"
  go_api_prove_e2e_principal grant-membership || go_api_prove_e2e_fail "could not grant the proof service principal a membership"

  # query-api serves every registered operation and no routing row decides it (CHAOS-8702): the candidate goes
  # through /graphql itself, the path a real client takes.
  echo "==> [go-api-prove e2e] running dho goapi prove -go-edge through /graphql with both bearers minted in-process"
  prove_log="${dir}/go-api-prove-edge.log"
  go_api_prove_e2e_prove "${prove_log}" || go_api_prove_e2e_fail "go-api-prove exited $?"
  grep -Eq "^go-api-prove:   ${GO_API_PROVE_E2E_OPERATION} +mode=canary +route=edge +PROVEN_GO_ONLY \(go-edge mode" "${prove_log}" \
    || go_api_prove_e2e_fail "${GO_API_PROVE_E2E_OPERATION} was not proven through /graphql in Go-edge mode"
  grep -Eq '^go-api-prove:   build binding per_request = [1-9][0-9]*$' "${prove_log}" \
    || go_api_prove_e2e_fail "the /graphql answer was not bound to the serving build"
  go_api_prove_e2e_assert_run "${prove_log}" "${commit}"

  echo "==> [go-api-prove e2e] the edge re-checks the principal row on every request"
  (umask 077 && go_api_prove_e2e_mint_edge_token > "${dir}/token-v0") || go_api_prove_e2e_fail "mint-edge-token refused a provisioned principal"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v0")"
  [ "${status}" = "200" ] || go_api_prove_e2e_fail "the edge answered ${status} to a fresh minted token, want 200"
  go_api_prove_e2e_edge_served || go_api_prove_e2e_fail "the edge answered 200 to a fresh minted token but did not serve ${GO_API_PROVE_E2E_OPERATION} from plane go"

  go_api_prove_e2e_principal bump-token-version || go_api_prove_e2e_fail "could not bump token_version"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v0")"
  [ "${status}" = "401" ] || go_api_prove_e2e_fail "the edge answered ${status} to a token minted before a token_version bump, want 401"
  (umask 077 && go_api_prove_e2e_mint_edge_token > "${dir}/token-v1") || go_api_prove_e2e_fail "mint-edge-token refused after a token_version bump"
  status="$(go_api_prove_e2e_edge_status "${dir}/token-v1")"
  [ "${status}" = "200" ] || go_api_prove_e2e_fail "the edge answered ${status} to a token minted after the bump, want 200"
  go_api_prove_e2e_edge_served || go_api_prove_e2e_fail "the edge answered 200 to a token minted after the bump but did not serve ${GO_API_PROVE_E2E_OPERATION} from plane go"

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
