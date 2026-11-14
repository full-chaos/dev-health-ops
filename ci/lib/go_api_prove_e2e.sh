# shellcheck shell=bash
# go-api-prove end to end, on the bearer path the tools pod uses.
#
# SOURCED by ci/run_live_backend_e2e.sh, never executed. Functions only, in
# the same caller-set-globals style as ci/lib/go_worker_fixture.sh.
#
# What it proves. go-api-prove runs against a real query-api, whose own
# /graphql is the product edge (CHAOS-6263: no Python process is involved),
# with NOTHING hand-minted: the proof credential is the e2e platform admin's
# user access token in a 0600 file (generate_platform_admin_token,
# ci/lib/e2e_fixture.sh, signed by the CI issuer stub's key), and the edge
# access token comes from mint-edge-token, signed locally from keys in its own
# environment, for the dedicated proof service principal. The prover runs in Go-edge mode (-go-edge), measuring through
# /graphql itself (query-api serves every registered operation: no routing row
# decides it, CHAOS-8702). Then the edge's own per-request checks
# run against that principal, with the registered document: a bumped
# token_version refuses the old token and a fresh mint is accepted, and an
# inactive principal refuses the token while the minter refuses to mint.
#
# Reads: ROOT_DIR, BIN_DIR, TMP_DIR, POSTGRES_URI, CLICKHOUSE_URI_NATIVE,
# E2E_ORG_ID, E2E_ISSUER_URL, E2E_ISSUER_KEY_FILE, E2E_ISSUER_CA_FILE, QUERY_API_PORT, READINESS_ATTEMPTS,
# READINESS_SLEEP_SECS, EXIT_FAILURE. Sets QUERY_API_PID (the caller declares
# it and stops it with stop_service). Requires the cigate and dho binaries in BIN_DIR, e2e_psql and e2e_dho_admin (ci/lib/e2e_fixture.sh) and
# wait_for_http_ready to be defined.
#
# CHAOS-6241: query_api_e2e_start is also
# called directly by the caller (ci/run_live_backend_e2e.sh) BEFORE
# run_go_api_prove_e2e, to validate the two REST routes (/api/v1/meta,
# /api/v1/home) query-api now serves against the SAME query-api process
# run_go_api_prove_e2e's GraphQL proof reuses -- one build, one process, one
# port.

GO_API_PROVE_E2E_OPERATION="featureFlags"
# Must match internal/edgetokenmint.ProvePrincipalID.
GO_API_PROVE_E2E_PRINCIPAL_ID="00000000-0000-4000-8000-00000000e0e1"

# Set by query_api_e2e_start; read by it and by run_go_api_prove_e2e. The envelope key pair stays only because
# query-api still checks its JWKS at start; the prove tools present no envelope.
GO_API_PROVE_E2E_DIR=""
GO_API_PROVE_E2E_ENVELOPE_PEM=""
GO_API_PROVE_E2E_JWKS=""

go_api_prove_e2e_fail() {
  echo "ERROR: go-api-prove e2e: $1"
  exit "${EXIT_FAILURE}"
}

# The Go binaries read POSTGRES_URI with pgx, which does not know the
# SQLAlchemy driver suffix the Python side uses.
go_api_prove_e2e_pgx_uri() {
  printf '%s' "${POSTGRES_URI/+asyncpg/}"
}

# The proof service principal fixture, through the dho admin verbs of the caller (e2e_dho_admin) where one exists (the
# membership is a raw write: the dho membership verbs go through auth-service), and
# a plain SELECT for the shape check (e2e_psql of the caller). The users row itself comes from the application schema
# migration, never from this fixture. E2E_ORG_ID is a fixed uuid. The one raw write left is bump-token-version: no
# dho verb exists for advancing token_version alone (`users delete` also ends the login, `users update --password`
# sets a password hash on a service principal).
go_api_prove_e2e_principal() {
  local id="${GO_API_PROVE_E2E_PRINCIPAL_ID}" shape session_id
  case "$1" in
    assert-migrated)
      shape="$(e2e_psql --field-separator=, -c "SELECT auth_provider, password_hash IS NULL, is_active, is_superuser, token_version,
        (SELECT count(*) FROM memberships WHERE user_id = users.id) FROM users WHERE id = '${id}'")" || return 1
      if [ -z "${shape}" ]; then
        echo "the migration did not create the proof service principal row"
        return 1
      fi
      if [ "${shape}" != "service,t,t,f,0,0" ]; then
        echo "the migrated proof service principal row has an unexpected shape: ${shape}"
        return 1
      fi
      ;;
    grant-membership)
      # A raw write: the dho membership verbs send the change to auth-service (CHAOS-11799), and this job runs none.
      e2e_psql -c "
        UPDATE memberships SET role = 'viewer', updated_at = now() WHERE org_id = '${E2E_ORG_ID}' AND user_id = '${id}';
        INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
          SELECT gen_random_uuid(), '${E2E_ORG_ID}', '${id}', 'viewer', now(), now(), now()
          WHERE NOT EXISTS (SELECT 1 FROM memberships WHERE org_id = '${E2E_ORG_ID}' AND user_id = '${id}');" >/dev/null
      ;;
    seed-session)
      # An access token is accepted only for a live session of its principal, pinned to the principal's token_version
      # (auth-service writes it at login; no login runs for a service principal). dho mint edge-token reads this row
      # and never writes it, so the fixture does: once after the membership, again after each token_version bump.
      session_id="$(e2e_psql -c "
        INSERT INTO auth.principals (id, kind, display_name) VALUES ('${id}', 'user', 'go-api-prove') ON CONFLICT (id) DO NOTHING;
        INSERT INTO auth.sessions (principal_id, token_hash, expires_at, legacy_token_version)
          SELECT id, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), now() + interval '2 hours', token_version
            FROM public.users WHERE id = '${id}'
          RETURNING id;" | tail -n 1)" || return 1
      case "${session_id}" in
        *[!0-9a-fA-F-]* | "")
          echo "could not read the proof principal session id"
          return 1
          ;;
      esac
      export EDGE_TOKEN_SESSION_ID="${session_id}"
      ;;
    bump-token-version)
      e2e_psql -c "UPDATE users SET token_version = token_version + 1 WHERE id = '${id}'" >/dev/null
      ;;
    deactivate)
      e2e_dho_admin users update --id "${id}" --no-active >/dev/null
      ;;
    *)
      echo "unknown action $1"
      return 1
      ;;
  esac
}

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

# The EdDSA signer the edge-token mint (dho mint edge-token and the in-process mint of dho goapi prove) reads: the CI
# issuer stub's key (ci/run_live_backend_e2e.sh), signing for the issuer both services verify against.
go_api_prove_e2e_signer_env() {
  export EDGE_TOKEN_SIGNING_KEY_FILE="${E2E_ISSUER_KEY_FILE}"
  export EDGE_TOKEN_KID="${E2E_ISSUER_KID:-ci-e2e-1}"
  export AUTH_ACCESS_TOKEN_ISSUER="${E2E_ISSUER_URL}"
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
  pgx_uri="$(go_api_prove_e2e_pgx_uri)"
  dir="${TMP_DIR}/go-api-prove"
  GO_API_PROVE_E2E_DIR="${dir}"
  mkdir -p "${dir}/artifacts"
  chmod 700 "${dir}"
  GO_API_PROVE_E2E_ENVELOPE_PEM="${dir}/envelope-keys/private/envelope-private-key.pem"
  GO_API_PROVE_E2E_JWKS="${dir}/envelope-keys/public/envelope-jwks.json"

  echo "==> [query-api] building dho (query-api, goapi prove, mint envelope-keys, mint edge-token)"
  # query-api refuses to identify an unstamped or modified build, and
  # `dho goapi prove` refuses to measure a candidate built from another
  # commit than its own, so dho is stamped the way the image build stamps
  # it: -buildvcs=false and the commit through -ldflags. The one dho build
  # serves `dho query-api` and covers mint envelope-keys (needed immediately
  # below), go-api-prove and mint-edge-token too, so the candidate and the
  # prover are the same build and run_go_api_prove_e2e (below) builds
  # nothing of its own.
  commit="${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
  go build -buildvcs=false -trimpath -ldflags "-X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${commit}" \
    -o "${BIN_DIR}/dho" ./cmd/dho
  go run -trimpath ./cmd/registrydump -file internal/queryapi/server/query_route.go > "${dir}/documents.json"

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
    # /graphql verifies the edge access token against the CI issuer stub (ci/run_live_backend_e2e.sh), as the
    # deployed query-api verifies against its issuer; the stub's CA is trusted for this process only.
    export AUTH_ACCESS_TOKEN_ISSUER="${E2E_ISSUER_URL}"
    export SSL_CERT_FILE="${E2E_ISSUER_CA_FILE}"
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

# go_api_prove_e2e_prove LOG -- runs dho goapi prove in Go-edge mode against
# the running query-api (its /registry, /buildinfo, /query/proof and its own
# /graphql as the edge), writes the output to LOG and prints it. Returns the
# command's exit status.
go_api_prove_e2e_prove() {
  local log="$1" rc query_api="http://127.0.0.1:${QUERY_API_PORT}"
  set +e
  (
    cd "${ROOT_DIR}"
    unset GO_API_PROVE_PROOF_ACCESS_TOKEN_FILE
    POSTGRES_URI="$(go_api_prove_e2e_pgx_uri)" \
      "${BIN_DIR}/dho" goapi prove \
      -proof-access-token-file "${GO_API_PROVE_E2E_DIR}/platform-admin-token" \
      -go-edge \
      -registry-url "${query_api}/registry" \
      -buildinfo-url "${query_api}/buildinfo" \
      -proof-url "${query_api}/query/proof" \
      -edge-url "${query_api}/graphql" \
      -documents "${GO_API_PROVE_E2E_DIR}/documents.json" \
      -org "${E2E_ORG_ID}" \
      -artifact-dir "${GO_API_PROVE_E2E_DIR}/artifacts" \
      -recorded-by live-e2e \
      -review-evidence "live-e2e: go-api-prove -go-edge with the platform admin's user access token file and the edge access token minted in process"
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
  grep -Eq '^go-api-prove:   proof access token reads = [1-9][0-9]*$' "${log}" \
    || go_api_prove_e2e_fail "the proof access token file was not read"
}

run_go_api_prove_e2e() {
  local dir commit prove_log rc status
  dir="${GO_API_PROVE_E2E_DIR}"

  # dho (goapi prove, mint edge-token) is already built, by
  # query_api_e2e_start -- nothing to build here.
  commit="${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
  go run -trimpath ./cmd/registrydump -file internal/queryapi/server/query_route.go > "${dir}/documents.json"

  "${BIN_DIR}/cigate" live-e2e edge-request "${dir}/documents.json" "${GO_API_PROVE_E2E_OPERATION}" "${E2E_ORG_ID}" > "${dir}/edge-request.json" \
    || go_api_prove_e2e_fail "could not build the registered ${GO_API_PROVE_E2E_OPERATION} request"

  go_api_prove_e2e_signer_env
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
  go_api_prove_e2e_principal seed-session || go_api_prove_e2e_fail "could not seed the proof service principal session"

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
  go_api_prove_e2e_principal seed-session || go_api_prove_e2e_fail "could not seed a session at the bumped token_version"
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
