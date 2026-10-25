#!/usr/bin/env bash
# Executed proof for the external auth corpus (CHAOS-10160): TestAuthFlowVenueOracle driven through the
# base-URL runner against a go-api that runs as its own container, on its own stores.
#
#   ci/auth_corpus_external.sh go-api   go-api alone
#   ci/auth_corpus_external.sh acp      go-api forwarding to auth-service each auth aggregate it serves (CHAOS-10231)
#
# 1. A private compose project (authcorpus<hex>; compose.yml is not changed, ci/auth_corpus_external.override.yml
#    releases its host-global names and ports) starts go-api with its dependencies: postgres, pgbouncer, clickhouse,
#    valkey, the migrations, the roles and the envelope key. The stores are published on ephemeral loopback ports.
# 2. The corpus seeds those stores as a superuser, then sends its requests to go-api's published api listener. Its
#    mail sink listens on the runner (0.0.0.0:2525); go-api mails to host.docker.internal.
# 3. Where a corpus case changes go-api's environment, ci/auth_corpus_set_env.sh recreates go-api through compose.
# Leg acp turns on the AUTH_PROXY_* flag of an aggregate only when the running auth-service serves every route of it
# (ci/auth_corpus_acp_routes.tsv): a route auth-service mounts answers OPTIONS with 405, one it does not with 404.
# An aggregate's flag therefore turns on in the change that lands its mount, and sessions gates the others, as go-api's
# own start-up check does. Leg acp also requires sessions and local_login to be served (REQUIRED).
# The project is private to this run and is removed with its volumes on exit. CI only (docker, the Go toolchain).
set -euo pipefail

LEG="${1:-}"
case "${LEG}" in go-api | acp) ;; *)
  printf 'usage: %s go-api|acp\n' "$0" >&2
  exit 2
  ;;
esac

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# From /dev/urandom: compose adopts a resource that already carries the project name, so it must not be guessable.
PROJECT="authcorpus$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
WORK="$(mktemp -d)"
STATE="${WORK}/state.env"

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || die "docker is required"
command -v go >/dev/null 2>&1 || die "go is required"

for kind in container volume network; do
  case "${kind}" in
    container) found="$(docker ps -aq --no-trunc --filter "label=com.docker.compose.project=${PROJECT}")" ;;
    volume) found="$(docker volume ls -q --filter "label=com.docker.compose.project=${PROJECT}")" ;;
    network) found="$(docker network ls -q --filter "label=com.docker.compose.project=${PROJECT}")" ;;
  esac
  [ -z "${found}" ] || die "a ${kind} of the compose project ${PROJECT} exists; refusing to recreate or remove it"
done

# The corpus's signing key and mail identity (sessionscenario.Key, authflowvenue fromAddress): the project is
# started with them, and ci/auth_corpus_set_env.sh sources the same state file on every recreate.
# A loopback port nothing listens on: a random port in the ephemeral range whose connect is refused.
free_port() {
  local port tries=0
  while [ "${tries}" -lt 200 ]; do
    port=$((32768 + RANDOM % 28000))
    if ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      printf '%s\n' "${port}"
      return 0
    fi
    tries=$((tries + 1))
  done
  die "no free loopback port after 200 tries"
}
API_PORT="$(free_port)"
OPERATOR_PORT="$(free_port)"
# The corpus provider stub's CA and certificate, made now and mounted read-only into the stub, go-api and auth-service
# (docker/providerstub.Dockerfile; nothing key-shaped is checked in). The directory and files are readable by the
# containers' non-root user: the directory is inside this run's private temporary directory.
STUB_DIR="${WORK}/stub"
(cd "${ROOT}" && go run -trimpath ./internal/testsupport/providerstub/cmd/providerstub certs -out "${STUB_DIR}") >/dev/null
chmod 0755 "${STUB_DIR}"
chmod 0644 "${STUB_DIR}"/ca.pem "${STUB_DIR}"/server.pem "${STUB_DIR}"/server.key
JWT_KEY="$(sed -n 's/^const Key = "\(.*\)"$/\1/p' "${ROOT}/internal/testsupport/sessionscenario/sessionscenario.go")"
[ -n "${JWT_KEY}" ] || die "could not read sessionscenario.Key"
# The session keys of auth-service's compat routes (leg acp), made now, masked in the CI log before anything else,
# and held only in the private state file (mode 0600, removed with ${WORK}): never in an argument or the output.
# AUTH_CORPUS_OMIT_AUTH_KEYS=1 leaves all of them out: the non-vacuity leg, which must then fail on a required
# aggregate that is not served.
SESSION_SUCCESSOR_KEY=""
LOGIN_LOCKOUT_KEY=""
if [ "${LEG}" = "acp" ] && [ -z "${AUTH_CORPUS_OMIT_AUTH_KEYS:-}" ]; then
  SESSION_SUCCESSOR_KEY="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
  LOGIN_LOCKOUT_KEY="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
  if [ -n "${GITHUB_ACTIONS:-}" ]; then
    printf '::add-mask::%s\n' "${SESSION_SUCCESSOR_KEY}" "${LOGIN_LOCKOUT_KEY}"
  fi
fi
(
  umask 077
  {
    printf 'export BUGSINK_SECRET_KEY=%q\n' "auth-corpus-external"
    printf 'export COMMIT=%q\n' "${COMMIT:-0000000000000000000000000000000000000000}"
    printf 'export AUTH_CORPUS_API_PORT=%s\n' "${API_PORT}"
    printf 'export AUTH_CORPUS_OPERATOR_PORT=%s\n' "${OPERATOR_PORT}"
    printf 'export AUTH_CORPUS_STUB_DIR=%q\n' "${STUB_DIR}"
    printf 'export JWT_SECRET_KEY=%q\n' "${JWT_KEY}"
    printf 'export EMAIL_PROVIDER=%q\n' "smtp"
    printf 'export EMAIL_FROM_ADDRESS=%q\n' "Dev Health <auth@example.test>"
    if [ "${LEG}" = "acp" ]; then
      printf 'export AUTH_PROXY_URL=%q\n' "http://auth-service:8095"
    fi
    if [ "${LEG}" = "acp" ] && [ -z "${AUTH_CORPUS_OMIT_AUTH_KEYS:-}" ]; then
      printf 'export AUTH_JWT_SECRET_KEY=%q\n' "${JWT_KEY}"
      printf 'export AUTH_SESSION_SUCCESSOR_KEY=%q\n' "${SESSION_SUCCESSOR_KEY}"
      printf 'export AUTH_LOGIN_LOCKOUT_KEY=%q\n' "${LOGIN_LOCKOUT_KEY}"
    fi
  } >"${STATE}"
)
chmod 0600 "${STATE}"
unset SESSION_SUCCESSOR_KEY LOGIN_LOCKOUT_KEY
# shellcheck disable=SC1090
. "${STATE}"

COMPOSE_FILES=(-f "${ROOT}/compose.yml" -f "${SCRIPT_DIR}/auth_corpus_external.override.yml")
if [ "${LEG}" = "acp" ]; then
  COMPOSE_FILES+=(-f "${SCRIPT_DIR}/auth_corpus_external.acp.override.yml")
fi
compose() {
  docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth "$@"
}

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- services at failure ---\n' >&2
    timeout --kill-after=5 60 docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth ps --all >&2 || true
    timeout --kill-after=5 60 docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth logs --no-color --tail 80 go-api go-river-migrate migrate >&2 || true
    timeout --kill-after=5 60 docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth logs --no-color --tail=3000 auth-service >&2 || true
    timeout --kill-after=5 60 docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth logs --no-color --tail=200 providerstub >&2 || true
  fi
  # Bounded: a hung docker daemon must not hold the step until the job timeout.
  timeout --kill-after=10 "${AUTH_CORPUS_DOWN_SECONDS:-180}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "${WORK}"
  exit "${status}"
}
trap cleanup EXIT

# compose.yml's migrate service refuses a database that already holds objects but no alembic_version table, and
# auth-migrate creates the auth schema: leg acp starts go-api (and with it migrate) first and auth-service after it.
printf 'step 1: up --build --wait go-api (project %s)\n' "${PROJECT}"
timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth up --build --detach --wait --wait-timeout 900 go-api
if [ "${LEG}" = "acp" ]; then
  printf 'step 1b: up --build --wait auth-service\n'
  timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth up --build --detach --wait --wait-timeout 900 auth-service
fi

AGGREGATES=(sessions local_login credentials social oidc saml)
# Leg acp serves these through auth-service (CHAOS-10410); one that is not served fails the leg by name.
REQUIRED=(sessions local_login)
# Served by auth-service but left to go-api on this leg: auth-service creates no account for a new social sign-in until it
# owns users, so forwarding social would refuse the corpus's new-user row.
KEPT_ON_GO_API=(social)
ROUTES="${SCRIPT_DIR}/auth_corpus_acp_routes.tsv"

hostport() {
  local port
  port="$(compose port "$1" "$2" | sed 's/.*://')"
  [[ "${port}" =~ ^[0-9]+$ ]] || die "could not read the published port of $1 $2 (${port})"
  printf '%s' "${port}"
}
STUB_ADMIN_PORT="$(hostport providerstub 9090)"
STUB_ADMIN="http://127.0.0.1:${STUB_ADMIN_PORT}"
curl --silent --show-error --fail --max-time 10 "${STUB_ADMIN}/requests" >/dev/null || die "the corpus provider stub's recorder does not answer at ${STUB_ADMIN}"
PG_PORT="$(hostport postgres 5432)"
CH_PORT="$(hostport clickhouse 9000)"
VALKEY_PORT="$(hostport valkey 6379)"
printf 'ports: api %s postgres %s clickhouse %s valkey %s\n' "${API_PORT}" "${PG_PORT}" "${CH_PORT}" "${VALKEY_PORT}"
export AUTH_CORPUS_PROJECT="${PROJECT}" AUTH_CORPUS_STATE="${STATE}" AUTH_CORPUS_LEG="${LEG}"

# probe AGGREGATE sets PROBED to "served" (auth-service answers OPTIONS on every route of it with 405) or "not served"
# (404 on every route). Some of them is a partial mount and fails the leg: a change that mounts an aggregate mounts all
# of it.
probe() {
  local aggregate="$1" name method pattern path code mounted=0 missing=0
  while IFS=$'\t' read -r name method pattern; do
    [ "${name}" = "${aggregate}" ] || continue
    path="${pattern//\{provider_id\}/p-probe}"
    code="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 10 -X OPTIONS "http://127.0.0.1:${AUTH_API_PORT}${path}" || true)"
    case "${code}" in
      405) mounted=$((mounted + 1)) ;;
      404) missing=$((missing + 1)) ;;
      *) die "auth-service answered OPTIONS ${path} (${method}) with '${code}', want 405 (served) or 404 (not served)" ;;
    esac
  done < <(grep -v '^#' "${ROUTES}")
  [ $((mounted + missing)) -gt 0 ] || die "${ROUTES} lists no route of aggregate ${aggregate}"
  if [ "${mounted}" -gt 0 ] && [ "${missing}" -gt 0 ]; then
    printf 'probe %s: partial (%d of %d routes served)\n' "${aggregate}" "${mounted}" "$((mounted + missing))"
    die "auth-service serves ${mounted} of the $((mounted + missing)) routes of aggregate ${aggregate}"
  fi
  if [ "${missing}" -eq 0 ]; then PROBED="served"; else PROBED="not served"; fi
  printf 'probe %s: %s (%d routes)\n' "${aggregate}" "${PROBED}" "$((mounted + missing))"
}

FORWARDED=()
if [ "${LEG}" = "acp" ]; then
  AUTH_API_PORT="$(hostport auth-service 8095)"
  printf 'step 1c: the aggregates auth-service serves\n'
  SERVED=()
  for aggregate in "${AGGREGATES[@]}"; do
    probe "${aggregate}"
    if [ "${PROBED}" = served ]; then
      SERVED+=("${aggregate}")
    fi
  done
  for aggregate in "${REQUIRED[@]}"; do
    [[ " ${SERVED[*]:-} " == *" ${aggregate} "* ]] || die "required aggregate ${aggregate} is not served by auth-service"
  done
  # The sessions aggregate gates the others, as go-api's own start-up check does.
  if [[ " ${SERVED[*]} " == *" sessions "* ]]; then
    for aggregate in "${SERVED[@]}"; do
      [[ " ${KEPT_ON_GO_API[*]} " == *" ${aggregate} "* ]] || FORWARDED+=("${aggregate}")
    done
  fi
  printf 'leg acp forwards: %s\n' "${FORWARDED[*]:-none (sessions is not served; every route stays in go-api)}"
  if [ "${#FORWARDED[@]}" -gt 0 ]; then
    # go-api asks auth-service's issue route for the sessions of the sign-in flows it still serves, with an
    # auth-session-issuer service credential: mint one into the project's database and hand go-api the token through
    # the state file, before the sessions flag turns on (go-api refuses to start with the flag and no credential).
    ISSUE_TOKEN="svc_authsess_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
    [[ "${ISSUE_TOKEN}" =~ ^svc_authsess_[A-Za-z0-9_-]{43}$ ]] || die "could not build a session issue credential"
    compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" >/dev/null \
      < <(printf '\\set token %s\n' "'${ISSUE_TOKEN}'"; cat <<'SQL'
INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
VALUES (gen_random_uuid(), 'auth-session-issuer', encode(sha256(convert_to(:'token', 'UTF8')), 'hex'), left(:'token', 16),
        '["sessions:issue"]', now());
SQL
      ) \
      || die "could not store the session issue credential"
    printf 'export AUTH_SESSION_ISSUE_TOKEN=%q\n' "${ISSUE_TOKEN}" >>"${STATE}"
    unset ISSUE_TOKEN
    if [[ " ${FORWARDED[*]} " == *" sessions "* ]]; then
      # The sessions flag drops the api role's refresh_tokens INSERT and UPDATE at migrate time, and go-api's
      # readiness check reads the same flag: apply the grants for it before go-api restarts with it.
      printf 'step 1d: grants for AUTH_PROXY_SESSIONS=true\n'
      timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
        run --rm --no-deps -e AUTH_PROXY_SESSIONS=true go-river-migrate >/dev/null
    fi
  fi
  for aggregate in "${FORWARDED[@]}"; do
    bash "${SCRIPT_DIR}/auth_corpus_set_env.sh" "AUTH_PROXY_$(tr '[:lower:]' '[:upper:]' <<<"${aggregate}")" true
  done
  # CHAOS-11387: this leg runs with auth-service owning users (CI and local only; prod and the other leg stay off).
  # users-backfill refuses once the flag is true and auth-service is not ready while public.users and auth.users
  # have gaps, so the copy runs first, with the flag still off; then grants and auth-service, then go-api.
  # AUTH_CORPUS_USERS_UNOWNED=1 skips it: the non-vacuity run, which must still pass the golden through the not-owned quirks.
  if [ -z "${AUTH_CORPUS_USERS_UNOWNED:-}" ]; then
    printf 'step 1e: users-backfill, then AUTH_USERS_OWNED=true\n'
    (
      export "AUTH_DATABASE_URI=host=postgres port=5432 user=${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth} password='${AUTH_RUNTIME_DATABASE_PASSWORD:-devhealth_auth}' dbname=${POSTGRES_DB:-postgres} sslmode=disable"
      timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
        run --rm --no-deps -e AUTH_DATABASE_URI migrate auth users-backfill
    ) || die "users-backfill failed"
    export AUTH_USERS_OWNED=true
    timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
      run --rm --no-deps auth-grants >/dev/null
    timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
      up --detach --no-deps --force-recreate --no-build --wait --wait-timeout 300 auth-service
    bash "${SCRIPT_DIR}/auth_corpus_set_env.sh" AUTH_USERS_OWNED true
  fi
fi

printf 'step 2: TestAuthFlowVenueOracle through the base-URL runner\n'
export DEV_HEALTH_VENUE_ORACLES=1
export DEVHEALTH_AUTH_ORACLE_BASE_URL="http://127.0.0.1:${API_PORT}"
export DEVHEALTH_AUTH_ORACLE_POSTGRES_URI="postgres://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD:-postgres}@127.0.0.1:${PG_PORT}/${POSTGRES_DB:-postgres}?sslmode=disable"
export DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI="clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@127.0.0.1:${CH_PORT}/${CLICKHOUSE_DB:-default}"
export DEVHEALTH_AUTH_ORACLE_VALKEY_URI="redis://127.0.0.1:${VALKEY_PORT}/1"
export DEVHEALTH_AUTH_ORACLE_SMTP_LISTEN="0.0.0.0:2525"
export DEVHEALTH_AUTH_ORACLE_STUB_ADMIN="${STUB_ADMIN}"
# The aggregates go-api forwarded, comma separated (empty on leg go-api): the corpus reads auth-service's rows for them
# (internal/testsupport/authrows).
DEVHEALTH_AUTH_ORACLE_FORWARDED="$(IFS=,; printf '%s' "${FORWARDED[*]:-}")"
export DEVHEALTH_AUTH_ORACLE_FORWARDED
# Whether auth-service owns users on this leg (leg acp only, step 1e): the corpus expects the users rows of go-api's golden.
if [ "${LEG}" = "acp" ] && [ -z "${AUTH_CORPUS_USERS_UNOWNED:-}" ]; then
  export DEVHEALTH_AUTH_ORACLE_USERS_OWNED=true
else
  export DEVHEALTH_AUTH_ORACLE_USERS_OWNED=false
fi
(cd "${ROOT}" && go test -trimpath -tags integration -count=1 -v -run '^TestAuthFlowVenueOracle$' ./internal/apiservice/authflowvenue)

if [ "${LEG}" = "acp" ]; then
  printf 'step 3: go-api forwarded exactly the served aggregates to auth-service\n'
  metrics="$(curl --silent --show-error --max-time 10 "http://127.0.0.1:${OPERATOR_PORT}/metrics")"
  for aggregate in "${AGGREGATES[@]}"; do
    pattern="^devhealth_api_auth_proxy_requests_total\{[^}]*aggregate=\"${aggregate}\"[^}]*outcome=\"forwarded\"[^}]*\} [1-9]"
    if [[ " ${FORWARDED[*]} " == *" ${aggregate} "* ]]; then
      grep -E "${pattern}" <<<"${metrics}" || die "no forwarded request for aggregate ${aggregate}"
    elif grep -E "${pattern}" <<<"${metrics}"; then
      die "go-api forwarded aggregate ${aggregate}, whose flag is off"
    fi
  done
  if grep -E '^devhealth_api_auth_proxy_requests_total\{[^}]*outcome="upstream_error"[^}]*\} [1-9]' <<<"${metrics}"; then
    die "go-api reported upstream_error requests to auth-service"
  fi
  # auth-service is its own process, so the corpus test cannot read policy.WriterViolations: its answers are bound to
  # the wrong body writer when it logs this line (policy.recordWriterViolation).
  printf 'step 4: auth-service wrote every body with its route'"'"'s writer\n'
  auth_logs="$(timeout --kill-after=5 60 docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth logs --no-color auth-service)" \
    || die "could not read the auth-service logs"
  # A log that is empty or lacks the line auth-service always logs once a listener is up (authruntime/runtime.go:557)
  # was not read in full, and an absent violation line would prove nothing.
  grep -q 'msg="listener bound"\|"msg":"listener bound"' <<<"${auth_logs}" || die "auth-service logs empty or unrecognised"
  violations="$(grep -c 'policy: wrong body writer for the route' <<<"${auth_logs}" || true)"
  if [ "${violations}" != "0" ]; then
    grep 'policy: wrong body writer for the route' <<<"${auth_logs}" >&2 || true
    die "auth-service logged ${violations} body-writer violations"
  fi
fi
printf 'AUTH CORPUS EXTERNAL (%s) PASSED\n' "${LEG}"
