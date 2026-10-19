#!/usr/bin/env bash
# Executed proof for the external auth corpus (CHAOS-10160): TestAuthFlowVenueOracle driven through the
# base-URL runner against a go-api that runs as its own container, on its own stores.
#
#   ci/auth_corpus_external.sh go-api   go-api alone
#   ci/auth_corpus_external.sh acp      go-api forwarding the six auth aggregates to auth-service (CHAOS-10231)
#
# 1. A private compose project (authcorpus<hex>; compose.yml is not changed, ci/auth_corpus_external.override.yml
#    releases its host-global names and ports) starts go-api with its dependencies: postgres, pgbouncer, clickhouse,
#    valkey, the migrations, the roles and the envelope key. The stores are published on ephemeral loopback ports.
# 2. The corpus seeds those stores as a superuser, then sends its requests to go-api's published api listener. Its
#    mail sink listens on the runner (0.0.0.0:2525); go-api mails to host.docker.internal.
# 3. Where a corpus case changes go-api's environment, ci/auth_corpus_set_env.sh recreates go-api through compose.
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
free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}
API_PORT="$(free_port)"
OPERATOR_PORT="$(free_port)"
JWT_KEY="$(sed -n 's/^const Key = "\(.*\)"$/\1/p' "${ROOT}/internal/testsupport/sessionscenario/sessionscenario.go")"
[ -n "${JWT_KEY}" ] || die "could not read sessionscenario.Key"
{
  printf 'export BUGSINK_SECRET_KEY=%q\n' "auth-corpus-external"
  printf 'export COMMIT=%q\n' "${COMMIT:-0000000000000000000000000000000000000000}"
  printf 'export AUTH_CORPUS_API_PORT=%s\n' "${API_PORT}"
  printf 'export AUTH_CORPUS_OPERATOR_PORT=%s\n' "${OPERATOR_PORT}"
  printf 'export JWT_SECRET_KEY=%q\n' "${JWT_KEY}"
  printf 'export EMAIL_PROVIDER=%q\n' "smtp"
  printf 'export EMAIL_FROM_ADDRESS=%q\n' "Dev Health <auth@example.test>"
  if [ "${LEG}" = "acp" ]; then
    printf 'export AUTH_PROXY_URL=%q\n' "http://auth-service:8095"
    for aggregate in SESSIONS LOCAL_LOGIN CREDENTIALS SOCIAL OIDC SAML; do
      printf 'export AUTH_PROXY_%s=true\n' "${aggregate}"
    done
  fi
} >"${STATE}"
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
compose up --build --detach --wait --wait-timeout 900 go-api
if [ "${LEG}" = "acp" ]; then
  printf 'step 1b: up --build --wait auth-service\n'
  compose up --build --detach --wait --wait-timeout 900 auth-service
fi

hostport() {
  local port
  port="$(compose port "$1" "$2" | sed 's/.*://')"
  [[ "${port}" =~ ^[0-9]+$ ]] || die "could not read the published port of $1 $2 (${port})"
  printf '%s' "${port}"
}
PG_PORT="$(hostport postgres 5432)"
CH_PORT="$(hostport clickhouse 9000)"
VALKEY_PORT="$(hostport valkey 6379)"
printf 'ports: api %s postgres %s clickhouse %s valkey %s\n' "${API_PORT}" "${PG_PORT}" "${CH_PORT}" "${VALKEY_PORT}"

printf 'step 2: TestAuthFlowVenueOracle through the base-URL runner\n'
export AUTH_CORPUS_PROJECT="${PROJECT}" AUTH_CORPUS_STATE="${STATE}" AUTH_CORPUS_LEG="${LEG}"
export DEV_HEALTH_VENUE_ORACLES=1
export DEVHEALTH_AUTH_ORACLE_BASE_URL="http://127.0.0.1:${API_PORT}"
export DEVHEALTH_AUTH_ORACLE_POSTGRES_URI="postgres://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD:-postgres}@127.0.0.1:${PG_PORT}/${POSTGRES_DB:-postgres}?sslmode=disable"
export DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI="clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@127.0.0.1:${CH_PORT}/${CLICKHOUSE_DB:-default}"
export DEVHEALTH_AUTH_ORACLE_VALKEY_URI="redis://127.0.0.1:${VALKEY_PORT}/1"
export DEVHEALTH_AUTH_ORACLE_SMTP_LISTEN="0.0.0.0:2525"
(cd "${ROOT}" && go test -trimpath -tags integration -count=1 -v -run '^TestAuthFlowVenueOracle$' ./internal/apiservice/authflowvenue)

if [ "${LEG}" = "acp" ]; then
  printf 'step 3: go-api forwarded every aggregate to auth-service\n'
  metrics="$(curl --silent --show-error --max-time 10 "http://127.0.0.1:${OPERATOR_PORT}/metrics")"
  for aggregate in sessions local_login credentials social oidc saml; do
    grep -E "^devhealth_api_auth_proxy_requests_total\{[^}]*aggregate=\"${aggregate}\"[^}]*outcome=\"forwarded\"[^}]*\} [1-9]" <<<"${metrics}" \
      || die "no forwarded request for aggregate ${aggregate}"
  done
  if grep -E '^devhealth_api_auth_proxy_requests_total\{[^}]*outcome="upstream_error"[^}]*\} [1-9]' <<<"${metrics}"; then
    die "go-api reported upstream_error requests to auth-service"
  fi
fi
printf 'AUTH CORPUS EXTERNAL (%s) PASSED\n' "${LEG}"
