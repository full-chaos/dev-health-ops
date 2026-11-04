#!/usr/bin/env bash
# The auth side of the web live e2e: auth-service as a bare process, so go-api issues every session through it the way
# prod does (the workload credential, EdDSA access token). It builds auth-service and auth-migrate, prepares the auth
# schema in the Postgres the caller already migrated, creates the signing key and the api workload credential, makes a
# job-local CA and a localhost certificate (the CA key is deleted at once), and starts auth-service.
#
#   bash ci/live_e2e_auth_stack.sh up STATE_DIR            needs migrated Postgres (superuser in PG* variables), Valkey,
#                                                          JWT_SECRET_KEY (the value go-api runs with), DHO_BIN
#   bash ci/live_e2e_auth_stack.sh with-env SIDE STATE_DIR -- CMD [ARG...]   run CMD with the environment of SIDE
#   bash ci/live_e2e_auth_stack.sh print-env SIDE STATE_DIR                 print NAME=VALUE lines of SIDE
#   bash ci/live_e2e_auth_stack.sh assert-audit STATE_DIR                   every delegated session issue used the workload credential
#   bash ci/live_e2e_auth_stack.sh down STATE_DIR                           stop auth-service by pid
# SIDE is go-api, query-api, auth-service or web. The flags are ci/live_e2e_auth_flags.tsv (the prod flag set).
# The https front (localhost:8443 to 127.0.0.1:8095) with STATE_DIR/tls is the caller's: the Go and web verifiers read
# the issuer over https and nothing here serves TLS.
set -euo pipefail

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
ROOT="$(dirname "$(dirname "${SELF}")")"
FLAGS="${ROOT}/ci/live_e2e_auth_flags.tsv"

ISSUER="${LIVE_E2E_AUTH_ISSUER:-https://localhost:8443}"
KEY_ID="${LIVE_E2E_AUTH_KEY_ID:-e2e-auth}"
AUTH_API_ADDR="${LIVE_E2E_AUTH_API_ADDR:-127.0.0.1:8095}"
AUTH_OPERATOR_ADDR="${LIVE_E2E_AUTH_OPERATOR_ADDR:-127.0.0.1:8096}"
SCHEMA=auth
RUNTIME_ROLE=devhealth_auth
MIGRATION_ROLE=devhealth_auth_migrator
PGHOST_USE="${PGHOST:-127.0.0.1}"
PGPORT_USE="${PGPORT:-5432}"
PGDATABASE_USE="${PGDATABASE:-test_db}"
VALKEY_URI="${LIVE_E2E_AUTH_VALKEY_URI:-redis://127.0.0.1:6379/1}"

die() {
  printf 'live_e2e_auth_stack: %s\n' "$1" >&2
  exit 1
}

# flag_rows SCOPE: NAME=VALUE for every row of the flag table with that scope.
flag_rows() {
  awk -F'\t' -v scope="$1" '/^#/ || $1 == "scope" || NF == 0 { next } $1 == scope { print $2 "=" $3 }' "${FLAGS}"
}

dsn() {
  printf "host=%s port=%s user=%s password='%s' dbname=%s sslmode=disable" "${PGHOST_USE}" "${PGPORT_USE}" "$1" "$2" "${PGDATABASE_USE}"
}

print_env() {
  local side="$1" state="$2"
  case "${side}" in
    go-api)
      flag_rows go-api
      printf '%s\n' \
        "AUTH_PROXY_URL=http://${AUTH_API_ADDR}" \
        "AUTH_DATABASE_SCHEMA=${SCHEMA}" \
        "AUTH_WORKLOAD_BOOTSTRAP_FILE=${state}/workload/api.bootstrap" \
        "AUTH_WORKLOAD_ISSUER=${ISSUER}" \
        "AUTH_ACCESS_TOKEN_ISSUER=${ISSUER}" \
        "SSL_CERT_FILE=${state}/tls/ca.pem"
      ;;
    query-api)
      flag_rows query-api
      printf '%s\n' \
        "AUTH_DATABASE_SCHEMA=${SCHEMA}" \
        "AUTH_ACCESS_TOKEN_ISSUER=${ISSUER}" \
        "SSL_CERT_FILE=${state}/tls/ca.pem"
      ;;
    web)
      printf '%s\n' \
        "AUTH_ACCESS_TOKEN_ISSUER=${ISSUER}" \
        "NODE_EXTRA_CA_CERTS=${state}/tls/ca.pem"
      ;;
    auth-service)
      : "${JWT_SECRET_KEY:?JWT_SECRET_KEY is not set}" "${AUTH_SESSION_SUCCESSOR_KEY:?AUTH_SESSION_SUCCESSOR_KEY is not set}"
      : "${AUTH_LOGIN_LOCKOUT_KEY:?AUTH_LOGIN_LOCKOUT_KEY is not set}" "${AUTH_RUNTIME_DATABASE_PASSWORD:?AUTH_RUNTIME_DATABASE_PASSWORD is not set}"
      : "${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT:?AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT is not set}"
      flag_rows auth-service
      printf '%s\n' \
        "AUTH_SERVICE_HTTP_ADDR=${AUTH_API_ADDR}" \
        "AUTH_SERVICE_OPERATOR_HTTP_ADDR=${AUTH_OPERATOR_ADDR}" \
        "AUTH_DATABASE_URI=$(dsn "${RUNTIME_ROLE}" "${AUTH_RUNTIME_DATABASE_PASSWORD}")" \
        "AUTH_SIGNING_KEY_FILE=${state}/keys/private/envelope-private-key.pem" \
        "AUTH_SIGNING_KEY_ID=${KEY_ID}" \
        "AUTH_TOKEN_ISSUER_URL=${ISSUER}" \
        "AUTH_JWT_SECRET_KEY=${JWT_SECRET_KEY}" \
        "AUTH_SESSION_SUCCESSOR_KEY=${AUTH_SESSION_SUCCESSOR_KEY}" \
        "AUTH_LOGIN_LOCKOUT_KEY=${AUTH_LOGIN_LOCKOUT_KEY}" \
        "AUTH_VALKEY_URI=${VALKEY_URI}" \
        "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT}"
      ;;
    *) die "unknown side ${side} (go-api, query-api, auth-service, web)" ;;
  esac
}

# with_env SIDE STATE -- CMD...: export the lines of print_env, then exec CMD. A value is taken whole (no shell parsing).
with_env() {
  local side="$1" state="$2" line
  shift 2
  [ "${1:-}" = "--" ] || die "with-env needs -- before the command"
  shift
  [ "$#" -gt 0 ] || die "with-env needs a command"
  while IFS= read -r line; do
    export "${line%%=*}=${line#*=}"
  done < <(print_env "${side}" "${state}")
  exec "$@"
}

new_secret() { openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'; }

# migrate_run ROLE PASSWORD ARG...: auth-migrate with the environment of the compose one-shots.
migrate_run() {
  local user="$1" password="$2"
  shift 2
  AUTH_MIGRATION_DATABASE_URI="$(dsn "${user}" "${password}")" \
    AUTH_DATABASE_SCHEMA="${SCHEMA}" AUTH_USERS_OWNED=false \
    AUTH_RUNTIME_DATABASE_ROLE="${RUNTIME_ROLE}" AUTH_SCHEMA_OWNER_ROLE="${MIGRATION_ROLE}" \
    "${STATE}/bin/auth-migrate" "$@"
}

# principal_ref UUID: prn_ and the 16 bytes of the uuid as unpadded base64url.
principal_ref() {
  printf 'prn_%s' "$(printf '%s' "${1//-/}" | xxd -r -p | base64 -w0 | tr '+/' '-_' | tr -d '=')"
}

make_tls() {
  local dir="${STATE}/tls"
  openssl ecparam -name prime256v1 -genkey -noout -out "${dir}/ca.key"
  openssl req -x509 -new -key "${dir}/ca.key" -subj "/CN=live-e2e-auth-ca" -days 1 -out "${dir}/ca.pem"
  openssl ecparam -name prime256v1 -genkey -noout -out "${dir}/tls.key"
  openssl req -new -key "${dir}/tls.key" -subj "/CN=localhost" -out "${dir}/tls.csr"
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' >"${dir}/ext.cnf"
  openssl x509 -req -in "${dir}/tls.csr" -CA "${dir}/ca.pem" -CAkey "${dir}/ca.key" -CAcreateserial -days 1 \
    -extfile "${dir}/ext.cnf" -out "${dir}/tls.crt"
  rm -f "${dir}/ca.key" "${dir}/ca.srl" "${dir}/tls.csr" "${dir}/ext.cnf"
}

up() {
  STATE="$(mkdir -p "$1" && cd "$1" && pwd)"
  local cmd
  for cmd in openssl psql go curl xxd base64; do
    command -v "${cmd}" >/dev/null 2>&1 || die "${cmd} is required"
  done
  : "${PGUSER:?PGUSER (the Postgres superuser) is not set}" "${PGPASSWORD:?PGPASSWORD is not set}"
  : "${JWT_SECRET_KEY:?JWT_SECRET_KEY is not set}"
  local dho="${DHO_BIN:-/tmp/dho}"
  [ -x "${dho}" ] || die "DHO_BIN ${dho} is not executable"
  umask 077
  mkdir -p "${STATE}/bin" "${STATE}/logs" "${STATE}/tls"
  mkdir -m 700 "${STATE}/workload"

  printf 'step 1: build auth-service and auth-migrate\n'
  (cd "${ROOT}" && go build -trimpath -o "${STATE}/bin/auth-service" ./cmd/auth-service && go build -trimpath -o "${STATE}/bin/auth-migrate" ./cmd/auth-migrate)

  printf 'step 2: signing key (dho mint envelope-keys)\n'
  "${dho}" mint envelope-keys --dir "${STATE}/keys" --key-id "${KEY_ID}" --skip-existing >/dev/null

  local runtime_password migration_password
  runtime_password="$(openssl rand -hex 24)"
  migration_password="$(openssl rand -hex 24)"

  printf 'step 3: auth roles and schema (compose auth-provision)\n'
  printf '%s\n' \
    '\getenv role AUTH_RUNTIME_DATABASE_ROLE' \
    '\getenv pw AUTH_RUNTIME_DATABASE_PASSWORD' \
    'SELECT format('\''CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'\'', :'\''role'\'', :'\''pw'\'')' \
    '  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'\''role'\'') \gexec' \
    'SELECT format('\''ALTER ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L'\'', :'\''role'\'', :'\''pw'\'') \gexec' \
    '\getenv mrole AUTH_MIGRATION_DATABASE_ROLE' \
    '\getenv mpw AUTH_MIGRATION_DATABASE_PASSWORD' \
    '\getenv schema AUTH_DATABASE_SCHEMA' \
    'SELECT format('\''CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'\'', :'\''mrole'\'', :'\''mpw'\'')' \
    '  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'\''mrole'\'') \gexec' \
    'SELECT format('\''ALTER ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L'\'', :'\''mrole'\'', :'\''mpw'\'') \gexec' \
    'SELECT format('\''CREATE SCHEMA %I AUTHORIZATION %I'\'', :'\''schema'\'', :'\''mrole'\'')' \
    '  WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :'\''schema'\'') \gexec' >"${STATE}/provision.sql"
  AUTH_RUNTIME_DATABASE_ROLE="${RUNTIME_ROLE}" AUTH_RUNTIME_DATABASE_PASSWORD="${runtime_password}" \
    AUTH_MIGRATION_DATABASE_ROLE="${MIGRATION_ROLE}" AUTH_MIGRATION_DATABASE_PASSWORD="${migration_password}" \
    AUTH_DATABASE_SCHEMA="${SCHEMA}" \
    PGHOST="${PGHOST_USE}" PGPORT="${PGPORT_USE}" PGDATABASE="${PGDATABASE_USE}" \
    psql -v ON_ERROR_STOP=1 -f "${STATE}/provision.sql" >/dev/null
  rm -f "${STATE}/provision.sql"

  printf 'step 4: auth schema, grants, fence, workload registry (compose auth-owner-grants, auth-migrate, auth-grants, auth-fence-init, auth-workload-seed)\n'
  migrate_run "${PGUSER}" "${PGPASSWORD}" owner-move -from "${PGUSER}" -to "${MIGRATION_ROLE}" -grants-only -apply >/dev/null
  migrate_run "${MIGRATION_ROLE}" "${migration_password}" >/dev/null
  migrate_run "${MIGRATION_ROLE}" "${migration_password}" grants >/dev/null
  migrate_run "${MIGRATION_ROLE}" "${migration_password}" restore-fence init -if-absent >/dev/null
  migrate_run "${MIGRATION_ROLE}" "${migration_password}" workloads seed -apply >/dev/null

  printf 'step 5: api workload bootstrap credential (compose auth-workload-bootstrap)\n'
  migrate_run "${MIGRATION_ROLE}" "${migration_password}" workloads bootstrap-create -workload api -out "${STATE}/workload/api.bootstrap" -apply >/dev/null
  local principal
  principal="$(PGHOST="${PGHOST_USE}" PGPORT="${PGPORT_USE}" PGDATABASE="${PGDATABASE_USE}" \
    psql -v ON_ERROR_STOP=1 -At -c "SELECT principal_id FROM ${SCHEMA}.workloads WHERE name = 'api'")"
  [[ "${principal}" =~ ^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$ ]] || die "the api workload row has no principal id"

  printf 'step 6: job-local CA and localhost certificate\n'
  make_tls

  printf 'step 7: start auth-service\n'
  export AUTH_RUNTIME_DATABASE_PASSWORD="${runtime_password}"
  AUTH_SESSION_SUCCESSOR_KEY="$(new_secret)"
  AUTH_LOGIN_LOCKOUT_KEY="$(new_secret)"
  AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT="$(principal_ref "${principal}")"
  export AUTH_SESSION_SUCCESSOR_KEY AUTH_LOGIN_LOCKOUT_KEY AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT
  (
    cd "${STATE}"
    nohup bash "${SELF}" with-env auth-service "${STATE}" -- "${STATE}/bin/auth-service" \
      >"${STATE}/logs/auth-service.log" 2>&1 &
    echo "$!" >"${STATE}/auth-service.pid"
  )
  for _ in $(seq 1 60); do
    if curl -fsS "http://${AUTH_OPERATOR_ADDR}/readyz" >/dev/null 2>&1; then
      printf 'auth-service is ready, issuer %s, workload subject %s\n' "${ISSUER}" "${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT}"
      return 0
    fi
    sleep 1
  done
  tail -n 40 "${STATE}/logs/auth-service.log" >&2 || true
  die "auth-service did not become ready"
}

# assert_audit STATE: every delegated session issue (go-api through the issue route) names a workload jti and no
# service credential, and at least one exists.
assert_audit() {
  STATE="$1"
  local row workload service
  row="$(PGHOST="${PGHOST_USE}" PGPORT="${PGPORT_USE}" PGDATABASE="${PGDATABASE_USE}" psql -v ON_ERROR_STOP=1 -At -F ' ' -c \
    "SELECT count(*) FILTER (WHERE attributes ? 'workload_jti'), count(*) FILTER (WHERE attributes ? 'credential_id') \
       FROM ${SCHEMA}.security_audit_events WHERE event_type = 'auth.session.issued' AND attributes->>'source' = 'go-api-delegated'")"
  workload="${row% *}"
  service="${row#* }"
  printf 'delegated session issues: workload=%s service_credential=%s\n' "${workload}" "${service}"
  [ "${workload}" -ge 1 ] || die "no delegated session issue used the workload credential"
  [ "${service}" -eq 0 ] || die "a delegated session issue used a service credential"
}

down() {
  STATE="$(cd "$1" && pwd)"
  local pidfile="${STATE}/auth-service.pid" pid
  [ -f "${pidfile}" ] || return 0
  pid="$(cat "${pidfile}")"
  [[ "${pid}" =~ ^[0-9]+$ ]] || die "pid file holds no pid"
  if [ -d "/proc/${pid}" ] && [ "$(readlink "/proc/${pid}/cwd")" = "${STATE}" ]; then
    kill "${pid}" || true
  fi
}

verb="${1:-}"
case "${verb}" in
  up)
    [ "$#" -eq 2 ] || die "usage: up STATE_DIR"
    up "$2"
    ;;
  with-env)
    [ "$#" -ge 5 ] || die "usage: with-env SIDE STATE_DIR -- CMD [ARG...]"
    with_env "${@:2}"
    ;;
  print-env)
    [ "$#" -eq 3 ] || die "usage: print-env SIDE STATE_DIR"
    print_env "$2" "$3"
    ;;
  assert-audit)
    [ "$#" -eq 2 ] || die "usage: assert-audit STATE_DIR"
    assert_audit "$2"
    ;;
  down)
    [ "$#" -eq 2 ] || die "usage: down STATE_DIR"
    down "$2"
    ;;
  *) die "usage: up | with-env | print-env | assert-audit | down" ;;
esac
