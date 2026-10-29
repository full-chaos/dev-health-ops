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

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

# AUTH_CORPUS_FORCE_SOCIAL=1 (with AUTH_CORPUS_USERS_UNOWNED=1 only) forwards social although users are not owned: the
# non-vacuity run, whose new-user social row must fail because auth-service refuses it (403 signup_unavailable).
FORCE_SOCIAL=""
if [ -n "${AUTH_CORPUS_FORCE_SOCIAL:-}" ]; then
  [ "${LEG}" = "acp" ] && [ -n "${AUTH_CORPUS_USERS_UNOWNED:-}" ] || die "AUTH_CORPUS_FORCE_SOCIAL needs AUTH_CORPUS_USERS_UNOWNED=1 on leg acp"
  FORCE_SOCIAL=1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# The job start of the deadline, taken once here: the job runs this script in up to three steps, and a later step reads it from GITHUB_ENV.
if [ -n "${CHECK_GO_JOB_CAP_MIN:-}" ] && [ -z "${CHECK_GO_JOB_START:-}" ]; then
  CHECK_GO_JOB_START="$(bash "${ROOT}/ci/check_go.sh" job-start)" || die "no job start for the corpus deadline"
  export CHECK_GO_JOB_START
fi
# CHAOS-11670: the tested package comes from the set the relevance script lists the closure of (ci/auth_corpus_set.sh).
# shellcheck source=ci/auth_corpus_set.sh
source "${SCRIPT_DIR}/auth_corpus_set.sh"
# From /dev/urandom: compose adopts a resource that already carries the project name, so it must not be guessable.
PROJECT="authcorpus$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
# The override tags the images this run builds with the project, so the trap removes exactly its own two.
export AUTH_CORPUS_PROJECT="${PROJECT}"
WORK="$(mktemp -d)"
STATE="${WORK}/state.env"
SEEDED_FILE="${WORK}/seeded"
FLIPPED_FILE="${WORK}/flipped"
TEST_PID=""

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

# join_comma prints its arguments joined with commas, no trailing newline; it assigns no IFS (semgrep bash ifs-tampering).
join_comma() {
  local joined="" item
  for item in "$@"; do
    joined="${joined:+${joined},}${item}"
  done
  printf '%s' "${joined}"
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
  # Exactly the two images this run built, by name, after down: no prune and no other image (the dev stack's own :local
  # images share tags with other projects and stay).
  for image in "authcorpus-providerstub:${PROJECT}" "authcorpus-smtpsink:${PROJECT}"; do
    timeout --kill-after=5 60 docker image rm "${image}" >/dev/null 2>&1 || true
  done
  [ -z "${TEST_PID}" ] || kill "${TEST_PID}" 2>/dev/null || true
  rm -rf -- "${WORK}"
  exit "${status}"
}
trap cleanup EXIT

# CHAOS-11670: `compose up --build` fails now and then for a reason outside the product. up_service retries it ONCE, and only
# when its output has a whole line of one of these classes (the texts are from venue-oracles runs on main) and no product line:
#   1 port already allocated      (run 40283712992, job 121442426149)
#   2 image mirror layer fetch    (run 40273038441, job 121409808895)
#   3 Go module proxy read error  (run 40431223738, job 121949107695)
# A product line is a container that is unhealthy or exited non-zero, or a service that did not complete. Any other failure is
# never retried. The retry runs `down` for this project only, without --volumes. Each attempt has its own log file; the whole
# first attempt stays in the job log (up_once copies the output to the console).
INFRA_CLASSES=(
  '^Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint [A-Za-z0-9_.-]+ \([0-9a-f]+\): Bind for 127\.0\.0\.1:[0-9]+ failed: port is already allocated$'
  '^#[0-9]+ ERROR: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor sha256:[0-9a-f]{64} \(application/vnd\.oci\.image\.layer\.v1\.tar\+gzip\) from remote: not found$'
  '^#[0-9]+ [0-9.]+ go: [^ ]+@[^ ]+: read "https://proxy\.golang\.org/[^"]*": stream error: stream ID [0-9]+; INTERNAL_ERROR; received from peer$'
)
PRODUCT_LINE='^dependency failed to start: |is unhealthy|exited \([1-9][0-9]*\)|didn.t complete successfully'
infra_class() {
  local text
  text="$(tr -d '\r' <"$1")"
  if LC_ALL=C grep -aEq "${PRODUCT_LINE}" <<<"${text}"; then
    return 0
  fi
  local n
  for n in "${!INFRA_CLASSES[@]}"; do
    if LC_ALL=C grep -aEq "${INFRA_CLASSES[n]}" <<<"${text}"; then
      echo $((n + 1))
      return 0
    fi
  done
}

# up_once SERVICE LOGFILE: one bounded up --build; its output goes to the console and to LOGFILE; returns up's status.
up_once() {
  local status=0
  set +e
  { timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth up --build --detach --wait --wait-timeout 900 "$1"; } 2>&1 | tee -- "$2"
  status="${PIPESTATUS[0]}"
  set -e
  return "${status}"
}

# up_service go-api|auth-service
up_service() {
  local service="$1" log="${WORK}/up-$1.1.log" status=0 class
  up_once "${service}" "${log}" || status=$?
  [ "${status}" -ne 0 ] || return 0
  class="$(infra_class "${log}")"
  [ -n "${class}" ] || return "${status}"
  printf 'AUTH-CORPUS-INFRA-RETRY class=%s step=%s\n' "${class}" "${service}"
  timeout --kill-after=10 "${AUTH_CORPUS_DOWN_SECONDS:-180}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth down --remove-orphans || true
  # down removed go-api too: the auth-service step starts it again before it tries auth-service again.
  if [ "${service}" = "auth-service" ]; then
    up_once go-api "${WORK}/up-go-api-retry.log" || return $?
  fi
  up_once "${service}" "${WORK}/up-${service}.2.log"
}

# compose.yml's migrate service refuses a database that already holds objects but no alembic_version table, and
# auth-migrate creates the auth schema: leg acp starts go-api (and with it migrate) first and auth-service after it.
printf 'step 1: up --build --wait go-api (project %s)\n' "${PROJECT}"
up_service go-api
if [ "${LEG}" = "acp" ]; then
  printf 'step 1b: up --build --wait auth-service\n'
  up_service auth-service
fi

AGGREGATES=(sessions local_login credentials social oidc saml)
# Leg acp serves these through auth-service (CHAOS-10410); one that is not served fails the leg by name.
REQUIRED=(sessions local_login)
# Aggregates kept on go-api whatever auth-service serves: none. Social is gated like credentials (gate_owned): auth-service
# creates no account for a new social sign-in until it owns users, so go-api keeps it until step 1e.
KEPT_ON_GO_API=()
ROUTES="${SCRIPT_DIR}/auth_corpus_acp_routes.tsv"
# Script-internal, never read from the caller's environment: only a step that owns the users sets it to 1 (step 1e, owned_flip).
USERS_OWNED=0
# Script-internal too: it follows this script's own key choice (the no-keys leg leaves the keys out), never the caller env.
SESSIONS_KEYED=1
[ -z "${AUTH_CORPUS_OMIT_AUTH_KEYS:-}" ] || SESSIONS_KEYED=0

hostport() {
  local port
  port="$(compose port "$1" "$2" | sed 's/.*://')"
  [[ "${port}" =~ ^[0-9]+$ ]] || die "could not read the published port of $1 $2 (${port})"
  printf '%s' "${port}"
}
STUB_ADMIN_PORT="$(hostport providerstub 9090)"
STUB_ADMIN="http://127.0.0.1:${STUB_ADMIN_PORT}"
curl --silent --show-error --fail --max-time 10 "${STUB_ADMIN}/requests" >/dev/null || die "the corpus provider stub's recorder does not answer at ${STUB_ADMIN}"
# The corpus mail sink (the smtpsink service of this project) publishes its read API on loopback only; the test reads the
# captured mail through it. A recorder that does not answer fails the leg before any step runs.
SMTP_ADMIN_PORT="$(hostport smtpsink 9090)"
SMTP_ADMIN="http://127.0.0.1:${SMTP_ADMIN_PORT}"
sink_up=""
for _ in $(seq 1 "${AUTH_CORPUS_SINK_TRIES:-30}"); do
  if curl --silent --fail --max-time 5 "${SMTP_ADMIN}/mails" >/dev/null; then sink_up=1; break; fi
  sleep 1
done
[ -n "${sink_up}" ] || die "the corpus mail sink's read API does not answer at ${SMTP_ADMIN}"
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

# gate_owned AGGREGATE narrows PROBED for an aggregate auth-service mounts before it owns it: auth-service answers its
# routes with a 503 stub until then, so go-api must keep them. Once USERS_OWNED=1 the aggregate must be served.
gate_owned() {
  # Without the session keys auth-service answers refresh and logout from a 503 stub that also answers OPTIONS 405.
  if [ "$1" = sessions ] && [ "${SESSIONS_KEYED}" = 0 ] && [ "${PROBED}" = served ]; then
    PROBED="not keyed"
    printf 'probe sessions: mounted, not keyed: a stub, not served\n'
    return 0
  fi
  local owned_gated=(credentials social)
  [[ " ${owned_gated[*]} " == *" $1 "* ]] || return 0
  # The forced non-vacuity run forwards social without the posture.
  if [ "$1" = social ] && [ -n "${FORCE_SOCIAL:-}" ]; then return 0; fi
  if [ "${USERS_OWNED}" = 1 ]; then
    [ "${PROBED}" = served ] || die "aggregate $1 is not served by auth-service, which owns the users"
  elif [ "${PROBED}" = served ]; then
    PROBED="not owned"
    printf 'probe %s: mounted, not owned yet: stays on go-api\n' "$1"
  fi
}

FORWARDED=()
if [ "${LEG}" = "acp" ]; then
  AUTH_API_PORT="$(hostport auth-service 8095)"
  printf 'step 1c: the aggregates auth-service serves\n'
  # classify probes every aggregate and sets SERVED and FORWARDED (the served ones, minus those kept on go-api, when
  # sessions is served). Run again after step 1e: auth-service answers the credentials routes with a stub until it owns users, and gate_owned keeps them on go-api until USERS_OWNED=1.
  classify() {
    local aggregate
    SERVED=()
    FORWARDED=()
    for aggregate in "${AGGREGATES[@]}"; do
      probe "${aggregate}"
      gate_owned "${aggregate}"
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
        [[ " ${KEPT_ON_GO_API[*]:-} " == *" ${aggregate} "* ]] || FORWARDED+=("${aggregate}")
      done
    fi
  }
  classify
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
  # The corpus test seeds its users first (it signals ${SEEDED_FILE}), so this runs once they exist, as a real switch has users
  # first; the test is then handed the forwarded set (${FLIPPED_FILE}) and goes on. Step 1e needs auth-service and go-api up, not the test.
  owned_flip() {
    printf 'step 1e: users-backfill, then AUTH_USERS_OWNED=true\n'
    (
      export "AUTH_DATABASE_URI=host=postgres port=5432 user=${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth} password='${AUTH_RUNTIME_DATABASE_PASSWORD:-devhealth_auth}' dbname=${POSTGRES_DB:-postgres} sslmode=disable"
      timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
        run --rm --no-deps -e AUTH_DATABASE_URI migrate auth users-backfill
    ) || die "users-backfill failed"
    # The flag is set for these two commands only (not exported): the go-api recreates that follow take it from the state.
    AUTH_USERS_OWNED=true timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
      run --rm --no-deps auth-grants >/dev/null
    AUTH_USERS_OWNED=true timeout --kill-after=10 "${AUTH_CORPUS_UP_SECONDS:-1500}" docker compose -p "${PROJECT}" "${COMPOSE_FILES[@]}" --profile auth \
      up --detach --no-deps --force-recreate --no-build --wait --wait-timeout 300 auth-service
    # go-api refuses AUTH_USERS_OWNED without AUTH_PROXY_CREDENTIALS, and auth-service serves the credentials routes only
    # now: probe every aggregate again, forward what it serves now, and turn the credentials flag on BEFORE the owned flag.
    printf 'step 1f: the aggregates auth-service serves with users owned\n'
    BEFORE_OWNED=("${FORWARDED[@]}")
    AUTH_API_PORT="$(hostport auth-service 8095)" # the recreate took a new ephemeral host port
    # The one place the script owns the users (never read from the caller's environment): gate_owned now requires the
    # credentials aggregate to be served and lets it be forwarded.
    USERS_OWNED=1
    classify
    [[ " ${SERVED[*]} " == *" credentials "* ]] || die "credentials is not served by auth-service after step 1e (users owned)"
    printf 'leg acp forwards (users owned): %s\n' "${FORWARDED[*]}"
    for aggregate in "${FORWARDED[@]}"; do
      [[ " ${BEFORE_OWNED[*]} " == *" ${aggregate} "* ]] || bash "${SCRIPT_DIR}/auth_corpus_set_env.sh" "AUTH_PROXY_$(tr '[:lower:]' '[:upper:]' <<<"${aggregate}")" true
    done
    bash "${SCRIPT_DIR}/auth_corpus_set_env.sh" AUTH_USERS_OWNED true
  }
fi

printf 'step 2: TestAuthFlowVenueOracle through the base-URL runner\n'
export DEV_HEALTH_VENUE_ORACLES=1
export DEVHEALTH_AUTH_ORACLE_BASE_URL="http://127.0.0.1:${API_PORT}"
export DEVHEALTH_AUTH_ORACLE_POSTGRES_URI="postgres://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD:-postgres}@127.0.0.1:${PG_PORT}/${POSTGRES_DB:-postgres}?sslmode=disable"
export DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI="clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@127.0.0.1:${CH_PORT}/${CLICKHOUSE_DB:-default}"
export DEVHEALTH_AUTH_ORACLE_VALKEY_URI="redis://127.0.0.1:${VALKEY_PORT}/1"
export DEVHEALTH_AUTH_ORACLE_SMTP_ADMIN="${SMTP_ADMIN}"
export DEVHEALTH_AUTH_ORACLE_STUB_ADMIN="${STUB_ADMIN}"
# The aggregates go-api forwarded, comma separated (empty on leg go-api): the corpus reads auth-service's rows for them
# (internal/testsupport/authrows).
DEVHEALTH_AUTH_ORACLE_FORWARDED="$(join_comma ${FORWARDED[@]+"${FORWARDED[@]}"})"
export DEVHEALTH_AUTH_ORACLE_FORWARDED
# Whether auth-service owns users on this leg (leg acp only, step 1e): the corpus expects the users rows of go-api's golden.
if [ "${LEG}" = "acp" ] && [ -z "${AUTH_CORPUS_USERS_UNOWNED:-}" ]; then
  export DEVHEALTH_AUTH_ORACLE_USERS_OWNED=true
else
  export DEVHEALTH_AUTH_ORACLE_USERS_OWNED=false
fi
# The test limit comes from the job budget (the queue job has 30 min, the external workflow 45): 27 min at most, less when the
# compose bring-up used part of the job.
tmo="$(bash "${ROOT}/ci/check_go.sh" job-timeout 27)" || die "no job time budget left for the corpus test"
TEST_CMD=(go test -trimpath -tags integration -count=1 -timeout="${tmo}" -v -run '^TestAuthFlowVenueOracle$' "${AUTH_CORPUS_TEST_PACKAGE}")
if [ "${LEG}" = "acp" ] && [ -z "${AUTH_CORPUS_USERS_UNOWNED:-}" ]; then
  # The owned leg: the test seeds, signals, and waits; the flip (step 1e and 1f) runs once the users exist.
  export DEVHEALTH_AUTH_ORACLE_SEEDED_FILE="${SEEDED_FILE}" DEVHEALTH_AUTH_ORACLE_FLIPPED_FILE="${FLIPPED_FILE}"
  # The test runs as its own compiled binary, so the pid kept here is the test itself and cleanup kills that pid and nothing else.
  TEST_BIN="${WORK}/authflowvenue.test"
  (cd "${ROOT}" && go test -c -trimpath -tags integration -o "${TEST_BIN}" "${AUTH_CORPUS_TEST_PACKAGE}")
  (cd "${ROOT}/${AUTH_CORPUS_TEST_PACKAGE#./}" && exec "${TEST_BIN}" -test.v -test.count=1 -test.timeout="${tmo}" -test.run '^TestAuthFlowVenueOracle$') &
  TEST_PID=$!
  while [ ! -e "${SEEDED_FILE}" ] && kill -0 "${TEST_PID}" 2>/dev/null; do sleep 1; done
  if [ ! -e "${SEEDED_FILE}" ]; then
    wait "${TEST_PID}" || true
    TEST_PID=""
    die "the corpus test ended before it seeded"
  fi
  owned_flip
  join_comma ${FORWARDED[@]+"${FORWARDED[@]}"} >"${FLIPPED_FILE}.tmp"
  mv "${FLIPPED_FILE}.tmp" "${FLIPPED_FILE}"
  status=0
  wait "${TEST_PID}" || status=$?
  TEST_PID=""
  [ "${status}" -eq 0 ] || exit "${status}"
else
  (cd "${ROOT}" && "${TEST_CMD[@]}")
fi

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
