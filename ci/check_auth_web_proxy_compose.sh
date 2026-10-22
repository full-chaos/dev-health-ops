#!/usr/bin/env bash
# Executed proof that Web reaches the Go auth-service THROUGH go-api's auth proxy (CHAOS-10230).
#
# Brings up go-api and auth-service in a private compose project with the proxy flags ON (only in
# ci/auth_webproof_compose.override.yml), seeds one user through go-api's own register route
# (auth-service sign-up stays gated), then runs the REAL dev-health-web src/lib/auth.ts (Auth.js
# authorize + jwt refresh callbacks, vitest) against go-api, and proves from the database that the
# session was written by auth-service:
#
#   1. the Web login, refresh and logout all succeed (tests/auth-proxy in dev-health-web);
#   2. auth.sessions holds a row for the user, revoked after the logout;
#   3. auth.refresh_credentials rows exist and none is live after the logout;
#   4. the auth runtime role reads public.users (the 11 manifest columns) only (CHAOS-10224 grants);
#   5. public.refresh_tokens holds no row for the user: go-api's own handlers never ran.
#
# Needs AUTH_PROOF_WEB_DIR: a dev-health-web checkout with dependencies installed. CI only.
# The project is private to this run and removed with its volumes on exit.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# The suffix comes from /dev/urandom: the preflight below is check-then-use, and compose adopts a
# resource that already carries the project name, so the name must not be guessable.
PROJECT="${AUTH_WEB_PROXY_PROOF_PROJECT:-authwebproof$$-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')}"

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || die "docker is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

# Refuse before the cleanup trap exists (it would remove the project): compose recreates the
# containers of a reused project name and cleanup deletes its volumes and networks, so any
# container, volume or network that carries the project's label or name is a stop.
refuse_if_present() {
  local kind="$1" found
  shift
  found="$("$@")"
  [ -z "${found}" ] || die "a ${kind} of the compose project ${PROJECT} exists; refusing to recreate or remove it"
}
refuse_if_present container docker ps -aq --no-trunc --filter "label=com.docker.compose.project=${PROJECT}"
refuse_if_present container docker ps -aq --no-trunc --filter "name=^/${PROJECT}[-_]"
refuse_if_present volume docker volume ls -q --filter "label=com.docker.compose.project=${PROJECT}"
refuse_if_present volume docker volume ls -q --filter "name=^${PROJECT}_"
refuse_if_present network docker network ls -q --filter "label=com.docker.compose.project=${PROJECT}"
refuse_if_present network docker network ls -q --filter "name=^${PROJECT}_"

# compose.yml interpolates this at parse time for a service the proof never
# starts; any non-empty value satisfies it.
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-auth-compose-proof}"
# Passwords with every URI-reserved character the DSNs must survive: the DSNs
# are keyword-form, so none of these may break a connection (a URI form failed
# on a trailing '#').
# go-api's chain builds a URL from this password (POSTGRES_URI), so here it is URI-safe; the auth
# runtime password below keeps every reserved character (keyword-form DSN).
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')}"
export AUTH_RUNTIME_DATABASE_PASSWORD="${AUTH_RUNTIME_DATABASE_PASSWORD:-au#th@pw/1:2?3%4&5=6 x}"
export COMMIT="${COMMIT:-0000000000000000000000000000000000000000}"

compose() {
  docker compose -p "${PROJECT}" -f "${ROOT}/compose.yml" -f "${SCRIPT_DIR}/auth_compose_proof.override.yml" -f "${SCRIPT_DIR}/auth_webproof_compose.override.yml" "$@"
}

SINK_PID=""
SINK_DIR=""
cleanup() {
  local status=$?
  [ -z "${SINK_PID}" ] || kill "${SINK_PID}" >/dev/null 2>&1 || true
  [ -z "${SINK_DIR}" ] || rm -rf "${SINK_DIR}"
  if [ "${status}" -ne 0 ]; then
    printf '\n--- auth services at failure ---\n' >&2
    compose --profile auth ps --all >&2 || true
    compose --profile auth logs --no-color --tail 80 >&2 || true
  fi
  compose --profile auth down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "${status}"
}
trap cleanup EXIT

# Every container of this project must carry the project prefix. Ownership is the project label,
# never a host-wide before/after diff: other users of a shared host create and remove containers.
assert_project_prefixed() {
  local id name count=0
  for id in $(docker ps -aq --no-trunc --filter "label=com.docker.compose.project=${PROJECT}"); do
    name="$(docker inspect --format '{{.Name}}' "${id}")"
    case "${name}" in "/${PROJECT}"[-_]*) ;; *) die "container ${name} does not carry the ${PROJECT} prefix" ;; esac
    count=$((count + 1))
  done
  printf 'every container of project %s carries the prefix: %s\n' "${PROJECT}" "${count}"
}



AUTH_SESSION_SUCCESSOR_KEY="${AUTH_SESSION_SUCCESSOR_KEY:-$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')}"
export AUTH_SESSION_SUCCESSOR_KEY
AUTH_LOGIN_LOCKOUT_KEY="${AUTH_LOGIN_LOCKOUT_KEY:-$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')}"
export AUTH_LOGIN_LOCKOUT_KEY
# go-api reads this auth-session-issuer credential when it asks auth-service for a session (sessions flag on); its
# row is stored once the stack is up. Built at run time, never printed.
AUTH_SESSION_ISSUE_TOKEN="svc_authsess_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
[[ "${AUTH_SESSION_ISSUE_TOKEN}" =~ ^svc_authsess_[A-Za-z0-9_-]{43}$ ]] || die "could not build a session issue credential"
export AUTH_SESSION_ISSUE_TOKEN
WEB_DIR="${AUTH_PROOF_WEB_DIR:?AUTH_PROOF_WEB_DIR must name a dev-health-web checkout}"
[ -f "${WEB_DIR}/vitest.auth-proxy.config.ts" ] || die "${WEB_DIR} has no vitest.auth-proxy.config.ts (the web side of CHAOS-10230)"

psql_one() {
  compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At -c "$1"
}

command -v node >/dev/null 2>&1 || die "node is required (SMTP sink)"
SINK_DIR="$(mktemp -d)"
node "${SCRIPT_DIR}/auth_webproof_smtp_sink.mjs" "${SINK_DIR}/port" "${SINK_DIR}/mail" &
SINK_PID=$!
for _ in $(seq 1 50); do [ -s "${SINK_DIR}/port" ] && break; sleep 0.1; done
AUTH_PROOF_SMTP_PORT="$(cat "${SINK_DIR}/port")"
[[ "${AUTH_PROOF_SMTP_PORT}" =~ ^[0-9]+$ ]] || die "the SMTP sink did not start"
export AUTH_PROOF_SMTP_PORT
printf 'SMTP sink pid %s on port %s\n' "${SINK_PID}" "${AUTH_PROOF_SMTP_PORT}"

printf 'step 1: up --build --wait go-api auth-service\n'
compose --profile auth up --build --detach --wait --wait-timeout 900 go-api auth-service
assert_project_prefixed
compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" >/dev/null \
  < <(printf '\\set token %s\n' "'${AUTH_SESSION_ISSUE_TOKEN}'"; cat <<'SQL'
INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
VALUES (gen_random_uuid(), 'auth-session-issuer', encode(sha256(convert_to(:'token', 'UTF8')), 'hex'), left(:'token', 16),
        '["sessions:issue"]', now());
SQL
  ) \
  || die "could not store the session issue credential"
API_PORT="$(compose port go-api 8000 | sed 's/.*://')"
[[ "${API_PORT}" =~ ^[0-9]+$ ]] || die "could not read the ephemeral go-api port (${API_PORT})"
BASE="http://127.0.0.1:${API_PORT}"
printf 'go-api (ephemeral): %s\n' "${BASE}"

# The origin check of /api/v1/auth/register stays on: the request carries the origin the real Web
# client sends, which compose.yml allows by default (CORS_ALLOWED_ORIGINS).
WEB_ORIGIN="${AUTH_PROOF_WEB_ORIGIN:-http://localhost:3000}"
printf 'step 2: seed one verified user through go-api register\n'
EMAIL="webproof-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')@example.com"
PASSWORD="Pw-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
status="$(curl --silent --show-error --max-time 20 --output /dev/stderr --write-out '%{http_code}' \
  -H 'Content-Type: application/json' -H "Origin: ${WEB_ORIGIN}" \
  --data "$(printf '{"email":"%s","password":"%s","full_name":"Web Proof","org_name":"Web Proof Org"}' "${EMAIL}" "${PASSWORD}")" \
  "${BASE}/api/v1/auth/register")"
printf '\nPOST /api/v1/auth/register -> %s\n' "${status}"
[ "${status}" = "200" ] || [ "${status}" = "201" ] || die "register answered ${status}"
token=""
for _ in $(seq 1 30); do
  if [ -s "${SINK_DIR}/mail.token" ]; then
    token="$(cat "${SINK_DIR}/mail.token")"
    break
  fi
  sleep 1
done
[ -n "${token}" ] || die "the captured mail carried no verification token"
status="$(curl --silent --show-error --max-time 20 --output /dev/null --write-out '%{http_code}' "${BASE}/api/v1/auth/verify?token=${token}")"
printf 'GET /api/v1/auth/verify -> %s\n' "${status}"
[ "${status}" = "200" ] || die "verify answered ${status}"
USER_ID="$(psql_one "SELECT id FROM public.users WHERE email = '${EMAIL}'")"
[[ "${USER_ID}" =~ ^[0-9a-f-]{36}$ ]] || die "the seeded user has no id (${USER_ID})"
printf 'seeded user %s\n' "${USER_ID}"

printf 'step 2b: the auth runtime role holds the public read grants and nothing wider (CHAOS-10224)\n'
ROLE="${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth}"
for col in id email password_hash full_name username auth_provider auth_provider_id is_active is_verified is_superuser token_version last_login_at; do
  can="$(psql_one "SELECT has_column_privilege('${ROLE}', 'public.users', '${col}', 'SELECT')")"
  printf 'SELECT public.users.%s: %s\n' "${col}" "${can}"
  [ "${can}" = t ] || die "the auth runtime role cannot read public.users.${col}"
done
for col in avatar_url created_at updated_at; do
  can="$(psql_one "SELECT has_column_privilege('${ROLE}', 'public.users', '${col}', 'SELECT')")"
  printf 'SELECT public.users.%s: %s\n' "${col}" "${can}"
  [ "${can}" = f ] || die "the auth runtime role can read public.users.${col}, outside the 10224 manifest"
done

before="$(psql_one "SELECT count(*) FROM public.refresh_tokens WHERE user_id = '${USER_ID}'")"
printf 'public.refresh_tokens rows for the user before the Web run: %s\n' "${before}"

printf 'step 3: the real Web auth code: login, refresh, logout\n'
(
  cd "${WEB_DIR}"
  AUTH_PROOF_BACKEND_URL="${BASE}" AUTH_PROOF_EMAIL="${EMAIL}" AUTH_PROOF_PASSWORD="${PASSWORD}" \
    pnpm exec vitest run --config vitest.auth-proxy.config.ts
)

printf 'step 4a: the proxy metric counts forwarded calls\n'
OPS_PORT="$(compose port go-api 8080 | sed 's/.*://')"
[[ "${OPS_PORT}" =~ ^[0-9]+$ ]] || die "could not read the ephemeral go-api operator port (${OPS_PORT})"
proxy_metrics="$(curl --silent --show-error --max-time 5 "http://127.0.0.1:${OPS_PORT}/metrics" | grep -a '^devhealth_api_auth_proxy_requests_total' || true)"
printf '%s\n' "${proxy_metrics}"
for aggregate in local_login sessions; do
  grep -E "aggregate=\"${aggregate}\"" <<<"${proxy_metrics}" | grep -E 'outcome="forwarded"' | grep -vE ' 0$' >/dev/null \
    || die "no forwarded ${aggregate} request was counted: the proxy did not carry the Web calls"
done
if grep -E 'outcome="upstream_error"' <<<"${proxy_metrics}" | grep -vE ' 0$'; then
  die "the proxy counted an upstream error"
fi

printf 'step 4: the sessions were written by auth-service, not by go-api\n'
sessions="$(psql_one "SELECT count(*) FROM auth.sessions WHERE principal_id = '${USER_ID}'")"
revoked="$(psql_one "SELECT count(*) FROM auth.sessions WHERE principal_id = '${USER_ID}' AND revoked_at IS NOT NULL")"
after="$(psql_one "SELECT count(*) FROM public.refresh_tokens WHERE user_id = '${USER_ID}'")"
printf 'auth.sessions rows: %s, revoked: %s; public.refresh_tokens rows after: %s\n' "${sessions}" "${revoked}" "${after}"
[ "${sessions}" -ge 1 ] || die "auth.sessions has no row for the user: auth-service did not serve the login"
creds="$(psql_one "SELECT count(*) FROM auth.refresh_credentials WHERE principal_id = '${USER_ID}'")"
live="$(psql_one "SELECT count(*) FROM auth.refresh_credentials WHERE principal_id = '${USER_ID}' AND consumed_at IS NULL AND revoked_at IS NULL")"
printf 'auth.refresh_credentials rows: %s, still live: %s\n' "${creds}" "${live}"
[ "${creds}" -ge 1 ] || die "auth.refresh_credentials has no row for the user"
[ "${live}" = 0 ] || die "${live} refresh credential(s) are neither consumed nor revoked after the logout"
[ "${revoked}" = "${sessions}" ] || die "the logout left ${sessions} session(s) of which ${revoked} are revoked"
[ "${after}" = "${before}" ] || die "go-api wrote public.refresh_tokens (${before} -> ${after}): its own handlers served a call"

printf 'step 5: flags OFF in the default stack\n'
if grep -E '^\s*AUTH_PROXY_(SESSIONS|LOCAL_LOGIN|CREDENTIALS|SOCIAL|OIDC|SAML)\s*[:=]\s*"?(true|1|on)' "${ROOT}/compose.yml"; then
  die "compose.yml turns an auth proxy flag on"
fi
printf 'OK\n'
