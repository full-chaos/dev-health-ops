#!/usr/bin/env bash
# Executed proof for the Auth Control Plane Compose wiring (CHAOS-4886).
#
# Brings up the dormant `auth` profile of compose.yml from images built from
# THIS tree, proves both binaries start clean, and proves the rollback leaves
# the rest of the stack alone:
#
#   1. `up --build --wait auth-service` runs auth-keys-init, auth-provision and
#      auth-migrate to completion, then auth-service until its exec-form
#      healthcheck (`auth-service --healthcheck`) reports healthy.
#   2. The operator listener answers /healthz and /readyz with 200 and /metrics
#      reports dev_health_runtime_ready 1, from the published loopback port.
#   3. `auth-migrate --check` (a second run of the built image, no DDL) reports
#      the schema at the embedded head.
#   4. The runtime login is not a superuser and owns nothing in the auth schema.
#   5. The signing key is mode 0400 and uid 65532 in its volume.
#   6. Rollback: the four auth services are stopped and removed; the postgres
#      service stays healthy, no auth container is left, and the default
#      (profile-less) service list names no auth service.
#
# Every step prints what it measured. The project is private to this run
# (-p) and is removed with its volumes on exit. CI only: compose.yml gives
# postgres a fixed container_name, so the proof overlays
# ci/auth_compose_proof.override.yml (no container_name, no published port) and
# asserts that every container it starts carries the project prefix.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
PROJECT="${AUTH_COMPOSE_PROOF_PROJECT:-authproof$$}"
AUTH_SERVICES=(auth-service auth-migrate auth-provision auth-keys-init)

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || die "docker is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

# Refuse before the cleanup trap exists: that trap would remove the project.
# A project name that already has containers would be recreated by `up` and removed by cleanup.
existing="$(docker ps -aq --no-trunc --filter "label=com.docker.compose.project=${PROJECT}")"
[ -z "${existing}" ] || die "the compose project ${PROJECT} already has containers; refusing to recreate or remove them"

# compose.yml interpolates this at parse time for a service the proof never
# starts; any non-empty value satisfies it.
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-auth-compose-proof}"
# Passwords with every URI-reserved character the DSNs must survive: the DSNs
# are keyword-form, so none of these may break a connection (a URI form failed
# on a trailing '#').
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-su#per@pw/1:2?3%4&5=6}"
export AUTH_RUNTIME_DATABASE_PASSWORD="${AUTH_RUNTIME_DATABASE_PASSWORD:-au#th@pw/1:2?3%4&5=6 x}"
export COMMIT="${COMMIT:-0000000000000000000000000000000000000000}"

compose() {
  docker compose -p "${PROJECT}" -f "${ROOT}/compose.yml" -f "${SCRIPT_DIR}/auth_compose_proof.override.yml" "$@"
}

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- auth services at failure ---\n' >&2
    compose --profile auth ps --all >&2 || true
    compose --profile auth logs --no-color --tail 60 "${AUTH_SERVICES[@]}" >&2 || true
  fi
  compose --profile auth down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "${status}"
}
trap cleanup EXIT

# Every container this run created (ids absent from the snapshot taken before anything started)
# must carry the project prefix. Called after each phase that can create one, and again at the end.
assert_project_prefixed() {
  local created id name
  created="$(comm -13 <(printf '%s\n' "${before}") <(docker ps -aq --no-trunc | sort))"
  for id in ${created}; do
    name="$(docker inspect --format '{{.Name}}' "${id}")"
    case "${name}" in "/${PROJECT}"[-_]*) ;; *) die "container ${name} does not carry the ${PROJECT} prefix" ;; esac
  done
  printf 'every container created so far carries the %s prefix: %s\n' "${PROJECT}" "$(printf '%s' "${created}" | wc -w | tr -d ' ')"
}

before="$(docker ps -aq --no-trunc | sort)"

printf 'step 1: up --build --wait auth-service\n'
compose --profile auth up --build --detach --wait --wait-timeout 600 auth-service
assert_project_prefixed
PORT="$(compose --profile auth port auth-service 8096 | sed 's/.*://')"
[[ "${PORT}" =~ ^[0-9]+$ ]] || die "could not read the ephemeral operator port (${PORT})"
printf 'operator port (ephemeral): %s\n' "${PORT}"

health="$(docker inspect --format '{{.State.Health.Status}}' "$(compose --profile auth ps -q auth-service)")"
printf 'auth-service container health: %s\n' "${health}"
[ "${health}" = "healthy" ] || die "auth-service container health is ${health}, want healthy"

for one_shot in auth-keys-init auth-provision auth-migrate; do
  code="$(docker inspect --format '{{.State.ExitCode}}' "$(compose --profile auth ps -a -q "${one_shot}")")"
  printf '%s exit code: %s\n' "${one_shot}" "${code}"
  [ "${code}" = "0" ] || die "${one_shot} exited ${code}, want 0"
done

printf 'step 2: operator listener\n'
base="http://127.0.0.1:${PORT}"
for endpoint in healthz readyz; do
  status="$(curl --silent --show-error --max-time 5 --output /dev/stderr --write-out '%{http_code}' "${base}/${endpoint}")"
  printf '\nGET /%s -> %s\n' "${endpoint}" "${status}"
  [ "${status}" = "200" ] || die "/${endpoint} answered ${status}, want 200"
done
metrics="$(curl --silent --show-error --max-time 5 "${base}/metrics")"
grep -E '^dev_health_runtime_ready(\{[^}]*\})? 1$' <<<"${metrics}" \
  || die "/metrics did not report dev_health_runtime_ready 1"
grep -E '^dev_health_runtime_live(\{[^}]*\})? 1$' <<<"${metrics}" \
  || die "/metrics did not report dev_health_runtime_live 1"

printf 'step 3: auth-migrate --check\n'
check_output="$(compose --profile auth run --rm --no-deps auth-migrate --check)"
printf '%s\n' "${check_output}"
grep -E '^auth schema current at version [0-9]+$' <<<"${check_output}" >/dev/null \
  || die "auth-migrate --check did not report the schema at its head"

printf 'step 4: runtime login posture\n'
posture="$(compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At \
  -c "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = '${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth}'")"
printf 'rolsuper|rolcreatedb|rolcreaterole|rolreplication|rolbypassrls = %s\n' "${posture}"
[ "${posture}" = "f|f|f|f|f" ] || die "runtime login holds an elevated attribute: ${posture}"
owned="$(compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At \
  -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner WHERE n.nspname = '${AUTH_DATABASE_SCHEMA:-auth}' AND r.rolname = '${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth}'")"
printf 'objects in the auth schema owned by the runtime login: %s\n' "${owned}"
[ "${owned}" = "0" ] || die "runtime login owns ${owned} objects in the auth schema"

printf 'step 4b: provisioning de-escalates a pre-existing elevated login\n'
compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -c "ALTER ROLE ${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth} CREATEDB CREATEROLE"
compose --profile auth run --rm --no-deps auth-provision
posture="$(compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -At \
  -c "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = '${AUTH_RUNTIME_DATABASE_ROLE:-devhealth_auth}'")"
printf 'posture after re-provisioning an elevated login = %s\n' "${posture}"
[ "${posture}" = "f|f|f|f|f" ] || die "re-provisioning left an elevated attribute: ${posture}"

printf 'step 4c: network boundary\n'
nets="$(docker inspect --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$(compose --profile auth ps -q auth-service)")"
printf 'auth-service networks: %s\n' "${nets}"
[ "${nets}" = "${PROJECT}_auth_plane " ] || die "auth-service is on '${nets}', want only ${PROJECT}_auth_plane"
members="$(docker network inspect "${PROJECT}_auth_plane" --format '{{range .Containers}}{{.Name}} {{end}}' | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ')"
printf 'auth_plane members: %s\n' "${members}"
expected="$(compose --profile auth ps -q auth-service postgres | xargs docker inspect --format '{{.Name}}' | tr -d '/' | sort | tr '\n' ' ')"
[ "${members}" = "${expected}" ] || die "auth_plane has members other than auth-service and postgres: ${members} (want ${expected})"

printf 'step 4d: no auth service is on the default network\n'
compose --profile auth config --format json | python3 -c '
import json, sys
services = json.load(sys.stdin)["services"]
bad = []
for name in ("auth-keys-init", "auth-provision", "auth-migrate", "auth-service"):
    service = services[name]
    nets = sorted((service.get("networks") or {}).keys())
    mode = service.get("network_mode")
    print(f"{name}: networks={nets} network_mode={mode}")
    if mode != "none" and nets != ["auth_plane"]:
        bad.append(name)
if bad:
    sys.exit("auth services outside auth_plane (or network_mode none): " + ", ".join(bad))
' || die "an auth service is attached to a network other than auth_plane"

printf 'step 4e: no secret reaches a process argument\n'
compose --profile auth config --format json | python3 -c '
import json, re, sys
services = json.load(sys.stdin)["services"]
bad = []
for name in ("auth-keys-init", "auth-provision", "auth-migrate", "auth-service"):
    service = services[name]
    argv = list(service.get("entrypoint") or []) + list(service.get("command") or [])
    text = " ".join(argv)
    hit = re.search(r"\$\{?[A-Za-z_]*(PASSWORD|SECRET|TOKEN|KEY)(?!_ID|_FILE)", text)
    print(f"{name}: argv expands a secret variable: {bool(hit)}")
    if hit:
        bad.append(name)
if bad:
    sys.exit("secret variable expanded into a process argument: " + ", ".join(bad))
' || die "a secret reaches a process argument (visible to docker top)"

printf 'step 5: signing key custody\n'
key_stat="$(docker run --rm --name "${PROJECT}-keystat" --entrypoint stat -v "${PROJECT}_auth_signing_keys:/k:ro" \
  postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15 \
  -c '%a %u' /k/private/envelope-private-key.pem)"
printf 'key mode and uid: %s\n' "${key_stat}"
assert_project_prefixed
[ "${key_stat}" = "400 65532" ] || die "signing key is '${key_stat}', want '400 65532'"

printf 'step 6: rollback\n'
compose --profile auth rm --stop --force "${AUTH_SERVICES[@]}"
left="$(compose --profile auth ps -a -q "${AUTH_SERVICES[@]}" | wc -l | tr -d ' ')"
printf 'auth containers left: %s\n' "${left}"
[ "${left}" = "0" ] || die "${left} auth containers remain after rollback"
pg_health="$(docker inspect --format '{{.State.Health.Status}}' "$(compose ps -q postgres)")"
printf 'postgres health after rollback: %s\n' "${pg_health}"
[ "${pg_health}" = "healthy" ] || die "postgres is ${pg_health} after rollback, want healthy"
if compose config --services | grep -E '^auth-' >/dev/null; then
  die "the default (profile-less) service list names an auth service"
fi
printf 'default service list names no auth service\n'

assert_project_prefixed
gone="$(comm -23 <(printf '%s\n' "${before}") <(docker ps -aq --no-trunc | sort))"
[ -z "${gone}" ] || die "containers outside the proof project disappeared: ${gone}"
printf 'AUTH COMPOSE PROOF PASSED\n'
