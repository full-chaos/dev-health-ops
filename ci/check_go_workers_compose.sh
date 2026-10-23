#!/usr/bin/env bash
# Executed proof for the Go worker services of compose.yml.
#
# Starts the four River worker services (go-worker-heavy, -ops, -sync, -sync-provider) and the three stream
# services (go-stream-external, -ingest, -pagerduty) from images built from THIS tree, in a private compose project,
# and proves for each of them:
#
#   1. Its container carries only the Postgres DSN names of its own process class: the full `*_URI`/`*_URI_FILE`
#      names and the component groups (DEV_HEALTH_PG_*_HOST) with a non-empty value. The class is the service's
#      dev-health.io/worker-group label; the allowed set is deploy/go-workers/deployment.json secret_env for that
#      process, filtered by the same name pattern. Names only are printed, never a value.
#   2. It stays running with RestartCount 0, answers /healthz 200 and, within the wait budget, /readyz 200.
#
# The order mirrors a first start of the stack: data services, the application and River migrations and role
# provisioning, the poolers (their logins exist only after provisioning), the contract check and the four
# sync route activations, then the workers. Every step prints what it measured. The project is private to this run
# (-p) and is removed with its volumes on exit. CI only: compose.yml gives the data services fixed container names
# and host ports, so the proof overlays ci/go_workers_compose_proof.override.yml (none of them, and an ephemeral
# loopback port for each worker), and refuses a project that already has resources.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
MANIFEST="${ROOT}/deploy/go-workers/deployment.json"
# The suffix comes from /dev/urandom: the preflight below is check-then-use, and compose adopts a
# resource that already carries the project name, so the name must not be guessable.
PROJECT="${GO_WORKERS_COMPOSE_PROOF_PROJECT:-gwproof$$-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')}"
RIVER_WORKERS=(go-worker-heavy go-worker-ops go-worker-sync go-worker-sync-provider)
STREAM_WORKERS=(go-stream-external go-stream-ingest go-stream-pagerduty)
WORKERS=("${RIVER_WORKERS[@]}" "${STREAM_WORKERS[@]}")
ROUTE_ACTIVATIONS=(go-sync-dispatch-route-activate go-sync-finalize-route-activate go-sync-post-route-activate go-sync-reference-route-activate)
READY_SECONDS="${GO_WORKERS_PROOF_READY_SECONDS:-300}"
SETTLE_SECONDS="${GO_WORKERS_PROOF_SETTLE_SECONDS:-20}"

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || die "docker is required"
command -v curl >/dev/null 2>&1 || die "curl is required"
command -v jq >/dev/null 2>&1 || die "jq is required"
[ -f "${MANIFEST}" ] || die "${MANIFEST} not found"

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

# compose.yml interpolates this at parse time for a service the proof never starts.
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-go-workers-compose-proof}"
export COMMIT="${COMMIT:-0000000000000000000000000000000000000000}"

compose() {
  docker compose -p "${PROJECT}" -f "${ROOT}/compose.yml" -f "${SCRIPT_DIR}/go_workers_compose_proof.override.yml" "$@"
}

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- services at failure ---\n' >&2
    compose ps --all >&2 || true
    compose logs --no-color --tail 40 "${WORKERS[@]}" >&2 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
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

# DSN names a container carries: a `*_URI`/`*_URI_FILE` name (DATABASE or POSTGRES in it) or a component group whose
# HOST is non-empty, mapped to its DSN name. An empty value counts as absent. Names only.
carried_dsn_names() {
  docker inspect "$1" | jq -r '
    [ .[0].Config.Env[] | capture("^(?<n>[^=]+)=(?<v>.+)$"; "s") ] as $e
    | ( [ $e[] | .n | sub("_FILE$"; "") | select(test("_URI$") and test("DATABASE|POSTGRES")) ]
      + [ $e[] | select(.n == "DEV_HEALTH_PG_DOMAIN_HOST") | "POSTGRES_URI" ]
      + [ $e[] | select(.n == "DEV_HEALTH_PG_QUEUE_HOST") | "WORKER_DATABASE_URI" ]
      + [ $e[] | select(.n == "DEV_HEALTH_PG_COORDINATOR_HOST") | "COORDINATOR_DATABASE_URI" ]
      + [ $e[] | select(.n == "DEV_HEALTH_PG_API_HOST") | "API_DATABASE_URI" ]
      + [ $e[] | select(.n == "DEV_HEALTH_MIGRATION_PG_HOST") | "MIGRATION_DATABASE_URI" ] )
    | unique | .[]'
}

allowed_dsn_names() {
  jq -r --arg class "$1" '
    .processes[] | select(.name == $class) | .secret_env[]
    | sub("_FILE$"; "") | select(test("_URI$") and test("DATABASE|POSTGRES"))' "${MANIFEST}" | sort -u
}

assert_class_names() {
  local service="$1" id class carried allowed foreign
  id="$(compose ps -q "${service}")"
  [ -n "${id}" ] || die "${service} has no container"
  class="$(docker inspect --format '{{index .Config.Labels "dev-health.io/worker-group"}}' "${id}")"
  [ -n "${class}" ] || die "${service} carries no dev-health.io/worker-group label"
  carried="$(carried_dsn_names "${id}" | sort -u)"
  allowed="$(allowed_dsn_names "${class}")"
  [ -n "${allowed}" ] || die "deployment.json lists no DSN secret for class ${class}: the check would measure nothing"
  grep -qx 'POSTGRES_URI' <<<"${carried}" || die "${service} carries no domain DSN: the check measured nothing"
  foreign="$(comm -23 <(printf '%s\n' "${carried}") <(printf '%s\n' "${allowed}"))"
  printf '%s (class %s): carries [%s] allowed [%s] foreign [%s]\n' "${service}" "${class}" \
    "$(tr '\n' ' ' <<<"${carried}")" "$(tr '\n' ' ' <<<"${allowed}")" "$(tr '\n' ' ' <<<"${foreign}")"
  [ -z "${foreign}" ] || die "${service} (class ${class}) carries a DSN of another class: $(tr '\n' ' ' <<<"${foreign}")"
}

wait_ready() {
  local service="$1" port body_file status deadline
  port="$(compose port "${service}" 8080 | sed 's/.*://')"
  [[ "${port}" =~ ^[0-9]+$ ]] || die "could not read the ephemeral operator port of ${service} (${port})"
  body_file="$(mktemp)"
  deadline=$((SECONDS + READY_SECONDS))
  while :; do
    status="$(curl --silent --max-time 5 --output "${body_file}" --write-out '%{http_code}' "http://127.0.0.1:${port}/readyz" || true)"
    [ "${status}" = "200" ] && break
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      printf '%s /readyz answered %s after %ss; body:\n' "${service}" "${status}" "${READY_SECONDS}" >&2
      cat "${body_file}" >&2 || true
      rm -f "${body_file}"
      die "${service} did not become ready"
    fi
    sleep 5
  done
  rm -f "${body_file}"
  status="$(curl --silent --max-time 5 --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${port}/healthz" || true)"
  printf '%s: /readyz 200, /healthz %s (port %s)\n' "${service}" "${status}" "${port}"
  [ "${status}" = "200" ] || die "${service} /healthz answered ${status}, want 200"
}

assert_stable() {
  local service="$1" id state
  id="$(compose ps -q "${service}")"
  state="$(docker inspect --format '{{.State.Running}} {{.RestartCount}}' "${id}")"
  printf '%s: running/restarts = %s\n' "${service}" "${state}"
  [ "${state}" = "true 0" ] || die "${service} is not running with RestartCount 0 (${state})"
}

printf 'step 1: build the images this tree defines\n'
compose build go-worker-heavy go-river-migrate go-contractcheck

printf 'step 2: data services\n'
compose up --detach --wait --wait-timeout 600 postgres clickhouse valkey pgbouncer

printf 'step 3: migrations and River roles\n'
compose run --rm --no-deps migrate
compose run --rm --no-deps go-river-provision
compose run --rm --no-deps go-river-migrate

printf 'step 4: River poolers (their logins exist now)\n'
compose up --detach --wait --wait-timeout 300 --no-deps pgbouncer-river-queue pgbouncer-river-coordinator

printf 'step 5: contract check and sync route activations\n'
compose run --rm --no-deps go-contractcheck
for one_shot in "${ROUTE_ACTIVATIONS[@]}"; do
  compose run --rm --no-deps "${one_shot}"
done

printf 'step 6: start the %s workers\n' "${#WORKERS[@]}"
compose up --detach --no-deps --no-build "${WORKERS[@]}"
assert_project_prefixed

printf 'step 7: each worker carries only the DSN names of its class\n'
for service in "${WORKERS[@]}"; do
  assert_class_names "${service}"
done

printf 'step 8: each worker becomes ready and stays up\n'
for service in "${WORKERS[@]}"; do
  wait_ready "${service}"
done
sleep "${SETTLE_SECONDS}"
for service in "${WORKERS[@]}"; do
  assert_stable "${service}"
done

assert_project_prefixed
printf 'GO WORKERS COMPOSE PROOF PASSED\n'
