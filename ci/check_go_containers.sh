#!/usr/bin/env bash
# Build and smoke-test the additive Go worker images without touching the shared
# development Compose project. Reproducibility is checked with fixed metadata.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
DOCKERFILE="${ROOT}/docker/go-worker.Dockerfile"
IMAGE_PREFIX="${DEV_HEALTH_GO_IMAGE_PREFIX:-dev-health-go}"

readonly VERSION="phase1-ci"
readonly COMMIT="0000000000000000000000000000000000000000"
readonly BUILD_TIME="1970-01-01T00:00:00Z"
readonly SOURCE_DATE_EPOCH="0"
readonly RUNTIME_TARGETS=(worker scheduler reconciler stream-runner)
readonly ALL_TARGETS=(worker scheduler reconciler stream-runner operator contractcheck)
readonly CONTAINER_SECURITY_ARGS=(
  --read-only
  --cap-drop ALL
  --security-opt no-new-privileges
)
ACTIVE_CONTAINER=""

usage() {
  cat <<'EOF'
Usage: ci/check_go_containers.sh [smoke|reproducible|all]

  smoke         Build every worker target and verify non-root runtime behavior.
  reproducible  Build every target twice from scratch and compare image IDs.
  all           Run smoke and reproducibility checks (default).
EOF
}

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 2
}

command -v docker >/dev/null 2>&1 || die "docker is required"
command -v curl >/dev/null 2>&1 || die "curl is required"
[ -f "${DOCKERFILE}" ] || die "missing ${DOCKERFILE}"

cleanup_active_container() {
  if [ -n "${ACTIVE_CONTAINER}" ]; then
    docker rm --force "${ACTIVE_CONTAINER}" >/dev/null 2>&1 || true
    ACTIVE_CONTAINER=""
  fi
}

trap cleanup_active_container EXIT

build_target() {
  local target="$1"
  local tag="$2"
  shift 2

  docker build \
    --file "${DOCKERFILE}" \
    --target "${target}" \
    --build-arg "VERSION=${VERSION}" \
    --build-arg "COMMIT=${COMMIT}" \
    --build-arg "BUILD_TIME=${BUILD_TIME}" \
    --build-arg "SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}" \
    --tag "${tag}" \
    "$@" \
    "${ROOT}"
}

wait_for_status() {
  local url="$1"
  local expected="$2"
  local attempts=0
  local status
  while [ "${attempts}" -lt 100 ]; do
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 1 "${url}" 2>/dev/null || true)"
    if [ "${status}" = "${expected}" ]; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 0.05
  done
  return 1
}

smoke_target() {
  local target="$1"
  local tag="${IMAGE_PREFIX}-${target}:ci"
  local container_name="dev-health-go-${target}-smoke-$$"
  local published_address
  local exit_code
  local readiness_body
  local dependency
  local dependencies

  build_target "${target}" "${tag}"

  [ "$(docker image inspect --format '{{.Config.User}}' "${tag}")" = "65532:65532" ] \
    || die "${target} image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "${target} did not report injected version metadata"

  ACTIVE_CONTAINER="${container_name}"
  docker run --detach \
    --name "${container_name}" \
    --publish "127.0.0.1::8080" \
    "${CONTAINER_SECURITY_ARGS[@]}" \
    "${tag}" >/dev/null
  published_address="$(docker port "${container_name}" 8080/tcp | head -n 1)"
  [ -n "${published_address}" ] || die "${target} did not publish its operator port"

  wait_for_status "http://${published_address}/healthz" 200 \
    || die "${target} health endpoint did not become available"
  # Foundation binaries deliberately remain live but fail readiness until
  # their required runtime dependencies are configured.
  wait_for_status "http://${published_address}/readyz" 503 \
    || die "${target} reported ready without required dependencies"
  readiness_body="$(curl --silent --show-error --max-time 1 "http://${published_address}/readyz")"
  if [ "${target}" = "worker" ]; then
    for dependency in domain_postgres profile_completeness queue_postgres river_schema; do
      grep -F "\"${dependency}\"" <<<"${readiness_body}" >/dev/null \
        || die "worker readiness omitted ${dependency}"
    done
    if grep -F '"job_registry"' <<<"${readiness_body}" >/dev/null; then
      die "worker image could not load its packaged job contract artifacts"
    fi
  else
    case "${target}" in
      scheduler)
        dependencies="domain_postgres queue_postgres river_schema scheduler_loop"
        ;;
      reconciler)
        dependencies="domain_postgres queue_postgres reconciler_loop river_schema"
        ;;
      stream-runner)
        dependencies="clickhouse domain_postgres stream_consumer valkey"
        ;;
      *)
        die "no readiness contract declared for ${target}"
        ;;
    esac
    for dependency in ${dependencies}; do
      grep -F "\"${dependency}\"" <<<"${readiness_body}" >/dev/null \
        || die "${target} readiness omitted ${dependency}"
    done
  fi
  wait_for_status "http://${published_address}/metrics" 200 \
    || die "${target} metrics endpoint did not become available"

  docker stop --time 5 "${container_name}" >/dev/null
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container_name}")"
  [ "${exit_code}" = "0" ] || die "${target} exited with status ${exit_code}"
  cleanup_active_container
}

smoke() {
  local target
  for target in "${RUNTIME_TARGETS[@]}"; do
    printf 'container smoke: %s\n' "${target}"
    smoke_target "${target}"
  done

  printf 'container smoke: contractcheck\n'
  build_target contractcheck "${IMAGE_PREFIX}-contractcheck:ci"
  [ "$(docker image inspect --format '{{.Config.User}}' "${IMAGE_PREFIX}-contractcheck:ci")" = "65532:65532" ] \
    || die "contractcheck image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-contractcheck:ci" validate \
    | grep -F "worker contracts valid" >/dev/null \
    || die "contractcheck image did not validate its embedded contract artifacts"

  printf 'container smoke: operator\n'
  build_target operator "${IMAGE_PREFIX}-operator:ci"
  [ "$(docker image inspect --format '{{.Config.User}}' "${IMAGE_PREFIX}-operator:ci")" = "65532:65532" ] \
    || die "operator image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-operator:ci" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "operator did not report injected version metadata"
}

reproducible() {
  local target
  local first_tag
  local second_tag
  local first_id
  local second_id

  for target in "${ALL_TARGETS[@]}"; do
    first_tag="${IMAGE_PREFIX}-${target}:repro-first"
    second_tag="${IMAGE_PREFIX}-${target}:repro-second"
    build_target "${target}" "${first_tag}" --no-cache --provenance=false
    build_target "${target}" "${second_tag}" --no-cache --provenance=false
    first_id="$(docker image inspect --format '{{.Id}}' "${first_tag}")"
    second_id="$(docker image inspect --format '{{.Id}}' "${second_tag}")"
    [ "${first_id}" = "${second_id}" ] \
      || die "${target} image is not reproducible: ${first_id} != ${second_id}"
    printf 'container reproducibility: %s %s\n' "${target}" "${first_id}"
  done
}

case "${1:-all}" in
  smoke)
    smoke
    ;;
  reproducible)
    reproducible
    ;;
  all)
    smoke
    reproducible
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
