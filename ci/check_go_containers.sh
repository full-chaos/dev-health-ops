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
readonly ALL_TARGETS=(worker scheduler reconciler stream-runner operator contractcheck migrate)
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
  local profile_env

  build_target "${target}" "${tag}"

  [ "$(docker image inspect --format '{{.Config.User}}' "${tag}")" = "65532:65532" ] \
    || die "${target} image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "${target} did not report injected version metadata"

  ACTIVE_CONTAINER="${container_name}"
  # A profile is startup configuration, not a runtime dependency, so a target
  # that requires one needs it even for this deliberately-unconfigured run.
  # CUT-02 removed dev-health-worker's DefaultProfile on purpose: every profile
  # it accepts owns registered job kinds, so a River consumer with nothing
  # registered is unrepresentable rather than permanently unready (see
  # cmd/dev-health-worker/main.go). Without DEV_HEALTH_PROFILE the worker now
  # exits with `configuration error: profile must be one of sync, heavy, ops`
  # before it ever binds :8080, and this check then reported only the much less
  # obvious "no public port '8080/tcp' published".
  #
  # This must be set per target, not globally: DEV_HEALTH_PROFILE takes
  # precedence OVER a spec's DefaultProfile (internal/platform/config's
  # profile()), and scheduler and reconciler declare no Profiles at all, so any
  # value at all makes them fail with "does not accept a profile". So pass the
  # worker's required profile, leave stream-runner on its own default, and pass
  # nothing to the two that accept nothing.
  profile_env=()
  case "${target}" in
    worker) profile_env=(--env "DEV_HEALTH_PROFILE=sync") ;;
  esac
  docker run --detach \
    --name "${container_name}" \
    --publish "127.0.0.1::8080" \
    "${profile_env[@]}" \
    "${CONTAINER_SECURITY_ARGS[@]}" \
    "${tag}" >/dev/null
  published_address="$(docker port "${container_name}" 8080/tcp | head -n 1)"
  if [ -z "${published_address}" ]; then
    # Surface why the container is gone rather than only that the port is
    # missing: startup configuration errors are the likeliest cause and they
    # are invisible in the port lookup's own failure message.
    printf 'container %s exited before publishing :8080; its output was:\n' "${target}" >&2
    docker logs "${container_name}" 2>&1 | tail -20 >&2
    die "${target} did not publish its operator port"
  fi

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
    if [ "${target}" = "reconciler" ] && grep -F '"job_registry"' <<<"${readiness_body}" >/dev/null; then
      die "reconciler image could not load its packaged job contract artifacts"
    fi
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
  local migrate_stderr
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

  # migrate (cmd/dev-health-worker-migrate) is a one-shot job, not a
  # long-running service: it has no readiness surface to smoke-test against,
  # so -- like contractcheck and operator above -- it is special-cased rather
  # than run through smoke_target. --version is its only no-op mode that
  # requires neither a live database nor MIGRATION_DATABASE_URI (--check
  # connects to PostgreSQL and applies/validates the pinned River schema, so
  # it cannot run here); running with no arguments at all still proves real
  # exit behavior by failing closed on the missing required configuration.
  printf 'container smoke: migrate\n'
  build_target migrate "${IMAGE_PREFIX}-migrate:ci"
  [ "$(docker image inspect --format '{{.Config.User}}' "${IMAGE_PREFIX}-migrate:ci")" = "65532:65532" ] \
    || die "migrate image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-migrate:ci" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "migrate did not report injected version metadata"
  # Assert the *specific* diagnostic, not merely a nonzero exit: a panic, a
  # missing shared library, or any unrelated startup regression also exits
  # nonzero, so `if ! docker run` alone would green-light an unusable image.
  migrate_stderr="$(docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-migrate:ci" 2>&1 >/dev/null)" \
    && die "migrate did not fail closed without MIGRATION_DATABASE_URI"
  printf '%s' "${migrate_stderr}" \
    | grep -F 'configuration error: MIGRATION_DATABASE_URI is required' >/dev/null \
    || die "migrate did not report the missing MIGRATION_DATABASE_URI diagnostic"
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
