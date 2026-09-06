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

# CODEX ROUND 2 (P1, accepted-and-documented, team-lead ruling): `RUN
# --mount=type=cache` mounts (/go/pkg/mod, /root/.cache/go-build) survive
# --no-cache by BuildKit design -- that's the entire point of a cache mount,
# and there is no per-invocation-unique cache id here (that would defeat
# this file's whole reason to exist: it would force a fresh `go mod
# download` every pass, reintroducing close to the O(14)-compile cost this
# fix removes). If a FUTURE instruction ever copies cache-mount-derived
# content into a real image path (a COPY, `cp`, or `mv` referencing either
# path OUTSIDE the `--mount=type=cache` declaration line itself), a
# non-deterministic value could ride the persistent cache mount straight
# past a --no-cache rebuild undetected -- exactly the class of bug already
# fixed twice above, one mechanism further down. No such instruction exists
# today (only compiled binaries under /out, which live outside both cache
# mounts, are ever copied into /runtime). This guard is what turns "no such
# instruction exists today" into an enforced, checked fact rather than an
# assumption that quietly stops being true.
assert_cache_mounts_not_copied_out() {
  local path leaked joined residual
  # CODEX ROUND 3 (P1, real false-negative in the round-2 guard): the
  # original version matched PATH and a copy verb on the SAME PHYSICAL
  # line, so a multi-line RUN -- this repo's own house style, e.g.
  #   RUN --mount=type=cache,target=/go/pkg/mod \
  #       cp \
  #       /go/pkg/mod/.leak \
  #       /runtime/worker/leak
  # -- defeats it entirely: the mount clause is on its own line (filtered
  # out), the verb "cp" is on a line with no path text, and the path is on
  # a line with no verb text. Fixed by joining backslash-continued
  # physical lines into one logical line PER INSTRUCTION first, so a leak
  # spread across several lines is checked as a whole. Joining first would
  # also put the mount clause's OWN legitimate `target=<path>` text on the
  # same logical line as everything else in that RUN -- so the exclusion
  # can no longer be "skip the whole line if it mentions --mount=type=cache
  # anywhere" (that would blind the guard to a real leak in the same RUN
  # as a legitimate mount) -- it strips only the mount clause's own token
  # before checking whether the path is ALSO referenced elsewhere by a
  # copy verb in that same instruction.
  #
  # CONFIRMATION PASS (P2): the join above inserted a SPACE at every join
  # point regardless of what was actually there. Real Dockerfile line
  # continuation does not add anything -- it removes the backslash+newline
  # and concatenates the two spans EXACTLY as written, so a continuation
  # that splits the cache path's own TEXT mid-word (no whitespace on
  # either side of the join, e.g. `cp /go/pkg/\` + `mod/.leak ...`)
  # reconstructs to the real, single-word path `/go/pkg/mod/.leak` in an
  # actual build, but the space-inserting join produced `/go/pkg/ mod/.leak`
  # -- two words -- which the literal-substring check on `/go/pkg/mod`
  # never matches. This is not a new heuristic, it's the join finally
  # matching Dockerfile's actual continuation semantics: concatenate
  # verbatim, add nothing. Ordinary multi-line style (each token on its
  # own line, a space already present before the trailing backslash) is
  # unaffected, since that space is part of the line's own text and is
  # preserved by the join either way -- only the artificial EXTRA space
  # this fix removes was ever wrong.
  joined="$(awk '{ if (sub(/\\[[:space:]]*$/, "")) { printf "%s", $0; next } print }' "${DOCKERFILE}")"
  for path in /go/pkg/mod /root/.cache/go-build; do
    residual="$(printf '%s\n' "${joined}" | sed -E 's/--mount=type=cache[^[:space:]]*//g')"
    leaked="$(printf '%s\n' "${residual}" | grep -F -- "${path}" | grep -E 'COPY|cp |mv ')" || true
    [ -z "${leaked}" ] \
      || die "cache-mount path ${path} is referenced by a COPY/cp/mv outside its --mount=type=cache declaration -- cache mounts survive --no-cache by design, so this could leak stale content past the reproducibility check undetected: ${leaked}"
  done
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
  local startup_env

  build_target "${target}" "${tag}"

  [ "$(docker image inspect --format '{{.Config.User}}' "${tag}")" = "65532:65532" ] \
    || die "${target} image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "${target} did not report injected version metadata"
  # The root Compose worker service inherits a pre-stop route rollback hook.
  # It overrides the worker entrypoint with this binary, so package it in the
  # worker target and prove that exact override is executable.
  if [ "${target}" = "worker" ]; then
    docker run --rm --entrypoint /usr/local/bin/dev-health-workerctl \
      "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
      | grep -F '"version":"phase1-ci"' >/dev/null \
      || die "worker image does not package the Compose lifecycle operator"
  fi

  ACTIVE_CONTAINER="${container_name}"
  # The worker requires an explicit queue set and an exact per-queue concurrency
  # map even for this deliberately unconfigured dependency run. Keep these
  # target-specific: scheduler and reconciler do not consume River queues, and
  # the stream runner has its own separate stream-profile contract.
  #
  # Queue topology is flag-only (CHAOS-3875), so it is passed as an argument
  # after the image, exactly the way every deploy artifact passes it. The
  # worker target declares an ENTRYPOINT and no CMD, so these append rather
  # than override anything.
  startup_env=()
  startup_args=()
  case "${target}" in
    worker)
      startup_env=(
        --env "DEV_HEALTH_QUEUE_CONCURRENCY=sync=4,sync_provider=2"
        --env "DEV_HEALTH_WORKER_GROUP=container-smoke"
      )
      startup_args=(--queues=sync,sync_provider)
      ;;
  esac
  docker run --detach \
    --name "${container_name}" \
    --publish "127.0.0.1::8080" \
    "${startup_env[@]}" \
    "${CONTAINER_SECURITY_ARGS[@]}" \
    "${tag}" "${startup_args[@]}" >/dev/null
  published_address="$(docker port "${container_name}" 8080/tcp 2>/dev/null | head -n 1 || true)"
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
    for dependency in domain_postgres queue_completeness queue_postgres river_schema; do
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
  local pass
  local target
  local first_id
  local second_id

  assert_cache_mounts_not_copied_out

  # CHAOS-5067 (option 5): the shared `build` stage compiles all 7 binaries
  # in one RUN layer; `docker build --no-cache --target <target>` invalidates
  # that whole ancestor stage, so building each of the 7 target images with
  # --no-cache separately recompiled every binary 7 times per pass -- 14 full
  # compiles for a check that only needs 2. Fix: force exactly one fresh
  # (--no-cache) compile of the `build` stage per pass, tagged, then build
  # each of the 7 target stages in that SAME pass with an EXPLICIT named
  # build-context override (`--build-context build=docker-image://<tag>`)
  # pointing at that exact tag.
  #
  # A first version of this fix let the target builds resolve the `build`
  # stage via NORMAL (ambient) BuildKit cache instead of an explicit
  # override, on the theory that identical inputs give the just-built
  # stage's layer the same cache key. A mutation proof (embedding a
  # per-pass nonce in the build stage, `date +%s%N`) caught this as WRONG:
  # BuildKit's cache entry for that RUN layer was written once on pass
  # "first" and never overwritten by pass "second"'s own --no-cache
  # rebuild, so every target's pass-"second" image silently carried
  # pass-"first"'s stage content -- a false PASS that never actually
  # re-verified anything past the first compile. Confirmed directly:
  # `docker cp`-extracting the nonce file from the two `build`-stage
  # TAGS themselves showed genuinely different values, but the same
  # extraction from a downstream (ambient-cache) target build showed the
  # SAME value for both passes. The explicit `--build-context` override
  # resolves the stage by the exact tag given, not by cache-key guessing,
  # and re-measuring with it showed the correct, differing nonces.
  #
  # CODEX ROUND 1 (P1, confirmed sound though not reproducible on this
  # platform): the fix above only forces the SHARED `build` stage fresh --
  # each target's OWN stage (the `runtime` base plus that target's own
  # instructions, e.g. a `WORKDIR`) still resolved via ORDINARY caching, so
  # a genuinely non-deterministic TARGET-LOCAL instruction (not in the
  # `build` stage) could still get silently cached-and-reused across passes,
  # same failure shape as the ambient-cache bug above, one layer further
  # down. `docker/go-worker.Dockerfile:158-169`'s own comment documents a
  # real historical instance: a since-removed `WORKDIR /app` on the
  # `migrate` target made it CI-non-reproducible (that comment also notes
  # local --no-cache probes never reproduced it -- confirmed again here,
  # `--build-context ... --no-cache` on that reintroduced line still gave
  # two independently-built images the SAME id on bigboy/arm64 -- so this
  # exact historical case is platform/BuildKit-version-dependent, but the
  # STRUCTURAL gap the round raised does not depend on that case reproducing
  # here). Fixed by adding `--no-cache` to the per-target build too:
  # measured directly (bigboy) that this does NOT re-trigger the `build`
  # stage's compile -- `--build-context` makes it an EXTERNAL image
  # reference rather than a graph node, so there is nothing for --no-cache
  # to invalidate there; only the target's OWN (cheap, non-compile) layers
  # re-run, at the same ~2s cost as before.
  #
  # The per-target image-ID comparison below is unchanged; timeout and
  # job routing untouched.
  for pass in first second; do
    build_target build "${IMAGE_PREFIX}-build:repro-${pass}" --no-cache --provenance=false
    for target in "${ALL_TARGETS[@]}"; do
      build_target "${target}" "${IMAGE_PREFIX}-${target}:repro-${pass}" \
        --build-context "build=docker-image://${IMAGE_PREFIX}-build:repro-${pass}" \
        --no-cache --provenance=false
    done
  done

  # CODEX ROUND 2 (P3): the previous form called `die()` (exit) on the
  # FIRST mismatch, so if two or more targets were simultaneously
  # non-reproducible, only the first one's diagnostic was ever printed --
  # accurate but incomplete. Every target is now compared and reported
  # before any exit, so a run with multiple failures names all of them in
  # one pass instead of costing a re-run per failure discovered.
  local -a mismatches=()
  for target in "${ALL_TARGETS[@]}"; do
    first_id="$(docker image inspect --format '{{.Id}}' "${IMAGE_PREFIX}-${target}:repro-first")"
    second_id="$(docker image inspect --format '{{.Id}}' "${IMAGE_PREFIX}-${target}:repro-second")"
    if [ "${first_id}" = "${second_id}" ]; then
      printf 'container reproducibility: %s %s\n' "${target}" "${first_id}"
    else
      printf 'ERROR: %s image is not reproducible: %s != %s\n' \
        "${target}" "${first_id}" "${second_id}" >&2
      mismatches+=("${target}")
    fi
  done
  [ "${#mismatches[@]}" -eq 0 ] \
    || die "not reproducible: ${mismatches[*]}"
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
