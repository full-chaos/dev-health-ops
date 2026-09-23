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
# The long-running services the smoke runs to readiness. stream-runner,
# reconciler and scheduler are not image targets: they are `dho stream-runner`,
# `dho reconciler` and `dho scheduler`, smoked on the dho image (the chart
# default) and on the operator image (what prod pins for them).
readonly RUNTIME_TARGETS=(worker scheduler scheduler-operator reconciler reconciler-operator stream-runner stream-runner-operator)
readonly ALL_TARGETS=(worker operator contractcheck migrate dho)
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
  # A leg that selects a platform (the CI arm64 leg sets
  # DOCKER_DEFAULT_PLATFORM) must have built THAT architecture: an image of
  # the host's architecture would pass every check below without the
  # emulated run the leg exists for.
  if [ -n "${DOCKER_DEFAULT_PLATFORM:-}" ]; then
    local want_arch="${DOCKER_DEFAULT_PLATFORM#*/}"
    want_arch="${want_arch%%/*}"
    local built_arch
    built_arch="$(docker image inspect --format '{{.Architecture}}' "${tag}")"
    printf 'image %s architecture: %s\n' "${tag}" "${built_arch}"
    [ "${built_arch}" = "${want_arch}" ] \
      || die "${tag} is ${built_arch}, but DOCKER_DEFAULT_PLATFORM=${DOCKER_DEFAULT_PLATFORM}"
  fi
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
  # A service folded into dho is smoked from the dho image, with its verb as
  # the first argument -- the way the chart and Compose run it.
  local image_target="${target}"
  local verb_args=()
  case "${target}" in
    stream-runner)
      image_target=dho
      verb_args=(stream-runner)
      ;;
    scheduler)
      image_target=dho
      verb_args=(scheduler)
      ;;
    scheduler-operator)
      image_target=operator
      verb_args=(scheduler)
      ;;
    reconciler)
      image_target=dho
      verb_args=(reconciler)
      ;;
    reconciler-operator)
      image_target=operator
      verb_args=(reconciler)
      ;;
    stream-runner-operator)
      image_target=operator
      verb_args=(stream-runner)
      ;;
  esac
  local tag="${IMAGE_PREFIX}-${image_target}:ci"
  local container_name="dev-health-go-${target}-smoke-$$"
  local published_address
  local exit_code
  local readiness_body
  local dependency
  local dependencies
  local startup_env

  build_target "${image_target}" "${tag}"

  [ "$(docker image inspect --format '{{.Config.User}}' "${tag}")" = "65532:65532" ] \
    || die "${target} image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "${target} did not report injected version metadata"
  # The worker image packages dho so operators can run `dho workers ...` in
  # a worker pod (spec S2 folded dev-health-workerctl into dho). Prove the
  # binary runs and its workers vertical is reachable.
  if [ "${target}" = "worker" ]; then
    docker run --rm --entrypoint /usr/local/bin/dho \
      "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
      | grep -F '"version":"phase1-ci"' >/dev/null \
      || die "worker image does not package dho"
    smoke_workers_vertical --entrypoint /usr/local/bin/dho "${tag}"
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
    "${tag}" "${verb_args[@]}" "${startup_args[@]}" >/dev/null
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
      scheduler | scheduler-operator)
        dependencies="domain_postgres queue_postgres river_schema scheduler_loop"
        ;;
      reconciler | reconciler-operator)
        dependencies="domain_postgres queue_postgres reconciler_loop river_schema"
        ;;
      stream-runner | stream-runner-operator)
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
    case "${target}" in
      reconciler | reconciler-operator)
        if grep -F '"job_registry"' <<<"${readiness_body}" >/dev/null; then
          die "${target} image could not load its packaged job contract artifacts"
        fi
        # The reconciler also loads contracts/sync-dispatch/v1 from /app; a
        # failed sync_dispatch_registry means the image did not package it.
        if grep -F '"sync_dispatch_registry"' <<<"${readiness_body}" >/dev/null; then
          die "${target} image could not load its packaged sync-dispatch contract"
        fi
        # The Compose probe is exec form (the runtime image has no shell):
        # `dho reconciler healthcheck` inside the container mirrors /readyz,
        # which is 503 here, so it must exit 1 -- not 0, and not 2 (unknown
        # verb) or 127 (no such binary).
        set +e
        docker exec "${container_name}" /usr/local/bin/dho reconciler healthcheck
        exit_code=$?
        set -e
        [ "${exit_code}" = "1" ] \
          || die "${target}: dho reconciler healthcheck exited ${exit_code} against a not-ready process, want 1"
        ;;
    esac
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
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-contractcheck:ci" contracts validate \
    | grep -F "worker contracts valid" >/dev/null \
    || die "contractcheck image did not validate its embedded contract artifacts"

  printf 'container smoke: operator\n'
  build_target operator "${IMAGE_PREFIX}-operator:ci"
  [ "$(docker image inspect --format '{{.Config.User}}' "${IMAGE_PREFIX}-operator:ci")" = "65532:65532" ] \
    || die "operator image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${IMAGE_PREFIX}-operator:ci" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "operator did not report injected version metadata"
  smoke_workers_vertical "${IMAGE_PREFIX}-operator:ci"
  smoke_migrate_river "${IMAGE_PREFIX}-operator:ci"

  # migrate (cmd/dev-health-worker-migrate, a thin main over the code `dho
  # migrate river` runs, kept until spec S10) is a one-shot job, not a
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
  # CHAOS-5560 (2026-09-11): migrate's configuration-error diagnostic moved
  # from a plain-text "configuration error: %s\n" line to a single JSON
  # object (config.WriteConfigError -- one shared writer, now also used by
  # internal/workersctl) so an operator script can parse the class and
  # the offending key reliably instead of substring-matching prose that is
  # free to be reworded. Parse the object and assert its stable shape --
  # never a substring match on `detail`'s prose, which is not a contract.
  printf '%s' "${migrate_stderr}" \
    | jq -e '.error.code == "configuration_error" and (.error.detail | contains("MIGRATION_DATABASE_URI"))' >/dev/null \
    || die "migrate did not report the missing MIGRATION_DATABASE_URI diagnostic"

  smoke_dho
}

# smoke_dho runs the operator binary's api service the way its Deployment
# does (`dho api`, both listeners published) and asserts the contract a
# route-less api keeps: live, READY (it has no dependency yet, only its own
# listener), metrics served, and every api path answered with the Python
# api's own 404 body plus its security headers. It then stops cleanly.
# smoke_migrate_river runs `dho migrate river --apply-and-check` -- the
# chart hook's and Compose's exact argv, with no shell -- with no database
# configured: it must reach the migrator and fail closed (exit 1) with the
# refusal the hook's old shell wrapper printed, verbatim, not an unknown
# command (exit 2) or a crash.
smoke_migrate_river() {
  local tag="$1" output code
  set +e
  output="$(docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" migrate river --apply-and-check 2>&1 >/dev/null)"
  code=$?
  set -e
  [ "${code}" = "1" ] || die "${tag}: dho migrate river --apply-and-check without a DSN exited ${code}, want 1: ${output}"
  [ "${output}" = "river-migrate: neither MIGRATION_DATABASE_URI nor POSTGRES_URI is set in the migration Secret; this Job needs an ELEVATED DSN pointed DIRECTLY at PostgreSQL (5432), never at a transaction pooler" ] \
    || die "${tag}: dho migrate river --apply-and-check did not report the missing-DSN refusal: ${output}"
}

# smoke_workers_vertical runs `dho workers status` with no database
# configured: the verb tree must be reached and fail closed with the JSON
# configuration error naming the first missing DSN (exit 1), not an unknown
# command (exit 2) or a crash.
smoke_workers_vertical() {
  local output code
  set +e
  output="$(docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "$@" workers status 2>&1 >/dev/null)"
  code=$?
  set -e
  [ "${code}" -eq 1 ] || die "dho workers status exited ${code}, want 1 (configuration error)"
  printf '%s' "${output}" | grep -v '^{"time"' \
    | jq -e '.error.code == "configuration_error" and (.error.detail | contains("POSTGRES_URI"))' >/dev/null \
    || die "dho workers status did not report the missing POSTGRES_URI configuration error"
}

smoke_dho() {
  local tag="${IMAGE_PREFIX}-dho:ci"
  local container_name="dev-health-go-dho-smoke-$$"
  local operator_address
  local api_address
  local headers
  local body
  local exit_code

  printf 'container smoke: dho\n'
  build_target dho "${tag}"
  [ "$(docker image inspect --format '{{.Config.User}}' "${tag}")" = "65532:65532" ] \
    || die "dho image is not configured for numeric non-root execution"
  docker run --rm "${CONTAINER_SECURITY_ARGS[@]}" "${tag}" --version \
    | grep -F '"version":"phase1-ci"' >/dev/null \
    || die "dho did not report injected version metadata"
  smoke_migrate_river "${tag}"

  ACTIVE_CONTAINER="${container_name}"
  docker run --detach \
    --name "${container_name}" \
    --publish "127.0.0.1::8080" \
    --publish "127.0.0.1::8000" \
    "${CONTAINER_SECURITY_ARGS[@]}" \
    "${tag}" api --http-addr=:8080 --api-addr=:8000 >/dev/null
  operator_address="$(docker port "${container_name}" 8080/tcp 2>/dev/null | head -n 1 || true)"
  api_address="$(docker port "${container_name}" 8000/tcp 2>/dev/null | head -n 1 || true)"
  if [ -z "${operator_address}" ] || [ -z "${api_address}" ]; then
    printf 'container dho exited before publishing its ports; its output was:\n' >&2
    docker logs "${container_name}" 2>&1 | tail -20 >&2
    die "dho did not publish its listeners"
  fi
  wait_for_status "http://${operator_address}/healthz" 200 \
    || die "dho health endpoint did not become available"
  wait_for_status "http://${operator_address}/readyz" 200 \
    || die "dho api did not become ready"
  wait_for_status "http://${operator_address}/metrics" 200 \
    || die "dho metrics endpoint did not become available"
  wait_for_status "http://${api_address}/api/v1/smoke" 404 \
    || die "dho api did not answer an unmounted path with 404"
  body="$(curl --silent --show-error --max-time 1 "http://${api_address}/api/v1/smoke")"
  [ "${body}" = '{"detail":"Not Found"}' ] \
    || die "dho api 404 body is not the Python api's: ${body}"
  headers="$(curl --silent --show-error --max-time 1 --dump-header - --output /dev/null "http://${api_address}/api/v1/smoke")"
  grep -i -F 'x-frame-options: DENY' <<<"${headers}" >/dev/null \
    || die "dho api response lacks the security headers"
  grep -i -F 'x-request-id:' <<<"${headers}" >/dev/null \
    || die "dho api response lacks a request id"

  docker stop --time 5 "${container_name}" >/dev/null
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container_name}")"
  [ "${exit_code}" = "0" ] || die "dho exited with status ${exit_code}"
  cleanup_active_container
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
