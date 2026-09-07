#!/usr/bin/env bash
# Assert every running go-* service container in a Compose project was built
# from the SAME image as its peers (CHAOS-5437).
#
# WHY THIS EXISTS
# ----------------
# A domain-posture manifest change (internal/storage/postgres/
# domain_authorization.go) is a lockstep go-worker-migrate + go-* worker
# deploy. On 2026-09-07, go-worker-heavy/go-worker-ops crash-looped and
# go-scheduler/go-reconciler/three stream runners sat `not_ready` for hours
# (streams: 9h) with no crash loop and no alert, because they were still
# running images built BEFORE #2361/165eed2e91 widened the manifest while
# go-worker-migrate had already re-run from a current image. See
# lane-heavy-diag/REPORT.md for the full incident.
#
# This is a LIVE-STACK operator check: it inspects a running Compose
# project's containers. It is distinct from ci/check_go_containers.sh, which
# builds and smoke-tests standalone images with fixed reproducible-build
# metadata and never touches a running Compose project -- there is no
# existing operator-facing running-stack check to extend (grepped for one;
# none found), so this is new.
#
# HOW IT DECIDES "FRESH"
# -----------------------
# Trap (lane-heavy-diag/REPORT.md "TRAPS FOR THE FLEET BRIEF"): a Compose tag
# can point at a DIFFERENT image than what a running container was actually
# created from, and two containers can share a tag while running genuinely
# different binaries. Comparing image IDs (`docker inspect -f '{{.Image}}'`)
# is what settled that incident in one step where four environment
# hypotheses could not -- so this script compares image IDs, never tags.
#
# The majority image ID among running go-* service containers is treated as
# the fleet's current build; anything else is reported as stale. This makes
# the check self-contained (no separate "expected build id" input to keep in
# sync) and matches the incident shape exactly: one or a few services lag
# behind the rest after a partial recreate.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy/go-workers/check_go_containers.sh [compose-project-name]

Compares the image ID of every running go-* service container in the given
Compose project (docker label com.docker.compose.project). Prints an ERROR
line per service whose image ID differs from the fleet majority and exits
non-zero if any do. compose-project-name defaults to $COMPOSE_PROJECT_NAME,
then "dev-health".
EOF
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

PROJECT="${1:-${COMPOSE_PROJECT_NAME:-dev-health}}"

command -v docker >/dev/null 2>&1 || {
  printf 'ERROR: docker is required\n' >&2
  exit 2
}

declare -A image_by_service=()
declare -A count_by_image=()

while IFS='|' read -r service container_id; do
  [ -n "${service}" ] || continue
  case "${service}" in
    go-*) ;;
    *) continue ;;
  esac
  image_id="$(docker inspect --format '{{.Image}}' "${container_id}" 2>/dev/null || true)"
  [ -n "${image_id}" ] || continue
  image_by_service["${service}"]="${image_id}"
  count_by_image["${image_id}"]=$(("${count_by_image["${image_id}"]:-0}" + 1))
done < <(docker ps \
  --filter "label=com.docker.compose.project=${PROJECT}" \
  --format '{{index .Labels "com.docker.compose.service"}}|{{.ID}}' 2>/dev/null)

if [ "${#image_by_service[@]}" -eq 0 ]; then
  printf 'ERROR: no running go-* service containers found in Compose project %s\n' "${PROJECT}" >&2
  exit 2
fi

majority_image=""
majority_count=0
for image_id in "${!count_by_image[@]}"; do
  if [ "${count_by_image[${image_id}]}" -gt "${majority_count}" ]; then
    majority_count="${count_by_image[${image_id}]}"
    majority_image="${image_id}"
  fi
done

status=0
for service in "${!image_by_service[@]}"; do
  image_id="${image_by_service[${service}]}"
  if [ "${image_id}" != "${majority_image}" ]; then
    printf 'ERROR: %s is running image %s, fleet build is %s (rebuild/redeploy this service)\n' \
      "${service}" "${image_id}" "${majority_image}" >&2
    status=1
  fi
done

if [ "${status}" -eq 0 ]; then
  printf 'ok: %d go-* service(s) in %s share one build (%s)\n' \
    "${#image_by_service[@]}" "${PROJECT}" "${majority_image}"
fi

exit "${status}"
