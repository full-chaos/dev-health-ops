#!/usr/bin/env bash
# The two-pass prod deploy for fullchaos.dev's docker-compose stack.
#
# CHAOS-4976: a plain `docker compose pull && up -d` silently skips every
# service gated behind `profiles: [go-workers]` -- the Go/River worker
# family plus metrics-api -- even though those containers are already
# running. The stale images never error; the deploy just quietly does
# nothing for that whole family, and a later incident (a Go-worker
# post-step's writes silently stopping) is the only signal something was
# ever wrong.
#
# This script makes the second pass STRUCTURAL instead of a step a human
# has to remember. It never hardcodes which services belong to the
# `go-workers` profile: it derives the exact list at RUN TIME by diffing
# `docker compose config --services` with and without `--profile
# go-workers` (the set of services that ONLY appear once the profile is
# added), and it never invokes a bare `--profile go-workers up -d` --
# every service the second pass touches is named explicitly, computed
# fresh, so a new service added to compose.go-workers.yml is picked up
# automatically without anyone needing to update this script by hand.
#
# Usage: deploy-prod.sh [--dry-run]
#   --dry-run   Print the plan (both passes, the exact service list) and
#               exit without pulling or starting anything.
#
# Always prints its plan before acting, dry-run or not.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
COMPOSE_ARGS=(
  -f "${SCRIPT_DIR}/compose.production.yml"
  -f "${SCRIPT_DIR}/compose.go-workers.yml"
)
readonly PROFILE="go-workers"

DRY_RUN=0
for arg in "$@"; do
  case "${arg}" in
    --dry-run | -n)
      DRY_RUN=1
      ;;
    *)
      echo "usage: $(basename "$0") [--dry-run]" >&2
      exit 2
      ;;
  esac
done

# The exact set of services that ONLY exist because of `--profile
# go-workers` -- base_services minus profile_services -- recomputed every
# run from the real, live-interpolated compose config, never a
# hand-maintained list. `docker compose config --services` is pure
# client-side parsing; it does not need a reachable daemon and does not
# start or touch any container.
base_services="$(docker compose "${COMPOSE_ARGS[@]}" config --services 2>/dev/null | sort)"
profile_services="$(docker compose "${COMPOSE_ARGS[@]}" --profile "${PROFILE}" config --services 2>/dev/null | sort)"

go_workers_only=()
while IFS= read -r svc; do
  [ -n "${svc}" ] && go_workers_only+=("${svc}")
done < <(comm -13 <(printf '%s\n' "${base_services}") <(printf '%s\n' "${profile_services}"))

if [ "${#go_workers_only[@]}" -eq 0 ]; then
  echo "::error::no service resolved under --profile ${PROFILE} -- compose.go-workers.yml may be missing or unreadable, or every profiled service vanished from it. Refusing to proceed with an empty second pass: an empty pass would exit 0 having pulled and started nothing, exactly the silent-skip shape CHAOS-4976 exists to close." >&2
  exit 1
fi

echo "Plan:"
echo "  1) base pass (immutable/default-profile services):"
echo "       docker compose ${COMPOSE_ARGS[*]} pull"
echo "       docker compose ${COMPOSE_ARGS[*]} up -d"
echo "  2) --profile ${PROFILE} pass (${#go_workers_only[@]} service(s), derived from"
echo "     'docker compose config' at run time, named explicitly -- never a bare"
echo "     '--profile ${PROFILE} up -d'):"
echo "       ${go_workers_only[*]}"
echo "       docker compose ${COMPOSE_ARGS[*]} --profile ${PROFILE} pull ${go_workers_only[*]}"
echo "       docker compose ${COMPOSE_ARGS[*]} --profile ${PROFILE} up -d --no-deps ${go_workers_only[*]}"

if [ "${DRY_RUN}" -eq 1 ]; then
  echo "--dry-run: not executing."
  exit 0
fi

docker compose "${COMPOSE_ARGS[@]}" pull
docker compose "${COMPOSE_ARGS[@]}" up -d
docker compose "${COMPOSE_ARGS[@]}" --profile "${PROFILE}" pull "${go_workers_only[@]}"
docker compose "${COMPOSE_ARGS[@]}" --profile "${PROFILE}" up -d --no-deps "${go_workers_only[@]}"
