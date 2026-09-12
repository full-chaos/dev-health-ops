#!/usr/bin/env bash
# The prod deploy for fullchaos.dev's docker-compose stack.
#
# CHAOS-4976 (2026-08-xx): a plain `docker compose pull && up -d` used to
# silently skip every service gated behind `profiles: [go-workers]` -- the
# Go/River worker family plus metrics-api -- even though those containers
# were already running, because they lived only in the separate
# compose.go-workers.yml overlay this script had to be told to also apply
# with `--profile go-workers`. CHAOS-5589 closed the gap at the root instead
# of continuing to route around it here: compose.production.yml now folds
# the Go/River fleet in directly as its unconditional default (no separate
# overlay file, no `--profile go-workers` gate, matching the shape
# CHAOS-3088 gave root compose.yml), so a single plain `pull && up -d`
# against this one file covers every service there is. There is no longer a
# second pass to forget.
#
# Usage: deploy-prod.sh [--dry-run]
#   --dry-run   Print the plan and exit without pulling or starting anything.
#
# Always prints its plan before acting, dry-run or not.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
COMPOSE_ARGS=(-f "${SCRIPT_DIR}/compose.production.yml")

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

echo "Plan:"
echo "  docker compose ${COMPOSE_ARGS[*]} pull"
echo "  docker compose ${COMPOSE_ARGS[*]} up -d"

if [ "${DRY_RUN}" -eq 1 ]; then
  echo "--dry-run: not executing."
  exit 0
fi

docker compose "${COMPOSE_ARGS[@]}" pull
docker compose "${COMPOSE_ARGS[@]}" up -d
