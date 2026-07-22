#!/usr/bin/env bash
set -euo pipefail

ops_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="$(cd "$ops_root/.." && pwd)"
acr_root="${DEV_HEALTH_ACR_DIR:-$workspace_root/dev-health-acr}"
web_root="${DEV_HEALTH_WEB_DIR:-$workspace_root/dev-health-web}"
launcher="$acr_root/scripts/dev/context-fabric-local.sh"

if [[ ! -f "$launcher" ]]; then
  printf 'Context Fabric launcher not found: %s\n' "$launcher" >&2
  printf 'Set DEV_HEALTH_ACR_DIR to the dev-health-acr checkout.\n' >&2
  exit 1
fi
if [[ ! -f "$web_root/package.json" ]]; then
  printf 'dev-health-web checkout not found: %s\n' "$web_root" >&2
  printf 'Set DEV_HEALTH_WEB_DIR to the dev-health-web checkout.\n' >&2
  exit 1
fi

exec bash "$launcher" --ops-dir "$ops_root" --web-dir "$web_root" "$@"
