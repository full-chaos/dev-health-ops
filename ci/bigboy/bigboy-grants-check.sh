#!/usr/bin/env bash
# bigboy: dho_api_ch effective ClickHouse grants == the APIPosture of a given ops sha (default: the sha in the record's sha.txt).
# CHAOS-8371 (D4566 class rules): ClickHouse is reached with `docker compose exec -T clickhouse` (compose verbs only, never a container name).
# usage: bigboy-grants-check.sh [record-dir]. Fails loudly (GRANTS_CHECK_FAIL) when the manifest cannot be parsed.
set -u; R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" && pwd); REC=${1:-$R/_records/bigboy-a2b9bf79}; SHA=$(cat "$REC/sha.txt")
HERE=$(cd "$(dirname "$0")" && pwd)
# CHAOS-10137: the manifest is read from BIGBOY_OPS_REPO (default: the checkout these tools come from), never the operator checkout.
OPS_REPO=$(cd "${BIGBOY_OPS_REPO:-$HERE/../..}" 2>/dev/null && pwd) || { echo "GRANTS_CHECK_FAIL BIGBOY_OPS_REPO=${BIGBOY_OPS_REPO:-$HERE/../..} is not a directory"; exit 1; }
# shellcheck disable=SC2046  # intentional: unset every COMPOSE_* name
unset $(compgen -v | grep '^COMPOSE_') 2>/dev/null
DC=(docker compose -p dev-health --project-directory "$R" --env-file ops/.env --env-file _records/images.env -f "$HERE/compose.bigboy.yml")
ACT=$(cd "$R" && "${DC[@]}" exec -T clickhouse clickhouse-client -q "SELECT access_type, table FROM system.grants WHERE user_name='dho_api_ch' ORDER BY table, access_type FORMAT TSV" 2>&1) || { echo "GRANTS_CHECK_FAIL query"; exit 1; }
SRC=$(git -C "$OPS_REPO" show "$SHA":internal/storage/clickhouse/authorization.go 2>/dev/null) || { echo "GRANTS_CHECK_FAIL no authorization.go at $SHA in the ops clone $OPS_REPO (git fetch)"; exit 1; }
# Extracted to a sibling file, not an inline here-document (CHAOS-3362/CHAOS-6964: body is well
# over the 400-byte here-document pipe budget).
HERE=$(cd "$(dirname "$0")" && pwd)
python3 "$HERE/bigboy-grants-check-compare.py" "$ACT" "$SRC"
