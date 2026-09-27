#!/usr/bin/env bash
# bigboy: dho_api_ch effective ClickHouse grants == the APIPosture of a given ops sha (default: the sha in the record's sha.txt).
# usage: bigboy-grants-check.sh [record-dir]. Fails loudly (GRANTS_CHECK_FAIL) when the manifest cannot be parsed.
set -u; R=/home/ubuntu/devhealth; REC=${1:-$R/_records/bigboy-a2b9bf79}; SHA=$(cat "$REC/sha.txt")
ACT=$(docker exec dev-health-clickhouse-1 clickhouse-client -q "SELECT access_type, table FROM system.grants WHERE user_name='dho_api_ch' ORDER BY table, access_type FORMAT TSV" 2>&1) || { echo "GRANTS_CHECK_FAIL query"; exit 1; }
SRC=$(git -C $R/ops show "$SHA":internal/storage/clickhouse/authorization.go 2>/dev/null) || { echo "GRANTS_CHECK_FAIL no authorization.go at $SHA in the local ops clone (git fetch)"; exit 1; }
# Extracted to a sibling file, not an inline here-document (CHAOS-3362/CHAOS-6964: body is well
# over the 400-byte here-document pipe budget).
HERE=$(cd "$(dirname "$0")" && pwd)
python3 "$HERE/bigboy-grants-check-compare.py" "$ACT" "$SRC"
