#!/usr/bin/env bash
# One execution of the filled FORM-14542-1 SQL on ClickHouse $CH_IMAGE, against the SigNoz v3 trace index
# (DDL from signoz-otel-collector 0476e453f957 traces_migrations.go:14-135) holding 4 synthetic spans.
# Variant "plain": distributed_signoz_index_v3 is a plain MergeTree. Variant "distributed": it is a Distributed table over
# signoz_index_v3 on a one-shard cluster `cluster` (cluster.xml; SigNoz names its cluster `cluster`) (the scalar WITH then also crosses a shard query).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${CH_IMAGE:?CH_IMAGE is required}"
# FORM-14542-1 window 2026-11-23T06:48:23Z..2026-11-24T06:48:23Z (24 h), fixed. Rows sit at START + 600 s (inside the
# window); the table TTL is 15 d after the row time, so the rows survive while the CI clock is before 2026-12-08.
start_ms=1795416503000
end_ms=1795502903000
start_s=$(( start_ms / 1000 ))
bucket_s=$(( start_s / 1800 * 1800 ))
now_s=$(date -u +%s)
echo "window [${start_ms}, ${end_ms}) ms; CI clock ${now_s} s; row time START+600 = $(( start_s + 600 )) s; TTL ends $(( start_s + 600 + 1296000 )) s"
if [ "$now_s" -ge $(( start_s + 600 + 1296000 )) ]; then echo "FAIL: CI clock is past the row TTL"; exit 1; fi
work=$(mktemp -d)

form_sha12=$(sha256sum "$here/form.json" | cut -c1-12)
[ "$form_sha12" = ea2708b03268 ] || { echo "FAIL: form.json sha12 ${form_sha12} != ea2708b03268"; exit 1; }
echo "form.json sha256: $(sha256sum "$here/form.json" | cut -c1-64)"
sed -e "s/\${START_MS}/${start_ms}/g" -e "s/\${END_MS}/${end_ms}/g" "$here/form.json" > "$work/filled.json"
python3 -I - "$work/filled.json" "$work/query.sql" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))["body"]
assert isinstance(body["start"], int) and isinstance(body["end"], int), "start/end are not JSON numbers"
queries = body["compositeQuery"]["queries"]
assert len(queries) == 1 and queries[0]["type"] == "clickhouse_sql", "not one clickhouse_sql query"
open(sys.argv[2], "w").write(queries[0]["spec"]["query"])
PY
if grep -q '\${' "$work/query.sql"; then echo "FAIL: an unfilled token is left"; exit 1; fi
echo "filled query sha256: $(sha256sum "$work/query.sql" | cut -c1-64)"

docker run -d --name ch --ulimit nofile=262144:262144 -v "$here/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:ro" "$CH_IMAGE" >/dev/null
for _ in $(seq 1 90); do
  if docker exec ch clickhouse-client -q 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 2
done
ch_version=$(docker exec ch clickhouse-client -q 'SELECT version()')
echo "ClickHouse version(): ${ch_version}"
case "$ch_version" in
  25.12.*) echo "PASS: version() starts with 25.12 (prod SigNoz ClickHouse 25.12.5.44)" ;;
  *) echo "FAIL: version() ${ch_version} is not 25.12.x"; exit 1 ;;
esac
docker exec ch clickhouse-client -q "SELECT name, value, changed FROM system.settings WHERE name IN ('enable_analyzer', 'allow_experimental_analyzer', 'enable_global_with_statement') ORDER BY name"
docker cp "$work/query.sql" ch:/tmp/query.sql

status=0
# PRECHECK-S-14542-1.sql (sha12 2e95c7acef5b): its cost gate reads system.parts of the LOCAL table signoz_index_v3, so the
# local table holds the fixture rows in both variants (the plain variant gets a second copy in a local table).
# Rows in the ts_bucket_start range 1795414703..1795502903: all 19 fixture rows (T4 bucket START-1 s is 1795415400, T5 bucket END+1 s is 1795501800).
run_precheck() {
  local variant=$1
  [ "$(sha256sum "$here/precheck_s.sql" | cut -c1-12)" = 2e95c7acef5b ] || { echo "FAIL ${variant}: precheck_s.sql sha12 differs"; status=1; return; }
  if [ "$variant" = plain ]; then
    docker cp "$here/ddl_local.sql" ch:/tmp/ddl_local.sql
    docker exec ch clickhouse-client --queries-file /tmp/ddl_local.sql || { echo "FAIL ${variant}: local DDL errored"; status=1; return; }
    sed -e "s/TABLE_NAME/signoz_traces.signoz_index_v3/" -e "s/__START_S__/${start_s}/g" "$here/rows.sql" > "$work/rows_local.sql"
    docker cp "$work/rows_local.sql" ch:/tmp/rows_local.sql
    docker exec ch clickhouse-client --queries-file /tmp/rows_local.sql || { echo "FAIL ${variant}: local insert errored"; status=1; return; }
  fi
  docker exec ch clickhouse-client -q 'SELECT count() FROM signoz_traces.signoz_index_v3'
  docker cp "$here/precheck_s.sql" ch:/tmp/precheck_s.sql
  # pass case
  local got
  got=$(docker exec ch clickhouse-client --format TSV --queries-file /tmp/precheck_s.sql 2>"$work/pc-err-${variant}.txt") || { echo "FAIL ${variant}: PRECHECK-S errored:"; cat "$work/pc-err-${variant}.txt"; status=1; return; }
  if [ "$got" = 19 ]; then echo "PASS ${variant}: PRECHECK-S s_spans = 19 (all fixture rows in the range)"; else echo "FAIL ${variant}: PRECHECK-S s_spans '${got}', want 19"; status=1; fi
  # gate fires at a low cap (the cap literal 20000000 -> 10, local rows 19)
  sed -e 's/20000000/10/' "$here/precheck_s.sql" > "$work/precheck_cap.sql"
  docker cp "$work/precheck_cap.sql" ch:/tmp/precheck_cap.sql
  if docker exec ch clickhouse-client --queries-file /tmp/precheck_cap.sql >/dev/null 2>"$work/pc-cap-${variant}.txt"; then
    echo "FAIL ${variant}: low-cap PRECHECK-S did not fire"; status=1
  elif grep -q 'Code: 395' "$work/pc-cap-${variant}.txt" && grep -q 'STOP-COST-GATE' "$work/pc-cap-${variant}.txt"; then
    echo "PASS ${variant}: low cap fires: $(head -c 200 "$work/pc-cap-${variant}.txt" | head -1)"
  else
    echo "FAIL ${variant}: low-cap error is not code 395 STOP-COST-GATE:"; head -3 "$work/pc-cap-${variant}.txt"; status=1
  fi
  # gate fires with 0 rows (empty local table)
  docker exec ch clickhouse-client -q 'TRUNCATE TABLE signoz_traces.signoz_index_v3'
  if docker exec ch clickhouse-client --queries-file /tmp/precheck_s.sql >/dev/null 2>"$work/pc-empty-${variant}.txt"; then
    echo "FAIL ${variant}: empty-table PRECHECK-S did not fire"; status=1
  elif grep -q 'Code: 395' "$work/pc-empty-${variant}.txt" && grep -q 'STOP-COST-GATE' "$work/pc-empty-${variant}.txt"; then
    echo "PASS ${variant}: empty table fires: $(head -c 200 "$work/pc-empty-${variant}.txt" | head -1)"
  else
    echo "FAIL ${variant}: empty-table error is not code 395 STOP-COST-GATE:"; head -3 "$work/pc-empty-${variant}.txt"; status=1
  fi
}
run_variant() {
  local variant=$1 target=$2; shift 2
  echo "=== variant ${variant}"
  docker exec ch clickhouse-client -q 'DROP DATABASE IF EXISTS signoz_traces SYNC'
  docker exec ch clickhouse-client -q 'CREATE DATABASE signoz_traces'
  for ddl in "$@"; do
    docker cp "$here/$ddl" "ch:/tmp/$ddl"
    docker exec ch clickhouse-client --queries-file "/tmp/$ddl" || { echo "FAIL ${variant}: DDL ${ddl} errored"; status=1; return; }
  done
  sed -e "s/TABLE_NAME/${target}/" -e "s/__START_S__/${start_s}/g" -e "s/__BUCKET_S__/${bucket_s}/g" "$here/rows.sql" > "$work/rows.sql"
  docker cp "$work/rows.sql" ch:/tmp/rows.sql
  docker exec ch clickhouse-client --queries-file /tmp/rows.sql || { echo "FAIL ${variant}: insert errored"; status=1; return; }
  if [ "$variant" = distributed ]; then
    docker exec ch clickhouse-client -q 'SYSTEM FLUSH DISTRIBUTED signoz_traces.distributed_signoz_index_v3' || true
  fi
  docker exec ch clickhouse-client -q 'SELECT count() FROM signoz_traces.distributed_signoz_index_v3'
  local qid="form14542-${variant}-$(date +%s)"
  if ! docker exec ch clickhouse-client --query_id "$qid" --format TSV --queries-file /tmp/query.sql > "$work/out-${variant}.tsv" 2> "$work/err-${variant}.txt"; then
    echo "FAIL ${variant}: the form query errored:"; cat "$work/err-${variant}.txt"; status=1; return
  fi
  echo "--- result (section, span_name, label, n, s, a, p50, p95, mx, mn)"; cat "$work/out-${variant}.tsv"
  echo "--- rows per section"; cut -f1 "$work/out-${variant}.tsv" | sort | uniq -c
  cut -f1-4 "$work/out-${variant}.tsv" | sort > "$work/got-${variant}.tsv"
  if ! diff <(sort "$here/expected.tsv") "$work/got-${variant}.tsv"; then
    echo "FAIL ${variant}: (section, span_name, label, n) differ from expected.tsv"; status=1
  else
    echo "PASS ${variant}: 37 rows = expected.tsv"
  fi
  # FORM-TEMPLATE-3 PASS-path WANT: read_rows of the statement = the work math on the fixture (19 rows):
  # 7 scans x 19 = 133 if the scalar WITH is read once per reference. Assert <= 133.
  docker exec ch clickhouse-client -q 'SYSTEM FLUSH LOGS'
  local rr
  rr=$(docker exec ch clickhouse-client -q "SELECT read_rows FROM system.query_log WHERE query_id = '${qid}' AND type = 'QueryFinish' AND is_initial_query")
  echo "query_log ${variant}: initial query read_rows = ${rr} (fixture rows 19; WANT <= 133)"
  docker exec ch clickhouse-client -q "SELECT 'secondary', count(), sum(read_rows) FROM system.query_log WHERE initial_query_id = '${qid}' AND type = 'QueryFinish' AND NOT is_initial_query" | sed "s/^/query_log ${variant}: /"
  if [ -n "$rr" ] && [ "$rr" -le 133 ] && [ "$rr" -gt 0 ]; then
    echo "PASS ${variant}: read_rows ${rr} <= 133"
  else
    echo "FAIL ${variant}: read_rows '${rr}' not in (0, 133]"; status=1
  fi
  while IFS=$'\t' read -r sec span label want; do
    got=$(awk -F'\t' -v s="$sec" -v n="$span" -v l="$label" '$1==s && $2==n && $3==l {print $5}' "$work/out-${variant}.tsv")
    if awk -v g="$got" -v w="$want" 'BEGIN{exit !(g != "" && g+0 == w+0)}'; then
      echo "PASS ${variant}: sum ${span} ${label} = ${got}"
    else
      echo "FAIL ${variant}: sum ${span} ${label} = '${got}', want ${want}"; status=1
    fi
  done < "$here/expected_sums.tsv"
  run_precheck "$variant"
}
run_variant plain signoz_traces.distributed_signoz_index_v3 ddl_plain.sql
run_variant distributed signoz_traces.signoz_index_v3 ddl_local.sql ddl_distributed.sql
docker rm -f ch >/dev/null
exit "$status"
