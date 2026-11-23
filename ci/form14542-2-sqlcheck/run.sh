#!/usr/bin/env bash
# Throwaway: executes FORM-14542-2.json (SigNoz) on ClickHouse $CH_IMAGE, Distributed shape (distributed_signoz_index_v3 over
# signoz_index_v3 on a one-shard cluster `cluster`). DRY fill, REAL fill, 3 gate fires, read_rows, 4 mutants.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${CH_IMAGE:?CH_IMAGE is required}"
start_ms=1795416503000
dry_end_ms=1795420103000
real_end_ms=1795502903000
start_s=$(( start_ms / 1000 ))
now_s=$(date -u +%s)
echo "CI clock ${now_s} s; the rows are within the 15 d TTL while the clock is before $(( start_s + 1296000 )) s"
if [ "$now_s" -ge $(( start_s + 1296000 )) ]; then echo "FAIL: CI clock is past the row TTL"; exit 1; fi
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }

sha12() { sha256sum "$1" | cut -c1-12; }
[ "$(sha12 "$here/form.json")" = ce617bd5e807 ] || { echo "FAIL: form.json sha12 $(sha12 "$here/form.json") != ce617bd5e807"; exit 1; }
echo "form.json sha256 $(sha256sum "$here/form.json" | cut -c1-64)"
body_sha12() {
  python3 -I -c 'import hashlib,sys; t=open(sys.argv[1],"rb").read(); i=t.index(b"\"body\": {"); print(hashlib.sha256(t[i:]).hexdigest()[:12])' "$1"
}
[ "$(body_sha12 "$here/form.json")" = b172fea9e853 ] || { echo "FAIL: template body sha12 differs"; exit 1; }
fill() { sed -e "s/\${START_MS}/${start_ms}/g" -e "s/\${END_MS}/$1/g" "$here/form.json" > "$2"; }
fill "$dry_end_ms" "$work/dry.json"
fill "$real_end_ms" "$work/real.json"
[ "$(body_sha12 "$work/dry.json")" = e3d92c09f18a ] || { echo "FAIL: DRY fill body sha12 $(body_sha12 "$work/dry.json") != e3d92c09f18a"; exit 1; }
[ "$(body_sha12 "$work/real.json")" = 7322df6abd48 ] || { echo "FAIL: REAL fill body sha12 $(body_sha12 "$work/real.json") != 7322df6abd48"; exit 1; }
echo "PASS: body shas: template b172fea9e853, DRY fill e3d92c09f18a, REAL fill 7322df6abd48"
extract() {
  python3 -I - "$1" "$2" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))["body"]
assert isinstance(body["start"], int) and isinstance(body["end"], int), "start/end are not JSON numbers"
q = body["compositeQuery"]["queries"]
assert len(q) == 1 and q[0]["type"] == "clickhouse_sql", "not one clickhouse_sql query"
open(sys.argv[2], "w").write(q[0]["spec"]["query"])
PY
  if grep -q '\${' "$2"; then echo "FAIL: an unfilled token is left in $2"; exit 1; fi
}
extract "$work/dry.json" "$work/dry.sql"
extract "$work/real.json" "$work/real.sql"

docker run -d --name ch --ulimit nofile=262144:262144 -v "$here/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:ro" "$CH_IMAGE" >/dev/null
for _ in $(seq 1 90); do
  if docker exec ch clickhouse-client -q 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 2
done
ch_version=$(docker exec ch clickhouse-client -q 'SELECT version()')
echo "ClickHouse version(): ${ch_version}"
case "$ch_version" in
  25.12.*) echo "PASS: version() starts with 25.12" ;;
  *) echo "FAIL: version() ${ch_version} is not 25.12.x"; exit 1 ;;
esac
ch() { docker exec ch clickhouse-client "$@"; }
ch -q 'CREATE DATABASE signoz_traces'
for ddl in ddl_local.sql ddl_distributed.sql; do
  docker cp "$here/$ddl" "ch:/tmp/$ddl"
  ch --queries-file "/tmp/$ddl" || { echo "FAIL: DDL $ddl errored"; exit 1; }
done
python3 -I "$here/gen_rows.py" "$start_s" > "$work/rows.sql"
docker cp "$work/rows.sql" ch:/tmp/rows.sql
ch --queries-file /tmp/rows.sql || { echo "FAIL: insert errored"; exit 1; }
echo "rows in local table: $(ch -q 'SELECT count() FROM signoz_traces.signoz_index_v3'); distributed: $(ch -q 'SELECT count() FROM signoz_traces.distributed_signoz_index_v3')"
ch -q "SELECT toDate(timestamp) AS part, count() FROM signoz_traces.signoz_index_v3 GROUP BY part ORDER BY part"
f_filler=2000
sys_rows=$(ch -q "SELECT (SELECT count() FROM system.parts) + (SELECT count() FROM system.clusters)")
echo "system table rows (parts + clusters): ${sys_rows}"
if [ "$sys_rows" -lt "$f_filler" ]; then echo "PASS: system table rows ${sys_rows} < F ${f_filler}"; else fail "system table rows ${sys_rows} >= F ${f_filler}"; fi

# run <label> <sql file> -> sets qid, rc; stdout in $work/out-<label>.tsv, stderr in $work/err-<label>.txt
run() {
  local label=$1 file=$2
  qid="f2-${label}-$(date +%s)-$RANDOM"
  docker cp "$file" "ch:/tmp/${label}.sql"
  rc=0
  ch --query_id "$qid" --format TSV --queries-file "/tmp/${label}.sql" > "$work/out-${label}.tsv" 2> "$work/err-${label}.txt" || rc=$?
}
flush_log() { ch -q 'SYSTEM FLUSH LOGS'; }

check_pass() {
  local label=$1 file=$2 want=$3 end_ms=$4
  run "$label" "$file"
  if [ "$rc" -ne 0 ]; then echo "FAIL ${label}: errored:"; cat "$work/err-${label}.txt"; status=1; return; fi
  echo "--- ${label} result"; cat "$work/out-${label}.tsv"
  if diff "$want" "$work/out-${label}.tsv"; then echo "PASS ${label}: output = $(basename "$want") ($(wc -l < "$want") rows)"; else fail "${label}: output differs from $(basename "$want")"; fi
  flush_log
  local rr
  rr=$(ch -q "SELECT read_rows FROM system.query_log WHERE query_id = '${qid}' AND type = 'QueryFinish' AND is_initial_query")
  echo "query_log ${label}: initial read_rows = ${rr}"
  ch -q "SELECT 'secondary', count(), sum(read_rows) FROM system.query_log WHERE initial_query_id = '${qid}' AND type = 'QueryFinish' AND NOT is_initial_query" | sed "s/^/query_log ${label}: /"
  ch -q "SELECT 'data rows in bucket range', count() FROM signoz_traces.signoz_index_v3 WHERE ts_bucket_start >= ${start_s} - 1800 AND ts_bucket_start <= $(( end_ms / 1000 ))" | sed "s/^/query_log ${label}: /"
  if [ -n "$rr" ] && [ "$rr" -ge "$f_filler" ] && [ "$rr" -lt $(( 2 * f_filler )) ]; then
    echo "PASS ${label}: F ${f_filler} <= read_rows ${rr} < 2F $(( 2 * f_filler )) (one data scan)"
  else
    fail "${label}: read_rows '${rr}' not in [${f_filler}, $(( 2 * f_filler )))"
  fi
}
check_pass dry "$work/dry.sql" "$here/expected_dry.tsv" "$dry_end_ms"
check_pass real "$work/real.sql" "$here/expected_real.tsv" "$real_end_ms"

# Gate fires (TEMPLATE-3): code 395 STOP-COST-GATE, query_log ExceptionBeforeStart.
mutate() {
  python3 -I - "$1" "$2" "$3" "$4" <<'PY'
import sys
src, dst, old, new = sys.argv[1:5]
t = open(src).read()
assert t.count(old) == 1, "pattern %r occurs %d times" % (old, t.count(old))
open(dst, "w").write(t.replace(old, new))
PY
}
expect_gate() {
  local label=$1
  run "$label" "$work/${label}.sql"
  if [ "$rc" -eq 0 ]; then fail "${label}: did not fire"; return; fi
  if ! { grep -q 'Code: 395' "$work/err-${label}.txt" && grep -q 'STOP-COST-GATE' "$work/err-${label}.txt"; }; then
    echo "FAIL ${label}: error is not code 395 STOP-COST-GATE:"; head -3 "$work/err-${label}.txt"; status=1; return
  fi
  flush_log
  local ty rr
  ty=$(ch -q "SELECT type FROM system.query_log WHERE query_id = '${qid}' ORDER BY event_time_microseconds DESC LIMIT 1")
  rr=$(ch -q "SELECT read_rows FROM system.query_log WHERE query_id = '${qid}' ORDER BY event_time_microseconds DESC LIMIT 1")
  echo "${label}: $(head -c 160 "$work/err-${label}.txt" | head -1)"
  echo "${label}: query_log type=${ty} read_rows=${rr}"
  if [ "$ty" = ExceptionBeforeStart ]; then echo "PASS ${label}: code 395 STOP-COST-GATE, ExceptionBeforeStart"; else fail "${label}: query_log type '${ty}', want ExceptionBeforeStart"; fi
}
mutate "$work/real.sql" "$work/g1.sql" "NOT BETWEEN 1 AND 20000000" "NOT BETWEEN 1 AND 0"
mutate "$work/real.sql" "$work/g2.sql" "WHERE cluster = 'cluster'" "WHERE cluster = 'nope'"
mutate "$work/real.sql" "$work/g3a.sql" "- 88200" "+ 775800"
mutate "$work/g3a.sql" "$work/g3.sql" "+ 88200" "+ 952200"
expect_gate g1
expect_gate g2
expect_gate g3

# Mutants: each must change the output (REAL fill) against expected_real.tsv.
mutant() {
  local label=$1 old=$2 new=$3
  mutate "$work/real.sql" "$work/${label}.sql" "$old" "$new"
  run "$label" "$work/${label}.sql"
  if [ "$rc" -ne 0 ]; then echo "FAIL ${label}: mutant errored (want a changed output):"; head -3 "$work/err-${label}.txt"; status=1; return; fi
  if diff -q "$here/expected_real.tsv" "$work/out-${label}.tsv" >/dev/null; then
    fail "${label}: mutant output = expected (not killed)"
  else
    echo "PASS ${label}: killed; diff vs expected_real.tsv:"; diff "$here/expected_real.tsv" "$work/out-${label}.tsv" | head -8 || true
  fi
}
mutant m1_drop_org_other "AND org_other = 0 AS kept" "AS kept"
mutant m2_drop_jobs_eq_1 "AND jobs = 1 AND org_other = 0 AS kept" "AND org_other = 0 AS kept"
mutant m3_sort_key "(x.1, 1 - x.2)" "(x.1, x.2)"
mutant m4_always_outside "if(has(dl_ids, q.3), 'in_download', 'outside')" "'outside'"
docker rm -f ch >/dev/null
exit "$status"
