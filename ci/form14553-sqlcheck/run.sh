#!/usr/bin/env bash
# Throwaway: executes FORM-14553-1.json on ClickHouse $CH_IMAGE (Distributed shape, one-shard cluster `cluster`):
# PRE-TTL-14542-1.sql, the fill, gate fires g1-g3, read_rows, mutants m1-m6.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${CH_IMAGE:?CH_IMAGE is required}"
now_s=$(date -u +%s)
end_s=$(( now_s / 86400 * 86400 ))
start_s=$(( end_s - 604800 ))
start_ms=$(( start_s * 1000 )); end_ms=$(( end_s * 1000 ))
echo "run window [${start_ms}, ${end_ms}) = $(date -u -d @${start_s} +%FT%TZ) .. $(date -u -d @${end_s} +%FT%TZ)"
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
sha12() { sha256sum "$1" | cut -c1-12; }
body_sha12() {
  python3 -I -c 'import hashlib,sys; t=open(sys.argv[1],"rb").read(); i=t.index(b"\"body\": {"); print(hashlib.sha256(t[i:]).hexdigest()[:12])' "$1"
}
[ "$(sha12 "$here/form.json")" = 00d094c96d51 ] || { echo "FAIL: form.json sha12 $(sha12 "$here/form.json")"; exit 1; }
[ "$(sha12 "$here/pre_ttl.sql")" = fe48b20a1f06 ] || { echo "FAIL: pre_ttl.sql sha12 $(sha12 "$here/pre_ttl.sql")"; exit 1; }
[ "$(body_sha12 "$here/form.json")" = 93c7fce802e9 ] || { echo "FAIL: template body sha12 differs"; exit 1; }
fill() { sed -e "s/\${START_MS}/$1/g" -e "s/\${END_MS}/$2/g" "$here/form.json" > "$3"; }
fill 1794873600000 1795478400000 "$work/example.json"
[ "$(body_sha12 "$work/example.json")" = ddf93f450367 ] || { echo "FAIL: example fill body sha12 $(body_sha12 "$work/example.json")"; exit 1; }
echo "PASS: form.json 00d094c96d51, template body 93c7fce802e9, example fill ddf93f450367, pre_ttl.sql fe48b20a1f06"
fill "$start_ms" "$end_ms" "$work/real.json"
echo "run fill body sha12 $(body_sha12 "$work/real.json")"
python3 -I - "$work/real.json" "$work/real.sql" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))["body"]
assert isinstance(body["start"], int) and isinstance(body["end"], int)
q = body["compositeQuery"]["queries"]
assert len(q) == 1 and q[0]["type"] == "clickhouse_sql"
open(sys.argv[2], "w").write(q[0]["spec"]["query"])
PY
if grep -q '\${' "$work/real.sql"; then echo "FAIL: unfilled token"; exit 1; fi
python3 -I "$here/fixture.py" expected "$start_s" > "$work/expected.tsv"
echo "expected rows: $(wc -l < "$work/expected.tsv")"

docker run -d --name ch --ulimit nofile=262144:262144 -v "$here/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:ro" "$CH_IMAGE" >/dev/null
for _ in $(seq 1 90); do
  if docker exec ch clickhouse-client -q 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 2
done
ch_version=$(docker exec ch clickhouse-client -q 'SELECT version()')
echo "ClickHouse version(): ${ch_version}"
case "$ch_version" in 25.12.*) echo "PASS: version() starts with 25.12" ;; *) echo "FAIL: version() ${ch_version}"; exit 1 ;; esac
ch() { docker exec ch clickhouse-client "$@"; }
ch -q 'CREATE DATABASE signoz_traces'
for ddl in ddl_local.sql ddl_distributed.sql; do
  docker cp "$here/$ddl" "ch:/tmp/$ddl"
  ch --queries-file "/tmp/$ddl" || { echo "FAIL: DDL $ddl"; exit 1; }
done
python3 -I "$here/fixture.py" rows "$start_s" > "$work/rows.sql"
docker cp "$work/rows.sql" ch:/tmp/rows.sql
ch --queries-file /tmp/rows.sql || { echo "FAIL: insert"; exit 1; }
echo "rows: local $(ch -q 'SELECT count() FROM signoz_traces.signoz_index_v3'), distributed $(ch -q 'SELECT count() FROM signoz_traces.distributed_signoz_index_v3')"
ch -q "SELECT toDate(timestamp) AS part, count() FROM signoz_traces.signoz_index_v3 GROUP BY part ORDER BY part"
f=5000
sys_rows=$(ch -q "SELECT (SELECT count() FROM system.parts) + (SELECT count() FROM system.clusters)")
echo "system table rows (parts + clusters): ${sys_rows}"
if [ "$sys_rows" -lt "$f" ]; then echo "PASS: system table rows ${sys_rows} < F ${f}"; else fail "system table rows ${sys_rows} >= F"; fi

# PRE-TTL-14542-1.sql: WANT active_parts > 0 and oldest_days >= 8 (fixture: one part 9 days before END)
docker cp "$here/pre_ttl.sql" ch:/tmp/pre_ttl.sql
pre=$(ch --format TSV --queries-file /tmp/pre_ttl.sql) || { echo "FAIL: PRE-TTL errored"; exit 1; }
echo "PRE-TTL out (active_parts, oldest_days): ${pre}"
parts=$(echo "$pre" | cut -f1); days=$(echo "$pre" | cut -f2)
if [ "$parts" -gt 0 ] && [ "$days" -ge 8 ]; then echo "PASS: PRE-TTL active_parts ${parts} > 0, oldest_days ${days} >= 8"; else fail "PRE-TTL active_parts ${parts}, oldest_days ${days}"; fi

run() {
  local label=$1 file=$2
  qid="f14553-${label}-$(date +%s)-$RANDOM"
  docker cp "$file" "ch:/tmp/${label}.sql"
  rc=0
  ch --query_id "$qid" --format TSV --queries-file "/tmp/${label}.sql" > "$work/out-${label}.tsv" 2> "$work/err-${label}.txt" || rc=$?
}
run real "$work/real.sql"
if [ "$rc" -ne 0 ]; then echo "FAIL real: errored:"; cat "$work/err-real.txt"; exit 1; fi
echo "real: $(wc -l < "$work/out-real.tsv") rows (WANT < 2000)"
if diff "$work/expected.tsv" "$work/out-real.tsv"; then echo "PASS real: output = expected ($(wc -l < "$work/expected.tsv") rows, derived from the fixture rule)"; else fail "real: output differs from expected"; fi
if [ "$(wc -l < "$work/out-real.tsv")" -lt 2000 ]; then echo "PASS real: row count < 2000"; else fail "row count"; fi
ch -q 'SYSTEM FLUSH LOGS'
rr=$(ch -q "SELECT read_rows FROM system.query_log WHERE query_id = '${qid}' AND type = 'QueryFinish' AND is_initial_query")
echo "query_log real: initial read_rows = ${rr}"
ch -q "SELECT 'secondary', count(), sum(read_rows) FROM system.query_log WHERE initial_query_id = '${qid}' AND type = 'QueryFinish' AND NOT is_initial_query" | sed 's/^/query_log real: /'
ch -q "SELECT 'data rows in bucket range', count() FROM signoz_traces.signoz_index_v3 WHERE ts_bucket_start >= ${start_s} - 1800 AND ts_bucket_start <= ${end_s}" | sed 's/^/query_log real: /'
if [ -n "$rr" ] && [ "$rr" -ge "$f" ] && [ "$rr" -lt $(( 2 * f )) ]; then echo "PASS real: F ${f} <= read_rows ${rr} < 2F $(( 2 * f )) (one data scan)"; else fail "read_rows '${rr}' not in [${f}, $(( 2 * f )))"; fi

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
    echo "FAIL ${label}: not code 395 STOP-COST-GATE:"; head -3 "$work/err-${label}.txt"; status=1; return
  fi
  ch -q 'SYSTEM FLUSH LOGS'
  local ty grr
  ty=$(ch -q "SELECT type FROM system.query_log WHERE query_id = '${qid}' ORDER BY event_time_microseconds DESC LIMIT 1")
  grr=$(ch -q "SELECT read_rows FROM system.query_log WHERE query_id = '${qid}' ORDER BY event_time_microseconds DESC LIMIT 1")
  echo "${label}: query_log type=${ty} read_rows=${grr}"
  if [ "$ty" = ExceptionBeforeStart ]; then echo "PASS ${label}: code 395 STOP-COST-GATE, ExceptionBeforeStart"; else fail "${label}: query_log type '${ty}'"; fi
}
mutate "$work/real.sql" "$work/g1.sql" "NOT BETWEEN 1 AND 20000000" "NOT BETWEEN 1 AND 0"
mutate "$work/real.sql" "$work/g2.sql" "WHERE cluster = 'cluster'" "WHERE cluster = 'nope'"
mutate "$work/real.sql" "$work/g3a.sql" "- 88200" "+ 2503800"
mutate "$work/g3a.sql" "$work/g3.sql" "+ 88200" "+ 2680200"
expect_gate g1; expect_gate g2; expect_gate g3

mutant() {
  local label=$1 old=$2 new=$3
  mutate "$work/real.sql" "$work/${label}.sql" "$old" "$new"
  run "$label" "$work/${label}.sql"
  if [ "$rc" -ne 0 ]; then echo "FAIL ${label}: mutant errored:"; head -3 "$work/err-${label}.txt"; status=1; return; fi
  if diff -q "$work/expected.tsv" "$work/out-${label}.tsv" >/dev/null; then
    fail "${label}: mutant output = expected (not killed)"
  else
    echo "PASS ${label}: killed; first diff rows vs expected:"; diff "$work/expected.tsv" "$work/out-${label}.tsv" | head -6 || true
  fi
}
mutant m1_drop_rl_arm "st = 403 AND et = 'rate_limited', '403_rate_limited', " ""
mutant m2_422 "st IN (400, 422)" "st = 400"
mutant m3_end_le "AND timestamp < fromUnixTimestamp64Milli" "AND timestamp <= fromUnixTimestamp64Milli"
mutant m4_drop_route "'/projects/{project}/releases', " ""
mutant m5_att_ge1 "countIf(att > 1) AS retried" "countIf(att >= 1) AS retried"
mutant m6_drop_name "AND name = 'dev_health.provider.request' " ""
docker rm -f ch >/dev/null
exit "$status"
