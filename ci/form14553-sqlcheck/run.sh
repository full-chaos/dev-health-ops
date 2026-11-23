#!/usr/bin/env bash
# Throwaway: executes FORM-14553-1.json on ClickHouse $CH_IMAGE (Distributed shape, one-shard cluster `cluster`):
# PRE-TTL-14542-1.sql, the fill, gate fires g1-g3, read_rows, mutants m1-m6.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${CH_IMAGE:?CH_IMAGE is required}"
# FORM-14553-2 N = 3 fill: START 2026-11-24T00:00Z, END = START + 3 d. Rows have 15 d TTL from the row time (last day 2026-11-26).
start_ms=1795478400000; end_ms=1795737600000
start_s=$(( start_ms / 1000 )); end_s=$(( end_ms / 1000 ))
now_s=$(date -u +%s)
if [ "$now_s" -ge $(( end_s + 1296000 - 86400 )) ]; then echo "FAIL: CI clock is past the row TTL"; exit 1; fi
old_s=$(( now_s / 86400 * 86400 - 5 * 86400 ))
echo "run window [${start_ms}, ${end_ms}) = $(date -u -d @${start_s} +%FT%TZ) .. $(date -u -d @${end_s} +%FT%TZ)"
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
sha12() { sha256sum "$1" | cut -c1-12; }
body_sha12() {
  python3 -I -c 'import hashlib,sys; t=open(sys.argv[1],"rb").read(); i=t.index(b"\"body\": {"); print(hashlib.sha256(t[i:]).hexdigest()[:12])' "$1"
}
[ "$(sha12 "$here/form.json")" = 3153f2f8023a ] || { echo "FAIL: form.json sha12 $(sha12 "$here/form.json")"; exit 1; }
[ "$(sha12 "$here/pre_ttl.sql")" = fe48b20a1f06 ] || { echo "FAIL: pre_ttl.sql sha12 $(sha12 "$here/pre_ttl.sql")"; exit 1; }
[ "$(body_sha12 "$here/form.json")" = 93c7fce802e9 ] || { echo "FAIL: template body sha12 differs"; exit 1; }
fill() { sed -e "s/\${START_MS}/$1/g" -e "s/\${END_MS}/$2/g" "$here/form.json" > "$3"; }
fill "$start_ms" "$end_ms" "$work/real.json"
[ "$(body_sha12 "$work/real.json")" = a6735dbdc17a ] || { echo "FAIL: N=3 fill body sha12 $(body_sha12 "$work/real.json")"; exit 1; }
echo "PASS: form.json 3153f2f8023a, template body 93c7fce802e9, N=3 fill a6735dbdc17a, pre_ttl.sql fe48b20a1f06"
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
python3 -I "$here/fixture.py" rows "$start_s" "$old_s" > "$work/rows.sql"
docker cp "$work/rows.sql" ch:/tmp/rows.sql
ch --queries-file /tmp/rows.sql || { echo "FAIL: insert"; exit 1; }
echo "rows: local $(ch -q 'SELECT count() FROM signoz_traces.signoz_index_v3'), distributed $(ch -q 'SELECT count() FROM signoz_traces.distributed_signoz_index_v3')"
ch -q "SELECT toDate(timestamp) AS part, count() FROM signoz_traces.signoz_index_v3 GROUP BY part ORDER BY part"
f=5000
sys_rows=$(ch -q "SELECT (SELECT count() FROM system.parts) + (SELECT count() FROM system.clusters)")
echo "system table rows (parts + clusters): ${sys_rows}"
if [ "$sys_rows" -lt "$f" ]; then echo "PASS: system table rows ${sys_rows} < F ${f}"; else fail "system table rows ${sys_rows} >= F"; fi

# PRE-TTL-14542-1.sql: WANT active_parts > 0 and oldest_days >= 4 (fixture: one part 5 days before now; the -2 WANT is N + 1 = 4)
docker cp "$here/pre_ttl.sql" ch:/tmp/pre_ttl.sql
pre=$(ch --format TSV --queries-file /tmp/pre_ttl.sql) || { echo "FAIL: PRE-TTL errored"; exit 1; }
echo "PRE-TTL out (active_parts, oldest_days): ${pre}"
parts=$(echo "$pre" | cut -f1); days=$(echo "$pre" | cut -f2)
if [ "$parts" -gt 0 ] && [ "$days" -ge 4 ]; then echo "PASS: PRE-TTL active_parts ${parts} > 0, oldest_days ${days} >= 4"; else fail "PRE-TTL active_parts ${parts}, oldest_days ${days}"; fi

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
# FORM-14553-2 WANT (P2-1): per route failed | rate_limited n >= 403_rate_limited n; on the first route the two extra 429 spans
# (cancelled, failed | other) are in class 429 (3) and not in failed | rate_limited (3)
awk -F'\t' '
  $1=="outcome" && $3=="failed | rate_limited" {o[$2]=$4}
  $1=="class" && $3=="403_rate_limited" {c403[$2]=$4}
  $1=="class" && $3=="429" {c429[$2]=$4}
  $1=="outcome" && $3=="cancelled | " {canc[$2]=$4}
  $1=="outcome" && $3=="failed | other" {oth[$2]=$4}
  END {
    bad=0; nr=0
    for (r in c403) { nr++; if (o[r] < c403[r]) { print "route " r ": failed|rate_limited " o[r] " < 403_rate_limited " c403[r]; bad=1 } }
    r="/repos/{owner}/{repo}/releases"
    if (c429[r] != 3 || o[r] != 3 || canc[r] != 2 || oth[r] != 1) { print "first route: class429=" c429[r] " fail_rl=" o[r] " cancelled=" canc[r] " failed_other=" oth[r]; bad=1 }
    if (nr != 11) { print "routes with a 403_rate_limited row: " nr; bad=1 }
    exit bad
  }' "$work/out-real.tsv" && echo "PASS real: P2-1 WANT (failed|rate_limited >= 403_rate_limited on 11 routes; the 2 extra 429 spans in class 429 n 3, outside failed|rate_limited n 3; cancelled| 2, failed|other 1)" || fail "real: P2-1 WANT"
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
