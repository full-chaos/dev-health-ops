#!/usr/bin/env bash
# One execution of the filled FORM-325-14486-5 SQL on ClickHouse $CH_IMAGE, against the SigNoz v3 trace index
# (DDL from signoz-otel-collector 0476e453f957 traces_migrations.go:14-135) holding 4 synthetic spans.
# Variant "plain": distributed_signoz_index_v3 is a plain MergeTree. Variant "distributed": it is a Distributed table over
# signoz_index_v3 on the server's test_shard_localhost cluster (the scalar WITH then also crosses a shard query).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${CH_IMAGE:?CH_IMAGE is required}"
start_ms=1764000000000
end_ms=1764003600000
work=$(mktemp -d)

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

docker run -d --name ch --ulimit nofile=262144:262144 "$CH_IMAGE" >/dev/null
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
run_variant() {
  local variant=$1 target=$2; shift 2
  echo "=== variant ${variant}"
  docker exec ch clickhouse-client -q 'DROP DATABASE IF EXISTS signoz_traces SYNC'
  docker exec ch clickhouse-client -q 'CREATE DATABASE signoz_traces'
  for ddl in "$@"; do
    docker cp "$here/$ddl" "ch:/tmp/$ddl"
    docker exec ch clickhouse-client --queries-file "/tmp/$ddl" || { echo "FAIL ${variant}: DDL ${ddl} errored"; status=1; return; }
  done
  sed -e "s/TABLE_NAME/${target}/" "$here/rows.sql" > "$work/rows.sql"
  docker cp "$work/rows.sql" ch:/tmp/rows.sql
  docker exec ch clickhouse-client --queries-file /tmp/rows.sql || { echo "FAIL ${variant}: insert errored"; status=1; return; }
  if [ "$variant" = distributed ]; then
    docker exec ch clickhouse-client -q 'SYSTEM FLUSH DISTRIBUTED signoz_traces.distributed_signoz_index_v3' || true
  fi
  docker exec ch clickhouse-client -q 'SELECT count() FROM signoz_traces.distributed_signoz_index_v3'
  if ! docker exec ch clickhouse-client --format TSV --queries-file /tmp/query.sql > "$work/out-${variant}.tsv" 2> "$work/err-${variant}.txt"; then
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
  while IFS=$'\t' read -r sec span label want; do
    got=$(awk -F'\t' -v s="$sec" -v n="$span" -v l="$label" '$1==s && $2==n && $3==l {print $5}' "$work/out-${variant}.tsv")
    if awk -v g="$got" -v w="$want" 'BEGIN{exit !(g != "" && g+0 == w+0)}'; then
      echo "PASS ${variant}: sum ${span} ${label} = ${got}"
    else
      echo "FAIL ${variant}: sum ${span} ${label} = '${got}', want ${want}"; status=1
    fi
  done < "$here/expected_sums.tsv"
}
run_variant plain signoz_traces.distributed_signoz_index_v3 ddl_plain.sql
run_variant distributed signoz_traces.signoz_index_v3 ddl_local.sql ddl_distributed.sql
docker rm -f ch >/dev/null
exit "$status"
