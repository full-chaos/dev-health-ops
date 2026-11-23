#!/usr/bin/env bash
# Throwaway: executes FORM-14542-2-PG.sql (form_pg.sql here) on PostgreSQL 18 with the real public.organizations and
# public.sync_run_units DDL taken from internal/pgmigrate/baseline/head.json at this PR's base (no FKs).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
: "${PG_IMAGE:?PG_IMAGE is required}"
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
[ "$(sha256sum "$here/form_pg.sql" | cut -c1-12)" = 2ce078513d7e ] || { echo "FAIL: form_pg.sql sha12 differs"; exit 1; }
echo "form_pg.sql sha256 $(sha256sum "$here/form_pg.sql" | cut -c1-64)"

python3 -I - "$root/internal/pgmigrate/baseline/head.json" "$work/schema.sql" <<'PY'
import json, re, sys
s = json.load(open(sys.argv[1]))["schema"]
out = []
for tbl in ("organizations", "sync_run_units"):
    m = re.search(r"CREATE TABLE public\.%s \(.*?\n\);" % tbl, s, re.S)
    assert m, tbl
    out.append(m.group(0))
    p = re.search(r"ALTER TABLE ONLY public\.%s\n    ADD CONSTRAINT %s_pkey PRIMARY KEY \(id\);" % (tbl, tbl), s)
    assert p, tbl + " pkey"
    out.append(p.group(0))
open(sys.argv[2], "w").write("\n\n".join(out) + "\n")
PY
cat "$work/schema.sql" | head -5
cat > "$work/rows.sql" <<'SQL'
INSERT INTO public.organizations (id, slug, name) VALUES
  ('c6a38355-0000-4000-8000-000000000001', 'org-target', 'Org Target'),
  ('99999999-0000-4000-8000-000000000009', 'org-other', 'Org Other');
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at) VALUES
  ('aaaaaaaa-0000-4000-8000-000000000001', 'c6a38355-0000-4000-8000-000000000001', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000c1', 'github', 'cicd', 'medium', 'incremental', 'done', 1, now(), now()),
  ('bbbbbbbb-0000-4000-8000-000000000002', 'c6a38355-0000-4000-8000-000000000001', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000c1', 'github', 'tests', 'heavy', 'incremental', 'done', 1, now(), now()),
  ('cccccccc-0000-4000-8000-000000000003', 'c6a38355-0000-4000-8000-000000000001', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000c1', 'github', 'cicd', 'medium', 'incremental', 'done', 1, now(), now()),
  ('cccccccc-1111-4000-8000-000000000004', 'c6a38355-0000-4000-8000-000000000001', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000c1', 'github', 'cicd', 'medium', 'incremental', 'done', 1, now(), now()),
  ('dddddddd-0000-4000-8000-000000000005', '99999999-0000-4000-8000-000000000009', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000c1', 'github', 'tests', 'heavy', 'incremental', 'done', 1, now(), now());
ANALYZE public.organizations;
ANALYZE public.sync_run_units;
SQL

docker run -d --name pg -e POSTGRES_PASSWORD=ci "$PG_IMAGE" >/dev/null
for _ in $(seq 1 60); do
  if docker exec pg pg_isready -U postgres >/dev/null 2>&1; then sleep 2; docker exec pg pg_isready -U postgres >/dev/null 2>&1 && break; fi
  sleep 1
done
echo "server_version: $(docker exec pg psql -X -U postgres -Atc 'SHOW server_version')"
docker exec pg psql -X -U postgres -Atc 'CREATE DATABASE fx' >/dev/null
psql_fx() { docker exec -i pg psql -X -U postgres -d fx -v ON_ERROR_STOP=1 "$@"; }
docker cp "$work/schema.sql" pg:/tmp/schema.sql
docker cp "$work/rows.sql" pg:/tmp/rows.sql
psql_fx -q -f /tmp/schema.sql
psql_fx -q -f /tmp/rows.sql
echo "reltuples: $(psql_fx -Atc "SELECT relname || '=' || reltuples::bigint FROM pg_class WHERE relname IN ('sync_run_units','organizations') ORDER BY 1" | tr '\n' ' ')"
docker cp "$here/form_pg.sql" pg:/tmp/form_pg.sql

# form <label> <u8s> [sql file in container] -> rc, $work/pg-<label>.out / .err
form() {
  local label=$1 u8s=$2 file=${3:-/tmp/form_pg.sql}
  rc=0
  docker exec -e PGOPTIONS='-c statement_timeout=30s' pg psql -X -U postgres -d fx --csv -v ON_ERROR_STOP=1 -v "u8s=${u8s}" -f "$file" > "$work/pg-${label}.out" 2> "$work/pg-${label}.err" || rc=$?
}

cat > "$work/want_pass.out" <<'OUT'
BEGIN
cost_gate_ok
0
orgs
1
input_ok
0
u8,matched,provider,dataset_key,cost_class,mode
aaaaaaaa,1,github,cicd,medium,incremental
bbbbbbbb,1,github,tests,heavy,incremental
cccccccc,2,github,cicd,medium,incremental
dddddddd,0,,,,
eeeeeeee,0,,,,
cuts,cuts_matched_1,cuts_unmatched,cuts_ambiguous,cicd,tests,other_dataset,cicd_not_medium,tests_not_heavy
5,2,2,1,1,1,0,0,0
ROLLBACK
OUT
form pass '{aaaaaaaa,bbbbbbbb,cccccccc,dddddddd,eeeeeeee}'
echo "--- pass run (rc=${rc})"; cat "$work/pg-pass.out"; cat "$work/pg-pass.err"
if [ "$rc" -ne 0 ]; then fail "pass run rc ${rc}"; fi
if diff "$work/want_pass.out" "$work/pg-pass.out"; then echo "PASS: pass-run output = the md WANTs (G0 0, stmt0 1, G1 0, S1 per cut, S2 5/2/2/1)"; else fail "pass-run output differs from the md WANTs"; fi

mutate() { python3 -I - "$here/form_pg.sql" "$1" "$2" "$3" <<'PY'
import sys
src, dst, old, new = sys.argv[1:5]
t = open(src).read()
assert t.count(old) == 1, "pattern %r occurs %d times" % (old, t.count(old))
open(dst, "w").write(t.replace(old, new))
PY
}
# after a STOP: rc != 0, the error text, and no later statement output (ON_ERROR_STOP)
expect_stop() {
  local label=$1 stop=$2; shift 2
  echo "--- ${label} (rc=${rc})"; cat "$work/pg-${label}.out"; cat "$work/pg-${label}.err"
  if [ "$rc" -eq 0 ]; then fail "${label}: no STOP (rc 0)"; return; fi
  if ! grep -q "$stop" "$work/pg-${label}.err"; then fail "${label}: stderr has no '${stop}'"; return; fi
  local hdr
  for hdr in "$@"; do
    if grep -q "$hdr" "$work/pg-${label}.out"; then fail "${label}: later output '${hdr}' ran after the STOP"; return; fi
  done
  echo "PASS ${label}: ${stop} fires, rc ${rc}, no later statement ran"
}
# G0: cap literals low (reltuples cap, heap cap), then a dropped pkey
mutate "$work/g0_reltuples.sql" 'BETWEEN 0 AND 1000000' 'BETWEEN 0 AND 1'
docker cp "$work/g0_reltuples.sql" pg:/tmp/g0_reltuples.sql
form g0_reltuples '{aaaaaaaa}' /tmp/g0_reltuples.sql
expect_stop g0_reltuples 'STOP-G0' orgs input_ok matched cuts
mutate "$work/g0_heap.sql" '<= 1073741824' '<= 1'
docker cp "$work/g0_heap.sql" pg:/tmp/g0_heap.sql
form g0_heap '{aaaaaaaa}' /tmp/g0_heap.sql
expect_stop g0_heap 'STOP-G0' orgs input_ok matched cuts
psql_fx -q -c 'ALTER TABLE public.sync_run_units DROP CONSTRAINT sync_run_units_pkey'
form g0_nopk '{aaaaaaaa}'
expect_stop g0_nopk 'STOP-G0' orgs input_ok matched cuts
psql_fx -q -c 'ALTER TABLE ONLY public.sync_run_units ADD CONSTRAINT sync_run_units_pkey PRIMARY KEY (id)'
# G1: bad cut, repeated cut, empty set (stmt0 runs before G1; S1 and S2 must not)
form g1_bad '{zzzzzzzz}'
expect_stop g1_bad 'STOP-G1' input_ok matched cuts
form g1_repeat '{aaaaaaaa,aaaaaaaa}'
expect_stop g1_repeat 'STOP-G1' input_ok matched cuts
form g1_empty '{}'
expect_stop g1_empty 'STOP-G1' input_ok matched cuts
docker rm -f pg >/dev/null
exit "$status"
