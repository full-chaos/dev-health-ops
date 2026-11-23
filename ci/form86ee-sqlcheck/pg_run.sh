#!/usr/bin/env bash
# Throwaway: executes FORM-W2-86EE-ERRCLASS-1 (form_pg.sql here = the FORM with the org literal swapped to the synthetic prefix)
# on PostgreSQL with the real public.organizations / sync_runs / sync_run_units DDL taken from internal/pgmigrate/baseline/head.json
# AT THE WORKERS PIN (no FKs). Modes: PG_IMAGE set = docker (CI); LOCAL_PG="-h <dir> -p <port> -U postgres" = a local server.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
PIN=a7febdf500025f5ac628a1bdf34d0f817a90e323
FORM_SHA12=57935db8607c
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
[ "$(sha256sum "$here/form_pg.sql" | cut -c1-12)" = "$FORM_SHA12" ] || { echo "FAIL: form_pg.sql sha12 differs"; exit 1; }
echo "form_pg.sql sha256 $(sha256sum "$here/form_pg.sql" | cut -c1-64)"

if [ -n "${LOCAL_PG:-}" ]; then
  psql_raw() { psql -X $LOCAL_PG "$@"; }
  stage() { echo "$1"; }
else
  : "${PG_IMAGE:?PG_IMAGE or LOCAL_PG is required}"
  docker run -d --name pg -e POSTGRES_PASSWORD=ci "$PG_IMAGE" >/dev/null
  for _ in $(seq 1 60); do
    if docker exec pg pg_isready -U postgres >/dev/null 2>&1; then sleep 2; docker exec pg pg_isready -U postgres >/dev/null 2>&1 && break; fi
    sleep 1
  done
  psql_raw() { docker exec -i -e PGOPTIONS="${PGOPTIONS:-}" pg psql -X -U postgres "$@"; }
  stage() { docker cp "$1" "pg:/tmp/$(basename "$1")"; echo "/tmp/$(basename "$1")"; }
fi
echo "server_version: $(psql_raw -Atc 'SHOW server_version')"

# schema from the pin
if [ -n "${PIN_HEAD_JSON:-}" ]; then
  cp "$PIN_HEAD_JSON" "$work/head.json"
else
  git -C "$here/../.." fetch -q --no-tags --depth=1 origin "$PIN"
  git -C "$here/../.." show "$PIN:internal/pgmigrate/baseline/head.json" > "$work/head.json"
fi
python3 -I - "$work/head.json" "$work/schema.sql" <<'PY'
import json, re, sys
d = json.load(open(sys.argv[1]))
s = d["schema"]
print("pin head.json heads:", d["heads"])
out = []
for tbl in ("organizations", "sync_runs", "sync_run_units"):
    m = re.search(r"CREATE TABLE public\.%s \(.*?\n\);" % tbl, s, re.S)
    assert m, tbl
    out.append(m.group(0))
    p = re.search(r"ALTER TABLE ONLY public\.%s\n    ADD CONSTRAINT %s_pkey PRIMARY KEY \(id\);" % (tbl, tbl), s)
    assert p, tbl + " pkey"
    out.append(p.group(0))
i = re.search(r"CREATE INDEX ix_sync_run_units_run_status ON public\.sync_run_units USING btree \(sync_run_id, status\);", s)
assert i, "run_status index"
out.append(i.group(0))
open(sys.argv[2], "w").write("\n\n".join(out) + "\n")
PY
head -3 "$work/schema.sql"

psql_raw -Atc 'DROP DATABASE IF EXISTS fx' >/dev/null
psql_raw -Atc 'CREATE DATABASE fx' >/dev/null
psql_fx() { psql_raw -d fx -v ON_ERROR_STOP=1 "$@"; }
psql_fx -q -f "$(stage "$work/schema.sql")"
psql_fx -q -f "$(stage "$here/seed.sql")"
echo "reltuples: $(psql_fx -Atc "SELECT relname || '=' || reltuples::bigint FROM pg_class WHERE relname IN ('sync_run_units','sync_runs','organizations') ORDER BY 1" | tr '\n' ' ')"
cp "$here/form_pg.sql" "$work/form_pg.sql"
stage "$work/form_pg.sql" >/dev/null

# form <label> <r1> <r2> [sql file] -> rc, $work/pg-<label>.out / .err
form() {
  local label=$1 r1=$2 r2=$3 file=${4:-$work/form_pg.sql} p
  p=$(stage "$file"); rc=0
  PGOPTIONS='-c statement_timeout=30s' psql_raw -d fx --csv -v ON_ERROR_STOP=1 -v "r1=${r1}" -v "r2=${r2}" -f "$p" > "$work/pg-${label}.out" 2> "$work/pg-${label}.err" || rc=$?
}

form pass aaaaaaaa bbbbbbbb
echo "--- pass run (rc=${rc})"; cat "$work/pg-pass.out"; cat "$work/pg-pass.err"
if [ "$rc" -ne 0 ]; then fail "pass run rc ${rc}"; fi
if diff "$here/want_pass.out" "$work/pg-pass.out"; then echo "PASS: pass-run output = the md WANTs"; else fail "pass-run output differs from the md WANTs"; fi

# segment mutate: replace OLD (exactly once) inside segment SEG ('G0' = the text before @@S1, else S1..S3)
mutate() { python3 -I - "$here/form_pg.sql" "$1" "$2" "$3" "$4" <<'PY'
import re, sys
src, dst, seg, old, new = sys.argv[1:6]
t = open(src).read()
parts = re.split(r"(?m)^(?=-- @@)", t)
hit = 0
for i, p in enumerate(parts):
    name = "G0" if i == 0 else p[5:7]
    if name == seg:
        assert p.count(old) >= 1, "segment %s has no %r" % (seg, old)
        hit = p.count(old)
        parts[i] = p.replace(old, new)
assert hit, "segment %s not found" % seg
open(dst, "w").write("".join(parts))
PY
}
expect_stop() {
  local label=$1 stop=$2 gotout=$3 noout=$4
  echo "--- ${label} (rc=${rc})"; cat "$work/pg-${label}.out"; cat "$work/pg-${label}.err"
  if [ "$rc" -eq 0 ]; then fail "${label}: no STOP (rc 0)"; return; fi
  if ! grep -q "$stop" "$work/pg-${label}.err"; then fail "${label}: stderr has no '${stop}'"; return; fi
  local h
  for h in $gotout; do grep -q "$h" "$work/pg-${label}.out" || { fail "${label}: earlier output '${h}' missing"; return; }; done
  for h in $noout; do if grep -q "$h" "$work/pg-${label}.out"; then fail "${label}: later output '${h}' ran after the STOP"; return; fi; done
  echo "PASS ${label}: ${stop} fires, rc ${rc}, no later statement ran"
}
# headers of the statements (csv): G0 cost_gate_ok, stmt0 orgs, G1 input_ok, S1 run_prefix,run8, S2 unit_status, S3 unit8
H_G0=cost_gate_ok H_O=orgs H_G1=input_ok H_S1=run_prefix,run8 H_S2=unit_status,error_code H_S3=unit8,dataset_key

# mutant 1: wrong org prefix -> ORG-1 (G0 carries orgs = 1) STOP
sed 's/f0f0f0f0/f0f0f0f1/g' "$work/form_pg.sql" > "$work/m_org.sql"
form m_org aaaaaaaa bbbbbbbb "$work/m_org.sql"
expect_stop m_org 'STOP-G0 orgs=0' '' "$H_O $H_G1 $H_S1"
# mutant 2: unknown run prefix (no such run) -> STOP in S1 gate; earlier statements ran
form m_unknown aaaaaaaa eeeeeeee
expect_stop m_unknown 'STOP-S1-G2 runs_matched_1=1' "$H_G0 $H_O $H_G1" "$H_S1 $H_S2 $H_S3"
# mutant 3: ambiguous prefix (2 runs of the org) -> STOP
form m_ambig aaaaaaaa cccccccc
expect_stop m_ambig 'STOP-S1-G2 runs_matched_1=1 runs_ambiguous=1' "$H_G0 $H_O $H_G1" "$H_S1 $H_S2 $H_S3"
# mutant 3b: prefix of a run of ANOTHER org -> matched 0 -> STOP
form m_otherorg aaaaaaaa dddddddd
expect_stop m_otherorg 'STOP-S1-G2 runs_matched_1=1 runs_ambiguous=0' "$H_G0 $H_O $H_G1" "$H_S1 $H_S2 $H_S3"
# G1: bad prefix, same prefix twice
form g1_bad aaaaaaaa zzzzzzzz
expect_stop g1_bad 'STOP-G1' "$H_G0 $H_O" "$H_S1 $H_S2 $H_S3"
form g1_same aaaaaaaa aaaaaaaa
expect_stop g1_same 'STOP-G1' "$H_G0 $H_O" "$H_S1 $H_S2 $H_S3"
# cost gates: cap 0 per statement. G0 standalone, then S1..S3 catalog cap (-G0) and unit-row cap (-G2).
mutate "$work/c_g0.sql" G0 '1073741824::bigint' '0::bigint'
form c_g0 aaaaaaaa bbbbbbbb "$work/c_g0.sql"
expect_stop c_g0 'STOP-G0 orgs=1' '' "$H_O $H_G1 $H_S1"
prev="$H_G0 $H_O $H_G1"
for s in S1 S2 S3; do
  case $s in S1) later="$H_S1 $H_S2 $H_S3";; S2) later="$H_S2 $H_S3";; S3) later="$H_S3";; esac
  case $s in S1) got="$prev";; S2) got="$prev $H_S1";; S3) got="$prev $H_S1 $H_S2";; esac
  mutate "$work/c_${s}_g0.sql" $s '2147483648::bigint' '0::bigint'
  form c_${s}_g0 aaaaaaaa bbbbbbbb "$work/c_${s}_g0.sql"
  expect_stop c_${s}_g0 "STOP-${s}-G0" "$got" "$later"
  mutate "$work/c_${s}_g2.sql" $s 'BETWEEN 1 AND 20000' 'BETWEEN 1 AND 0'
  form c_${s}_g2 aaaaaaaa bbbbbbbb "$work/c_${s}_g2.sql"
  expect_stop c_${s}_g2 "STOP-${s}-G2" "$got" "$later"
done
# unit cap low (3 < the 6 + 5 seeded unit rows): S1 STOPs on the data gate
mutate "$work/c_cap3.sql" S1 'BETWEEN 1 AND 20000' 'BETWEEN 1 AND 3'
form c_cap3 aaaaaaaa bbbbbbbb "$work/c_cap3.sql"
expect_stop c_cap3 'STOP-S1-G2 runs_matched_1=2 runs_ambiguous=0 unit_rows_capped=11' "$prev" "$H_S1"
# dropped index (G0 ix = 1) and invalid-by-drop pkey
psql_fx -q -c 'DROP INDEX public.ix_sync_run_units_run_status'
form g0_noix aaaaaaaa bbbbbbbb
expect_stop g0_noix 'STOP-G0 orgs=1 rels=3 relok=3 ix=1' '' "$H_O $H_G1 $H_S1"
# read-only transaction: the form writes nothing (row counts unchanged)
echo "tables after: $(psql_fx -Atc "SELECT (SELECT count(*) FROM public.sync_runs) || '/' || (SELECT count(*) FROM public.sync_run_units)")"
[ -z "${LOCAL_PG:-}" ] && docker rm -f pg >/dev/null
exit "$status"
