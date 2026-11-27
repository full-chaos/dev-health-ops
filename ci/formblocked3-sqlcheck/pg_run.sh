#!/usr/bin/env bash
# Throwaway (never merged): executes BLOCKED-14297-3.pg.sql (the W2 driver's post-row gate read, option A) on PostgreSQL with the real
# public.organizations / integration_sources / sync_runs / sync_run_units / sync_run_unit_chunk_checkpoints DDL from
# internal/pgmigrate/baseline/head.json AT THE WORKERS PIN, a synthetic fixture (seed.sql), the driver's own psql flags (W2-DRIVER-5 D:85:
# -X -q -A -F"|" -v ON_ERROR_STOP=1 -f -) and the driver's own nrows awk (D:92, byte copy below). -2 runs as the control.
# Public-repo rule: the two SQL files here = the lane files (-3 4539c3359dd4, -2 85c4263b8e59) with ONE comment literal swapped to a
# synthetic one (line 51 / the stmt2 WANT note: a prod run / unit prefix pair -> aaaaaaaa: bbbbbbbb); the pins below are the swapped files.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
PIN=7555c48d4ce67ba6402ae2e41ac966524fc130eb
SQL3_SHA12=602bab26db4e
SQL2_SHA12=f9b308176075
ORG8=f0f0f0f0
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
[ "$(sha256sum "$here/BLOCKED-14297-3.pg.sql" | cut -c1-12)" = "$SQL3_SHA12" ] || { echo "FAIL: -3 sha12 differs"; exit 1; }
[ "$(sha256sum "$here/BLOCKED-14297-2.pg.sql" | cut -c1-12)" = "$SQL2_SHA12" ] || { echo "FAIL: -2 sha12 differs"; exit 1; }
echo "BLOCKED-14297-3.pg.sql sha256 $(sha256sum "$here/BLOCKED-14297-3.pg.sql" | cut -c1-64)"
: "${PG_IMAGE:?PG_IMAGE is required}"
docker run -d --name pg -e POSTGRES_PASSWORD=ci "$PG_IMAGE" >/dev/null
for _ in $(seq 1 60); do
  if docker exec pg pg_isready -U postgres >/dev/null 2>&1; then sleep 2; docker exec pg pg_isready -U postgres >/dev/null 2>&1 && break; fi
  sleep 1
done
psql_raw() { docker exec -i pg psql -X -U postgres "$@"; }
echo "server_version: $(psql_raw -Atc 'SHOW server_version')"
git -C "$here/../.." fetch -q --no-tags --depth=1 origin "$PIN"
git -C "$here/../.." show "$PIN:internal/pgmigrate/baseline/head.json" > "$work/head.json"
python3 -I - "$work/head.json" "$work/schema.sql" <<'PY'
import json, re, sys
d = json.load(open(sys.argv[1])); s = d["schema"]
print("pin head.json heads:", d["heads"])
out = []
for tbl in ("organizations", "integration_sources", "sync_runs", "sync_run_units", "sync_run_unit_chunk_checkpoints"):
    m = re.search(r"CREATE TABLE public\.%s \(.*?\n\);" % tbl, s, re.S)
    assert m, tbl
    out.append(m.group(0))
open(sys.argv[2], "w").write("\n\n".join(out) + "\n")
PY
psql_raw -Atc 'CREATE DATABASE devhealth' >/dev/null
psql_fx() { docker exec -i pg psql -X -U postgres -d devhealth -v ON_ERROR_STOP=1 "$@"; }
psql_fx -q < "$work/schema.sql"
psql_fx -q < "$here/seed.sql"
before=$(psql_fx -Atc "SELECT (SELECT count(*) FROM public.sync_runs) || '/' || (SELECT count(*) FROM public.sync_run_units)")
# the driver's nrows (W2-DRIVER-5 / -6 D:92), byte copy:
nrows() { awk -v h="$1" 'index($0, h) == 1 { n = 0; while ((getline) > 0) { if ($0 ~ /^\([0-9]+ rows?\)$/) break; n++ } print n; exit }'; }
# run <label> <sql file> <run prefix>: the driver's psql_read form (sed of the 2 placeholders, D:83; psql flags D:85; rc line D:87)
run() {
  local label=$1 file=$2 pre=$3 rc=0
  sed -e "s/{{ORG_PREFIX}}/$ORG8/g" -e "s/{{RUN_PREFIX}}/$pre/g" "$file" > "$work/$label.sql"
  [ "$(grep -c '{{' "$work/$label.sql")" = 0 ] || { fail "$label: placeholder left"; return; }
  docker exec -i pg psql -X -q -A -F"|" -v ON_ERROR_STOP=1 -U postgres -d devhealth -f - < "$work/$label.sql" > "$work/$label.out" 2>&1 || rc=$?
  echo "rc=$rc" >> "$work/$label.out"
  echo "--- $label ($pre)"; cat "$work/$label.out"
}
# every statement executed: each header line present once, rc=0 last
expect() { # label stmt2_rows stmt3_rows runs [-2 mode: no stmt3]
  local label=$1 w2=$2 w3=$3 wr=$4 mode=${5:-3} f="$work/$1.out" h n2 n3
  for h in '^orgs$' '^pgv$' '^runs$' '^status\|units\|budget_blocked$' '^unit_8\|source_8\|repo_key\|status\|'; do
    [ "$(grep -cE "$h" "$f")" = 1 ] || { fail "$label: header $h count != 1"; return; }
  done
  if [ "$mode" = 3 ]; then [ "$(grep -cE '^adv_unit_8\|adv_source_8\|adv_repo_key\|adv_status\|' "$f")" = 1 ] || { fail "$label: stmt3 header count != 1"; return; }
  else [ "$(grep -c '^adv_' "$f")" = 0 ] || { fail "$label: -2 has an adv_ header"; return; }; fi
  [ "$(tail -n 1 "$f")" = "rc=0" ] || { fail "$label: rc"; return; }
  [ "$(sed -n '/^orgs$/{n;p;}' "$f")" = 1 ] || { fail "$label: ORG-1 != 1"; return; }
  [ "$(sed -n '/^runs$/{n;p;}' "$f")" = "$wr" ] || { fail "$label: stmt0 runs != $wr"; return; }
  n2=$(nrows 'unit_8|source_8|repo_key|' < "$f")
  [ "$n2" = "$w2" ] || { fail "$label: driver nrows stmt2 = $n2, want $w2"; return; }
  if [ "$mode" = 3 ]; then n3=$(nrows 'adv_unit_8|adv_source_8|adv_repo_key|' < "$f"); [ "$n3" = "$w3" ] || { fail "$label: stmt3 rows = $n3, want $w3"; return; }; fi
  echo "PASS $label: every statement ran (rc=0), ORG-1 = 1, runs $wr, driver nrows(stmt2) = $n2$([ "$mode" = 3 ] && echo ", stmt3 rows = $n3")"
}
S3="$here/BLOCKED-14297-3.pg.sql"; S2="$here/BLOCKED-14297-2.pg.sql"
run adv   "$S3" a1a1a1a1; expect adv   0 1 1
run inv   "$S3" b2b2b2b2; expect inv   1 0 1
run mixed "$S3" c3c3c3c3; expect mixed 1 0 1
run shape "$S3" d4d4d4d4; expect shape 2 0 1
run two   "$S3" e5e5e5e5; expect two   1 1 1
run other "$S3" f6f6f6f6; expect other 0 0 0
# controls that can fail: -2 on the same runs counts the advancing-only units (the gate the driver hit)
run c2adv "$S2" a1a1a1a1; expect c2adv 1 x 1 2
run c2two "$S2" e5e5e5e5; expect c2two 2 x 1 2
# the stmt2 row of 'inv' is the budget unit and of 'mixed' is the run_artifacts one: the entries print
grep -q '|run_inventory:page_budget_exhausted:1|' "$work/inv.out" && echo "PASS inv: the stmt2 row carries run_inventory:page_budget_exhausted:1" || fail "inv: entry text"
grep -q '|run_artifacts:page_budget_exhausted:2,run_reports:per_run_cap:9|' "$work/mixed.out" && echo "PASS mixed: the stmt2 row carries both entries" || fail "mixed: entry text"
grep -q '^a1000001|00000000|.*|report_member:malformed:6,run_reports:per_run_cap:141|.*|7|100|3|2|5|11|abc$' "$work/adv.out" && echo "PASS adv: the stmt3 row = the advancing-only unit with its checkpoint counters" || fail "adv: stmt3 row text"
# mutant: a pair list without run_reports:per_run_cap -> the advancing-only unit moves back to stmt2 (the list is the gate)
python3 -I - "$S3" "$work/m_nopair.sql" <<'PY'
import sys
s = open(sys.argv[1]).read(); old = "        ('run_reports', 'per_run_cap'),\n"
assert s.count(old) == 2, s.count(old)
open(sys.argv[2], "w").write(s.replace(old, ""))
PY
run m_nopair "$work/m_nopair.sql" a1a1a1a1; expect m_nopair 1 0 1
after=$(psql_fx -Atc "SELECT (SELECT count(*) FROM public.sync_runs) || '/' || (SELECT count(*) FROM public.sync_run_units)")
if [ "$after" = "$before" ]; then echo "PASS read-only: row counts $after unchanged"; else fail "row counts changed: $before -> $after"; fi
docker rm -f pg >/dev/null
exit "$status"
