#!/usr/bin/env bash
# Throwaway (never merged): executes OPENRUNS-14297-2.pg.sql (the W2 driver -7 open-runs WAIT read) on PostgreSQL with the real
# public.organizations / sync_runs / sync_run_units DDL + the ix_sync_run_units_run_status index from internal/pgmigrate/baseline/head.json
# AT THE WORKERS PIN, a synthetic fixture (seed.sql), the driver's psql flags (W2-DRIVER-7 psql_read: -X -q -A -F"|" -v ON_ERROR_STOP=1
# -f -) and the driver's col awk (byte copy below). Every gate is fired once (named STOP, rc != 0 = the driver's WAIT).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
PIN=7555c48d4ce67ba6402ae2e41ac966524fc130eb
SQL_SHA12=1c5e7d000e0d
ORG8=f0f0f0f0
work=$(mktemp -d)
status=0
fail() { echo "FAIL: $*"; status=1; }
[ "$(sha256sum "$here/OPENRUNS-14297-2.pg.sql" | cut -c1-12)" = "$SQL_SHA12" ] || { echo "FAIL: sql sha12 differs"; exit 1; }
echo "OPENRUNS-14297-2.pg.sql sha256 $(sha256sum "$here/OPENRUNS-14297-2.pg.sql" | cut -c1-64)"
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
for tbl in ("organizations", "sync_runs", "sync_run_units"):
    m = re.search(r"CREATE TABLE public\.%s \(.*?\n\);" % tbl, s, re.S)
    assert m, tbl
    out.append(m.group(0))
i = re.search(r"CREATE INDEX ix_sync_run_units_run_status ON public\.sync_run_units USING btree \(sync_run_id, status\);", s)
assert i, "run_status index"
out.append(i.group(0))
open(sys.argv[2], "w").write("\n\n".join(out) + "\n")
PY
psql_raw -Atc 'CREATE DATABASE devhealth' >/dev/null
psql_fx() { docker exec -i pg psql -X -U postgres -d devhealth -v ON_ERROR_STOP=1 "$@"; }
psql_fx -q < "$work/schema.sql"
psql_fx -q < "$here/seed.sql"
before=$(psql_fx -Atc "SELECT (SELECT count(*) FROM public.sync_runs) || '/' || (SELECT count(*) FROM public.sync_run_units)")
# the driver's col (W2-DRIVER-7 = W2-DRIVER-5 D:91), byte copy:
col() { awk -F'|' -v h="$1" -v c="$2" '($0 == h) || (substr(h, length(h)) == "|" && index($0, h) == 1) { for (i = 1; i <= NF; i++) if ($i == c) k = i; getline; print $k; exit }'; }
run() { # <label> <sql file> <org8>: the driver's psql_read form (the 2 placeholder fills, its psql flags, its rc line)
  local label=$1 file=$2 org=$3 rc=0
  sed -e "s/{{ORG_PREFIX}}/$org/g" -e "s/{{RUN_PREFIX}}/org/g" "$file" > "$work/$label.sql"
  docker exec -i pg psql -X -q -A -F"|" -v ON_ERROR_STOP=1 -U postgres -d devhealth -f - < "$work/$label.sql" > "$work/$label.out" 2>&1 || rc=$?
  echo "rc=$rc" >> "$work/$label.out"; RC=$rc
  echo "--- $label (rc=$rc)"; cat "$work/$label.out"
}
expect() { # <label> <open_runs> <open_units> <open_prefixes>: every statement ran, the driver's reads
  local f="$work/$1.out" g n u pl o
  [ "$RC" = 0 ] || { fail "$1: rc $RC"; return; }
  for h in '^cost_gate_ok$' '^orgs$' '^open_runs\|open_units\|open_prefixes$'; do [ "$(grep -cE "$h" "$f")" = 1 ] || { fail "$1: header $h"; return; }; done
  g=$(col cost_gate_ok cost_gate_ok < "$f"); o=$(col orgs orgs < "$f"); n=$(col 'open_runs|' open_runs < "$f"); u=$(col 'open_runs|' open_units < "$f"); pl=$(col 'open_runs|' open_prefixes < "$f")
  [ "$g|$o|$n|$u|$pl" = "0|1|$2|$3|$4" ] || { fail "$1: got $g|$o|$n|$u|$pl want 0|1|$2|$3|$4"; return; }
  echo "PASS $1: every statement ran (rc=0); cost_gate_ok 0, ORG-1 1, open_runs $n, open_units $u, open_prefixes $pl (the driver's col reads)"
}
expect_stop() { # <label> <stop text>
  if [ "$RC" != 0 ] && grep -q "$2" "$work/$1.out" && ! grep -q '^open_runs|' "$work/$1.out"; then echo "PASS $1: $2 fires, rc $RC, no later statement ran (the driver WAITs on rc != 0)"
  else fail "$1: no $2 (rc $RC)"; fi
}
mut() { python3 -I - "$here/OPENRUNS-14297-2.pg.sql" "$work/$1.src.sql" "$2" "$3" <<'PY'
import sys
s = open(sys.argv[1]).read(); old, new = sys.argv[3], sys.argv[4]
assert s.count(old) >= 1, old
open(sys.argv[2], "w").write(s.replace(old, new))
PY
}
S="$here/OPENRUNS-14297-2.pg.sql"
run open "$S" "$ORG8"; expect open 2 3 'a1a1a1a1,b2b2b2b2'
run g0_org "$S" eeeeeeee; expect_stop g0_org 'STOP-G0 orgs=0'
mut c_bytes '1073741824::bigint' '0::bigint'; run c_bytes "$work/c_bytes.src.sql" "$ORG8"; expect_stop c_bytes 'STOP-G0 orgs=1 rels=2 runs_bytes='
mut c_s1 'LIMIT 1001' 'LIMIT 2'; python3 -I - "$work/c_s1.src.sql" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read(); assert s.count("<= 1000") == 1; open(p, "w").write(s.replace("<= 1000", "<= 1"))
PY
run c_s1 "$work/c_s1.src.sql" "$ORG8"
if [ "$RC" != 0 ] && grep -q 'STOP-S1 open_runs=2"' "$work/c_s1.out" && ! grep -q '^open_runs|' "$work/c_s1.out"; then echo "PASS c_s1: the S1 cap fires and names open_runs=2 (cap 1 < 2 open runs), rc $RC"; else fail "c_s1: no STOP-S1 open_runs=2"; fi
psql_fx -q -c "UPDATE public.sync_runs SET status = 'success' WHERE id IN ('a1a1a1a1-0000-4000-8000-0000000000a1', 'b2b2b2b2-0000-4000-8000-0000000000b2')"
run closed "$S" "$ORG8"; expect closed 0 0 '-'
psql_fx -q -c "UPDATE public.sync_runs SET status = 'dispatching' WHERE id = 'a1a1a1a1-0000-4000-8000-0000000000a1'"
psql_fx -q -c "UPDATE public.sync_runs SET status = 'running' WHERE id = 'b2b2b2b2-0000-4000-8000-0000000000b2'"
after=$(psql_fx -Atc "SELECT (SELECT count(*) FROM public.sync_runs) || '/' || (SELECT count(*) FROM public.sync_run_units)")
if [ "$after" = "$before" ]; then echo "PASS read-only: row counts $after unchanged"; else fail "row counts $before -> $after"; fi
psql_fx -q -c 'DROP INDEX public.ix_sync_run_units_run_status'
run g0_ix "$S" "$ORG8"; expect_stop g0_ix 'STOP-G0 orgs=1 rels=2 runs_bytes=[0-9]* ix=0'
docker rm -f pg >/dev/null
exit "$status"
