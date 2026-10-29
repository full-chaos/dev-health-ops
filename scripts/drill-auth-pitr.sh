#!/usr/bin/env bash
# scripts/drill-auth-pitr.sh -- backup / restore / PITR drill of the auth schema.
#
# Runs on a THROWAWAY compose project (drill-11232): one Postgres source with
# WAL archiving, then a restore copy at a chosen time T, then a restore copy to
# the end of the WAL. It never touches the standing dev stack and never removes
# a Docker volume (bind mounts under DRILL_DIR only; teardown is `down`, no -v).
#
# The driver is TestDrillAuthPITR (internal/audit/drill_pitr_integration_test.go).
# Passwords are generated at run time and live only in this shell's environment.
#
# Usage: DRILL_DIR=<new empty dir> scripts/drill-auth-pitr.sh
#
# DRILL_FENCE=1 runs the FENCED drill (CHAOS-11271): the source is initialised with `auth-migrate restore-fence init`
# (a fresh database on timeline 1, as the first roll does), the end-of-WAL copy is restored FIRST and `restore-fence
# export` runs against it, the PITR copy then starts read-only (default_transaction_read_only=on in its
# postgresql.auto.conf), `restore-fence status` must say not fenced, `apply` runs as a dry run and then with -apply
# (a per-connection override of the read-only default), the read-only default is reset, and the verify and post
# phases must show no revocation after T alive and no key re-issued with different content. The default (unfenced)
# drill is unchanged and still reports the GAP.
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
compose_file="$repo_root/scripts/drill-auth-pitr.compose.yml"
project=drill-11232
capped="${DRILL_CAPPED:-/home/ubuntu/devhealth/scripts/capped.sh}"
: "${DRILL_DIR:?set DRILL_DIR to a new directory for the drill data and evidence}"
if [[ -e "$DRILL_DIR" ]] && [[ -n "$(ls -A "$DRILL_DIR" 2>/dev/null)" ]]; then
  echo "drill-auth-pitr.sh: $DRILL_DIR exists and is not empty" >&2
  exit 1
fi
mkdir -p "$DRILL_DIR"
DRILL_DIR="$(cd "$DRILL_DIR" && pwd -P)"
export DRILL_DIR
mkdir -p "$DRILL_DIR"/{src-data,pitr-data,latest-data,wal,backup,evidence}
chmod 0777 "$DRILL_DIR/wal" "$DRILL_DIR/backup"
# the postgres uid must traverse the mount root (initdb needs it); umask 077 would deny it
chmod 0755 "$DRILL_DIR/src-data" "$DRILL_DIR/pitr-data" "$DRILL_DIR/latest-data"
evidence="$DRILL_DIR/evidence"

port_src="${DRILL_PORT_SRC:-55432}"
port_pitr="${DRILL_PORT_PITR:-55433}"
port_latest="${DRILL_PORT_LATEST:-55434}"
export DRILL_PORT_SRC="$port_src" DRILL_PORT_PITR="$port_pitr" DRILL_PORT_LATEST="$port_latest"
POSTGRES_PASSWORD="$(openssl rand -hex 16)"
DRILL_RUNTIME_PASSWORD="$(openssl rand -hex 16)"
export POSTGRES_PASSWORD DRILL_RUNTIME_PASSWORD

fence_mode=0
[[ "${DRILL_FENCE:-}" == "1" ]] && fence_mode=1
unset DRILL_FENCE # forwarded to the driver only for the fenced post phase, by drill() arguments
runtime_role="${DRILL_RUNTIME_ROLE:-drill_runtime}"

export GOMODCACHE=/var/lib/oci-cache/go-mod GOCACHE=/var/lib/oci-cache/go-build GOTMPDIR=/var/lib/oci-cache/go-tmp GOMAXPROCS=8

dc() { docker compose -f "$compose_file" -p "$project" "$@"; }
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$evidence/drill.log"; }

teardown() {
  # Postgres wrote the bind mounts as its own uid: hand them back so the
  # invoking user can delete DRILL_DIR by path afterwards. No volume is touched.
  local svc
  dc --profile src --profile pitr --profile latest logs --no-color >"$evidence/compose-logs.txt" 2>&1 || true
  for svc in src pitr latest; do
    dc --profile "$svc" run --rm --no-deps -T --user root --entrypoint sh "$svc" -c \
      "chown -R $(id -u):$(id -g) /var/lib/postgresql /wal /backup 2>/dev/null || true" >/dev/null 2>&1 || true
  done
  printf "%s teardown: compose down (containers only, no volume removal)\n" "$(date -u +%FT%TZ)" >>"$evidence/drill.log"
  dc --profile src --profile pitr --profile latest down || true
}
trap teardown EXIT

drill() {
  local port="$1" phase="$2"
  shift 2
  # exported in a subshell, never passed through `env`: the DSN holds a password
  # and an argv is readable by every user of the host.
  (
    export DRILL_PHASE="$phase" DRILL_EVIDENCE_DIR="$evidence"
    export DRILL_DSN="postgres://postgres:${POSTGRES_PASSWORD}@127.0.0.1:${port}/postgres?sslmode=disable"
    # shellcheck disable=SC2163 # each argument is a NAME=value assignment
    # a bare `export` (no arguments) would print every exported variable of the invoking shell into the drill log
    if (($#)); then export "$@"; fi
    "$capped" -t 600 -- "$DRILL_DIR/drill.test" -test.run '^TestDrillAuthPITR$' -test.count=1 -test.v \
      >>"$evidence/driver-$phase.log" 2>&1
  )
}

load_gate() {
  local load1 i
  for i in $(seq 0 20); do
    load1="$(cut -d' ' -f1 /proc/loadavg)"
    if [[ "${load1%.*}" -lt 64 ]]; then return 0; fi
    log "load gate: 1-min load $load1 >= 64, waiting (try $i of 20)"
    sleep 30
  done
  echo "drill-auth-pitr.sh: 1-min load stayed >= 64 for 10 min, not starting a container" >&2
  exit 1
}

psql_in() { # service, sql
  dc exec -T "$1" psql -U postgres -At -c "$2"
}

# psql_rw runs a statement with the read-only default overridden for that session only (the PITR copy starts read-only).
psql_rw() { # service, sql
  dc exec -T -e PGOPTIONS='-c default_transaction_read_only=off' "$1" psql -U postgres -At -c "$2"
}

wait_promoted() { # service
  for _ in $(seq 1 90); do
    if [[ "$(psql_in "$1" 'select pg_is_in_recovery()' 2>/dev/null || true)" == "f" ]]; then return 0; fi
    sleep 2
  done
  echo "drill-auth-pitr.sh: $1 did not leave recovery in 180 s" >&2
  return 1
}

# fence runs auth-migrate restore-fence <args> against a copy as the migration role. The DSN holds the password, so it is
# exported inside a subshell (never argv). The per-connection options override lets apply write although the copy was
# started read-only; export and status only read.
fence() { # port, args...
  local port="$1"
  shift
  (
    export AUTH_MIGRATION_DATABASE_URI="host=127.0.0.1 port=${port} user=postgres password='${POSTGRES_PASSWORD}' dbname=postgres sslmode=disable options='-c default_transaction_read_only=off'"
    export AUTH_RUNTIME_DATABASE_ROLE="$runtime_role" AUTH_DATABASE_SCHEMA=auth
    "$capped" -t 300 -- "$DRILL_DIR/auth-migrate" restore-fence "$@"
  )
}

prepare_restore() { # service, target-time-or-empty, optional "readonly"
  # shellcheck disable=SC2016 # the script body is expanded by the container's sh
  dc run --rm --no-deps -T --user root --entrypoint sh -e DRILL_T="$2" -e DRILL_RO="${3:-}" "$1" -c '
    set -eu
    d=/var/lib/postgresql/pgdata
    mkdir -p "$d"
    cp -a /backup/base/. "$d"/
    touch "$d/recovery.signal"
    printf "restore_command = \047cp /wal/%%f %%p\047\n" >>"$d/postgresql.auto.conf"
    printf "recovery_target_action = \047promote\047\n" >>"$d/postgresql.auto.conf"
    if [ -n "$DRILL_T" ]; then
      printf "recovery_target_time = \047%s\047\n" "$DRILL_T" >>"$d/postgresql.auto.conf"
    fi
    if [ "$DRILL_RO" = readonly ]; then
      printf "default_transaction_read_only = \047on\047\n" >>"$d/postgresql.auto.conf"
    fi
    chown -R postgres:postgres /var/lib/postgresql
    chmod 0700 "$d"
  '
}

log "compile driver"
(cd "$repo_root" && "$capped" -t 900 -- go test -p 4 -c -trimpath -tags=integration -o "$DRILL_DIR/drill.test" ./internal/audit/)

if [[ "$fence_mode" -eq 1 ]]; then
  log "compile auth-migrate (fenced drill)"
  (cd "$repo_root" && "$capped" -t 900 -- go build -trimpath -o "$DRILL_DIR/auth-migrate" ./cmd/auth-migrate)
fi

log "S1 start source (WAL archive on)"
load_gate
dc --profile src up -d --wait src
if [[ "$fence_mode" -eq 1 ]]; then
  drill "$port_src" migrate DRILL_FENCE=1 # the full migrated schema: the fence carries the legacy public tables too
else
  drill "$port_src" migrate
fi
if [[ "$fence_mode" -eq 1 ]]; then
  # a database that is not a restore: the explicit init, once, on timeline 1 (the first-roll step)
  fence "$port_src" init >"$evidence/fence-init-src.txt"
  grep -q 'written=true' "$evidence/fence-init-src.txt"
fi
drill "$port_src" write DRILL_LABEL=A
drill "$port_src" snapshot DRILL_LABEL=A

log "S3 base backup"
dc exec -T src pg_basebackup -U postgres -D /backup/base -Fp -X stream -c fast

log "S3 writes B, snapshot T, writes C after T, snapshot F"
drill "$port_src" write DRILL_LABEL=B
drill "$port_src" snapshot DRILL_LABEL=T
sleep 3
drill "$port_src" write DRILL_LABEL=C
drill "$port_src" snapshot DRILL_LABEL=F
last_lsn="$(psql_in src 'select pg_current_wal_lsn()')"
psql_in src 'select pg_switch_wal()' >/dev/null
for _ in $(seq 1 60); do
  if [[ "$(psql_in src "select coalesce(pg_walfile_name('$last_lsn') <= last_archived_wal, false) from pg_stat_archiver")" == "t" ]]; then break; fi
  sleep 2
done
[[ "$(psql_in src "select coalesce(pg_walfile_name('$last_lsn') <= last_archived_wal, false) from pg_stat_archiver")" == "t" ]] || {
  echo "drill-auth-pitr.sh: WAL up to $last_lsn was not archived in 120 s" >&2
  exit 1
}
dc --profile src stop src

t_target="$(cat "$evidence/time-T.txt")"
[[ "$t_target" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9:.]+\+00$ ]] || {
  echo "drill-auth-pitr.sh: unexpected target time shape" >&2
  exit 1
}
# a failing check must not stop the run before the other copy is checked
result=0

if [[ "$fence_mode" -eq 1 ]]; then
  log "S5 restore to end of WAL (shadow copy, first)"
  prepare_restore latest ""
  load_gate
  dc --profile latest up -d --wait latest
  wait_promoted latest
  drill "$port_latest" verify DRILL_EXPECT=F DRILL_MODE=exact || result=1
  # the manifest is exported BEFORE any new write on the shadow copy
  fence "$port_latest" export -out "$evidence/fence-manifest.json" >"$evidence/fence-export.txt" || result=1
  drill "$port_latest" post DRILL_EXPECT=F DRILL_FINAL=F || result=1
  dc --profile latest stop latest

  log "S4 PITR to $t_target (started read-only)"
  prepare_restore pitr "$t_target" readonly
  load_gate
  dc --profile pitr up -d --wait pitr
  wait_promoted pitr
  [[ "$(psql_in pitr 'show default_transaction_read_only')" == "on" ]] || { echo "drill-auth-pitr.sh: the PITR copy is not read-only" >&2; result=1; }
  drill "$port_pitr" verify DRILL_EXPECT=T DRILL_MODE=exact || result=1
  drill "$port_pitr" verify DRILL_EXPECT=A DRILL_MODE=atleast || result=1
  # readiness before the fence: a restored timeline has no fence row, status must say so (exit 1)
  status_rc=0
  fence "$port_pitr" status >"$evidence/fence-status-before.txt" || status_rc=$?
  { [[ "$status_rc" -eq 1 ]] && grep -q 'fenced=false' "$evidence/fence-status-before.txt"; } || { echo "drill-auth-pitr.sh: status before apply is not 'fenced=false' (exit $status_rc)" >&2; result=1; }
  fence "$port_pitr" apply -manifest "$evidence/fence-manifest.json" >"$evidence/fence-apply-dry-run.txt" || result=1
  grep -q '"dry_run":true' "$evidence/fence-apply-dry-run.txt" || { echo "drill-auth-pitr.sh: the dry run did not report dry_run" >&2; result=1; }
  fence "$port_pitr" apply -manifest "$evidence/fence-manifest.json" -apply >"$evidence/fence-apply.txt" || result=1
  grep -q '"dry_run":false' "$evidence/fence-apply.txt" || { echo "drill-auth-pitr.sh: apply did not report dry_run false" >&2; result=1; }
  fence "$port_pitr" status >"$evidence/fence-status-after.txt" || result=1
  grep -q 'fenced=true' "$evidence/fence-status-after.txt" || { echo "drill-auth-pitr.sh: status after apply is not 'fenced=true'" >&2; result=1; }
  # reset the read-only default, as the runbook does before auth-service starts
  psql_rw pitr 'ALTER SYSTEM RESET default_transaction_read_only' >/dev/null
  psql_rw pitr 'SELECT pg_reload_conf()' >/dev/null
  for _ in $(seq 1 10); do # the reload is asynchronous
    [[ "$(psql_in pitr 'show default_transaction_read_only')" == "off" ]] && break
    sleep 1
  done
  [[ "$(psql_in pitr 'show default_transaction_read_only')" == "off" ]] || { echo "drill-auth-pitr.sh: the read-only default was not reset" >&2; result=1; }
  drill "$port_pitr" post DRILL_EXPECT=T DRILL_FINAL=F DRILL_FENCE=1 || result=1
  # the apply must have re-applied at least one revocation (a credential and a principal created before T and removed
  # after it), and the rows the PITR copy lacks must be exactly the ones apply reported as absent (it cannot
  # authenticate: absence is correct, ACP-PLAN-GO-11271 item 6)
  apply_json="$(sed -n 's/^restore-fence apply //p' "$evidence/fence-apply.txt")"
  reapplied="$(jq -r '.revocations_reapplied' <<<"$apply_json" 2>/dev/null || echo n/a)"
  apply_absent="$(jq -r '[.sections[] | select(.name == "auth.credential_registry" or .name == "auth.principals") | .absent] | add // 0' <<<"$apply_json" 2>/dev/null || echo n/a)"
  post_absent="$(jq -r '.absent_after_restore' "$evidence/post-T.json" 2>/dev/null || echo n/a)"
  { [[ "$reapplied" =~ ^[0-9]+$ ]] && [[ "$reapplied" -ge 1 ]]; } || { echo "drill-auth-pitr.sh: apply re-applied $reapplied revocation(s), want at least 1" >&2; result=1; }
  [[ "$apply_absent" == "$post_absent" ]] || { echo "drill-auth-pitr.sh: absent rows: apply reported $apply_absent, the driver counted $post_absent" >&2; result=1; }
  dc --profile pitr stop pitr
else
log "S4 PITR to $t_target"
prepare_restore pitr "$t_target"
load_gate
dc --profile pitr up -d --wait pitr
wait_promoted pitr
drill "$port_pitr" verify DRILL_EXPECT=T DRILL_MODE=exact || result=1
drill "$port_pitr" verify DRILL_EXPECT=A DRILL_MODE=atleast || result=1
drill "$port_pitr" post DRILL_EXPECT=T DRILL_FINAL=F || result=1
dc --profile pitr stop pitr

log "S5 restore to end of WAL"
prepare_restore latest ""
load_gate
dc --profile latest up -d --wait latest
wait_promoted latest
drill "$port_latest" verify DRILL_EXPECT=F DRILL_MODE=exact || result=1
drill "$port_latest" post DRILL_EXPECT=F DRILL_FINAL=F || result=1
dc --profile latest stop latest

fi

log "evidence in $evidence"
jq -c . "$evidence"/verify-*.json "$evidence"/post-*.json | tee "$evidence/summary.txt"
# (a) revoked before T stays revoked, (d) no revision below T, (e) no duplicate: any failure above = FAIL.
# (b) revokes after T that came back and (c) keys re-issued with different content = GAP.
pitr_b="$(jq -r '.revokes_after_target_that_came_back' "$evidence/post-T.json" 2>/dev/null || echo n/a)"
pitr_c="$(jq -r '.keys_reissued_with_different_content' "$evidence/post-T.json" 2>/dev/null || echo n/a)"
verdict=PASS
if [[ "$result" -ne 0 ]]; then
  verdict=FAIL
elif [[ "$pitr_b" -ne 0 || "$pitr_c" -ne 0 ]]; then
  verdict=GAP
fi
printf 'DRILL RESULT: %s (b revokes-after-T-back=%s, c reissued-different=%s)\n' "$verdict" "$pitr_b" "$pitr_c" | tee -a "$evidence/summary.txt"
[[ "$verdict" != FAIL ]]
