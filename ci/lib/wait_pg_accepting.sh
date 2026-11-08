#!/usr/bin/env bash
# Sourced. wait_pg_accepting: wait for a real accepted PostgreSQL connection, not pod Ready. The image's init server
# stops and restarts after the pod is Ready, and psql in that window fails with "the database system is shutting down".
# Caller defines K (kubectl in the namespace), PSU (psql in the pod) and die. Each try is pg_isready AND `SELECT 1`
# through PSU. Bound by attempts only (PG_WAIT_ATTEMPTS, default 60; PG_WAIT_SLEEP seconds between tries, default 2).
wait_pg_accepting() {
  local pod="$1" attempts="${PG_WAIT_ATTEMPTS:-60}" sleep_s="${PG_WAIT_SLEEP:-2}" i=0 err=""
  while [ "${i}" -lt "${attempts}" ]; do
    i=$((i + 1))
    if err="$(K exec "${pod}" -- pg_isready -U devhealth -d devhealth -q 2>&1 \
      && PSU -c "SELECT 1" </dev/null 2>&1)"; then
      return 0
    fi
    sleep "${sleep_s}"
  done
  die "postgres accepted no connection after ${attempts} attempts; last error: ${err}"
}
