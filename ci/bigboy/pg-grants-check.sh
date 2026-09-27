#!/usr/bin/env bash
# POST-ROLL STEP LINE (named): Postgres role privilege READ-BACK via has_table_privilege (CHAOS-6666 deploy-order note: the migrate hook
# must grant BEFORE the api serves the route, else the route answers a generic 500 "permission denied").
# usage: pg-grants-check.sh <bigboy|prod> <expect-file>   expect-file lines: `<role> <table> <PRIV>[,<PRIV>...]`  (# comments ok)
# Read-only. PG_OK when every expected privilege is held; PG_FAIL lists the missing ones. Role/table names only, no secrets.
set -u
MODE=${1:?bigboy|prod}; EXP=${2:?expect file}
run_psql() {
  if [ "$MODE" = bigboy ]; then docker exec -i dev-health-postgres-1 sh -c 'PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d ${POSTGRES_DB:-devhealth} -X -A -t'
  else KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n dev-health exec -i dev-health-ops-postgresql-0 -- sh -c 'PGPASSWORD=$POSTGRES_PASSWORD psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -q -A -t'; fi
}
SQL=""; N=0
while read -r role table privs; do
  [ -z "${role:-}" ] && continue; case "$role" in \#*) continue;; esac
  for p in ${privs//,/ }; do
    SQL+="select '$role','$table','$p', coalesce((select has_table_privilege('$role','public.$table','$p') where to_regclass('public.$table') is not null),false), to_regclass('public.$table') is not null;"$'\n'; N=$((N+1))
  done
done < "$EXP"
[ "$N" -gt 0 ] || { echo "PG_FAIL no expectations parsed"; exit 1; }
OUT=$(printf '%s' "$SQL" | run_psql 2>&1) || { echo "PG_FAIL psql failed"; exit 1; }
BAD=$(echo "$OUT" | awk -F'|' 'NF>=5 && ($4!="t"){print $1" "$2" "$3" held="$4" table_exists="$5}')
echo "$OUT" | awk -F'|' 'NF>=5{print $1" "$2" "$3" -> "($4=="t"?"held":"MISSING")}'
[ "$(echo "$OUT" | awk -F'|' 'NF>=5' | wc -l)" = "$N" ] || { echo "PG_FAIL measured $(echo "$OUT" | awk -F'|' 'NF>=5' | wc -l) of $N expectations"; exit 1; }
[ -z "$BAD" ] && { echo "PG_OK expectations=$N"; exit 0; } || { echo "PG_FAIL missing:"; echo "$BAD"; exit 1; }
