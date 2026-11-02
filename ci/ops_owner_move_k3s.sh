#!/usr/bin/env bash
# Rehearsal of the ops owner move (CHAOS-12614) on a real API server (bigboy k3s), in the chart form.
# Not a CI job (no kind): the operator runs it on bigboy in a FRESH namespace and the script deletes that namespace on exit.
#
#   ci/ops_owner_move_k3s.sh --print-plan   no cluster: print the plan and check the chart renders the pin env (needs helm)
#   ci/ops_owner_move_k3s.sh                the three legs below; the namespace is always deleted on exit
#
# Leg 1 FRESH     install without ownerRole (the ops hooks as the superuser `devhealth`, provision-roles creates the
#                 migration login), `owner-move -plan` then `-apply` (river, then public + the database), the hook Secret
#                 rotated to the migration login, helm upgrade with ownerRole: every ops hook runs as the pinned login.
# Leg 2 UPGRADE   the same database shape, with a domain-login and a queue-login read+write loop running through the
#                 plan, both applies, the Secret rotation and the pinned upgrade: zero failed statements; every pod Ready.
# Leg 3 ROLLBACK  ACL snapshot; forward move, rotate, pinned upgrade; Secret back to the superuser, `owner-move -restore
#                 -apply`; the ACL snapshot equals the first byte for byte; upgrade without the pin; a second forward move.
# Every leg prints literal "PASS: <name>" lines and stops on the first failure with "FAIL: <name>".
#
#   OPS_MOVE_IMAGE   the tree's operator image (docker/go-worker.Dockerfile target operator), already in the node's
#                    containerd: repo:local (pullPolicy Never) or repo@sha256:<64 hex> (IfNotPresent).
# Optional: OPS_MOVE_KUBECONFIG (default /etc/rancher/k3s/k3s.yaml). PostgreSQL (superuser devhealth, as in prod) and
# ClickHouse run as plain Deployments in the namespace from images the node already holds. go-api, query-api, the
# workers and auth-service are not deployed: the runtime logins' traffic is the psql loop of leg 2. Secrets travel on
# stdin only. Nothing here touches another namespace, a cluster-wide object, or prod.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR ROOT
readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly CH_IMAGE="clickhouse/clickhouse-server:latest@sha256:d7556a3841027651307b5aa08d72b5c467d0241d3db5b67d9e158ef3975626f5"
readonly OLD="devhealth" NEW="devhealth_migrator"

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}
pass() { printf 'PASS: %s\n' "$1"; }

PRINT_PLAN=0
for arg in "$@"; do
  case "${arg}" in
    --print-plan) PRINT_PLAN=1 ;;
    *) die "usage: ci/ops_owner_move_k3s.sh [--print-plan]" ;;
  esac
done

if [ "${PRINT_PLAN}" = 1 ]; then
  IMG="dho-ops-owner-move:local"
else
  IMG="${OPS_MOVE_IMAGE:-}"
fi
case "${IMG}" in
  *@sha256:*) [[ "${IMG}" =~ ^[a-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || die "OPS_MOVE_IMAGE is not repo@sha256:<64 hex>"; PULL=IfNotPresent ;;
  *:local) [[ "${IMG}" =~ ^[a-z0-9./_-]+:local$ ]] || die "OPS_MOVE_IMAGE is not repo:local"; PULL=Never ;;
  *) die "OPS_MOVE_IMAGE must be repo:local or repo@sha256:<64 hex>: ${IMG:-<unset>}" ;;
esac
for tool in helm jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

NS="opsmove-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
readonly NS
KUBECONFIG_FILE="${OPS_MOVE_KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

# helm_args RELEASE DATABASE PIN INDEX  -> HELM_ARGS (INDEX=1 also renders the open-units and streak index Jobs: they need
# the baseline's tables, so they run on upgrades only)
helm_args() {
  HELM_ARGS=(
    "$1" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}"
    --set web.enabled=false --set goApi.enabled=false --set goWorkers.enabled=false --set queryApi.enabled=false
    --set valkey.enabled=false --set clickhouse.enabled=false --set postgresql.enabled=false --set networkPolicy.enabled=false
    --set secrets.create=false --set secrets.externalSecretName=ops-app --set hs256Secret.externalSecretName=ops-hs256
    --set "migrations.hook.externalSecretName=sec-migrate-$2" --set "migrations.hook.provisionRoles.externalSecretName=sec-owner-$2"
    --set "migrations.hook.image=${IMG}" --set "migrations.hook.pullPolicy=${PULL}"
    --set "migrations.hook.riverMigrate.image=${IMG}" --set "migrations.hook.riverMigrate.pullPolicy=${PULL}"
    --set "migrations.hook.routeActivate.image=${IMG}" --set "migrations.hook.routeActivate.pullPolicy=${PULL}"
    --set migrations.hook.routeActivate.enabled=false
    --set migrations.hook.provisionRoles.enabled=true --set migrations.hook.riverMigrate.enabled=true
    --set "migrations.hook.provisionRoles.migratorRole.name=${NEW}" --set migrations.hook.provisionRoles.migratorRole.secretName=ops-migrator
    --set goWorkers.pgbouncer.secret.create=false --set goWorkers.pgbouncer.secret.externalSecretName=ops-pooler
    --set "migrations.hook.openUnitsIndex.enabled=$([ "$4" = 1 ] && echo true || echo false)"
    --set "migrations.hook.streakIndex.enabled=$([ "$4" = 1 ] && echo true || echo false)"
  )
  if [ -n "$3" ]; then HELM_ARGS+=(--set "migrations.hook.ownerRole=$3"); fi
}

print_plan() {
  printf 'plan: namespace %s on %s, image %s\n' "${NS}" "${KUBECONFIG_FILE}" "${IMG}"
  printf 'plan: leg 1 FRESH    database omf: install, plan, move river then public+database, rotate, upgrade with ownerRole %s\n' "${NEW}"
  printf 'plan: leg 2 UPGRADE  database omu: the same, under a domain and a queue read+write loop; 0 failures; pods Ready\n'
  printf 'plan: leg 3 ROLLBACK database omr: snapshot, move, rotate, upgrade; Secret back, -restore, snapshot equal, upgrade, move again\n'
  local rendered
  helm_args omf omf "${NEW}" 1
  rendered="$(helm template "${HELM_ARGS[@]}")"
  [ "$(grep -c 'name: MIGRATION_OWNER_ROLE' <<<"${rendered}")" = 4 ] || die "ownerRole set: MIGRATION_OWNER_ROLE is not rendered on exactly the four ops migrator Jobs"
  pass "plan: the chart renders the pin on the four ops migrator Jobs when ownerRole is set"
  helm_args omf omf "" 1
  rendered="$(helm template "${HELM_ARGS[@]}")"
  if grep -q 'MIGRATION_OWNER_ROLE' <<<"${rendered}"; then die "the chart renders the pin with ownerRole empty"; fi
  grep -q 'name: OPS_MIGRATOR_DATABASE_ROLE' <<<"${rendered}" || die "provision-roles renders no migration login"
  pass "plan: no pin with ownerRole empty; provision-roles provisions the migration login"
}

if [ "${PRINT_PLAN}" = 1 ]; then
  print_plan
  exit 0
fi

# ---------------------------------------------------------------------------------------------------------------------
case "${KUBECONFIG_FILE}" in *k3s*) ;; *) die "OPS_MOVE_KUBECONFIG must name a k3s kubeconfig (no kind, no prod)" ;; esac
for tool in kubectl openssl od envsubst; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
[ -r "${KUBECONFIG_FILE}" ] || die "the kubeconfig ${KUBECONFIG_FILE} is not readable"
nodes="$(kubectl --kubeconfig "${KUBECONFIG_FILE}" get nodes -o jsonpath='{.items[*].status.nodeInfo.kubeletVersion}' 2>/dev/null || true)"
case "${nodes}" in *+k3s*) ;; *) die "the cluster behind ${KUBECONFIG_FILE} is not a k3s cluster (kubelet versions: ${nodes:-none}); no kind, no prod" ;; esac
case "${NS}" in dev-health | default | kube-*) die "refusing the namespace ${NS}" ;; esac

export KUBECONFIG="${KUBECONFIG_FILE}"
kc() { kubectl --kubeconfig "${KUBECONFIG_FILE}" "$@"; }
K() { kc -n "${NS}" "$@"; }

RUN_TOKEN="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
owned() { [ "$(kc get namespace "${NS}" -o 'jsonpath={.metadata.labels.opsmove-run}' 2>/dev/null)" = "${RUN_TOKEN}" ]; }
cleanup() {
  local status=$?
  if owned; then
    if [ "${status}" -ne 0 ]; then
      printf '\n--- namespace state at failure ---\n' >&2
      K get pods,jobs -o wide >&2 || true
      for job in $(K get jobs -o name 2>/dev/null); do
        printf -- '--- %s\n' "${job}" >&2
        K logs --tail 40 "${job}" >&2 || true
      done
    fi
    kc delete namespace "${NS}" --wait=false >/dev/null 2>&1 || true
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

if kc get namespace "${NS}" >/dev/null 2>&1; then die "the namespace ${NS} exists"; fi
kc create -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NS}
  labels:
    opsmove-run: ${RUN_TOKEN}
EOF
printf 'namespace %s created on %s\n' "${NS}" "${KUBECONFIG_FILE}"

# put_secret NAME KEY=VALUE...: create or replace; values travel through a file on the script's own stdin pipe
put_secret() {
  local name=$1 entry
  shift
  {
    printf 'apiVersion: v1\nkind: Secret\nmetadata: {name: %s}\ntype: Opaque\nstringData:\n' "${name}"
    for entry in "$@"; do printf '  %s: %s\n' "${entry%%=*}" "$(jq -Rn --arg v "${entry#*=}" '$v')"; done
  } | K apply -f - >/dev/null
}

PG_PASSWORD="$(openssl rand -hex 12)"
CH_PASSWORD="$(openssl rand -hex 12)"
NEW_PASSWORD="$(openssl rand -hex 12)"
DOMAIN_PASSWORD="$(openssl rand -hex 12)"
QUEUE_PASSWORD="$(openssl rand -hex 12)"
COORDINATOR_PASSWORD="$(openssl rand -hex 12)"
put_secret ops-pg POSTGRES_PASSWORD="${PG_PASSWORD}" CLICKHOUSE_PASSWORD="${CH_PASSWORD}"
put_secret ops-pooler RIVER_DOMAIN_DATABASE_PASSWORD="${DOMAIN_PASSWORD}" RIVER_QUEUE_DATABASE_PASSWORD="${QUEUE_PASSWORD}" \
  RIVER_COORDINATOR_DATABASE_PASSWORD="${COORDINATOR_PASSWORD}"
put_secret ops-migrator OPS_MIGRATOR_DATABASE_PASSWORD="${NEW_PASSWORD}"
put_secret ops-app UNUSED=1
put_secret ops-hs256 JWT_SECRET_KEY="$(openssl rand -hex 32)"

# shellcheck disable=SC2016
OPS_PG_IMAGE="${PG_IMAGE}" OPS_CH_IMAGE="${CH_IMAGE}" envsubst '${OPS_PG_IMAGE} ${OPS_CH_IMAGE}' <"${SCRIPT_DIR}/ops_k8s/infra.yaml" | K apply -f - >/dev/null
K rollout status deploy/ops-postgres --timeout=240s >/dev/null
K rollout status deploy/ops-clickhouse --timeout=300s >/dev/null
pass "postgres (superuser ${OLD}) and clickhouse are up"

pg() { K exec -i deploy/ops-postgres -- psql -U "${OLD}" -v ON_ERROR_STOP=1 -At "$@"; }
# shellcheck disable=SC2016
ch() { printf '%s' "$1" | K exec -i deploy/ops-clickhouse -- sh -c 'clickhouse-client --user dho --password "$CLICKHOUSE_PASSWORD" --multiquery'; }
dsn() { printf 'postgresql://%s:%s@ops-postgres:5432/%s?sslmode=disable' "$1" "$2" "$3"; }
chdsn() { printf 'clickhouse://dho:%s@ops-clickhouse:9000/%s' "${CH_PASSWORD}" "$1"; }

# hook Secrets of database DB: sec-owner-DB (the superuser DSN, kept; provision-roles and owner-move read it) and
# sec-migrate-DB (the four ops migrator Jobs; LOGIN is OLD or NEW)
hook_secret() { # NAME DB LOGIN
  local password=${PG_PASSWORD}
  [ "$3" = "${NEW}" ] && password=${NEW_PASSWORD}
  put_secret "$1" MIGRATION_DATABASE_URI="$(dsn "$3" "${password}" "$2")" POSTGRES_URI="$(dsn "$3" "${password}" "$2")" \
    CLICKHOUSE_URI="$(chdsn "$2")"
}

# one-off pod of the operator image: oneoff NAME SECRET PIN ARGS... (prints the run's output; fails on a non-zero exit
# unless ONEOFF_MAY_FAIL=1, then prints "exit=<code>" last)
oneoff() {
  local name=$1 secret=$2 pin=$3 args overrides phase
  shift 3
  args="$(printf '%s\n' "$@" | jq -R . | jq -sc 'map(select(. != ""))')"
  overrides="$(jq -nc --arg name "${name}" --arg image "${IMG}" --arg pull "${PULL}" --arg secret "${secret}" --arg pin "${pin}" --argjson args "${args}" '
    {spec: {restartPolicy: "Never", containers: [{name: $name, image: $image, imagePullPolicy: $pull, args: $args,
      env: ([{name: "MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: $secret, key: "MIGRATION_DATABASE_URI"}}},
             {name: "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", value: "1"}, {name: "RIVER_DATABASE_SCHEMA", value: "river"}]
            + (if $pin == "" then [] else [{name: "MIGRATION_OWNER_ROLE", value: $pin}] end))}]}}')"
  K run "${name}" --restart=Never --image="${IMG}" --override-type=strategic --overrides="${overrides}" >/dev/null
  K wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${name}" --timeout=240s >/dev/null 2>&1 || true
  phase="$(K get pod "${name}" -o jsonpath='{.status.phase}')"
  K logs "${name}" 2>&1
  K delete pod "${name}" --wait=true >/dev/null
  if [ "${phase}" != Succeeded ]; then
    [ "${ONEOFF_MAY_FAIL:-0}" = 1 ] && { printf 'exit=failed\n'; return 0; }
    die "the one-off pod ${name} ended ${phase}"
  fi
}

helm_up() { helm upgrade --install --kubeconfig "${KUBECONFIG_FILE}" "$@" --timeout 10m >/dev/null; }

acl_snapshot() { # DB: every ACL entry (stored or default) of the database, schemas public and river and their objects
  pg -d "$1" -f - <"${SCRIPT_DIR}/ops_k8s/acl_snapshot.sql"
}

owned_by() { # DB ROLE: relations, routines and types of public and river, the schema river and the database owned by ROLE
  pg -d "$1" -v "role=$2" -f - <"${SCRIPT_DIR}/ops_k8s/owned_by.sql"
}

install() { # DB: database and ClickHouse database, hook Secrets on the superuser, release without the pin
  pg -c "CREATE DATABASE $1" >/dev/null
  ch "CREATE DATABASE IF NOT EXISTS $1" >/dev/null
  hook_secret "sec-owner-$1" "$1" "${OLD}"
  hook_secret "sec-migrate-$1" "$1" "${OLD}"
  helm_args "$1" "$1" "" 0
  helm_up "${HELM_ARGS[@]}"
  [ "$(owned_by "$1" "${NEW}")" = 0 ] || die "$1: the migration login owns objects before the move"
  [ "$(pg -c "SELECT rolsuper::text || rolcanlogin::text || rolcreatedb::text || rolcreaterole::text FROM pg_roles WHERE rolname = '${NEW}'")" = falsetruefalsefalse ] \
    || die "$1: provision-roles did not create the migration login as a plain login"
  pass "$1: installed as the superuser; provision-roles created ${NEW} (LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE), it owns nothing"
}

move() { # DB MODE(plan|apply) FROM TO [restore]
  local db=$1 mode=$2 from=$3 to=$4 extra=() out step
  [ "${5:-}" = restore ] && extra=(-restore)
  for step in river public; do
    local flags=(-schemas "${step}")
    [ "${step}" = public ] && flags+=(-database-owner)
    out="$(oneoff "move-${step}-${mode}-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')" "sec-owner-${db}" "" migrate owner-move "${flags[@]}" -from "${from}" -to "${to}" "-${mode}" "${extra[@]}")"
    printf '%s\n' "${out}" | grep '^owner-move ' || true
    case "${mode}" in
      plan) grep -q "^owner-move mode=plan applied=false schemas=\[${step}\]" <<<"${out}" || die "${db}: the ${step} plan did not report mode=plan applied=false" ;;
      apply) grep -q "^owner-move mode=apply applied=true schemas=\[${step}\].* old_grantor_rows_left=0 " <<<"${out}" || die "${db}: the ${step} apply did not report applied=true with 0 rows left" ;;
    esac
  done
}

pinned_upgrade() { # DB: Secret rotated to the migration login, release upgraded with the pin and the index Jobs
  hook_secret "sec-migrate-$1" "$1" "${NEW}"
  helm_args "$1" "$1" "${NEW}" 1
  helm_up "${HELM_ARGS[@]}"
  local logs
  logs="$(K logs "job/$1-dev-health-river-migrate" 2>&1)"
  grep -qF 'verified ops schema owner pin' <<<"${logs}" || die "$1: river-migrate did not log the pin it verified"
  pass "$1: the pinned upgrade ran every ops hook as ${NEW}; river-migrate verified the pin"
}

readbacks() { # DB
  local db=$1 out
  [ "$(owned_by "${db}" "${OLD}")" = 0 ] || die "${db}: the superuser still owns objects of public or river, or the database"
  [ "$(pg -d "${db}" -c "SELECT pg_get_userbyid(datdba) || '|' || (SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'river') || '|' || (SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public') FROM pg_database WHERE datname = current_database()")" = "${NEW}|${NEW}|pg_database_owner" ] \
    || die "${db}: the database / river / public owners are not ${NEW} / ${NEW} / pg_database_owner"
  [ "$(pg -d "${db}" -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, aclexplode(c.relacl) a WHERE n.nspname IN ('public','river') AND a.grantor = '${OLD}'::regrole")" = 0 ] \
    || die "${db}: ACL entries with grantor ${OLD} are left on public or river relations"
  pass "${db}: ${NEW} owns the database and schema river; the superuser owns nothing there and grants nothing there"
  out="$(oneoff "pin-up-${db}" "sec-migrate-${db}" "${NEW}" migrate postgres upgrade)"
  if ! grep -qF 'verified ops schema owner pin' <<<"${out}" || ! grep -qF '"up_to_date"' <<<"${out}"; then die "${db}: migrate postgres upgrade as the pinned login: ${out}"; fi
  out="$(oneoff "pin-ix-${db}" "sec-migrate-${db}" "${NEW}" migrate postgres build-index open-units)"
  grep -qF 'verified ops schema owner pin' <<<"${out}" || die "${db}: build-index as the pinned login did not verify the pin"
  pass "${db}: migrate postgres upgrade (up_to_date) and build-index ran as ${NEW} and verified the pin"
  out="$(ONEOFF_MAY_FAIL=1 oneoff "pin-su-${db}" "sec-owner-${db}" "${NEW}" migrate postgres upgrade)"
  if ! grep -q 'owner_pin_failed' <<<"${out}" || ! grep -q '^exit=failed$' <<<"${out}"; then die "${db}: a superuser DSN under the pin was not refused: ${out}"; fi
  pass "${db}: a superuser DSN under the pin is refused before any DDL"
}

# ===================================================================================================================
printf '=== leg 1: FRESH (Q13: install, move, then ownerRole) ===\n'
install omf
before="$(acl_snapshot omf)"
move omf plan "${OLD}" "${NEW}"
[ "$(acl_snapshot omf)" = "${before}" ] && [ "$(owned_by omf "${NEW}")" = 0 ] || die "omf: the plan changed the catalog"
pass "omf: owner-move -plan reports counts and changes nothing"
move omf apply "${OLD}" "${NEW}"
pinned_upgrade omf
readbacks omf

# ===================================================================================================================
# Roles are cluster-wide and the runtime-role closure refuses grants held in another database: prod has one
# application database, so each leg drops the one before it.
pg -c "DROP DATABASE omf WITH (FORCE)" >/dev/null
printf '=== leg 2: UPGRADE under runtime traffic ===\n'
install omu
K exec -i deploy/ops-postgres -- sh -c 'rm -f /tmp/loop.*; touch /tmp/loop.go' >/dev/null
for who in domain queue; do
  {
    if [ "${who}" = domain ]; then
      printf "export PGPASSWORD='%s'\n" "${DOMAIN_PASSWORD}"
      # shellcheck disable=SC2016
      printf '%s\n' 'n=0; f=0; while [ -e /tmp/loop.go ]; do n=$((n+1)); psql -h 127.0.0.1 -U devhealth_domain -d omu -v ON_ERROR_STOP=1 -At -c "SELECT count(*) FROM public.integrations" -c "UPDATE public.integration_credentials SET updated_at = updated_at WHERE id IS NULL" >/dev/null 2>>/tmp/loop.domain.err || f=$((f+1)); sleep 0.1; done; echo "$n $f" >/tmp/loop.domain.out'
    else
      printf "export PGPASSWORD='%s'\n" "${QUEUE_PASSWORD}"
      # shellcheck disable=SC2016
      printf '%s\n' 'n=0; f=0; while [ -e /tmp/loop.go ]; do n=$((n+1)); psql -h 127.0.0.1 -U devhealth_queue -d omu -v ON_ERROR_STOP=1 -At -c "WITH j AS (INSERT INTO river.river_job (kind, args, max_attempts, state, finalized_at) VALUES ('"'"'owner_move.probe'"'"', '"'"'{}'"'"', 1, '"'"'cancelled'"'"', now()) RETURNING id) DELETE FROM river.river_job WHERE id IN (SELECT id FROM j)" -c "SELECT count(*) FROM public.sync_run_units" >/dev/null 2>>/tmp/loop.queue.err || f=$((f+1)); sleep 0.1; done; echo "$n $f" >/tmp/loop.queue.out'
    fi
  } | K exec -i deploy/ops-postgres -- sh -s >/dev/null 2>&1 &
done
sleep 5
move omu plan "${OLD}" "${NEW}"
move omu apply "${OLD}" "${NEW}"
pinned_upgrade omu
sleep 3
K exec -i deploy/ops-postgres -- sh -c 'rm -f /tmp/loop.go' >/dev/null
wait
for who in domain queue; do
  result="$(K exec -i deploy/ops-postgres -- sh -c "cat /tmp/loop.${who}.out" 2>/dev/null || true)"
  printf '%s login read+write statements (runs failures): %s\n' "${who}" "${result}"
  if [ -z "${result}" ] || [ "${result##* }" != 0 ] || [ "${result%% *}" -lt 10 ]; then
    K exec -i deploy/ops-postgres -- sh -c "tail -5 /tmp/loop.${who}.err" >&2 || true
    die "omu: the ${who} login failed a read or write during the move (${result:-no result})"
  fi
  pass "omu: the ${who} login ran ${result%% *} read+write rounds through the plan, the move, the rotation and the pinned upgrade with 0 failures"
done
readbacks omu
K wait --for=condition=Ready pod --field-selector=status.phase=Running --timeout=120s >/dev/null || die "omu: not every running pod is Ready"
[ -z "$(K get pods --field-selector=status.phase=Failed -o name)" ] || die "omu: a pod failed"
[ "$(helm status omu --kubeconfig "${KUBECONFIG_FILE}" -n "${NS}" -o json | jq -r .info.status)" = deployed ] || die "omu: the release is not deployed"
pass "omu: the release is deployed, every running pod is Ready and no pod failed (hook pods Completed)"

# ===================================================================================================================
pg -c "DROP DATABASE omu WITH (FORCE)" >/dev/null
printf '=== leg 3: ROLLBACK ===\n'
install omr
snap_before="$(acl_snapshot omr)"
[ -n "${snap_before}" ] || die "omr: the ACL snapshot is empty"
pass "omr: superuser-owned database at the head; ACL snapshot taken ($(wc -l <<<"${snap_before}") entries)"
move omr apply "${OLD}" "${NEW}"
pinned_upgrade omr
pass "omr: forward move, Secret rotate and upgrade with the pin"
hook_secret sec-migrate-omr omr "${OLD}"
move omr apply "${NEW}" "${OLD}" restore
snap_after="$(acl_snapshot omr)"
if [ "${snap_before}" != "${snap_after}" ]; then
  diff <(printf '%s\n' "${snap_before}") <(printf '%s\n' "${snap_after}") | head -20 >&2 || true
  die "omr: the ACL snapshot after the restore differs from the one before the move"
fi
pass "omr: Secret back and owner-move -restore: the ACL snapshot equals the one before the move, byte for byte"
pg -d omr -c "SELECT 'default acl of ${NEW} left after the restore: ' || coalesce(n.nspname, '*') || ' ' || d.defaclobjtype::text || ' ' || d.defaclacl::text FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace WHERE d.defaclrole = '${NEW}'::regrole" || true
helm_args omr omr "" 1
helm_up "${HELM_ARGS[@]}"
[ "$(owned_by omr "${NEW}")" = 0 ] || die "omr: the upgrade without the pin left objects to the migration login"
pass "omr: the upgrade without the pin passes on the superuser DSN"
move omr apply "${OLD}" "${NEW}"
pinned_upgrade omr
readbacks omr
pass "omr: a second forward move works"

printf 'OPS OWNER MOVE K3S REHEARSAL PASSED\n'
