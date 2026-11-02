#!/usr/bin/env bash
# Rehearsal of the least-privilege auth migration login on a real API server (bigboy k3s), in the chart form.
# Not a CI job (no kind): the lead runs it on bigboy in a FRESH namespace and the script deletes that namespace on exit.
#
#   ci/auth_owner_move_k3s.sh --print-plan   no cluster: print the plan and check the chart renders the pin env (needs helm)
#   ci/auth_owner_move_k3s.sh                the three legs below; the namespace is always deleted on exit
#
# Leg 1 FRESH     schema created AUTHORIZATION the NOSUPERUSER migration login; chart install with ownerRole.
# Leg 2 UPGRADE   a superuser-owned database one migration behind (the PRIOR auth-migrate image), `owner-move` (dry run, then
#                 -apply) from a one-off pod, Secret rotate, helm upgrade with the pin and the tree's image (the last migration
#                 runs AS the login); the runtime login reads an auth table in a loop during the apply: zero failures.
# Leg 3 ROLLBACK  at head: ACL snapshot before the move; owner-move, rotate, upgrade with the pin; Secret back to the superuser
#                 DSN, `owner-move -restore -apply` and ownerRole empty (the objects are the old owner's again); the upgrade passes;
#                 the ACL snapshot equals the first byte for byte; a second forward move works.
# Every leg prints literal "PASS: <name>" lines and stops on the first failure with "FAIL: <name>".
#
# Images are registry DIGEST references from the CI Inspect image step of the PR head (no local build, no ctr import):
#   AUTH_MOVE_IMAGE_MIGRATE        the tree's auth-migrate         repo@sha256:<64 hex>
#   AUTH_MOVE_IMAGE_MIGRATE_PRIOR  the auth-migrate one migration older (the base state of leg 2)
#   AUTH_MOVE_IMAGE_SERVICE        the tree's auth-service (the chart renders its Deployment; it need not become Ready)
# Optional: AUTH_MOVE_KUBECONFIG (default /etc/rancher/k3s/k3s.yaml). The nodes must be able to pull the images.
# The database is auth-only (no ops tables): the ops-schema privileges are proved by ci/check_auth_compose.sh. Secrets travel on
# stdin only. Nothing here touches another namespace, a cluster-wide object other than the two database logins it creates in its
# own PostgreSQL pod, or prod.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR ROOT
readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly DIGEST_RE='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
readonly MIGRATOR="devhealth_auth_migrator"
readonly RUNTIME="devhealth_auth"
readonly RELEASE_FRESH="amf" RELEASE_UPG="amu" RELEASE_RB="amr"

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}
pass() { printf 'PASS: %s\n' "$1"; }

PRINT_PLAN=0
for arg in "$@"; do
  case "${arg}" in
    --print-plan) PRINT_PLAN=1 ;;
    *) die "usage: ci/auth_owner_move_k3s.sh [--print-plan]" ;;
  esac
done

if [ "${PRINT_PLAN}" = 1 ]; then
  zeros="$(printf '0%.0s' $(seq 1 64))"
  ones="$(printf '1%.0s' $(seq 1 64))"
  IMG_MIG="registry.invalid/auth-migrate@sha256:${zeros}"
  IMG_PRIOR="registry.invalid/auth-migrate-prior@sha256:${ones}"
  IMG_SVC="registry.invalid/auth-service@sha256:${zeros}"
else
  IMG_MIG="${AUTH_MOVE_IMAGE_MIGRATE:-}"
  IMG_PRIOR="${AUTH_MOVE_IMAGE_MIGRATE_PRIOR:-}"
  IMG_SVC="${AUTH_MOVE_IMAGE_SERVICE:-}"
fi
for image in "${IMG_MIG}" "${IMG_PRIOR}" "${IMG_SVC}"; do
  [[ "${image}" =~ ${DIGEST_RE} ]] || die "an image is not a registry digest reference (repo@sha256:<64 hex>): ${image:-<unset>}"
done
[ "${IMG_MIG##*@}" != "${IMG_PRIOR##*@}" ] || die "the prior auth-migrate image equals the tree's: leg 2 needs a database one migration behind"
for tool in helm jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

NS="authmove-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
readonly NS
KUBECONFIG_FILE="${AUTH_MOVE_KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

# helm_args RELEASE MIGRATE_IMAGE OWNER_ROLE MIGRATE_SECRET DB_SECRET  -> HELM_ARGS
helm_args() {
  HELM_ARGS=(
    "$1" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}"
    --set web.enabled=false --set goWorkers.enabled=false --set valkey.enabled=false
    --set clickhouse.enabled=false --set migrations.hook.enabled=false --set secrets.create=false
    --set authService.phase=1
    --set "authService.image.repository=${IMG_SVC}" --set authService.image.pullPolicy=IfNotPresent
    --set "authService.migrate.image.repository=$2" --set authService.migrate.image.pullPolicy=IfNotPresent
    --set "authService.migrate.runtimeRole=${RUNTIME}"
    --set "authService.database.secretName=$5"
    --set "authService.migrate.secretName=$4"
    --set authService.signingKey.secretName=auth-key --set authService.signingKey.keyId=k3s-move
    --set-json 'authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]'
  )
  if [ -n "$3" ]; then HELM_ARGS+=(--set "authService.migrate.ownerRole=$3"); fi
}

print_plan() {
  printf 'plan: namespace %s on %s\n' "${NS}" "${KUBECONFIG_FILE}"
  printf 'plan: leg 1 FRESH    database authmove_fresh, schema AUTHORIZATION %s, release %s, pin %s\n' "${MIGRATOR}" "${RELEASE_FRESH}" "${MIGRATOR}"
  printf 'plan: leg 2 UPGRADE  database authmove_upg, base = prior image as superuser, owner-move dry run then -apply, rotate, upgrade with the pin\n'
  printf 'plan: leg 3 ROLLBACK database authmove_rb, snapshot, move, rotate, upgrade, Secret back + owner-move -restore, upgrade, snapshot equal, move again\n'
  local rendered
  helm_args "${RELEASE_FRESH}" "${IMG_MIG}" "${MIGRATOR}" sec-migrate sec-db
  rendered="$(helm template "${HELM_ARGS[@]}" --show-only templates/auth-migrate-job.yaml)"
  grep -q 'name: AUTH_SCHEMA_OWNER_ROLE' <<<"${rendered}" || die "the migrate Job renders no AUTH_SCHEMA_OWNER_ROLE with ownerRole set"
  grep -q "value: \"${MIGRATOR}\"" <<<"${rendered}" || die "the migrate Job does not carry the owner role"
  pass "plan: the chart renders the pin on the migrate Job when ownerRole is set"
  helm_args "${RELEASE_UPG}" "${IMG_PRIOR}" "" sec-migrate sec-db
  rendered="$(helm template "${HELM_ARGS[@]}" --show-only templates/auth-migrate-job.yaml)"
  if grep -q 'AUTH_SCHEMA_OWNER_ROLE' <<<"${rendered}"; then die "the migrate Job renders the pin with ownerRole empty"; fi
  pass "plan: the chart renders no pin when ownerRole is empty (the base state of leg 2)"
}

if [ "${PRINT_PLAN}" = 1 ]; then
  print_plan
  exit 0
fi

# ---------------------------------------------------------------------------------------------------------------------
case "${KUBECONFIG_FILE}" in *k3s*) ;; *) die "AUTH_MOVE_KUBECONFIG must name a k3s kubeconfig (no kind, no prod)" ;; esac
for tool in kubectl openssl envsubst od; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
[ -r "${KUBECONFIG_FILE}" ] || die "the kubeconfig ${KUBECONFIG_FILE} is not readable"
# The file name proves nothing: ask the cluster. k3s kubelets report a version like v1.30.2+k3s1; kind and other clusters do not.
nodes="$(kubectl --kubeconfig "${KUBECONFIG_FILE}" get nodes -o jsonpath='{.items[*].status.nodeInfo.kubeletVersion}' 2>/dev/null || true)"
case "${nodes}" in *+k3s*) ;; *) die "the cluster behind ${KUBECONFIG_FILE} is not a k3s cluster (kubelet versions: ${nodes:-none}); no kind, no prod" ;; esac
case "${NS}" in dev-health | default | kube-*) die "refusing the namespace ${NS}" ;; esac

# kubectl and helm both get the kubeconfig by flag; the export is for any other client the tools start.
export KUBECONFIG="${KUBECONFIG_FILE}"
kc() { kubectl --kubeconfig "${KUBECONFIG_FILE}" "$@"; }
K() { kc -n "${NS}" "$@"; }
export LOCAL_PG="${PG_IMAGE}"

# CREATED is set only after this run created the namespace: a name that already existed, or a create that failed, is never deleted.
CREATED=0
cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ] && [ "${CREATED}" = 1 ]; then
    printf '\n--- namespace state at failure ---\n' >&2
    K get pods,jobs -o wide >&2 || true
    K logs --all-containers --tail 60 -l app.kubernetes.io/component=auth-migrate >&2 || true
  fi
  if [ "${CREATED}" = 1 ]; then
    kc delete namespace "${NS}" --wait=false >/dev/null 2>&1 || true
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

if kc get namespace "${NS}" >/dev/null 2>&1; then die "the namespace ${NS} exists"; fi
kc create namespace "${NS}" >/dev/null
CREATED=1
printf 'namespace %s created on %s\n' "${NS}" "${KUBECONFIG_FILE}"

pg() { K exec -i deploy/auth-postgres -- psql -U postgres -v ON_ERROR_STOP=1 -At "$@"; }

# A keyword-form DSN with the password in single quotes. dsn USER PASSWORD DATABASE
dsn() { printf "host=auth-postgres port=5432 user=%s password='%s' dbname=%s sslmode=disable" "$1" "$2" "$3"; }
# put_secret NAME KEY VALUE: create or replace, the value on stdin only
put_secret() {
  printf '%s' "$3" | K create secret generic "$1" --from-file="$2=/dev/stdin" --dry-run=client -o yaml | K apply -f - >/dev/null
}

PG_PASSWORD="$(openssl rand -hex 12)"
MIGRATOR_PASSWORD="$(openssl rand -hex 12)"
RUNTIME_PASSWORD="$(openssl rand -hex 12)"
printf '%s' "${PG_PASSWORD}" | K create secret generic auth-pg-superuser --from-file=password=/dev/stdin >/dev/null
# shellcheck disable=SC2016
envsubst '${LOCAL_PG}' <"${SCRIPT_DIR}/auth_k8s/postgres.yaml" | K apply -f - >/dev/null
K rollout status deploy/auth-postgres --timeout=240s
{
  printf "CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" "${RUNTIME}" "${RUNTIME_PASSWORD}"
  printf "CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" "${MIGRATOR}" "${MIGRATOR_PASSWORD}"
} | pg -f - >/dev/null
openssl genpkey -algorithm ed25519 2>/dev/null | K create secret generic auth-key --from-file=signing-key.pem=/dev/stdin >/dev/null
pass "postgres is up, the migration login and the runtime login exist and are plain"

# one-off pod of the auth-migrate image: oneoff NAME IMAGE SECRET OWNER_PIN ARGS...  (prints stdout and stderr of the run)
oneoff() {
  local name=$1 image=$2 secret=$3 pin=$4 args overrides
  shift 4
  args="$(printf '%s\n' "$@" | jq -R . | jq -sc 'map(select(. != ""))')"
  overrides="$(jq -nc --arg name "${name}" --arg image "${image}" --arg secret "${secret}" --arg pin "${pin}" --arg runtime "${RUNTIME}" --argjson args "${args}" '
    {spec: {containers: [{name: $name, image: $image, args: $args,
      env: ([{name: "AUTH_MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: $secret, key: "AUTH_MIGRATION_DATABASE_URI"}}},
             {name: "AUTH_RUNTIME_DATABASE_ROLE", value: $runtime}]
            + (if $pin == "" then [] else [{name: "AUTH_SCHEMA_OWNER_ROLE", value: $pin}] end))}]}}')"
  K run "${name}" --restart=Never --image="${image}" --override-type=strategic --overrides="${overrides}" >/dev/null
  if ! K wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${name}" --timeout=240s >/dev/null; then
    K logs "${name}" >&2 || true
    die "the one-off pod ${name} did not succeed"
  fi
  K logs "${name}" 2>&1
  K delete pod "${name}" --wait=true >/dev/null
}

applied_versions() { pg -d "$1" -c 'SELECT version FROM auth.schema_migrations ORDER BY version' | tr '\n' ' '; }
embedded_versions() {
  find "${ROOT}/internal/storage/postgres/authschema/migrations" -name '[0-9][0-9][0-9][0-9]_*.sql' -printf '%f\n' | sort | cut -c1-4 | sed 's/^0*//' | tr '\n' ' '
}
owner_misses() { pg -d "$1" -v "role=$2" -f - <"${SCRIPT_DIR}/auth_k8s/owner_count.sql"; }
acl_snapshot() { pg -d "$1" -f - <"${SCRIPT_DIR}/auth_k8s/acl_snapshot.sql"; }
is_super() { pg -c "SELECT rolsuper FROM pg_roles WHERE rolname = '$1'"; }
completed_events() { # DATABASE-RELEASE: Completed events of the auth-migrate hook Job of a release
  K get events --field-selector "involvedObject.name=$1-dev-health-auth-migrate,reason=Completed" -o json | jq '[.items[].count // 1] | add // 0'
}
helm_up() { helm upgrade --install --kubeconfig "${KUBECONFIG_FILE}" "$@" --timeout 8m >/dev/null; }

# readbacks of a database at the tree's head owned by the migration login, with the pin verified by a one-off grants run
readbacks() { # LEG DATABASE SECRET
  local leg=$1 db=$2 secret=$3 out head
  head="$(embedded_versions)"
  [ -n "${head}" ] || die "could not read the embedded lineage"
  [ "$(applied_versions "${db}")" = "${head}" ] || die "${leg}: the applied migrations ($(applied_versions "${db}")) are not the embedded lineage (${head})"
  pass "${leg}: the schema is at the embedded head"
  [ "$(is_super "${MIGRATOR}")" = f ] || die "${leg}: the migration login is a superuser"
  pass "${leg}: the migration login is not a superuser"
  [ "$(owner_misses "${db}" "${MIGRATOR}")" = 0 ] || die "${leg}: relations or the schema of the auth schema are not owned by the migration login"
  pass "${leg}: every relation of the auth schema and the schema are owned by the migration login (owner count 0)"
  out="$(oneoff "run-${leg}" "${IMG_MIG}" "${secret}" "${MIGRATOR}")"
  grep -qF 'verified auth schema owner pin' <<<"${out}" || die "${leg}: the auth-migrate run did not log the owner pin it verified"
  pass "${leg}: auth-migrate as the migration login, with nothing pending, verified the owner pin"
  out="$(oneoff "check-${leg}" "${IMG_MIG}" "${secret}" "" --check)"
  grep -qE '^auth schema current at version [0-9]+$' <<<"${out}" || die "${leg}: auth-migrate --check did not report the head"
  pass "${leg}: auth-migrate --check reports the head"
}

# ===================================================================================================================
printf '=== leg 1: FRESH ===\n'
pg -c 'CREATE DATABASE authmove_fresh' >/dev/null
pg -d authmove_fresh -c "CREATE SCHEMA auth AUTHORIZATION ${MIGRATOR}" >/dev/null
put_secret sec-db-fresh AUTH_DATABASE_URI "$(dsn "${RUNTIME}" "${RUNTIME_PASSWORD}" authmove_fresh)"
put_secret sec-mig-fresh AUTH_MIGRATION_DATABASE_URI "$(dsn "${MIGRATOR}" "${MIGRATOR_PASSWORD}" authmove_fresh)"
helm_args "${RELEASE_FRESH}" "${IMG_MIG}" "${MIGRATOR}" sec-mig-fresh sec-db-fresh
helm_up "${HELM_ARGS[@]}"
[ "$(completed_events "${RELEASE_FRESH}")" -ge 1 ] || die "leg 1: the migrate hook Job did not complete"
pass "leg 1: the pre-install hook Job ran as the migration login with the pin and completed"
if ! helm get hooks "${RELEASE_FRESH}" --kubeconfig "${KUBECONFIG_FILE}" -n "${NS}" | grep -q 'AUTH_SCHEMA_OWNER_ROLE'; then die "leg 1: the rendered hook carries no AUTH_SCHEMA_OWNER_ROLE"; fi
pass "leg 1: the hook Job carries AUTH_SCHEMA_OWNER_ROLE"
readbacks "leg1" authmove_fresh sec-mig-fresh

# ===================================================================================================================
printf '=== leg 2: UPGRADE from a superuser-owned database one migration behind ===\n'
pg -c 'CREATE DATABASE authmove_upg' >/dev/null
put_secret sec-db-upg AUTH_DATABASE_URI "$(dsn "${RUNTIME}" "${RUNTIME_PASSWORD}" authmove_upg)"
put_secret sec-su-upg AUTH_MIGRATION_DATABASE_URI "$(dsn postgres "${PG_PASSWORD}" authmove_upg)"
put_secret sec-mig-upg AUTH_MIGRATION_DATABASE_URI "$(dsn postgres "${PG_PASSWORD}" authmove_upg)"
helm_args "${RELEASE_UPG}" "${IMG_PRIOR}" "" sec-mig-upg sec-db-upg
helm_up "${HELM_ARGS[@]}"
base="$(applied_versions authmove_upg)"
head="$(embedded_versions)"
[ -n "${base}" ] && [ "${base}" != "${head}" ] || die "leg 2: the base state (${base:-none}) is not behind the embedded lineage (${head})"
pass "leg 2: base state is behind the head (applied: ${base}| head: ${head})"
[ "$(owner_misses authmove_upg postgres)" = 0 ] || die "leg 2: the base state is not owned by the superuser"
pass "leg 2: the base state is owned by the superuser"

# the runtime login reads an auth table in a loop while the move runs (the script goes in on stdin, the password with it)
K exec -i deploy/auth-postgres -- sh -c 'rm -f /tmp/reads.out /tmp/reads.err; touch /tmp/reads.go' >/dev/null
{
  printf "export PGPASSWORD='%s'\n" "${RUNTIME_PASSWORD}"
  # shellcheck disable=SC2016
  printf '%s\n' 'n=0; f=0; while [ -e /tmp/reads.go ]; do n=$((n+1)); psql -h 127.0.0.1 -U devhealth_auth -d authmove_upg -At -c "SELECT count(*) FROM auth.roles" >/dev/null 2>>/tmp/reads.err || f=$((f+1)); sleep 0.1; done; echo "$n $f" >/tmp/reads.out'
} | K exec -i deploy/auth-postgres -- sh -s >/dev/null 2>&1 &
reader_pid=$!
sleep 3

out="$(oneoff owner-dry "${IMG_MIG}" sec-su-upg "" owner-move -from postgres -to "${MIGRATOR}" -runtime-role "${RUNTIME}")"
printf '%s\n' "${out}"
grep -qF 'owner-move applied=false' <<<"${out}" || die "leg 2: the dry run did not report applied=false"
[ "$(owner_misses authmove_upg "${MIGRATOR}")" != 0 ] || die "leg 2: the dry run changed owners"
pass "leg 2: the owner-move dry run reports counts and changes nothing"
out="$(oneoff owner-apply "${IMG_MIG}" sec-su-upg "" owner-move -from postgres -to "${MIGRATOR}" -runtime-role "${RUNTIME}" -apply)"
printf '%s\n' "${out}"
grep -qF 'owner-move applied=true' <<<"${out}" || die "leg 2: the apply did not report applied=true"
pass "leg 2: owner-move -apply ran from a one-off pod as the superuser"
[ "$(owner_misses authmove_upg "${MIGRATOR}")" = 0 ] || die "leg 2: objects are still not owned by the migration login after the apply"
pass "leg 2: after the apply the migration login owns the schema and every relation"

put_secret sec-mig-upg AUTH_MIGRATION_DATABASE_URI "$(dsn "${MIGRATOR}" "${MIGRATOR_PASSWORD}" authmove_upg)"
helm_args "${RELEASE_UPG}" "${IMG_MIG}" "${MIGRATOR}" sec-mig-upg sec-db-upg
helm_up "${HELM_ARGS[@]}"
[ "$(completed_events "${RELEASE_UPG}")" -ge 2 ] || die "leg 2: the pre-upgrade hook Job did not complete"
pass "leg 2: the pre-upgrade hook ran as the migration login with the pin and the tree's image"
K exec -i deploy/auth-postgres -- sh -c 'rm -f /tmp/reads.go' >/dev/null
wait "${reader_pid}" || true
reads="$(K exec -i deploy/auth-postgres -- sh -c 'cat /tmp/reads.out' 2>/dev/null || true)"
printf 'runtime reads (count failures): %s\n' "${reads}"
[ -n "${reads}" ] && [ "${reads##* }" = 0 ] && [ "${reads%% *}" -gt 0 ] || die "leg 2: the runtime login failed a read during the move (${reads:-no result})"
pass "leg 2: the runtime login read an auth table ${reads%% *} times during the move with 0 failures"
readbacks "leg2" authmove_upg sec-mig-upg

# ===================================================================================================================
printf '=== leg 3: ROLLBACK ===\n'
pg -c 'CREATE DATABASE authmove_rb' >/dev/null
put_secret sec-db-rb AUTH_DATABASE_URI "$(dsn "${RUNTIME}" "${RUNTIME_PASSWORD}" authmove_rb)"
put_secret sec-su-rb AUTH_MIGRATION_DATABASE_URI "$(dsn postgres "${PG_PASSWORD}" authmove_rb)"
put_secret sec-mig-rb AUTH_MIGRATION_DATABASE_URI "$(dsn postgres "${PG_PASSWORD}" authmove_rb)"
helm_args "${RELEASE_RB}" "${IMG_MIG}" "" sec-mig-rb sec-db-rb
helm_up "${HELM_ARGS[@]}"
[ "$(applied_versions authmove_rb)" = "$(embedded_versions)" ] || die "leg 3: the superuser-owned database is not at the head"
snap_before="$(acl_snapshot authmove_rb)"
[ -n "${snap_before}" ] || die "leg 3: the ACL snapshot is empty"
pass "leg 3: superuser-owned database at the head; ACL snapshot taken ($(wc -l <<<"${snap_before}") entries)"

move_forward() {
  oneoff "$1" "${IMG_MIG}" sec-su-rb "" owner-move -from postgres -to "${MIGRATOR}" -runtime-role "${RUNTIME}" -apply | grep -qF 'owner-move applied=true' \
    || die "leg 3: the forward move did not report applied=true"
  put_secret sec-mig-rb AUTH_MIGRATION_DATABASE_URI "$(dsn "${MIGRATOR}" "${MIGRATOR_PASSWORD}" authmove_rb)"
  helm_args "${RELEASE_RB}" "${IMG_MIG}" "${MIGRATOR}" sec-mig-rb sec-db-rb
  helm_up "${HELM_ARGS[@]}"
  [ "$(owner_misses authmove_rb "${MIGRATOR}")" = 0 ] || die "leg 3: the migration login does not own the schema after the move"
}
move_back() {
  # The rollback is the Secret back, the reverse move AND the values back (ownerRole empty): the objects are the old owner's again,
  # so a pin that still named the migration login would refuse them.
  put_secret sec-mig-rb AUTH_MIGRATION_DATABASE_URI "$(dsn postgres "${PG_PASSWORD}" authmove_rb)"
  oneoff "$1" "${IMG_MIG}" sec-su-rb "" owner-move -from "${MIGRATOR}" -to postgres -runtime-role "${RUNTIME}" -restore -apply | grep -qF 'owner-move applied=true' \
    || die "leg 3: the restore did not report applied=true"
  helm_args "${RELEASE_RB}" "${IMG_MIG}" "" sec-mig-rb sec-db-rb
  helm_up "${HELM_ARGS[@]}"
}

move_forward move-1
pass "leg 3: forward move, Secret rotate and upgrade with the pin"
move_back restore-1
pass "leg 3: Secret back, owner-move -restore and ownerRole empty; the upgrade passes on the old DSN"
snap_after="$(acl_snapshot authmove_rb)"
if [ "${snap_before}" != "${snap_after}" ]; then
  diff <(printf '%s\n' "${snap_before}") <(printf '%s\n' "${snap_after}") | head -20 >&2 || true
  die "leg 3: the ACL snapshot after the restore differs from the one before the move"
fi
pass "leg 3: the ACL snapshot after the restore equals the one before the move, byte for byte"
move_forward move-2
pass "leg 3: a second forward move works"

printf 'AUTH OWNER MOVE K3S REHEARSAL PASSED\n'
