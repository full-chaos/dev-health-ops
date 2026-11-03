#!/usr/bin/env bash
# Issuer rollback of the auth signing-key rotation in the CHART form, on bigboy k3s (stage rollback = S3 of
# ci/rehearse_auth_key_rotation.sh): the real auth-service and auth-migrate images, the real chart (authService.keySet,
# authService.accessToken), `helm upgrade` to hs256_compat and `helm rollback` to eddsa. Not a CI job; the lead runs it
# on bigboy in a namespace it made. Never prod, never kind.
#
#   ci/rehearse_auth_key_rotation_k3s.sh --print-plan   no cluster: print the plan, render the chart for both classes (needs helm)
#   ci/rehearse_auth_key_rotation_k3s.sh --stage rollback
#
# Environment (no value is printed):
#   REHEARSAL_KUBECONFIG  a ServiceAccount kubeconfig, mode 0600, scoped to one namespace (never the k3s admin file)
#   REHEARSAL_NAMESPACE   that namespace; it carries the label rehearsal-run equal to REHEARSAL_RUN_TOKEN
#   REHEARSAL_RUN_TOKEN   the run token the lead put in that label
#   REHEARSAL_DIR         a new, empty directory (0700): keys, records, evidence
#   REHEARSAL_IMAGES_ENV  a file with DEV_HEALTH_GO_{AUTH_MIGRATE,AUTH_SERVICE}_IMAGE=repo@sha256:<64 hex>
#   REHEARSAL_RUN_ID      optional: the CI run id the digests come from (digits only)
#
# This script never creates or deletes a namespace and never uses the admin kubeconfig. Every kubectl and helm call
# names --kubeconfig. Keys, DSNs and tokens go to the cluster on stdin and are never printed, never put on argv and never
# written to a record: kids, states, status codes, revision numbers and seconds only. Each cell prints a literal
# "PASS: <name>" or "FAIL: <name>"; the first FAIL stops the run.
set -euo pipefail
umask 077
case $- in
  *x*) printf 'refused: xtrace is on\n' >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# shellcheck source=ci/lib/auth_rotation_probe.sh
source "${SCRIPT_DIR}/lib/auth_rotation_probe.sh"

readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly VALKEY_IMAGE="valkey/valkey:9-alpine@sha256:ee91f7a174ac4d6a6b0685b3a60e321f0a9dbbb691f9b0e285be2ba1d1be8328"
readonly DIGEST_RE='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
readonly NS_RE='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
readonly ADMIN_KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
readonly ISSUER_URL="https://auth-issuer.rehearsal.test:8443"
readonly RELEASE="rot"
readonly SERVICE="${RELEASE}-dev-health-auth-service"
readonly DEPLOYMENT="${SERVICE}"
# internal/auth/keyrotation: PendingFloor = JWKS cache TTL (5 min).
readonly PENDING_FLOOR=300
readonly RUNTIME_ROLE="devhealth_auth"
readonly API_ROLE="devhealth_api"
readonly QUERY_ROLE="devhealth_query_api"

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}
say() { printf '%s\n' "$*"; }

STAGE="rollback"
PRINT_PLAN=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --print-plan) PRINT_PLAN=1; shift ;;
    --stage) STAGE="${2:-}"; shift 2 ;;
    *) die "usage: ci/rehearse_auth_key_rotation_k3s.sh [--print-plan] [--stage rollback]" ;;
  esac
done
[ "${STAGE}" = rollback ] || die "--stage is rollback"

# chart_args CLASS IMAGE_SERVICE IMAGE_MIGRATE -> CHART_ARGS (the same flags for install, upgrade and the render check)
chart_args() {
  CHART_ARGS=(
    "${RELEASE}" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}"
    --set web.enabled=false --set goApi.enabled=false --set queryApi.enabled=false --set goWorkers.enabled=false
    --set valkey.enabled=false --set clickhouse.enabled=false --set migrations.hook.enabled=false --set secrets.create=false
    --set secrets.externalSecretName=rehearsal-unused --set hs256Secret.externalSecretName=rehearsal-hs256
    --set authService.phase=2 --set authService.replicas=1
    --set "authService.image.repository=$2" --set authService.image.pullPolicy=IfNotPresent
    --set "authService.migrate.image.repository=$3" --set authService.migrate.image.pullPolicy=IfNotPresent
    --set "authService.migrate.runtimeRole=${RUNTIME_ROLE}"
    --set "authService.migrate.sessionReaders.goApi=${API_ROLE}" --set "authService.migrate.sessionReaders.queryApi=${QUERY_ROLE}"
    --set authService.database.secretName=rehearsal-db --set authService.migrate.secretName=rehearsal-migrate
    --set authService.valkey.secretName=rehearsal-valkey
    --set authService.sessionKeys.successor.secretName=rehearsal-session
    --set authService.keySet.enabled=true
    --set authService.keySet.keys[0].kid=a --set authService.keySet.keys[0].state=active --set authService.keySet.keys[0].file=a.pem
    --set authService.keySet.keys[0].secretName=rehearsal-key-a --set authService.keySet.keys[0].secretKey=signing-key.pem
    --set authService.keySet.keys[1].kid=b --set authService.keySet.keys[1].state=pending --set authService.keySet.keys[1].file=b.pem
    --set authService.keySet.keys[1].secretName=rehearsal-key-b --set authService.keySet.keys[1].secretKey=signing-key.pem
    --set "authService.accessToken.class=$1" --set "authService.accessToken.issuerURL=${ISSUER_URL}"
    --set-json 'authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]'
    --set-json 'authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]'
  )
}

if [ "${PRINT_PLAN}" = 1 ]; then
  NS="plan"
  zeros="$(printf '0%.0s' $(seq 1 64))"
  printf '%s\n' \
    'stage rollback: namespace-scoped ServiceAccount kubeconfig only; postgres, valkey, key and DSN Secrets (stdin), chart install' \
    '                with authService.keySet (a active, b pending) and class eddsa; port-forward auth-service; discovery issuer pinned.' \
    'S3 chart:       adopt, register b, publish floor 300 s (real), compromise a -successor b: a leaves the JWKS and is denied;' \
    '                helm upgrade class hs256_compat: Deployment env flips, a still absent and compromised, keys rollback a refused;' \
    '                helm rollback to the eddsa revision: Deployment env eddsa, a still absent and compromised, b published.' \
    'not covered:    token cells (login, HS256 alg, revoked sid): they need go-api and a seeded user, see the compose driver.' \
    'needs:          kubectl, helm, jq, openssl, curl; REHEARSAL_KUBECONFIG, REHEARSAL_NAMESPACE, REHEARSAL_RUN_TOKEN, REHEARSAL_DIR, REHEARSAL_IMAGES_ENV'
  for class in eddsa hs256_compat; do
    chart_args "${class}" "registry.invalid/auth-service@sha256:${zeros}" "registry.invalid/auth-migrate@sha256:${zeros}"
    rendered="$(KUBECONFIG=/dev/null helm template "${CHART_ARGS[@]}" --show-only templates/auth-service.yaml)" || die "the chart does not render for class ${class}"
    grep -qF "value: \"${class}\"" <<<"${rendered}" || die "the chart does not render AUTH_ACCESS_TOKEN_CLASS ${class}"
    grep -qF 'checksum/keyset:' <<<"${rendered}" || die "the chart does not render the key set"
    say "PASS: plan: the chart renders class ${class} with the key set"
  done
  exit 0
fi

: "${REHEARSAL_KUBECONFIG:?set REHEARSAL_KUBECONFIG to the ServiceAccount kubeconfig}"
: "${REHEARSAL_NAMESPACE:?set REHEARSAL_NAMESPACE}"
: "${REHEARSAL_RUN_TOKEN:?set REHEARSAL_RUN_TOKEN}"
: "${REHEARSAL_DIR:?set REHEARSAL_DIR to a new directory}"
: "${REHEARSAL_IMAGES_ENV:?set REHEARSAL_IMAGES_ENV to the image digest file}"
valid_run_id "${REHEARSAL_RUN_ID:-}" || die "REHEARSAL_RUN_ID must be digits only (the CI run id)"
NS="${REHEARSAL_NAMESPACE}"
[[ "${NS}" =~ ${NS_RE} ]] || die "REHEARSAL_NAMESPACE is not a plain namespace name"
case "${NS}" in dev-health | default | kube-*) die "refusing the namespace ${NS}" ;; esac
[[ "${REHEARSAL_RUN_TOKEN}" =~ ^[0-9a-f]{16,64}$ ]] || die "REHEARSAL_RUN_TOKEN is not a hex token"
KCFG="${REHEARSAL_KUBECONFIG}"
case "${KCFG}" in "${ADMIN_KUBECONFIG}" | */k3s.yaml) die "REHEARSAL_KUBECONFIG is the k3s admin kubeconfig: use the ServiceAccount file" ;; esac
[ -f "${KCFG}" ] || die "REHEARSAL_KUBECONFIG names no file"
[ "$(stat -c %a "${KCFG}")" = 600 ] || die "REHEARSAL_KUBECONFIG must be mode 0600"
[ -f "${REHEARSAL_IMAGES_ENV}" ] || die "REHEARSAL_IMAGES_ENV names no file"
for tool in kubectl helm jq openssl curl base64 od envsubst; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
for name in DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE DEV_HEALTH_GO_AUTH_SERVICE_IMAGE; do
  value="$(sed -n "s/^${name}=//p" "${REHEARSAL_IMAGES_ENV}" | tail -n 1)"
  [[ "${value}" =~ ${DIGEST_RE} ]] || die "${name} in ${REHEARSAL_IMAGES_ENV} is not a registry digest reference"
  export "${name}=${value}"
done
IMG_SVC="${DEV_HEALTH_GO_AUTH_SERVICE_IMAGE}"
IMG_MIG="${DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE}"

kc() { kubectl --kubeconfig "${KCFG}" "$@"; }
K() { kc -n "${NS}" "$@"; }
hm() { helm --kubeconfig "${KCFG}" --namespace "${NS}" "$@"; }

# The file name proves nothing: ask the API server. The ServiceAccount must be k3s, namespaced and not an admin.
version="$(kc version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion // ""')" || version=""
case "${version}" in *+k3s*) ;; *) die "the cluster behind REHEARSAL_KUBECONFIG is not k3s (server version: ${version:-none}); no kind, no prod" ;; esac
[ "$(kc auth can-i list nodes 2>/dev/null || true)" = no ] || die "REHEARSAL_KUBECONFIG can list nodes: it is not a namespace-scoped ServiceAccount"
[ "$(kc auth can-i create clusterroles 2>/dev/null || true)" = no ] || die "REHEARSAL_KUBECONFIG can create clusterroles: it is not a namespace-scoped ServiceAccount"
owner_token="$(kc get namespace "${NS}" -o 'jsonpath={.metadata.labels.rehearsal-run}' 2>/dev/null || true)"
[ -n "${owner_token}" ] && [ "${owner_token}" = "${REHEARSAL_RUN_TOKEN}" ] || die "the namespace ${NS} does not carry this run's token"

if [ -e "${REHEARSAL_DIR}" ] && [ -n "$(ls -A "${REHEARSAL_DIR}" 2>/dev/null)" ]; then
  die "${REHEARSAL_DIR} exists and is not empty"
fi
mkdir -p "${REHEARSAL_DIR}"
chmod 0700 "${REHEARSAL_DIR}"
DIR="$(cd -- "${REHEARSAL_DIR}" && pwd -P)"
mkdir -p "${DIR}/keys" "${DIR}/records" "${DIR}/evidence"

PF_PID=""
cleanup() {
  local status=$?
  [ -z "${PF_PID}" ] || kill "${PF_PID}" >/dev/null 2>&1 || true
  if [ "${status}" -ne 0 ]; then
    printf '\n--- namespace state at failure ---\n' >&2
    K get pods,jobs -o wide >&2 || true
    K logs --tail 60 -l app.kubernetes.io/component=auth-service >&2 || true
  fi
  hm uninstall "${RELEASE}" >/dev/null 2>&1 || true
  K delete deployment auth-postgres auth-valkey --ignore-not-found >/dev/null 2>&1 || true
  K delete service auth-postgres auth-valkey --ignore-not-found >/dev/null 2>&1 || true
  K delete secret rehearsal-db rehearsal-migrate rehearsal-valkey rehearsal-session rehearsal-hs256 rehearsal-unused \
    rehearsal-key-a rehearsal-key-b auth-pg-superuser --ignore-not-found >/dev/null 2>&1 || true
  shred -u "${DIR}"/keys/*.pem 2>/dev/null || true
  printf 'CLEANUP namespace %s: release and rehearsal objects removed by name; the namespace stays for its owner\n' "${NS}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

check() {
  local name="$1"
  shift
  if "$@"; then rec_pass "${name}"; else rec_fail "${name}"; exit 1; fi
}
eq() { [ "$1" = "$2" ]; }

rec_open "${DIR}/records/50-S3-issuer-rollback-k3s.txt" "S3 issuer rollback on k3s, chart form $(date -u +%Y-%m-%dT%H:%M:%SZ)"
rec_line "namespace=${NS} ci_run=${REHEARSAL_RUN_ID:-unset} kubernetes=${version}"
rec_line "auth_service_digest=${IMG_SVC##*@} auth_migrate_digest=${IMG_MIG##*@}"

# put_secret NAME KEY: the value on stdin only
put_secret() { K create secret generic "$1" --from-file="$2=/dev/stdin" --dry-run=client -o yaml | K apply -f - >/dev/null; }

say "step 1: postgres, valkey, Secrets"
PG_PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
RUNTIME_PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
printf '%s' "${PG_PASSWORD}" | K create secret generic auth-pg-superuser --from-file=password=/dev/stdin >/dev/null
export LOCAL_PG="${PG_IMAGE}" LOCAL_VALKEY="${VALKEY_IMAGE}"
# shellcheck disable=SC2016
envsubst '${LOCAL_PG}' <"${SCRIPT_DIR}/auth_k8s/postgres.yaml" | K apply -f - >/dev/null
# shellcheck disable=SC2016
envsubst '${LOCAL_VALKEY}' <"${SCRIPT_DIR}/auth_k8s/valkey.yaml" | K apply -f - >/dev/null
K rollout status deploy/auth-postgres --timeout=240s >/dev/null
K rollout status deploy/auth-valkey --timeout=240s >/dev/null
pg() { K exec -i deploy/auth-postgres -- psql -U postgres -v ON_ERROR_STOP=1 -At "$@"; }
printf "CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER;\nCREATE ROLE %s NOLOGIN;\nCREATE ROLE %s NOLOGIN;\n" \
  "${RUNTIME_ROLE}" "${RUNTIME_PASSWORD}" "${API_ROLE}" "${QUERY_ROLE}" | pg -f - >/dev/null
dsn() { printf "host=auth-postgres port=5432 user=%s password='%s' dbname=postgres sslmode=disable" "$1" "$2"; }
dsn "${RUNTIME_ROLE}" "${RUNTIME_PASSWORD}" | put_secret rehearsal-db AUTH_DATABASE_URI
dsn postgres "${PG_PASSWORD}" | put_secret rehearsal-migrate AUTH_MIGRATION_DATABASE_URI
printf 'valkey://auth-valkey:6379/1' | put_secret rehearsal-valkey AUTH_VALKEY_URI
head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n' | put_secret rehearsal-session AUTH_SESSION_SUCCESSOR_KEY
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' | put_secret rehearsal-hs256 JWT_SECRET_KEY
K create secret generic rehearsal-unused >/dev/null
for kid in a b; do
  openssl genpkey -algorithm ed25519 -out "${DIR}/keys/${kid}.pem" 2>/dev/null
  put_secret "rehearsal-key-${kid}" signing-key.pem <"${DIR}/keys/${kid}.pem"
done
shred -u "${DIR}"/keys/*.pem

say "step 2: chart install, class eddsa, key set a active + b pending"
chart_args eddsa "${IMG_SVC}" "${IMG_MIG}"
hm upgrade --install "${CHART_ARGS[@]}" --wait --timeout 8m >/dev/null
check install-class-is-eddsa eq "$(K get "deploy/${DEPLOYMENT}" -o json | jq -r '.spec.template.spec.containers[0].env[] | select(.name == "AUTH_ACCESS_TOKEN_CLASS") | .value')" eddsa

# forward auth-service to a loopback port the kernel picks; read it from kubectl's own line
fwd() {
  [ -z "${PF_PID}" ] || kill "${PF_PID}" >/dev/null 2>&1 || true
  K port-forward "svc/${SERVICE}" ":8095" >"${DIR}/evidence/forward.out" 2>&1 9>&- &
  PF_PID=$!
  for _ in $(seq 1 30); do
    PF_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) ->.*/\1/p' "${DIR}/evidence/forward.out" | head -n 1)"
    [ -z "${PF_PORT}" ] || return 0
    sleep 1
  done
  die "port-forward to ${SERVICE} did not start"
}
jwks_doc() { curl -fsS --max-time 10 "http://127.0.0.1:${PF_PORT}/.well-known/jwks.json"; }
discovery_doc() { curl -fsS --max-time 10 "http://127.0.0.1:${PF_PORT}/.well-known/openid-configuration"; }
jwks_kid_set() { jwks_kids "$(jwks_doc)" | tr '\n' ' ' | sed 's/ $//'; }
jwks_lacks() {
  local doc rc=0
  doc="$(jwks_doc)" || return 1
  jwks_has_kid "${doc}" "$1" || rc=$?
  [ "${rc}" = 1 ]
}
wait_jwks_kids() { # wait_jwks_kids "a b" TIMEOUT_S
  local want="$1" deadline=$((SECONDS + $2))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    [ "$(jwks_kid_set 2>/dev/null || true)" = "${want}" ] && return 0
    sleep 3
  done
  return 1
}

# keys_cmd ARGS...: a one-off pod of the auth-migrate image with the live key-set volume of the Deployment -> KOUT KERR KRC
keys_cmd() {
  local name args volume overrides phase deadline=$((SECONDS + 240))
  name="keys-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
  args="$(printf '%s\n' keys "$@" | jq -R . | jq -sc .)"
  volume="$(K get "deploy/${DEPLOYMENT}" -o json | jq -c '.spec.template.spec.volumes[] | select(.name == "signing-key-source")')"
  overrides="$(jq -nc --arg name "${name}" --arg image "${IMG_MIG}" --arg runtime "${RUNTIME_ROLE}" --argjson args "${args}" --argjson volume "${volume}" '
    {spec: {securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532},
      volumes: [$volume],
      containers: [{name: $name, image: $image, args: $args,
        volumeMounts: [{name: "signing-key-source", mountPath: "/run/auth/signing", readOnly: true}],
        env: [{name: "AUTH_MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: "rehearsal-migrate", key: "AUTH_MIGRATION_DATABASE_URI"}}},
              {name: "AUTH_RUNTIME_DATABASE_ROLE", value: $runtime},
              {name: "AUTH_DATABASE_SCHEMA", value: "auth"},
              {name: "AUTH_SIGNING_KEYSET_FILE", value: "/run/auth/signing/keyset.json"}]}]}}')"
  KRC=0
  K run "${name}" --restart=Never --image="${IMG_MIG}" --override-type=strategic --overrides="${overrides}" >/dev/null
  while :; do
    phase="$(K get "pod/${name}" -o 'jsonpath={.status.phase}' 2>/dev/null || true)"
    case "${phase}" in Succeeded) break ;; Failed) KRC=1; break ;; esac
    [ "${SECONDS}" -lt "${deadline}" ] || { KRC=1; break; }
    sleep 2
  done
  KOUT="$(K logs "${name}" 2>"${DIR}/evidence/keys.err" || true)"
  KERR="${KOUT}"
  K delete pod "${name}" --wait=true >/dev/null
}
key_state() { keys_cmd list; [ "${KRC}" = 0 ] && keys_list_state "${KOUT}" "$1"; }
refused_as() { [ "${KRC}" = 1 ] && [ "$(refusal_reason "${KERR}")" = "$1" ]; }
class_is() { [ "$(K get "deploy/${DEPLOYMENT}" -o json | jq -r '.spec.template.spec.containers[0].env[] | select(.name == "AUTH_ACCESS_TOKEN_CLASS") | .value')" = "$1" ]; }
helm_revision() { hm history "${RELEASE}" -o json | jq -r 'map(select(.status == "deployed")) | last | .revision'; }

say "step 3: discovery, adopt, register b"
fwd
check discovery-issuer-pinned eq "$(discovery_doc | jq -r .issuer)" "${ISSUER_URL}"
check jwks-lists-a wait_jwks_kids "a" 60
keys_cmd adopt -apply
check adopt-applied eq "${KRC}" 0
check a-active eq "$(key_state a)" active
keys_cmd register -kid b -apply
check register-b-applied eq "${KRC}" 0
B_REGISTER_EPOCH="$(date +%s)"
check jwks-lists-a-and-b wait_jwks_kids "a b" 90
EVIDENCE_EDDSA_REVISION="$(helm_revision)"
rec_line "eddsa_helm_revision=${EVIDENCE_EDDSA_REVISION}"

say "step 4: the real publish floor (${PENDING_FLOOR} s), then compromise a with successor b"
until keys_cmd activate -kid b && [ "${KRC}" = 0 ]; do
  [ "${KRC}" = 1 ] && [ "$(refusal_reason "${KERR}")" = floor_not_reached ] || die "the activate dry run of b was refused for another reason"
  [ $(($(date +%s) - B_REGISTER_EPOCH)) -lt $((PENDING_FLOOR + 120)) ] || die "the publish floor did not pass within 120 s of ${PENDING_FLOOR} s"
  sleep 10
done
keys_cmd compromise -kid a -successor b
check compromise-dry-run-ok eq "${KRC}" 0
keys_cmd compromise -kid a -successor b -apply
check compromise-applied eq "${KRC}" 0
check a-compromised eq "$(key_state a)" compromised
check b-active eq "$(key_state b)" active
check a-left-the-jwks wait_jwks_kids "b" 60

say "step 5: issuer rollback by helm upgrade, class hs256_compat"
chart_args hs256_compat "${IMG_SVC}" "${IMG_MIG}"
hm upgrade "${CHART_ARGS[@]}" --wait --timeout 8m >/dev/null
fwd
check rollback-class-is-hs256-compat class_is hs256_compat
rec_line "hs256_compat_helm_revision=$(helm_revision)"
check rollback-a-still-absent-from-the-jwks jwks_lacks a
check rollback-a-still-compromised eq "$(key_state a)" compromised
keys_cmd rollback -kid a -apply
check rollback-key-a-refused-terminal refused_as compromised_terminal

say "step 6: chart rollback to the eddsa revision"
hm rollback "${RELEASE}" "${EVIDENCE_EDDSA_REVISION}" --wait --timeout 8m >/dev/null
fwd
check forward-class-is-eddsa class_is eddsa
rec_line "after_helm_rollback_revision=$(helm_revision)"
check forward-a-still-absent-from-the-jwks jwks_lacks a
check forward-a-still-compromised eq "$(key_state a)" compromised
check forward-b-still-published wait_jwks_kids "b" 60
rec_line "S3 k3s rehearsal complete"
say "records:"
for f in "${DIR}"/records/*.txt; do printf '%s  %s\n' "$(sha256sum "${f}" | cut -c1-64)" "${f##*/}"; done
say "AUTH KEY ROTATION K3S REHEARSAL PASSED"
