#!/usr/bin/env bash
# shellcheck disable=SC2016,SC2015  # jq programs name their own variables; A && B || die chains are tests
# Rehearsal of the users-owned flip-back for SSO accounts on a real API server (bigboy k3s), in the chart form.
# Not a CI job (no kind): the lead runs it on bigboy in a FRESH namespace that the lead owns. This script never makes or deletes a
# namespace and never holds more than a namespace-scoped kubeconfig.
#
#   ci/auth_flipback_sso_k3s.sh --print-plan   no cluster: print the steps and check that helm renders every chart step (needs helm)
#   ci/auth_flipback_sso_k3s.sh                the steps below
#
# S0 install at phase 3 (go-api forwards login), the hooks, the restore fence
# S1 an admin user and an organization (one-off pods of the dho image)
# S2 a SAML provider row: ONE INSERT in this rehearsal namespace (go-api serves no provider create route; pm 2026-11-02)
# S3 a SAML sign-in through go-api at phase 3: go-api writes the SSO user (the join is off in auth-service)
# S4 flip 3 to 4: U1 (stage goApi), barrier up-go-api
# S5 users-backfill (the SSO user reaches the auth schema), users-reconcile (0 gated differences)
# S6 barrier up-owned, U3 (stage owned)
# S7 the sign-in again under owned: same accounts, and no write to public.users
# S8 flip back 4 to 3: D1 (stage goApi, grants kept), barrier down-auth, D2 (phase 3)
# S9 the sign-in through go-api again: same accounts, no new account, no identity conflict
#
# What is NOT here (pm 2026-11-02): an account that auth-service provisions through its sign-in join under owned. AUTH_SSO_JIT renders only
# from phase 5, so it belongs to the orgs-owned flip (CHAOS-10998). Nothing here sets AUTH_SSO_JIT.
#
# The identity provider is the test stub internal/testsupport/idpstub (SAML mode), built and imported into the k3s containerd by the lead.
# Scope (pm 2026-11-02, answers-11620-1.md amendment): SAML only. go-api dials an OIDC issuer only at a global address
# (internal/api/externalurl), which a cluster stub cannot have without a host nat rule, so the OIDC leg stays in the integration tests
# (internal/api/sso/flipback_owned_integration_test.go and the OIDC sign-in tests of that package). SAML makes no outbound call at sign-in.
#
# Images: go-api, auth-service and auth-migrate are registry DIGEST references (the Inspect image step of the main push run whose head is
# the merge base); the stub is a local import ref idpstub-fb/idpstub:<tag> with pull policy Never.
# Env (all required except --print-plan): AUTH_FB_KUBECONFIG (a namespace-scoped ServiceAccount kubeconfig), AUTH_FB_NAMESPACE (authfb-<8 hex>),
# AUTH_FB_RUN_TOKEN (16 hex; the lead puts it in ConfigMap fb-run key token of the namespace), AUTH_FB_IMAGE_DHO, AUTH_FB_IMAGE_SERVICE,
# AUTH_FB_IMAGE_MIGRATE, AUTH_FB_IMAGE_IDPSTUB, AUTH_FB_POD_CIDR (the node's pod /24),
# AUTH_FB_WORK (a directory for the run's own files). Secrets travel on stdin or in 0600 files of the run; none is on argv or in output.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR ROOT
readonly CHART="${ROOT}/deploy/helm/dev-health"
readonly DIGEST_RE='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
readonly IDP_RE='^idpstub-fb/idpstub:[a-z0-9][a-z0-9_.-]{0,62}$'
readonly HOST_DEFAULT_KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
readonly RELEASE="ap" FULL="ap-dev-health"
readonly IDP_HOST="idp.fb.test"
readonly SP_ENTITY="https://sp.fb.test/saml/metadata"
readonly DOMAIN="fbrehearsal.dev"
PASSES=0

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}
pass() {
  PASSES=$((PASSES + 1))
  printf 'PASS: %s\n' "$1"
}

PRINT_PLAN=0
PRINT_COUNT=0
for arg in "$@"; do
  case "${arg}" in
    --print-plan) PRINT_PLAN=1 ;;
    --print-live-count-sql) PRINT_COUNT=1 ;;
    *) die "usage: ci/auth_flipback_sso_k3s.sh [--print-plan|--print-live-count-sql]" ;;
  esac
done

# The live phase 4 step is read-only first: the operator counts the providers and the SSO-linked users and creates nothing. The SQL goes
# in on stdin of a read-only psql session; the script touches no cluster in this mode.
if [ "${PRINT_COUNT}" = 1 ]; then
  cat <<'SQL'
BEGIN READ ONLY;
SELECT 'sso_providers=' || count(*) FROM public.sso_providers;
SELECT 'sso_linked_users=' || count(*) FROM public.users WHERE auth_provider IN ('oidc', 'saml') AND sso_provider_id IS NOT NULL;
ROLLBACK;
SQL
  exit 0
fi

if [ "${PRINT_PLAN}" = 1 ]; then
  IMG_DHO="registry.invalid/dev-health-go-dho@sha256:$(printf '0%.0s' $(seq 1 64))"
  IMG_SVC="registry.invalid/auth-service@sha256:$(printf '1%.0s' $(seq 1 64))"
  IMG_MIG="registry.invalid/auth-migrate@sha256:$(printf '2%.0s' $(seq 1 64))"
  IMG_IDP="idpstub-fb/idpstub:plan"
  NS="authfb-00000000"
  RUN_TOKEN="0000000000000000"
  POD_CIDR="10.42.0.0/24"
else
  IMG_DHO="${AUTH_FB_IMAGE_DHO:-}"
  IMG_SVC="${AUTH_FB_IMAGE_SERVICE:-}"
  IMG_MIG="${AUTH_FB_IMAGE_MIGRATE:-}"
  IMG_IDP="${AUTH_FB_IMAGE_IDPSTUB:-}"
  NS="${AUTH_FB_NAMESPACE:-}"
  RUN_TOKEN="${AUTH_FB_RUN_TOKEN:-}"
  POD_CIDR="${AUTH_FB_POD_CIDR:-}"
fi
for image in "${IMG_DHO}" "${IMG_SVC}" "${IMG_MIG}"; do
  [[ "${image}" =~ ${DIGEST_RE} ]] || die "an image is not a registry digest reference (repo@sha256:<64 hex>): ${image:-<unset>}"
done
[[ "${IMG_IDP}" =~ ${IDP_RE} ]] && [ "${IMG_IDP##*:}" != latest ] || die "AUTH_FB_IMAGE_IDPSTUB must be a local import ref idpstub-fb/idpstub:<tag> (not latest): ${IMG_IDP:-<unset>}"
[[ "${NS}" =~ ^authfb-[0-9a-f]{8}$ ]] || die "AUTH_FB_NAMESPACE must be authfb-<8 hex>: ${NS:-<unset>}"
[[ "${RUN_TOKEN}" =~ ^[0-9a-f]{16}$ ]] || die "AUTH_FB_RUN_TOKEN must be 16 lowercase hex characters"
[[ "${POD_CIDR}" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.0/24$ ]] || die "AUTH_FB_POD_CIDR must be an IPv4 /24 network (the node pod range): ${POD_CIDR:-<unset>}"
for tool in helm jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

POD_NET="${POD_CIDR%/24}"
POD_BASE="${POD_NET%.0}"
KUBECONFIG_FILE="${AUTH_FB_KUBECONFIG:-}"

# ---- helm layers: one function per chart step, used by the plan and by the run -----------------------------------------------------
write_base_values() { # FILE
  cat >"$1" <<EOF
web: {enabled: false}
goWorkers: {enabled: false}
valkey: {enabled: true}
postgresql: {enabled: true}
clickhouse: {enabled: true}
secrets: {create: true}
goApi:
  enabled: true
  replicas: 1
  image: {repository: "${IMG_DHO}", pullPolicy: IfNotPresent}
  sessionIssue: {secretName: session-issue}
  extraArgs: ["--health-check-timeout=10s"]
  readinessProbe: {timeoutSeconds: 10}
  livenessProbe: {timeoutSeconds: 10}
  extraEnv:
    - {name: VALKEY_URI, value: "redis://${FULL}-valkey:6379/1"}
authService:
  phase: 3
  loginStage: goApi
  replicas: 1
  image: {repository: "${IMG_SVC}", pullPolicy: IfNotPresent}
  database: {secretName: auth-db}
  signingKey: {secretName: auth-key, keyId: fb}
  sessionKeys: {successor: {secretName: auth-session}, loginLockout: {secretName: auth-lockout}}
  valkey: {secretName: auth-ratelimit}
  social: {secretName: auth-social}
  mail: {secretName: auth-mail}
  trustedProxies: ["${POD_CIDR}", "!${POD_NET}", "!${POD_BASE}.1"]
  trustedProxiesWideAccepted: true
  migrate:
    secretName: auth-migrate-db
    image: {repository: "${IMG_MIG}", pullPolicy: IfNotPresent}
    sessionReaders: {goApi: devhealth_api, queryApi: devhealth_query_api}
  networkPolicy:
    enabled: true
    egress:
      database:
        to: [{podSelector: {matchLabels: {app.kubernetes.io/name: dev-health, app.kubernetes.io/instance: ${RELEASE}, app.kubernetes.io/component: postgresql}}}]
      valkey:
        to: [{podSelector: {matchLabels: {app.kubernetes.io/name: dev-health, app.kubernetes.io/instance: ${RELEASE}, app.kubernetes.io/component: valkey}}}]
      mail:
        to: [{ipBlock: {cidr: 198.51.100.7/32}}]
        ports: [{port: 443, protocol: TCP}]
migrations:
  hook:
    image: "${IMG_DHO}"
    pullPolicy: IfNotPresent
    provisionRoles: {enabled: true, image: "${IMG_DHO}", pullPolicy: IfNotPresent}
    riverMigrate: {enabled: true, image: "${IMG_DHO}", pullPolicy: IfNotPresent}
    routeActivate: {enabled: false, image: "${IMG_DHO}", pullPolicy: IfNotPresent}
EOF
}
write_secret_values() { # FILE API_PASSWORD JWT_KEY  (the file is mode 0600; the session issue token lives in its own Secret)
  (
    umask 077
    printf 'secrets:\n  data:\n    API_DATABASE_URI: "postgres://devhealth_api:%s@%s-postgresql:5432/devhealth?sslmode=disable"\n    JWT_SECRET_KEY: "%s"\n' \
      "$2" "${FULL}" "$3" >"$1"
  )
}

ACK=""
# layer NAME -> LAYER (an array of helm arguments), one per chart step
layer() {
  local ack=()
  if [ -n "${ACK}" ]; then ack=(--set "authService.migrate.acknowledgeGrantNarrowing=${ACK}"); fi
  case "$1" in
    P3) LAYER=(--set authService.phase=3 --set authService.loginStage=goApi) ;;
    U1) LAYER=(--set authService.phase=4 --set authService.loginStage= --set authService.usersOwnedStage=goApi) ;;
    U2) LAYER=(--set authService.phase=4 --set authService.loginStage= --set authService.usersOwnedStage=goApi --set authService.backfill.enabled=true
      --set authService.backfill.verb=users-backfill --set authService.backfill.dryRun=false --set "authService.backfill.runId=fb-backfill-${RUN_TOKEN}") ;;
    REC) LAYER=(--set authService.phase=4 --set authService.loginStage= --set authService.usersOwnedStage=goApi --set authService.backfill.enabled=true
      --set authService.backfill.verb=users-reconcile --set authService.backfill.dryRun=true --set "authService.backfill.runId=fb-reconcile-${RUN_TOKEN}") ;;
    U3) LAYER=(--set authService.phase=4 --set authService.loginStage= --set authService.usersOwnedStage=owned) ;;
    D1) LAYER=(--set authService.phase=4 --set authService.loginStage= --set authService.usersOwnedStage=goApi "${ack[@]}") ;;
    D2) LAYER=(--set authService.phase=3 --set authService.loginStage=goApi "${ack[@]}") ;;
    *) die "unknown layer $1" ;;
  esac
}

VALUES_BASE=""
VALUES_SECRET=""
# helm_run VERB LAYER-NAME [extra args]: helm template (plan) or helm upgrade --install (run)
helm_cluster() { # LAYER-NAME [extra helm args]
  local name=$1
  shift
  layer "${name}"
  helm upgrade --install "${RELEASE}" "${CHART}" --kubeconfig "${KUBECONFIG_FILE}" --namespace "${NS}" -f "${VALUES_BASE}" -f "${VALUES_SECRET}" \
    "${LAYER[@]}" --timeout 10m "$@"
}
render() { # LAYER-NAME TEMPLATE
  layer "$1"
  helm template "${RELEASE}" "${CHART}" --namespace "${NS}" -f "${VALUES_BASE}" -f "${VALUES_SECRET}" "${LAYER[@]}" --show-only "templates/$2"
}
env_of() { # NAME (rendered manifest on stdin): the value of the first container env entry, empty when absent
  awk -v n="$1" '$1 == "-" && $2 == "name:" && $3 == n {getline; gsub(/"/, "", $2); print $2; exit}'
}
expect_env() { # LAYER TEMPLATE NAME WANT LABEL
  local got
  got="$(render "$1" "$2" | env_of "$3")"
  [ "${got}" = "$4" ] || die "plan: $5: $3 is '${got}' in $2 at $1, want '$4'"
}

print_plan() {
  local i step
  local steps=(
    "S0 install at phase 3"
    "S1 admin and organization"
    "S2 the SAML provider row, one INSERT in the rehearsal namespace"
    "S3 SSO sign-in at phase 3 creates the SAML user"
    "S4 flip 3 to 4: U1 go-api forwards, barrier up-go-api"
    "S5 users-backfill and users-reconcile"
    "S6 barrier up-owned, U3 owned"
    "S7 SSO sign-in under owned finds the same accounts"
    "S8 flip back 4 to 3: D1, barrier down-auth, D2"
    "S9 SSO sign-in through go-api finds the same accounts"
  )
  for i in "${!steps[@]}"; do
    step="${steps[$i]}"
    printf 'plan: %s\n' "${step}"
  done
  local work
  work="$(mktemp -d)"
  VALUES_BASE="${work}/base.yaml"
  VALUES_SECRET="${work}/secrets.yaml"
  write_base_values "${VALUES_BASE}"
  write_secret_values "${VALUES_SECRET}" plan-pw plan-key
  ACK="0123456789abcdef"

  expect_env P3 go-api-deployment.yaml AUTH_PROXY_LOCAL_LOGIN true "phase 3"
  expect_env P3 go-api-deployment.yaml AUTH_USERS_OWNED "" "phase 3"
  expect_env P3 auth-service.yaml AUTH_USERS_OWNED "" "phase 3"
  pass "plan: phase 3 renders go-api forwarding login and auth-service not owned"
  expect_env U1 go-api-deployment.yaml AUTH_USERS_OWNED true "U1"
  expect_env U1 go-api-deployment.yaml AUTH_PROXY_CREDENTIALS true "U1"
  expect_env U1 auth-service.yaml AUTH_USERS_OWNED "" "U1"
  pass "plan: U1 renders go-api owned and forwarding credentials, auth-service not owned"
  expect_env U3 auth-service.yaml AUTH_USERS_OWNED true "U3"
  pass "plan: U3 renders auth-service owned"
  render D1 auth-migrate-job.yaml | grep -q -- '-defer-narrowing' || die "plan: D1 renders no -defer-narrowing on the auth-migrate Job"
  expect_env D1 auth-service.yaml AUTH_USERS_OWNED "" "D1"
  pass "plan: D1 renders stage goApi with the narrowing deferred"
  expect_env D2 go-api-deployment.yaml AUTH_USERS_OWNED "" "D2"
  expect_env D2 auth-service.yaml AUTH_USERS_OWNED "" "D2"
  expect_env D2 go-api-deployment.yaml AUTH_PROXY_LOCAL_LOGIN true "D2"
  pass "plan: D2 renders phase 3 with neither go-api nor auth-service owned"
  local name all
  for name in P3 U1 U2 REC U3 D1 D2; do
    layer "${name}"
    all="$(helm template "${RELEASE}" "${CHART}" --namespace "${NS}" -f "${VALUES_BASE}" -f "${VALUES_SECRET}" "${LAYER[@]}")"
    if grep -q 'AUTH_SSO_JIT' <<<"${all}"; then die "plan: ${name} renders AUTH_SSO_JIT"; fi
  done
  pass "plan: no step renders AUTH_SSO_JIT"
  rm -rf "${work}"
}

if [ "${PRINT_PLAN}" = 1 ]; then
  print_plan
  exit 0
fi

# ---- the cluster: a namespace-scoped kubeconfig of a namespace this run's lead made --------------------------------------------------
[ -n "${KUBECONFIG_FILE}" ] || die "AUTH_FB_KUBECONFIG is required"
case "${KUBECONFIG_FILE}" in /etc/rancher/*) die "refusing the host default kubeconfig (${KUBECONFIG_FILE}): use the namespace-scoped ServiceAccount kubeconfig" ;; esac
[ "${KUBECONFIG_FILE}" != "${HOST_DEFAULT_KUBECONFIG}" ] || die "refusing the host default kubeconfig"
[ -r "${KUBECONFIG_FILE}" ] || die "the kubeconfig ${KUBECONFIG_FILE} is not readable"
[ -n "${AUTH_FB_WORK:-}" ] || die "AUTH_FB_WORK is required"
for tool in kubectl openssl curl od sed grep awk; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
export KUBECONFIG="${KUBECONFIG_FILE}"
kc() { kubectl --kubeconfig "${KUBECONFIG_FILE}" "$@"; }
K() { kc -n "${NS}" "$@"; }

for verb_resource in "list nodes" "create ns" "create clusterroles"; do
  # shellcheck disable=SC2086
  [ "$(kc auth can-i ${verb_resource} 2>/dev/null || true)" = no ] || die "the kubeconfig is not namespace scoped: it may ${verb_resource}"
done
[ "$(K get configmap fb-run -o 'jsonpath={.data.token}' 2>/dev/null || true)" = "${RUN_TOKEN}" ] || die "the namespace ${NS} does not carry this run's token (ConfigMap fb-run)"
pass "the kubeconfig is namespace scoped and ${NS} carries this run's token"

umask 077
mkdir -p "${AUTH_FB_WORK}"
WORK="$(mktemp -d "${AUTH_FB_WORK}/fb.XXXXXX")"
readonly WORK
PF_PIDS=()
PF_LEFT=0
readonly PF_COMM=kube"ctl"
pf_stop() {
  local pid
  for pid in "${PF_PIDS[@]:-}"; do
    [ -n "${pid}" ] || continue
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    if [ "$(cat "/proc/${pid}/comm" 2>/dev/null || true)" = "${PF_COMM}" ]; then
      printf 'LEFTOVER child pid=%s cwd=%s\n' "${pid}" "$(readlink "/proc/${pid}/cwd" 2>/dev/null || true)" >&2
      PF_LEFT=1
    fi
  done
  PF_PIDS=()
}
cleanup() {
  local status=$?
  pf_stop
  if [ "${PF_LEFT}" = 1 ] && [ "${status}" -eq 0 ]; then status=98; fi
  if [ "${status}" -ne 0 ]; then
    printf '\n--- namespace state at failure ---\n' >&2
    K get pods,jobs -o wide >&2 || true
    for pod in $(K get pods -o name 2>/dev/null | grep -E 'migrate|provision|grants|backfill|go-api|auth-service|idpstub' || true); do
      printf '\n--- logs %s ---\n' "${pod}" >&2
      K logs "${pod}" --all-containers --tail 40 >&2 || true
    done
    helm --kubeconfig "${KUBECONFIG_FILE}" -n "${NS}" history "${RELEASE}" >&2 || true
  fi
  rm -rf "${WORK}"
  printf 'SUMMARY: passes=%s\n' "${PASSES}"
  if [ "${status}" -eq 0 ]; then printf 'RESULT: PASS\n'; else printf 'RESULT: FAIL (exit %s)\n' "${status}"; fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

put_secret() { # NAME KEY VALUE: create or replace, the value on stdin only
  printf '%s' "$3" | K create secret generic "$1" --from-file="$2=/dev/stdin" --dry-run=client -o yaml | K apply -f - >/dev/null
}
dsn() { printf "host=%s-postgresql port=5432 user=%s password='%s' dbname=devhealth sslmode=disable" "${FULL}" "$1" "$2"; }
PG_POD="${FULL}-postgresql-0"
pgq() { K exec -i "${PG_POD}" -- psql -X -q -U devhealth -d devhealth -At -v ON_ERROR_STOP=1 -f -; }

# ---- the identity provider stub and its TLS material ---------------------------------------------------------------------------------
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 2 -subj "/CN=idpstub-fb" \
  -addext "subjectAltName=DNS:${IDP_HOST}" -keyout "${WORK}/idp.key" -out "${WORK}/idp.pem" 2>/dev/null
K create secret generic idp-tls --from-file=server.pem="${WORK}/idp.pem" --from-file=server.key="${WORK}/idp.key" >/dev/null
IDP_USER="saml-sub-1,saml.one@${DOMAIN},SAML One"
K apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: idpstub, labels: {app: idpstub}}
spec:
  replicas: 1
  selector: {matchLabels: {app: idpstub}}
  template:
    metadata: {labels: {app: idpstub}}
    spec:
      automountServiceAccountToken: false
      securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, seccompProfile: {type: RuntimeDefault}}
      containers:
        - name: idpstub
          image: ${IMG_IDP}
          imagePullPolicy: Never
          args: ["saml", "-listen", ":8443", "-cert", "/certs/server.pem", "-key", "/certs/server.key", "-base-url", "https://${IDP_HOST}",
                 "-sp-entity-id", "${SP_ENTITY}", "-user", "${IDP_USER}"]
          ports: [{name: https, containerPort: 8443}]
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          volumeMounts: [{name: tls, mountPath: /certs, readOnly: true}]
      volumes: [{name: tls, secret: {secretName: idp-tls}}]
---
apiVersion: v1
kind: Service
metadata: {name: idpstub}
spec:
  selector: {app: idpstub}
  ports: [{name: https, port: 443, targetPort: https}]
EOF
K rollout status deploy/idpstub --timeout=180s >/dev/null
pass "S0 the identity provider stub runs (SAML mode, local image ${IMG_IDP})"

# ---- S0 install at phase 3 -----------------------------------------------------------------------------------------------------------
API_PASSWORD="$(openssl rand -hex 12)"
AUTH_PASSWORD="$(openssl rand -hex 12)"
JWT_KEY="$(openssl rand -hex 24)"
ISSUE_TOKEN="svc_authsess_$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=\n' | cut -c1-43)"
VALUES_BASE="${WORK}/base.yaml"
VALUES_SECRET="${WORK}/secrets.yaml"
write_base_values "${VALUES_BASE}"
write_secret_values "${VALUES_SECRET}" "${API_PASSWORD}" "${JWT_KEY}"

helm_cluster P3 --no-hooks >/dev/null
K rollout status statefulset/"${FULL}-postgresql" --timeout=240s >/dev/null
K rollout status statefulset/"${FULL}-clickhouse" --timeout=240s >/dev/null
K rollout status deploy/"${FULL}-valkey" --timeout=240s >/dev/null
printf "CREATE ROLE devhealth_api LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\nCREATE ROLE devhealth_query_api NOLOGIN;\nCREATE ROLE devhealth_auth LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" \
  "${API_PASSWORD}" "${AUTH_PASSWORD}" | pgq >/dev/null
put_secret session-issue AUTH_SESSION_ISSUE_TOKEN "${ISSUE_TOKEN}"
put_secret auth-db AUTH_DATABASE_URI "$(dsn devhealth_auth "${AUTH_PASSWORD}")"
put_secret auth-migrate-db AUTH_MIGRATION_DATABASE_URI "$(dsn devhealth devhealth)"
openssl genpkey -algorithm ed25519 2>/dev/null | K create secret generic auth-key --from-file=signing-key.pem=/dev/stdin >/dev/null
put_secret auth-ratelimit AUTH_VALKEY_URI "redis://${FULL}-valkey:6379/1"
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' | K create secret generic auth-session --from-file=AUTH_SESSION_SUCCESSOR_KEY=/dev/stdin >/dev/null
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' | K create secret generic auth-lockout --from-file=AUTH_LOGIN_LOCKOUT_KEY=/dev/stdin >/dev/null
K create secret generic auth-social >/dev/null
{
  printf 'EMAIL_PROVIDER=resend\nEMAIL_FROM_ADDRESS=noreply@%s\nEMAIL_API_KEY=re_fb_not_a_real_key\nAPP_BASE_URL=https://app.%s/\n' "${DOMAIN}" "${DOMAIN}"
} | K create secret generic auth-mail --from-env-file=/dev/stdin >/dev/null

# no --wait: a fresh database gets the restore-fence row only from the explicit init below
helm_cluster P3 --history-max 0 >/dev/null
fence_reason=""
for _ in $(seq 1 60); do
  fence_reason="$(K logs -l app.kubernetes.io/component=auth-service --tail=-1 2>/dev/null | grep -o -m1 'timeline_not_fenced' || true)"
  [ -n "${fence_reason}" ] && break
  sleep 3
done
[ "${fence_reason}" = timeline_not_fenced ] || die "auth-service never reported timeline_not_fenced before the restore-fence init"
K run auth-fence-init --restart=Never --image="${IMG_MIG}" --override-type=strategic \
  --overrides="$(jq -nc --arg image "${IMG_MIG}" '{spec: {containers: [{name: "auth-fence-init", image: $image, args: ["restore-fence", "init"],
    env: [{name: "AUTH_MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: "auth-migrate-db", key: "AUTH_MIGRATION_DATABASE_URI"}}},
          {name: "AUTH_RUNTIME_DATABASE_ROLE", value: "devhealth_auth"}]}]}}')" >/dev/null
K wait --for=jsonpath='{.status.phase}'=Succeeded pod/auth-fence-init --timeout=180s >/dev/null || die "the restore-fence init did not succeed"
K delete pod auth-fence-init --wait=true >/dev/null
K rollout status deploy/"${FULL}-auth-service" --timeout=480s >/dev/null
K rollout status deploy/"${FULL}-go-api" --timeout=300s >/dev/null
pass "S0 phase 3 is up: go-api and auth-service Ready, hooks and restore fence done"

# ---- port-forward and HTTP helpers --------------------------------------------------------------------------------------------------------
PF_GO=""
PF_IDP=""
pf_start() { # VAR SERVICE REMOTE-PORT
  local log="${WORK}/pf.$2.$$.log" port=""
  : >"${log}"
  kubectl --kubeconfig "${KUBECONFIG_FILE}" -n "${NS}" port-forward "svc/$2" ":$3" >"${log}" 2>&1 9>&- &
  PF_PIDS+=("$!")
  for _ in $(seq 1 60); do
    port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) ->.*/\1/p' "${log}" | head -1)"
    [ -n "${port}" ] && break
    sleep 1
  done
  [ -n "${port}" ] || die "port-forward to $2 did not start"
  printf -v "$1" '%s' "${port}"
}
pf_all() {
  pf_stop
  pf_start PF_GO "${FULL}-go-api" 8000
  pf_start PF_IDP idpstub 443
}
GO() { printf 'http://127.0.0.1:%s' "${PF_GO}"; }
BODY="${WORK}/body.json"
# api METHOD PATH [DATAFILE] [HEADERFILE] -> prints the HTTP status; the body is in ${BODY}
api() {
  local method=$1 path=$2 data="${3:-}" hdr="${4:-}" args=()
  if [ -n "${data}" ]; then args+=(-H 'Content-Type: application/json' --data-binary "@${data}"); fi
  if [ -n "${hdr}" ]; then args+=(-H "@${hdr}"); fi
  curl -sS --max-time 90 -X "${method}" -b "${WORK}/jar" -c "${WORK}/jar" -o "${BODY}" -w '%{http_code}' "${args[@]}" "$(GO)${path}"
}
idp_get() { # URL -> headers in ${WORK}/idp.hdr, body in ${WORK}/idp.body
  curl -sS --max-time 60 --cacert "${WORK}/idp.pem" --connect-to "${IDP_HOST}:443:127.0.0.1:${PF_IDP}" -D "${WORK}/idp.hdr" -o "${WORK}/idp.body" "$1"
}
jfile() { jq -n "$@" >"${WORK}/req.json"; }

# ---- S1 admin and organization ------------------------------------------------------------------------------------------------------
ADMIN_EMAIL="admin@${DOMAIN}"
ADMIN_PASSWORD="Fb$(openssl rand -hex 12)"
oneoff_dho() { # NAME STDIN-VALUE ARGS...: a one-off pod of the dho image with the API login; the stdin value is the password
  local name=$1 stdin=$2 overrides
  shift 2
  overrides="$(jq -nc --arg name "${name}" --arg image "${IMG_DHO}" --arg secret "${FULL}-secrets" '{spec: {containers: [{name: $name, image: $image,
    env: [{name: "API_DATABASE_URI", valueFrom: {secretKeyRef: {name: $secret, key: "API_DATABASE_URI"}}}]}]}}')"
  printf '%s\n' "${stdin}" | K run "${name}" --restart=Never -i --rm --quiet --image="${IMG_DHO}" --override-type=strategic --overrides="${overrides}" -- "$@" >/dev/null
}
oneoff_dho fb-admin-user "${ADMIN_PASSWORD}" admin users create --email "${ADMIN_EMAIL}" --full-name "FB Admin" --superuser --password-stdin
oneoff_dho fb-admin-org "" admin orgs create --name "FB Org" --tier enterprise --owner-email "${ADMIN_EMAIL}"
# the verb reads MIGRATION_DATABASE_URI (the owner DSN); go-api presents an auth-session-issuer row of its own internal_service_credentials to auth-service's session issue route (401 for a token with no row)
ISSUE_TOKEN="$(printf '\n' | K run fb-issue-cred --restart=Never -i --rm --quiet --image="${IMG_DHO}" --override-type=strategic \
  --overrides="$(jq -nc --arg image "${IMG_DHO}" --arg secret auth-migrate-db '{spec: {containers: [{name: "fb-issue-cred", image: $image,
    env: [{name: "MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: $secret, key: "AUTH_MIGRATION_DATABASE_URI"}}}]}]}}')" -- \
  service-credentials create --service auth-session-issuer --scope sessions:issue)"
ISSUE_TOKEN="${ISSUE_TOKEN//[$'\r\n ']/}"
[[ "${ISSUE_TOKEN}" =~ ^svc_authsess_[A-Za-z0-9_-]{43}$ ]] || die "S1: the session issue credential was not minted"
put_secret session-issue AUTH_SESSION_ISSUE_TOKEN "${ISSUE_TOKEN}"
K rollout restart deploy/"${FULL}-go-api" >/dev/null
K rollout status deploy/"${FULL}-go-api" --timeout=300s >/dev/null
pass "S1 admin user and organization created by the admin verbs; go-api holds a minted session issue credential"

# ---- S2 the SAML provider row: one INSERT, as the integration tests seed it (internal/api/sso seedProvider) -------------------------------------
pf_all
idp_get "https://${IDP_HOST}/saml/cert"
SAML_CERT="$(cat "${WORK}/idp.body")"
grep -q 'BEGIN CERTIFICATE' <<<"${SAML_CERT}" || die "S2: the stub served no SAML certificate"
SAML_CONFIG_JSON="$(jq -nc --arg ip "${IDP_HOST}" --arg cert "${SAML_CERT}" --arg sp "${SP_ENTITY}" '{entity_id: ("https://" + $ip + "/saml/metadata"), sso_url: ("https://" + $ip + "/saml/sso"),
  certificate: $cert, sp_entity_id: $sp, sp_acs_url: "https://sp.fb.test/api/v1/auth/saml/{provider_id}/acs"}')"
SAML_ID="$(pgq <<EOF
WITH p AS (SELECT gen_random_uuid() AS id)
INSERT INTO sso_providers (id, org_id, name, protocol, status, is_default, allow_idp_initiated, auto_provision_users, default_role, config, encrypted_secrets, allowed_domains, created_at, updated_at)
SELECT p.id, o.id, 'fb-saml', 'saml', 'active', false, true, true, 'member',
  replace(\$cfg\$${SAML_CONFIG_JSON}\$cfg\$, '{provider_id}', p.id::text)::json, NULL, '["${DOMAIN}"]'::json, now(), now()
FROM p, organizations o WHERE o.name = 'FB Org'
RETURNING id;
EOF
)"
[[ "${SAML_ID}" =~ ^[0-9a-f-]{36}$ ]] || die "S2: the provider INSERT returned no id: ${SAML_ID}"
pass "S2 a SAML provider row exists and is active (one INSERT in this namespace, auto-provision on, allowed_domains ${DOMAIN})"

# ---- sign-ins ---------------------------------------------------------------------------------------------------------------------------
signin_saml() { # SUB
  local url resp relay status
  : >"${WORK}/jar"
  jfile '{}'
  [ "$(api POST "/api/v1/auth/saml/${SAML_ID}/initiate" "${WORK}/req.json")" = 200 ] || { printf 'initiate-refused'; return 0; }
  url="$(jq -r '.redirect_url' "${BODY}")"
  idp_get "${url}&user=$1"
  resp="$(sed -n 's/.*name="SAMLResponse" value="\([^"]*\)".*/\1/p' "${WORK}/idp.body" | head -1)"
  relay="$(sed -n 's/.*name="RelayState" value="\([^"]*\)".*/\1/p' "${WORK}/idp.body" | head -1)"
  [ -n "${resp}" ] || { printf 'idp-no-response'; return 0; }
  jfile --arg r "${resp}" --arg s "${relay}" '{SAMLResponse: $r} + (if $s == "" then {} else {RelayState: $s} end)'
  status="$(api POST "/api/v1/auth/saml/${SAML_ID}/acs" "${WORK}/req.json")"
  if [ "$(jq -r '.access_token // empty | length > 0' "${BODY}" 2>/dev/null)" = true ]; then printf '%s token' "${status}"; else printf '%s no-token' "${status}"; fi
}
signin_both() { # LABEL: the sign-in must answer "200 token"
  local s
  pf_all
  s="$(signin_saml saml-sub-1)"
  [ "${s}" = "200 token" ] || die "$1: the SAML sign-in answered '${s}', want '200 token'"
  pf_stop
}
SSO_EMAILS="'saml.one@${DOMAIN}'"
identity_snapshot() { # the identity columns of the SSO user, and the table counts
  pgq <<EOF
SELECT concat_ws('|', id, email, auth_provider, sso_provider_id, auth_provider_id) FROM public.users WHERE email IN (${SSO_EMAILS}) ORDER BY email;
SELECT 'users=' || (SELECT count(*) FROM public.users) || ' memberships=' || (SELECT count(*) FROM public.memberships);
EOF
}
row_hash() { pgq <<<"SELECT md5(coalesce(string_agg(u::text, '|' ORDER BY u.id), '')) FROM public.users u WHERE email IN (${SSO_EMAILS});"; }
auth_counts() { pgq <<<"SELECT 'auth.users=' || (SELECT count(*) FROM auth.users) || ' auth.external_identities=' || (SELECT count(*) FROM auth.external_identities) || ' auth.memberships=' || (SELECT count(*) FROM auth.memberships);"; }

# ---- S3 sign-in at phase 3 creates the user -----------------------------------------------------------------------------------------
signin_both "S3"
SNAP3="$(identity_snapshot)"
[ "$(grep -c '^[0-9a-f-]\{36\}|' <<<"${SNAP3}")" = 1 ] || die "S3: public.users does not hold exactly the SSO user: ${SNAP3}"
if ! grep -q "|saml|${SAML_ID}|" <<<"${SNAP3}"; then die "S3: the SSO user does not carry auth_provider and the provider row id: ${SNAP3}"; fi
pass "S3 go-api wrote the SAML user at phase 3 (auth_provider, sso_provider_id set); ${SNAP3##*$'\n'}"

# ---- S4 U1 and the barrier -------------------------------------------------------------------------------------------------------------------
barrier() { RELEASE="${RELEASE}" NAMESPACE="${NS}" bash "${ROOT}/deploy/scripts/auth-users-owned-barrier.sh" "$1"; }
helm_cluster U1 --wait >/dev/null
K rollout status deploy/"${FULL}-go-api" --timeout=300s >/dev/null
barrier up-go-api || die "S4: the up-go-api barrier did not pass"
pass "S4 U1 rolled (go-api forwards the credential routes and carries AUTH_USERS_OWNED); barrier up-go-api passed"

# ---- S5 backfill and reconcile -----------------------------------------------------------------------------------------------------------------
helm_cluster U2 --wait >/dev/null
K wait --for=condition=complete job -l app.kubernetes.io/component=auth-backfill --timeout=300s >/dev/null || die "S5: the users-backfill Job did not complete"
helm_cluster REC --wait >/dev/null
K wait --for=condition=complete job -l app.kubernetes.io/component=auth-backfill --timeout=300s >/dev/null || die "S5: the users-reconcile Job did not complete"
gaps="$(K logs -l app.kubernetes.io/component=auth-backfill --tail=-1 | grep -E '^(gated_diffs|diff\.(missing_in_auth|extra_in_auth|identity_missing))=' || true)"
[ "$(grep -c . <<<"${gaps}")" -ge 4 ] && ! grep -qvE '=0$' <<<"${gaps}" || die "S5: users-reconcile does not read 0 on every gated line: ${gaps:-no lines}"
AUTH_BEFORE="$(auth_counts)"
pass "S5 users-backfill ran and users-reconcile reads 0 gated differences (${AUTH_BEFORE})"

# ---- S6 U3 --------------------------------------------------------------------------------------------------------------------------------------
barrier up-owned || die "S6: the up-owned barrier did not pass"
helm_cluster U3 --wait >/dev/null
K rollout status deploy/"${FULL}-auth-service" --timeout=300s >/dev/null
K rollout status deploy/"${FULL}-go-api" --timeout=300s >/dev/null
pass "S6 barrier up-owned passed; U3 rolled (auth-service owns users)"

# ---- S7 sign-in under owned ---------------------------------------------------------------------------------------------------------------------
HASH_BEFORE="$(row_hash)"
signin_both "S7"
[ "$(identity_snapshot)" = "${SNAP3}" ] || die "S7: the identity columns or the table counts changed under owned"
[ "$(row_hash)" = "${HASH_BEFORE}" ] || die "S7: a sign-in under owned wrote a public.users row"
pass "S7 the SAML sign-in under owned answered 200 as the same accounts and wrote nothing to public.users"

# ---- S8 flip back ---------------------------------------------------------------------------------------------------------------------------------
up_with_digest() { # LAYER: helm upgrade; a refused narrowing prints a digest in the Job log, which the acknowledgement then names
  local layer_name=$1 digest
  if helm_cluster "${layer_name}" --wait >"${WORK}/helm.out" 2>&1; then return 0; fi
  digest="$(K logs -l app.kubernetes.io/component=auth-migrate --tail=-1 2>/dev/null | grep -oE '(digest=|"digest":")[0-9a-f]{16}' | tail -1 | grep -oE '[0-9a-f]{16}$' || true)"
  [ -n "${digest}" ] || { tail -20 "${WORK}/helm.out" >&2; die "S8: ${layer_name} failed and no narrowing digest is in the auth-migrate log"; }
  ACK="${digest}"
  helm_cluster "${layer_name}" --wait >/dev/null || die "S8: ${layer_name} failed with the acknowledgement"
}
up_with_digest D1
K rollout status deploy/"${FULL}-auth-service" --timeout=300s >/dev/null
barrier down-auth || die "S8: the down-auth barrier did not pass"
up_with_digest D2
K rollout status deploy/"${FULL}-go-api" --timeout=300s >/dev/null
K rollout status deploy/"${FULL}-auth-service" --timeout=300s >/dev/null
pass "S8 D1 rolled, barrier down-auth passed, D2 rolled (phase 3, go-api serves the credential routes and writes again)"

# ---- S9 sign-in through go-api -------------------------------------------------------------------------------------------------------------------
signin_both "S9"
[ "$(identity_snapshot)" = "${SNAP3}" ] || die "S9: after the flip back the identity columns or the table counts differ from phase 3 (a new account or a conflict)"
AUTH_AFTER="$(auth_counts)"
[ "${AUTH_AFTER}" = "${AUTH_BEFORE}" ] || die "S9: the auth schema counts changed over the flip back: ${AUTH_BEFORE} -> ${AUTH_AFTER}"
pass "S9 the SAML sign-in through go-api after the flip back answered 200 as the same accounts; no new account, no identity conflict"

printf 'AUTH FLIPBACK SSO K3S REHEARSAL PASSED\n'
