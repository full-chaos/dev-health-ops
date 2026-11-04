#!/usr/bin/env bash
# PDP load and latency run (POST /v1/authorize and :batch) on bigboy k3s, in the chart form. Not a CI job (no kind): the lead
# runs it in a FRESH namespace and the script deletes that namespace on exit. The ticket line: p95 < 25 ms and p99 < 75 ms.
#
#   ci/pdp_load_k3s.sh --print-plan   no cluster: print the plan and check that the chart renders what the run sets (needs helm)
#   ci/pdp_load_k3s.sh                the legs below
#
# Leg 1 INSTALL   chart install (auth-service phase 2, eddsa, 2 replicas, the chart's own PostgreSQL, Valkey and ClickHouse; the migrate hook needs the ClickHouse), one human
#                 with an admin membership, the api workload row and its bootstrap value (made in the runner pod, never copied out).
# Leg 2 KNEE      RECORDED, not gated: the DEFAULT route limiter (20 rps, burst 40 per route per pod). It shows the 429 knee.
# Leg 3 GATED     the route limiter LIFTED (AUTH_SERVICE_RATE_LIMIT_* = 100000) and the PDP caller and route limiters LIFTED
#                 (authService.pdpRateLimit.* = 100000), this namespace only, never a prod value: every
#                 class alone and the mix, normal then peak, single route and batch-10, at p95 25 ms / p99 75 ms / errors 0.
#                 batch-100 is recorded, not gated. The first breach is reported and the run stops (no threshold change).
# Leg 4 CACHE-ON  only with AUTH_PDP_CACHE_ON=1: revision feed on, the mix recorded.
# The gated SLO is the PDP with the limiter lifted; with the default limiter each PDP route is capped at about 20 rps per pod.
# Cache OFF is the gated case (the worse one); cache-on is reported.
#
# Every run mints a fresh workload token in the pod (a token lives at most 300 s) and ends inside RUN_CAP_SECONDS.
# Images are registry DIGEST references (repo@sha256:<64 hex>) from the CI Inspect image step of the PR head:
#   AUTH_PDP_IMAGE_SERVICE  AUTH_PDP_IMAGE_MIGRATE  AUTH_PDP_IMAGE_DHO
# AUTH_PDP_LOADTEST_BIN: a static linux loadtest for the node architecture (CGO_ENABLED=0 GOOS=linux GOARCH=<node arch> go build -o <file> ./cmd/loadtest). AUTH_PDP_OUT_DIR: a new
# directory for the reports and the summary. Optional: AUTH_PDP_KUBECONFIG (default /etc/rancher/k3s/k3s.yaml).
# Secrets travel on stdin or in files in the cluster only. Nothing here touches another namespace or prod.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR ROOT
readonly RUNNER_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly DIGEST_RE='^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$'
readonly RELEASE="pdp" FULL="pdp-dev-health" SVC_PORT=8095
readonly SLO_P95="25ms" SLO_P99="75ms" SLO_ERR="0"
readonly RUN_CAP_SECONDS=240
readonly KNEE_RATE=100 KNEE_SECONDS=30

die() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}
pass() { printf 'PASS: %s\n' "$1"; }

PRINT_PLAN=0
for arg in "$@"; do
  case "${arg}" in
    --print-plan) PRINT_PLAN=1 ;;
    *) die "usage: ci/pdp_load_k3s.sh [--print-plan]" ;;
  esac
done

if [ "${PRINT_PLAN}" = 1 ]; then
  zeros="$(printf '0%.0s' $(seq 1 64))"
  IMG_SVC="registry.invalid/dev-health-go-auth-service@sha256:${zeros}"
  IMG_MIG="registry.invalid/dev-health-go-auth-migrate@sha256:${zeros}"
  IMG_DHO="registry.invalid/dev-health-go-dho@sha256:${zeros}"
else
  IMG_SVC="${AUTH_PDP_IMAGE_SERVICE:-}"
  IMG_MIG="${AUTH_PDP_IMAGE_MIGRATE:-}"
  IMG_DHO="${AUTH_PDP_IMAGE_DHO:-}"
fi
for image in "${IMG_SVC}" "${IMG_MIG}" "${IMG_DHO}"; do
  [[ "${image}" =~ ${DIGEST_RE} ]] || die "an image is not a registry digest reference (repo@sha256:<64 hex>): ${image:-<unset>}"
done
for tool in helm jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

NS="pdpload-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
readonly NS
KUBECONFIG_FILE="${AUTH_PDP_KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
SVC_HOST="${FULL}-auth-service"
readonly SVC_HOST
SECRETS_VALUES=""

# helm_args EXTRA_VALUES_FILE...  -> HELM_ARGS (the release, the chart, the images)
helm_args() {
  HELM_ARGS=(
    "${RELEASE}" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}"
    -f "${SCRIPT_DIR}/pdp_load_k3s/release.yaml" -f "${SECRETS_VALUES}"
    --set "authService.image.repository=${IMG_SVC}" --set authService.image.pullPolicy=IfNotPresent
    --set "authService.migrate.image.repository=${IMG_MIG}" --set authService.migrate.image.pullPolicy=IfNotPresent
    --set "migrations.hook.image=${IMG_DHO}" --set migrations.hook.pullPolicy=IfNotPresent
    --set "migrations.hook.provisionRoles.image=${IMG_DHO}" --set migrations.hook.provisionRoles.pullPolicy=IfNotPresent
    --set "migrations.hook.riverMigrate.image=${IMG_DHO}" --set migrations.hook.riverMigrate.pullPolicy=IfNotPresent
  )
  local f
  for f in "$@"; do HELM_ARGS+=(-f "${f}"); done
}

# write_extra FILE SUBJECT LIFT: the per-leg auth-service env. SUBJECT empty: the bootstrap exchange only (the first install).
write_extra() {
  {
    printf 'authService:\n  extraEnv:\n    - {name: AUTH_WORKLOAD_BOOTSTRAP, value: "true"}\n'
    if [ -n "$2" ]; then
      printf '    - {name: AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT, value: "%s"}\n' "$2"
      printf '    - {name: AUTH_AUTHORIZE_CALLER_SUBJECTS, value: "%s"}\n' "$2"
    fi
    if [ "$3" = lift ]; then
      printf '    - {name: AUTH_SERVICE_RATE_LIMIT_PER_SECOND, value: "100000"}\n'
      printf '    - {name: AUTH_SERVICE_RATE_LIMIT_BURST, value: "100000"}\n'
      printf '  pdpRateLimit: {callerPerSecond: 100000, callerBurst: 100000, routePerSecond: 100000, routeBurst: 100000}\n'
    fi
  } >"$1"
}

write_secrets_values() { # FILE: the chart's own Secret data (go-api is off: placeholders the render needs)
  printf 'secrets:\n  data:\n    API_DATABASE_URI: "postgres://devhealth_api:%s@%s-postgresql:5432/devhealth?sslmode=disable"\n    JWT_SECRET_KEY: "%s"\n' "$1" "${FULL}" "$2" >"$3"
}

# pdp_limits RENDERED lifted|default: the four PDP limiter variables of the auth-service render are all 100000 (lifted) or
# none is (default).
pdp_limits() {
  local name value
  for name in AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_PER_SECOND AUTH_SERVICE_PDP_CALLER_RATE_LIMIT_BURST \
    AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_PER_SECOND AUTH_SERVICE_PDP_ROUTE_RATE_LIMIT_BURST; do
    value="$(grep -A1 -E "name: ${name}\$" <<<"$1" | sed -n 's/^ *value: "\(.*\)"$/\1/p')"
    [ -n "${value}" ] || return 1
    case "$2" in
      lifted) [ "${value}" = 100000 ] || return 1 ;;
      *) [ "${value}" != 100000 ] || return 1 ;;
    esac
  done
}

print_plan() {
  local work rendered subject="prn_AAAAAAAAAAAAAAAAAAAAAA"
  work="$(mktemp -d)"
  SECRETS_VALUES="${work}/secrets.yaml"
  write_secrets_values plan-placeholder plan-placeholder-plan-placeholder-plan "${SECRETS_VALUES}"
  printf 'plan: namespace %s on %s, release %s, runner image %s\n' "${NS}" "${KUBECONFIG_FILE}" "${RELEASE}" "${RUNNER_IMAGE##*/}"
  printf 'plan: leg 1 INSTALL  chart phase 2, eddsa, 2 replicas, PostgreSQL, Valkey and ClickHouse (migrate step) of the chart, one admin human, the api workload\n'
  printf 'plan: leg 2 KNEE     recorded, not gated: the default route limiter (20 rps, burst 40 per route per pod) at %s rps for %s s\n' "${KNEE_RATE}" "${KNEE_SECONDS}"
  printf 'plan: leg 3 GATED    route limiter lifted (AUTH_SERVICE_RATE_LIMIT_PER_SECOND and _BURST = 100000) and PDP caller and route limiters lifted (authService.pdpRateLimit.* = 100000), this namespace only; cache OFF\n'
  printf 'plan: gated SLO      p95=%s,p99=%s,err=%s per class and for the mix, normal then peak; batch-10 gated, batch-100 recorded\n' "${SLO_P95}" "${SLO_P99}" "${SLO_ERR}"
  printf 'plan: leg 4 CACHE-ON only with AUTH_PDP_CACHE_ON=1 (revision feed on): the mix recorded, not gated\n'
  printf 'plan: every run mints a fresh workload token and is capped at %s s (a workload token lives 300 s)\n' "${RUN_CAP_SECONDS}"
  printf 'plan: the report says: gated SLO = PDP with the limiter lifted; the default limiter caps each PDP route at about 20 rps per pod\n'
  printf 'plan: the namespace is deleted on exit\n'
  write_extra "${work}/extra-default.yaml" "${subject}" keep
  write_extra "${work}/extra-lift.yaml" "${subject}" lift
  helm_args "${work}/extra-default.yaml"
  rendered="$(helm template "${HELM_ARGS[@]}" --show-only templates/auth-service.yaml)"
  grep -q 'name: AUTH_AUTHORIZE_CALLER_SUBJECTS' <<<"${rendered}" || die "the chart renders no AUTH_AUTHORIZE_CALLER_SUBJECTS"
  grep -q 'name: AUTH_ACCESS_TOKEN_CLASS' <<<"${rendered}" || die "the chart renders no AUTH_ACCESS_TOKEN_CLASS"
  grep -q 'name: AUTH_WORKLOAD_BOOTSTRAP' <<<"${rendered}" || die "the chart renders no AUTH_WORKLOAD_BOOTSTRAP"
  if grep -q 'AUTH_SERVICE_RATE_LIMIT' <<<"${rendered}"; then die "the knee form renders a route limiter override"; fi
  pass "plan: the knee form renders the caller subject and the eddsa class with the default route limiter"
  pdp_limits "${rendered}" default || die "the knee form renders a lifted PDP limiter variable"
  pass "plan: the knee form renders the default PDP caller and route limiters"
  helm_args "${work}/extra-lift.yaml"
  rendered="$(helm template "${HELM_ARGS[@]}" --show-only templates/auth-service.yaml)"
  [ "$(grep -c 'AUTH_SERVICE_RATE_LIMIT_' <<<"${rendered}")" = 2 ] || die "the gated form does not render both route limiter variables"
  pass "plan: the gated form renders the lifted route limiter (per second and burst)"
  pdp_limits "${rendered}" lifted || die "the gated form does not render all four PDP limiter variables at 100000"
  pass "plan: the gated form renders the lifted PDP caller and route limiters (per second and burst)"
  rendered="$(helm template "${HELM_ARGS[@]}" --show-only templates/migrate-job.yaml)"
  grep -q 'CLICKHOUSE_URI:' <<<"${rendered}" || die "the chart renders no CLICKHOUSE_URI for the migrate hook (dho migrate upgrade needs it)"
  pass "plan: the migrate hook Secret carries CLICKHOUSE_URI (the ClickHouse step of dho migrate upgrade)"
  rm -rf "${work}"
}

if [ "${PRINT_PLAN}" = 1 ]; then
  print_plan
  exit 0
fi

# ---------------------------------------------------------------------------------------------------------------------
case "${KUBECONFIG_FILE}" in *k3s*) ;; *) die "AUTH_PDP_KUBECONFIG must name a k3s kubeconfig (no kind, no prod)" ;; esac
for tool in kubectl openssl od sha256sum envsubst; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done
[ -r "${KUBECONFIG_FILE}" ] || die "the kubeconfig ${KUBECONFIG_FILE} is not readable"
LOADTEST_BIN="${AUTH_PDP_LOADTEST_BIN:-}"
[ -n "${LOADTEST_BIN}" ] && [ -x "${LOADTEST_BIN}" ] || die "AUTH_PDP_LOADTEST_BIN must name an executable loadtest binary"
OUT_DIR="${AUTH_PDP_OUT_DIR:-}"
[ -n "${OUT_DIR}" ] || die "AUTH_PDP_OUT_DIR must name a new directory for the reports"
[ ! -e "${OUT_DIR}" ] || die "AUTH_PDP_OUT_DIR ${OUT_DIR} exists: a run writes a new directory"
nodes="$(kubectl --kubeconfig "${KUBECONFIG_FILE}" get nodes -o jsonpath='{.items[*].status.nodeInfo.kubeletVersion}' 2>/dev/null || true)"
case "${nodes}" in *+k3s*) ;; *) die "the cluster behind ${KUBECONFIG_FILE} is not a k3s cluster (kubelet versions: ${nodes:-none}); no kind, no prod" ;; esac
case "${NS}" in dev-health | default | kube-*) die "refusing the namespace ${NS}" ;; esac

export KUBECONFIG="${KUBECONFIG_FILE}"
kc() { kubectl --kubeconfig "${KUBECONFIG_FILE}" "$@"; }
K() { kc -n "${NS}" "$@"; }

WORK="$(mktemp -d)"
readonly WORK
RUN_TOKEN="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
owned() { [ "$(kc get namespace "${NS}" -o 'jsonpath={.metadata.labels.pdpload-run}' 2>/dev/null)" = "${RUN_TOKEN}" ]; }
# Logs (all containers, the previous run too) of every pod that is not Succeeded or not Ready, and a describe of every failed Job.
# Never a describe of a Secret or a Pod, never an env dump: the dho and auth logs redact their own DSNs.
dump_failure() {
  local name
  printf '\n--- namespace state at failure ---\n' >&2
  K get pods -o wide >&2 || true
  K get events --sort-by=.lastTimestamp >&2 || true
  while read -r name; do
    [ -n "${name}" ] || continue
    printf '\n--- describe job %s ---\n' "${name}" >&2
    K describe job "${name}" >&2 || true
  done < <(K get jobs -o json 2>/dev/null | jq -r '.items[] | select((.status.failed // 0) > 0) | .metadata.name' || true)
  while read -r name; do
    [ -n "${name}" ] || continue
    printf '\n--- logs pod %s (all containers) ---\n' "${name}" >&2
    K logs "${name}" --all-containers --tail 200 >&2 || true
    printf '\n--- logs pod %s (all containers, previous) ---\n' "${name}" >&2
    K logs "${name}" --all-containers --previous --tail 200 >&2 || true
  done < <(K get pods -o json 2>/dev/null | jq -r '.items[] | select(.status.phase != "Succeeded") | select(.status.phase != "Running" or ([.status.containerStatuses[]?.ready] | all | not)) | .metadata.name' || true)
}
cleanup() {
  local status=$?
  if owned; then
    if [ "${status}" -ne 0 ]; then dump_failure; fi
    kc delete namespace "${NS}" --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

if kc get namespace "${NS}" >/dev/null 2>&1; then die "the namespace ${NS} exists"; fi
kc create -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NS}
  labels:
    pdpload-run: ${RUN_TOKEN}
YAML
printf 'namespace %s created on %s\n' "${NS}" "${KUBECONFIG_FILE}"
mkdir -p "${OUT_DIR}"
SUMMARY="${OUT_DIR}/summary.tsv"
printf 'leg\tprofile\tload\treport\tsha256\tslo\n' >"${SUMMARY}"
printf 'loadtest binary sha256 %s\n' "$(sha256sum "${LOADTEST_BIN}" | cut -d' ' -f1)" | tee -a "${OUT_DIR}/notes.txt"
printf 'images: service %s migrate %s dho %s\n' "${IMG_SVC}" "${IMG_MIG}" "${IMG_DHO}" >>"${OUT_DIR}/notes.txt"
printf 'nodes: %s\n' "$(K get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.status.nodeInfo.kubeletVersion} {.status.capacity.cpu}cpu{"\n"}{end}' 2>/dev/null || true)" >>"${OUT_DIR}/notes.txt"

# ---------------------------------------------------------------------------------------------------------------------
printf '=== leg 1: INSTALL ===\n'
umask 077
DB_PASSWORD="devhealth"
API_PASSWORD="$(openssl rand -hex 12)"
AUTH_PASSWORD="$(openssl rand -hex 12)"
SECRETS_VALUES="${WORK}/secrets.yaml"
write_secrets_values "${API_PASSWORD}" "$(openssl rand -hex 24)" "${SECRETS_VALUES}"
write_extra "${WORK}/extra-first.yaml" "" keep
helm_args "${WORK}/extra-first.yaml"
helm install --kubeconfig "${KUBECONFIG_FILE}" "${HELM_ARGS[@]}" --no-hooks >/dev/null
K rollout status "statefulset/${FULL}-postgresql" --timeout=240s >/dev/null
K rollout status "statefulset/${FULL}-clickhouse" --timeout=240s >/dev/null
K rollout status "deploy/${FULL}-valkey" --timeout=240s >/dev/null
PSU() { K exec -i "${FULL}-postgresql-0" -- psql -U devhealth -d devhealth -At -v ON_ERROR_STOP=1 "$@"; }
printf "CREATE ROLE devhealth_api LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\nCREATE ROLE devhealth_query_api NOLOGIN;\nCREATE ROLE devhealth_auth LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" \
  "${API_PASSWORD}" "${AUTH_PASSWORD}" | PSU -f - >/dev/null
printf 'host=%s-postgresql port=5432 user=devhealth_auth password=%s dbname=devhealth sslmode=disable' "${FULL}" "'${AUTH_PASSWORD}'" |
  K create secret generic auth-db --from-file=AUTH_DATABASE_URI=/dev/stdin >/dev/null
printf 'host=%s-postgresql port=5432 user=devhealth password=%s dbname=devhealth sslmode=disable' "${FULL}" "${DB_PASSWORD}" |
  K create secret generic auth-migrate-db --from-file=AUTH_MIGRATION_DATABASE_URI=/dev/stdin >/dev/null
openssl genpkey -algorithm ed25519 | K create secret generic auth-key --from-file=signing-key.pem=/dev/stdin >/dev/null
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' | K create secret generic auth-session --from-file=AUTH_SESSION_SUCCESSOR_KEY=/dev/stdin >/dev/null
printf 'redis://%s-valkey:6379/1' "${FULL}" | K create secret generic auth-ratelimit --from-file=AUTH_VALKEY_URI=/dev/stdin >/dev/null
pass "leg 1: the databases are up and the roles and Secrets exist"

helm upgrade --kubeconfig "${KUBECONFIG_FILE}" "${HELM_ARGS[@]}" --history-max 0 --timeout 12m >/dev/null
K run auth-fence-init --restart=Never --image="${IMG_MIG}" --override-type=strategic --overrides="$(jq -nc --arg image "${IMG_MIG}" '
  {spec: {containers: [{name: "auth-fence-init", image: $image, args: ["restore-fence", "init"],
    env: [{name: "AUTH_MIGRATION_DATABASE_URI", valueFrom: {secretKeyRef: {name: "auth-migrate-db", key: "AUTH_MIGRATION_DATABASE_URI"}}},
          {name: "AUTH_RUNTIME_DATABASE_ROLE", value: "devhealth_auth"}]}]}}')" >/dev/null
K wait --for=jsonpath='{.status.phase}'=Succeeded pod/auth-fence-init --timeout=240s >/dev/null || { K logs auth-fence-init >&2 || true; die "restore-fence init did not succeed"; }
K delete pod auth-fence-init --wait=true >/dev/null
K rollout status "deploy/${FULL}-auth-service" --timeout=480s >/dev/null
pass "leg 1: chart installed, migrated, auth-service Ready"

USER_ID="$(cat /proc/sys/kernel/random/uuid)"
ORG_ID="$(cat /proc/sys/kernel/random/uuid)"
PSU -v "user_id=${USER_ID}" -v "org_id=${ORG_ID}" -f - <"${SCRIPT_DIR}/pdp_load_k3s/seed.sql" >/dev/null
pass "leg 1: one human with an admin membership"

# The runner pod (ci/pdp_load_k3s/runner-pod.tpl): its init containers seed the workload registry and write the bootstrap value into the
# pod's own volume; the value never leaves the pod.
# shellcheck disable=SC2016
env IMG_MIG="${IMG_MIG}" RUNNER_IMAGE="${RUNNER_IMAGE}" envsubst '${IMG_MIG} ${RUNNER_IMAGE}' <"${SCRIPT_DIR}/pdp_load_k3s/runner-pod.tpl" | K apply -f - >/dev/null
K wait --for=condition=Ready pod/pdp-runner --timeout=300s >/dev/null || { K describe pod pdp-runner >&2 || true; die "the runner pod did not become Ready"; }
SUBJECT="$(PSU -f - <"${SCRIPT_DIR}/pdp_load_k3s/subject.sql")"
[[ "${SUBJECT}" =~ ^prn_[A-Za-z0-9_-]{22}$ ]] || die "the api workload principal id is not a wire principal id"
pass "leg 1: the api workload and its bootstrap value exist (subject ${SUBJECT})"
K exec pdp-runner -- mkdir -p /work/profiles >/dev/null
K cp "${LOADTEST_BIN}" "${NS}/pdp-runner:/work/loadtest" >/dev/null
K cp "${SCRIPT_DIR}/pdp_load_k3s/pod.sh" "${NS}/pdp-runner:/work/pod.sh" >/dev/null
K cp "${ROOT}/internal/loadharness/testdata/pdp/." "${NS}/pdp-runner:/work/profiles" >/dev/null
K exec pdp-runner -- chmod 0700 /work/loadtest /work/pod.sh

pod() { K exec pdp-runner -- env "PDP_RUN_CAP=${RUN_CAP_SECONDS}" "PDP_HOST=${SVC_HOST}" "PDP_PORT=${SVC_PORT}" "PDP_PRINCIPAL=${USER_ID}" "PDP_ORG=${ORG_ID}" sh /work/pod.sh "$@"; }

set_env() { # lift|keep: the caller subject always, the route limiter lifted or default
  write_extra "${WORK}/extra-$1.yaml" "${SUBJECT}" "$1"
  helm_args "${WORK}/extra-$1.yaml"
  helm upgrade --kubeconfig "${KUBECONFIG_FILE}" "${HELM_ARGS[@]}" --history-max 0 --wait --timeout 8m "${@:2}" >/dev/null
  K rollout status "deploy/${FULL}-auth-service" --timeout=480s >/dev/null
}

SLO_FAILED=0
# run_one LEG PROFILE LOAD NAME... : mint, run loadtest, copy the report out, write its sha256 into the summary. SLO names gate.
# Callers use it in && chains and || lists, where bash ignores set -e inside it: every step is checked here, and a failed step is
# named on stderr, marks the row FAILED(step), sets SLO_FAILED and returns 1.
run_one() {
  local leg=$1 profile=$2 load=$3 report out slo=() args=() name rc=0 sha="" step=""
  shift 3
  report="${leg}-${profile}-${load//:/-}.json"
  for name in "$@"; do slo+=(--slo "${name}:p95=${SLO_P95},p99=${SLO_P99},err=${SLO_ERR}"); done
  args=(--config "/work/profiles/${profile}.json" --profile "${load%%:*}" --base-url "http://${SVC_HOST}:${SVC_PORT}" --allow-host "${SVC_HOST}" --out "/work/${report}")
  case "${load}" in *:*) args+=(--rate "${load#*:}" --duration "${KNEE_SECONDS}s") ;; esac
  pod mint >/dev/null || step="mint"
  if [ -z "${step}" ]; then
    pod run "${args[@]}" "${slo[@]}" || rc=$?
    K cp "${NS}/pdp-runner:/work/${report}" "${OUT_DIR}/${report}" >/dev/null || step="copy"
  fi
  if [ -z "${step}" ]; then
    sha="$(sha256sum "${OUT_DIR}/${report}" | cut -d' ' -f1)" || step="hash"
    [ -n "${sha}" ] || step="hash"
  fi
  out="recorded"
  [ "${#slo[@]}" -eq 0 ] || out="gated"
  [ "${rc}" -eq 0 ] || out="${out}-FAILED(exit ${rc})"
  [ -z "${step}" ] || out="${out}-FAILED(${step})"
  if ! printf '%s\t%s\t%s\t%s\t%s\t%s\n' "${leg}" "${profile}" "${load}" "${report}" "${sha}" "${out}" >>"${SUMMARY}"; then
    printf 'FAIL: %s %s %s: step summary failed\n' "${leg}" "${profile}" "${load}" >&2
    SLO_FAILED=1
    return 1
  fi
  printf '%s %s %s: %s, report sha256 %s\n' "${leg}" "${profile}" "${load}" "${out}" "${sha}"
  if [ -n "${step}" ]; then
    printf 'FAIL: %s %s %s: step %s failed\n' "${leg}" "${profile}" "${load}" "${step}" >&2
    SLO_FAILED=1
    return 1
  fi
  [ "${rc}" -eq 0 ] || { SLO_FAILED=1; return 1; }
}

# ---------------------------------------------------------------------------------------------------------------------
printf '=== leg 2: KNEE (default route limiter, recorded) ===\n'
set_env keep
pod mint >/dev/null
pod preflight || die "leg 2: the pre-flight decisions are not what their names say"
run_one knee authorize-knee "peak:${KNEE_RATE}" || die "leg 2: the knee run failed an assertion (a 5xx or a status outside 200 and 429)"

printf '=== leg 3: GATED (route limiter lifted, cache off) ===\n'
set_env lift
pod mint >/dev/null
pod preflight || die "leg 3: the pre-flight decisions are not what their names say"
for load in normal peak; do
  if ! {
    run_one gated authorize-low "${load}" authorize-low &&
      run_one gated authorize-low-hot "${load}" authorize-low-hot &&
      run_one gated authorize-high "${load}" authorize-high &&
      run_one gated authorize-deny "${load}" authorize-deny &&
      run_one gated authorize-mix "${load}" authorize-low authorize-low-hot authorize-high authorize-deny &&
      run_one gated authorize-batch-10 "${load}" authorize-batch-10 &&
      run_one recorded authorize-batch-100 "${load}"
  }; then
    printf 'REPORT: a gated objective or assertion failed at %s load; the run stops here, nothing is loosened\n' "${load}"
    break
  fi
done

if [ "${AUTH_PDP_CACHE_ON:-0}" = 1 ] && [ "${SLO_FAILED}" = 0 ]; then
  printf '=== leg 4: CACHE-ON (revision feed on, recorded) ===\n'
  set_env lift --set authService.revisionFeed.enabled=true
  sleep 20
  pod mint >/dev/null
  for load in normal peak; do run_one cacheon authorize-mix "${load}" || break; done
fi

printf 'summary %s sha256 %s\n' "${SUMMARY}" "$(sha256sum "${SUMMARY}" | cut -d' ' -f1)"
[ "${SLO_FAILED}" = 0 ] || die "a gated objective or assertion failed: see ${SUMMARY}"
printf 'PDP LOAD K3S RUN PASSED\n'
