#!/usr/bin/env bash
# Rollback rehearsal for the sessions flag (goApi.authProxy.sessions), in the chart's real form.
#
# Installs the dev-health chart (go-api, auth-service, bundled PostgreSQL/ClickHouse/Valkey and the
# real migration hooks) into a kind cluster from images built from this tree, then rehearses, only
# inside that cluster:
#
#   baseline   sessions=false, session readers set (the deploy repo's prod values shape)
#   prep       the successor-key Secret and a stand-in issuer token, as the flip steps create them
#   guards     sessions=true is refused while the readers or the successor key are unset
#   FLIP       one `helm upgrade` with sessions=true: the river hook revokes the api role's INSERT and
#              UPDATE on refresh_tokens, then go-api rolls
#   ROLLBACK_UP  `helm upgrade` with sessions=false: the hook re-GRANTs, then go-api rolls
#   REFLIP     the flip again (repeatable)
#   ROLLBACK_HELM  `helm rollback` to the baseline revision: does the grant come back? (measured)
#
# Probes run through every phase: a read probe on go-api's public Service (every 0.25 s), a grant probe
# on refresh_tokens (every second), and a pod sampler (every second). Each phase reports the old-pod
# window: from the REVOKE (or re-GRANT) commit to the last old pod gone, with the read failures and the
# seconds with no Ready go-api pod. A CONTROL upgrade (a pod annotation, no flag change) runs first so the
# read-drop shape of a plain pod handover is on record beside the flip. Read drops are a named class (F-A):
# per upgrade status 000 only, at most 8 probes, inside one window of 2 s at the pod handover; anything else
# (a 5xx, another answer, more probes, outside the handover) is a FINDING, and REHEARSAL_STRICT=1 fails the
# job on a FINDING. `helm rollback` not re-granting is a KNOWN finding (F-B), recorded, not failed. A broken
# mechanism (an end state not Ready, a grant not applied) always fails.
#
# CI only (needs docker, kind, kubectl, helm). Test Secrets are made in the cluster by this script and
# die with it. The two-upgrade form of the flip (migrate first, flag second) is NOT run: the river hook
# and go-api both read goApi.authProxy.sessions, so the chart cannot express it; the script says so.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"

readonly KIND_VERSION="v0.33.0"
readonly KIND_SHA256_AMD64="aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d"
readonly HELM_VERSION="v3.21.4"
readonly HELM_SHA256_AMD64="61f88ab166748cb19604d7884cb100ae9ccb13804ddeb98e08af167eacbb6a14"
readonly KUBECTL_VERSION="v1.37.0"
readonly KUBECTL_SHA256_AMD64="6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f"
readonly NODE_IMAGE="kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
LOCAL_PG="${PG_IMAGE}"
export LOCAL_PG
readonly NS="rehearsal"
readonly CLUSTER="rehearsal-$$"
readonly RELEASE="ap"
readonly FULL="ap-dev-health"
readonly STRICT="${REHEARSAL_STRICT:-0}"
# REHEARSAL_ZERO_DROPS=1: CONTROL and FLIP must show no dropped read at all (the go-api handover drain bar).
readonly ZERO_DROPS="${REHEARSAL_ZERO_DROPS:-0}"
# REHEARSAL_PRESTOP: goApi.drain.preStopSeconds for the whole run; unset keeps the chart default, 0 is the drain-off contrast.
readonly PRESTOP="${REHEARSAL_PRESTOP:-}"
WORK="$(mktemp -d)"
readonly WORK
SAMPLER_PID=""
FINDINGS=0

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

for tool in docker openssl curl sha256sum envsubst jq tar awk; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

K() { kubectl --context "kind-${CLUSTER}" -n "${NS}" "$@"; }
K_RENDER() {
  # shellcheck disable=SC2016
  envsubst '${LOCAL_PG}' <"$1"
}

cleanup() {
  local status=$?
  if [ -n "${SAMPLER_PID}" ]; then kill "${SAMPLER_PID}" 2>/dev/null || true; fi
  if [ "${status}" -ne 0 ]; then
    printf '\n--- cluster state at failure ---\n' >&2
    K get pods,jobs,statefulsets -o wide >&2 || true
    K describe pods >&2 || true
    for pod in $(K get pods -o name 2>/dev/null | grep -E 'migrate|provision|grants|go-api|auth-service' || true); do
      printf '\n--- logs %s ---\n' "${pod}" >&2
      K logs "${pod}" --all-containers --tail 80 >&2 || true
    done
    helm --kube-context "kind-${CLUSTER}" -n "${NS}" history "${RELEASE}" >&2 || true
  fi
  "${WORK}/kind" delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  docker rm --force --volumes "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  rm -rf "${WORK}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

# --- tooling: pinned by checksum --------------------------------------------------------------
printf 'step 0: kind %s, helm %s, kubectl %s (sha256 pinned)\n' "${KIND_VERSION}" "${HELM_VERSION}" "${KUBECTL_VERSION}"
curl --fail --silent --show-error --location --output "${WORK}/kind" \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
printf '%s  %s\n' "${KIND_SHA256_AMD64}" "${WORK}/kind" | sha256sum --check --status || die "kind does not match its pinned sha256"
chmod +x "${WORK}/kind"
mkdir -p "${WORK}/bin"
curl --fail --silent --show-error --location --output "${WORK}/helm.tgz" "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz"
printf '%s  %s\n' "${HELM_SHA256_AMD64}" "${WORK}/helm.tgz" | sha256sum --check --status || die "helm does not match its pinned sha256"
tar -xzf "${WORK}/helm.tgz" -C "${WORK}" linux-amd64/helm
mv "${WORK}/linux-amd64/helm" "${WORK}/bin/helm"
curl --fail --silent --show-error --location --output "${WORK}/bin/kubectl" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
printf '%s  %s\n' "${KUBECTL_SHA256_AMD64}" "${WORK}/bin/kubectl" | sha256sum --check --status || die "kubectl does not match its pinned sha256"
chmod +x "${WORK}/bin/helm" "${WORK}/bin/kubectl"
export PATH="${WORK}/bin:${PATH}"

# --- images and cluster --------------------------------------------------------------------------
printf 'step 1: build dho, auth-service and auth-migrate from this tree; create the cluster\n'
build() {
  BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker build --file "${ROOT}/docker/go-worker.Dockerfile" --target "$1" \
    --build-arg VERSION=rehearsal --build-arg COMMIT=0000000000000000000000000000000000000000 \
    --build-arg BUILD_TIME=1970-01-01T00:00:00Z --build-arg SOURCE_DATE_EPOCH=0 \
    --tag "$2" "${ROOT}"
}
build dho rehearsal/dho:local
build auth-service rehearsal/auth-service:local
build auth-migrate rehearsal/auth-migrate:local
"${WORK}/kind" create cluster --name "${CLUSTER}" --image "${NODE_IMAGE}" --wait 180s
for image in rehearsal/dho:local rehearsal/auth-service:local rehearsal/auth-migrate:local; do
  "${WORK}/kind" load docker-image "${image}" --name "${CLUSTER}"
done
kubectl --context "kind-${CLUSTER}" create namespace "${NS}"
KUBE_SERVER="$(kubectl --context "kind-${CLUSTER}" version -o json | jq -r .serverVersion.gitVersion)"
printf 'kind node kube version: %s (the native preStop sleep needs >= 1.30)\n' "${KUBE_SERVER}"
KUBE_MINOR="$(sed -E 's/^v[0-9]+\.([0-9]+).*/\1/' <<<"${KUBE_SERVER}")"
[ "${KUBE_MINOR}" -ge 30 ] || die "kind node ${KUBE_SERVER} is older than 1.30: the drain hook would be silently dropped"

# --- credentials: test values, mode 0600 files, never argv ----------------------------------------
API_PASSWORD="$(openssl rand -hex 12)"
AUTH_PASSWORD="$(openssl rand -hex 12)"
JWT_KEY="$(openssl rand -hex 24)"
ISSUE_TOKEN="svc_authsess_$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=\n' | cut -c1-43)"
umask 077
printf 'secrets:\n  data:\n    API_DATABASE_URI: "postgres://devhealth_api:%s@%s-postgresql:5432/devhealth?sslmode=disable"\n    JWT_SECRET_KEY: "%s"\n' \
  "${API_PASSWORD}" "${FULL}" "${JWT_KEY}" >"${WORK}/secrets-base.yaml"
printf 'secrets:\n  data:\n    AUTH_SESSION_ISSUE_TOKEN: "%s"\n' "${ISSUE_TOKEN}" >"${WORK}/secrets-session.yaml"
printf 'authService:\n  migrate:\n    sessionReaders:\n      queryApi: devhealth_query\n  sessionKeys:\n    successor:\n      secretName: auth-session\n' >"${WORK}/successor.yaml"

CHART="${ROOT}/deploy/helm/dev-health"
HELM_BASE=("${RELEASE}" "${CHART}" --kube-context "kind-${CLUSTER}" --namespace "${NS}"
  -f "${SCRIPT_DIR}/session_rehearsal/rehearsal-release.yaml" -f "${WORK}/secrets-base.yaml")
if [ -n "${PRESTOP}" ]; then
  case "${PRESTOP}" in *[!0-9]*) die "REHEARSAL_PRESTOP must be whole seconds, got ${PRESTOP}" ;; esac
  HELM_BASE+=(--set "goApi.drain.preStopSeconds=${PRESTOP}")
fi
HELM_SESSION=(-f "${WORK}/secrets-session.yaml" -f "${WORK}/successor.yaml")

# --- rev 1: objects only, no hooks, so PostgreSQL exists before the hooks need it -------------------
printf 'step 2: helm install --no-hooks (the databases), then the roles and Secrets the chart only references\n'
helm install "${HELM_BASE[@]}" --no-hooks
K rollout status statefulset/"${FULL}-postgresql" --timeout=240s
K rollout status statefulset/"${FULL}-clickhouse" --timeout=240s
K rollout status deploy/"${FULL}-valkey" --timeout=240s

PSU() { K exec -i "${FULL}-postgresql-0" -- psql -U devhealth -d devhealth -At -v ON_ERROR_STOP=1 "$@"; }
printf "CREATE ROLE devhealth_api LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\nCREATE ROLE devhealth_query NOLOGIN;\nCREATE ROLE devhealth_auth LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" \
  "${API_PASSWORD}" "${AUTH_PASSWORD}" | PSU -f -
printf 'host=%s-postgresql port=5432 user=devhealth_auth password=%s dbname=devhealth sslmode=disable' "${FULL}" "'${AUTH_PASSWORD}'" \
  | K create secret generic auth-db --from-file=AUTH_DATABASE_URI=/dev/stdin
printf 'host=%s-postgresql port=5432 user=devhealth password=devhealth dbname=devhealth sslmode=disable' "${FULL}" \
  | K create secret generic auth-migrate-db --from-file=AUTH_MIGRATION_DATABASE_URI=/dev/stdin
printf 'devhealth' | K create secret generic pg-probe --from-file=password=/dev/stdin
openssl genpkey -algorithm ed25519 -out "${WORK}/signing-key.pem"
K create secret generic auth-key --from-file=signing-key.pem="${WORK}/signing-key.pem"

# --- helpers: measurement ---------------------------------------------------------------------------
priv() { # insert|update -> t or f for the api role on refresh_tokens
  PSU -c "SELECT has_table_privilege('devhealth_api','public.refresh_tokens','$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')')"
}
col_update() { PSU -c "SELECT has_column_privilege('devhealth_api','public.refresh_tokens','revoked_at','UPDATE')"; }
go_api_env() { K get deploy/"${FULL}-go-api" -o json | jq -r '[.spec.template.spec.containers[0].env[]|select(.name=="AUTH_PROXY_SESSIONS")|.value]|first // "unset"'; }
go_api_ready() { K get deploy/"${FULL}-go-api" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true; }
rev() { helm --kube-context "kind-${CLUSTER}" -n "${NS}" status "${RELEASE}" -o json | jq -r .version; }

sample_pods() {
  while true; do
    now="$(date +%s)"
    printf '%s _tick _ 0\n' "${now}" >>"${WORK}/pods.log"
    K get pods -l app.kubernetes.io/component=go-api -o json 2>/dev/null \
      | jq -r --arg t "${now}" '.items[]|[$t,.metadata.name,((.status.conditions//[])|map(select(.type=="Ready"))|first|.status//"False"),(if .metadata.deletionTimestamp then 1 else 0 end)]|join(" ")' \
      >>"${WORK}/pods.log" || true
    sleep 1
  done
}

finding() {
  FINDINGS=$((FINDINGS + 1))
  printf 'FINDING: %s\n' "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then printf -- '- FINDING: %s\n' "$1" >>"${GITHUB_STEP_SUMMARY}"; fi
}
note() {
  printf '%s\n' "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then printf -- '- %s\n' "$1" >>"${GITHUB_STEP_SUMMARY}"; fi
}

# The read-drop class (F-A), as ruled: per upgrade, failures are status 000 only (no 5xx, no answer
# other than the baseline answer), at most 8 probes (4 per second), inside ONE window of at most 2 s that
# overlaps the pod handover (first new pod Ready .. last old pod gone, 2 s slack). Anything else is a hard finding.
ROWS=()
EXPECT_CODE=""

# analyse <phase> <t0> <t1> <revoke|regrant|none>: the old-pod window and the probe results of one phase.
# kind none: no grant change is expected in the phase (the control, the helm rollback).
analyse() {
  local name="$1" t0="$2" t1="$3" kind="$4" end change old_gone first_new reads fails five wrong first_fail last_fail zero trans before lo hi class window
  end=$((t1 + 8))
  K logs probe-read >"${WORK}/read.log" 2>/dev/null || true
  K logs probe-grant >"${WORK}/grant.log" 2>/dev/null || true
  local want="f"
  if [ "${kind}" = regrant ]; then want="t"; fi
  change="$(awk -v a="${t0}" -v w="${want}" '$1>=a && $2==w {print $1; exit}' "${WORK}/grant.log")"
  trans="$(awk -v a="${t0}" -v b="${end}" '$1>=a && $1<=b && $2 ~ /^[tf]$/ {if (p != "" && $2 != p) n++; p=$2} END {print n+0}' "${WORK}/grant.log")"
  read -r reads fails five wrong first_fail last_fail < <(awk -v a="${t0}" -v b="${end}" -v ok="${EXPECT_CODE}" '
    $1>=a && $1<=b {n++
      if ($2=="000") {f++; if (!ff) ff=$1; lf=$1}
      else if ($2>=500) {v++; if (!ff) ff=$1; lf=$1}
      else if ($2!=ok) {w++; if (!ff) ff=$1; lf=$1}}
    END {print n+0, f+0, v+0, w+0, ff+0, lf+0}' "${WORK}/read.log")
  before="$(awk -v a="${t0}" '$1<a {t=$1} END {print t}' "${WORK}/pods.log")"
  # Serving pods = those Ready at the last sample before the phase. old_gone: the last second one of them is
  # still listed. first_new: the first second a Ready pod appears that was not Ready before the phase (a new
  # pod, or a pod that was stuck NotReady and recovers).
  old_gone="$(awk -v a="${t0}" -v b="${end}" -v before="${before}" '
    $1==before && $2!="_tick" && $3=="True" {old[$2]=1}
    $1>=a && $1<=b && ($2 in old) {last=$1}
    END {print last ? last + 1 : 0}' "${WORK}/pods.log")"
  first_new="$(awk -v a="${t0}" -v before="${before}" '
    $1==before && $2!="_tick" && $3=="True" {old[$2]=1}
    $1>=a && $2!="_tick" && !($2 in old) && $3=="True" {print $1; exit}' "${WORK}/pods.log")"
  zero="$(awk -v a="${t0}" -v b="${t1}" '
    $1>=a && $1<=b {seen[$1]=1; if ($2!="_tick" && $3=="True") ready[$1]=1}
    END {for (s in seen) if (!(s in ready)) n++; print n+0}' "${WORK}/pods.log")"
  printf '\n=== %s ===\n' "${name}"
  note "${name}: phase ${t0}..${t1} (${kind})"
  window="n/a"
  if [ "${kind}" != none ]; then
    note "${name}: ${kind} committed at ${change:-never}"
    if [ -n "${change}" ]; then
      note "${name}: hook start to ${kind} commit: $((change - t0)) s"
      if [ "${old_gone}" -gt 0 ]; then window="$((old_gone - change)) s"; note "${name}: OLD-POD WINDOW (${kind} commit to last old pod gone): ${window}"; fi
      if [ -n "${first_new}" ]; then note "${name}: ${kind} commit to first new pod Ready: $((first_new - change)) s"; fi
    fi
  elif [ "${old_gone}" -gt 0 ] && [ -n "${first_new}" ]; then
    window="$((old_gone - first_new)) s handover"
    note "${name}: pod handover (first new pod Ready to last old pod gone): ${window}"
  fi
  note "${name}: seconds with no Ready go-api pod: ${zero}"
  note "${name}: read probes ${reads}: status 000 ${fails}, 5xx ${five}, other answer ${wrong} (baseline answer ${EXPECT_CODE}); first ${first_fail} last ${last_fail}"
  class="clean"
  if [ "${zero}" -gt 0 ]; then
    class="HARD"
    finding "${name}: ${zero} second(s) with no Ready go-api pod"
  fi
  if [ $((fails + five + wrong)) -gt 0 ]; then
    lo="${first_new:-${old_gone}}"
    hi="${old_gone}"
    if [ "${five}" -gt 0 ] || [ "${wrong}" -gt 0 ] || [ "${fails}" -gt 8 ] || [ $((last_fail - first_fail)) -gt 1 ] \
      || [ "${hi}" -eq 0 ] || [ "${first_fail}" -lt $((lo - 2)) ] || [ "${last_fail}" -gt $((hi + 2)) ]; then
      class="HARD"
      finding "${name}: read drops outside the named class F-A: 000=${fails} 5xx=${five} other=${wrong}, ${first_fail}..${last_fail}, handover ${lo}..${hi}"
    else
      if [ "${class}" != HARD ]; then class="F-A"; fi
      note "${name}: NAMED CLASS F-A: ${fails} probe(s) with status 000 inside one window of $((last_fail - first_fail + 1)) s at the pod handover (${lo}..${hi})"
    fi
  fi
  if [ "${ZERO_DROPS}" = 1 ] && { [ "${name}" = CONTROL ] || [ "${name}" = FLIP ]; } && [ $((fails + five + wrong)) -gt 0 ]; then
    class="HARD"
    finding "${name}: the zero-drop bar (go-api handover drain) is not met: 000=${fails} 5xx=${five} other=${wrong}"
  fi
  ROWS+=("| ${name} | ${window} | ${zero} | ${fails} / ${five} / ${wrong} | ${class} |")
  if [ "${kind}" != none ]; then
    note "${name}: grant transitions in the phase: ${trans}"
    if [ "${trans}" -gt 1 ]; then finding "${name}: the refresh_tokens grant changed ${trans} times (want 1)"; fi
    if [ -z "${change}" ]; then die "${name}: the ${kind} never showed in the grant probe"; fi
  fi
}

# phase <name> <revoke|regrant> <helm args...>
phase() {
  local name="$1" kind="$2" t0 t1
  shift 2
  t0="$(date +%s)"
  helm upgrade "${HELM_BASE[@]}" "$@" --wait --timeout 8m
  t1="$(date +%s)"
  sleep 10
  analyse "${name}" "${t0}" "${t1}" "${kind}"
}

assert_state() { # <name> <flag:true|unset> <insert> <update>
  local name="$1" flag="$2" ins="$3" upd="$4"
  [ "$(priv insert)" = "${ins}" ] || die "${name}: api role INSERT on refresh_tokens is $(priv insert), want ${ins}"
  [ "$(priv update)" = "${upd}" ] || die "${name}: api role UPDATE on refresh_tokens is $(priv update), want ${upd}"
  [ "$(go_api_env)" = "${flag}" ] || die "${name}: go-api AUTH_PROXY_SESSIONS is $(go_api_env), want ${flag}"
  [ "$(go_api_ready)" = 1 ] || die "${name}: go-api is not Ready"
  K rollout status deploy/"${FULL}-auth-service" --timeout=240s >/dev/null
  printf '%s: INSERT %s UPDATE %s, AUTH_PROXY_SESSIONS %s, go-api Ready, auth-service Ready\n' "${name}" "${ins}" "${upd}" "${flag}"
}

# --- baseline ------------------------------------------------------------------------------------
printf 'step 3: baseline upgrade with the hooks: sessions=false, readers set (prod today plus the deploy prep leaf)\n'
helm upgrade "${HELM_BASE[@]}" --wait --timeout 12m
BASE_REV="$(rev)"
printf 'baseline revision %s\n' "${BASE_REV}"
K rollout status deploy/"${FULL}-go-api" --timeout=240s
assert_state baseline unset t t
helm show values "${CHART}" >"${WORK}/chart-values.yaml"
EXPECT_SLEEP="${PRESTOP:-$(awk '/^  drain:/ {f=1} f && /preStopSeconds:/ {print $2; exit}' "${WORK}/chart-values.yaml")}"
LIVE_SLEEP="$(K get deploy/"${FULL}-go-api" -o jsonpath='{.spec.template.spec.containers[0].lifecycle.preStop.sleep.seconds}')"
LIVE_GRACE="$(K get deploy/"${FULL}-go-api" -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
printf 'go-api drain in the live Deployment: preStop sleep [%s], terminationGracePeriodSeconds %s (expected sleep %s)\n' "${LIVE_SLEEP}" "${LIVE_GRACE}" "${EXPECT_SLEEP}"
if [ "${EXPECT_SLEEP}" = 0 ]; then
  [ -z "${LIVE_SLEEP}" ] || die "drain is off but the live Deployment has a preStop sleep (${LIVE_SLEEP})"
else
  [ "${LIVE_SLEEP}" = "${EXPECT_SLEEP}" ] || die "the live Deployment's preStop sleep is [${LIVE_SLEEP}], want ${EXPECT_SLEEP}: the cluster dropped the hook"
fi
reader_ok() { # <role>: SELECT on exactly the session columns the cutover reads (a column-level grant)
  local col
  for col in id principal_id revoked_at; do
    [ "$(PSU -c "SELECT has_column_privilege('$1','auth.sessions','${col}','SELECT')")" = t ] || die "$2: reader $1 cannot SELECT auth.sessions.${col}"
  done
  printf '%s: reader %s can SELECT auth.sessions(id, principal_id, revoked_at)\n' "$2" "$1"
}
reader_ok devhealth_api baseline

K_RENDER "${SCRIPT_DIR}/session_rehearsal/probe-read.yaml" | K apply -f -
K_RENDER "${SCRIPT_DIR}/session_rehearsal/probe-grant.yaml" | K apply -f -
K wait --for=condition=Ready pod/probe-read pod/probe-grant --timeout=180s
sample_pods &
SAMPLER_PID=$!
sleep 5
EXPECT_CODE="$(K logs probe-read | awk '{print $2}' | sort | uniq -c | sort -rn | awk 'NR==1 {print $2}')"
printf 'read probe baseline answer: %s\n' "${EXPECT_CODE}"
case "${EXPECT_CODE}" in 000 | 5??) die "the read probe path does not answer (${EXPECT_CODE})" ;; esac

printf 'step 3b: CONTROL: an upgrade that rolls go-api with no flag change (a pod annotation only)\n'
T0="$(date +%s)"
helm upgrade "${HELM_BASE[@]}" --set goApi.podAnnotations.rehearsal=control --wait --timeout 8m
T1="$(date +%s)"
sleep 10
analyse CONTROL "${T0}" "${T1}" none
assert_state CONTROL unset t t

# --- prep, as the chris-word flip steps do it, but in the cluster only ----------------------------------
printf 'step 4: the successor-key Secret and the issuer token (test values, this cluster only)\n'
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' | K create secret generic auth-session --from-file=AUTH_SESSION_SUCCESSOR_KEY=/dev/stdin

printf 'step 5: guards: sessions=true is refused while a setting is unset\n'
guard() { # <must-name> <helm --set...>
  local must="$1" out
  shift
  if out="$(helm template "${RELEASE}" "${CHART}" --namespace "${NS}" -f "${SCRIPT_DIR}/session_rehearsal/rehearsal-release.yaml" -f "${WORK}/secrets-base.yaml" "$@" 2>&1)"; then
    die "guard: the render passed; it must name ${must}"
  fi
  grep -q "${must}" <<<"${out}" || die "guard: the refusal does not name ${must}: ${out:0:400}"
  printf 'guard: refused, naming %s\n' "${must}"
}
guard authService.sessionKeys.successor.secretName --set goApi.authProxy.sessions=true \
  --set authService.migrate.sessionReaders.queryApi=devhealth_query
guard sessionReaders.goApi --set goApi.authProxy.sessions=true --set authService.migrate.sessionReaders.goApi= \
  --set authService.migrate.sessionReaders.queryApi=devhealth_query \
  --set authService.sessionKeys.successor.secretName=auth-session
guard sessionReaders.queryApi --set goApi.authProxy.sessions=true \
  --set authService.sessionKeys.successor.secretName=auth-session

# --- the rehearsal ---------------------------------------------------------------------------------------
printf 'step 6: FLIP: one helm upgrade, sessions=true\n'
phase FLIP revoke "${HELM_SESSION[@]}" --set goApi.authProxy.sessions=true
assert_state FLIP true f f
[ "$(col_update)" = t ] || die "FLIP: the api role lost UPDATE on refresh_tokens.revoked_at (the posture keeps it)"
[ "$(priv insert)" = f ] || die "FLIP: INSERT not revoked"
printf 'FLIP: column UPDATE(revoked_at) kept\n'
reader_ok devhealth_api FLIP
reader_ok devhealth_query FLIP

printf 'step 7: ROLLBACK_UP: helm upgrade, sessions=false (the re-GRANT)\n'
phase ROLLBACK_UP regrant "${HELM_SESSION[@]}" --set goApi.authProxy.sessions=false
assert_state ROLLBACK_UP unset t t

printf 'step 8: REFLIP: the flip again\n'
phase REFLIP revoke "${HELM_SESSION[@]}" --set goApi.authProxy.sessions=true
assert_state REFLIP true f f

printf 'step 9: ROLLBACK_HELM: helm rollback to revision %s\n' "${BASE_REV}"
T0="$(date +%s)"
ROLLBACK_RC=0
helm rollback "${RELEASE}" "${BASE_REV}" --kube-context "kind-${CLUSTER}" --namespace "${NS}" --wait --timeout 4m || ROLLBACK_RC=$?
T1="$(date +%s)"
sleep 10
RB_INSERT="$(priv insert)"
note "ROLLBACK_HELM: helm rollback rc=${ROLLBACK_RC}; api role INSERT on refresh_tokens afterwards: ${RB_INSERT}; go-api env AUTH_PROXY_SESSIONS: $(go_api_env); go-api ready replicas: $(go_api_ready)"
if [ "${RB_INSERT}" = f ]; then
  note "KNOWN FINDING F-B: helm rollback alone did not re-GRANT (the hooks run on pre-install and pre-upgrade only): the prod rollback is a helm upgrade with the flag off, never helm rollback"
  analyse ROLLBACK_HELM "${T0}" "${T1}" none
  printf 'step 10: FIXUP: helm upgrade, sessions=false, after the helm rollback\n'
  phase FIXUP regrant --set goApi.authProxy.sessions=false
  assert_state FIXUP unset t t
else
  analyse ROLLBACK_HELM "${T0}" "${T1}" regrant
  assert_state ROLLBACK_HELM unset t t
fi

# --- F3 and the verdict ---------------------------------------------------------------------------------
note "F3: the two-upgrade form (hook with sessions=true, then go-api flag) is not expressible: templates/river-hooks.yaml and templates/go-api-deployment.yaml both read goApi.authProxy.sessions; the one-upgrade windows above are the whole answer"
kill "${SAMPLER_PID}" 2>/dev/null || true
SAMPLER_PID=""

printf 'step 11: helm uninstall leaves no object of the release behind\n'
K delete pod probe-read probe-grant --ignore-not-found --wait=true >/dev/null
helm uninstall "${RELEASE}" --kube-context "kind-${CLUSTER}" --namespace "${NS}" --wait
release_objects="$(K get deploy,svc,job,configmap -l "app.kubernetes.io/instance=${RELEASE}" -o json)"
hooks="$(jq -r '.items[]|select((.metadata.annotations // {})["helm.sh/hook"] != null)|(.kind|ascii_downcase)+"/"+.metadata.name' <<<"${release_objects}")"
left="$(jq -r '.items[]|select((.metadata.annotations // {})["helm.sh/hook"] == null)|(.kind|ascii_downcase)+"/"+.metadata.name' <<<"${release_objects}")"
printf 'hook objects helm does not delete (excluded from the check): [%s]\n' "$(tr '\n' ' ' <<<"${hooks}")"
[ -z "${left}" ] || die "objects of the release remain after uninstall: ${left}"

printf '\n| phase | old-pod window | s with no Ready pod | reads 000 / 5xx / other | class |\n|---|---|---|---|---|\n'
printf '%s\n' "${ROWS[@]}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  printf '\n| phase | old-pod window | s with no Ready pod | reads 000 / 5xx / other | class |\n|---|---|---|---|---|\n' >>"${GITHUB_STEP_SUMMARY}"
  printf '%s\n' "${ROWS[@]}" >>"${GITHUB_STEP_SUMMARY}"
fi
printf '\nREHEARSAL DONE: %s finding(s)\n' "${FINDINGS}"
if [ "${STRICT}" = 1 ] && [ "${FINDINGS}" -gt 0 ]; then die "REHEARSAL_STRICT=1 and ${FINDINGS} finding(s)"; fi
