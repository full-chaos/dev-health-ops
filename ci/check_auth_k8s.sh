#!/usr/bin/env bash
# Executed Kubernetes proof for the dormant Auth Control Plane (CHAOS-10079).
#
# Applies the chart's authService manifests to a real (kind) cluster and proves,
# with output, what the render tests can only reason about:
#
#   1. the migration Job (a Helm pre-install hook in a release) completes and
#      leaves the auth schema at its head;
#   2. the Deployment becomes Ready and /readyz is 200, from images built from
#      this tree;
#   3. the key the service reads is mode 0400 and owned by uid 65532 (read
#      through an ephemeral debug container: the image has no shell);
#   4. the design the staging init container exists for is the only one that
#      works: a Secret mounted straight in is a symlink the custody check refuses;
#      as a regular file (subPath) at 0400 it is root-owned and unreadable by uid
#      65532, and with fsGroup it becomes 0440, which the check rejects;
#   5. the NetworkPolicy is ENFORCED: a labelled consumer reaches the API port, an
#      unlabelled pod does not. If the cluster does not enforce policy this step
#      FAILS; it is never skipped;
#   6. removing the manifests leaves no auth object behind.
#
# CI only (needs docker, kind, kubectl, helm). A failure prints what it measured.
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
# One pinned image serves PostgreSQL and every helper pod (its busybox has
# wget, stat, ls and sh), and is the chart's own staging-image default.
readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
# The pinned image is an OCI index (several platforms + attestations): `kind load` of a
# single-platform pull cannot import it, so the NODE pulls it by digest instead.
readonly LOCAL_PG="${PG_IMAGE}"
readonly LOCAL_SVC="auth-proof/auth-service:ci"
readonly LOCAL_MIG="auth-proof/auth-migrate:ci"
readonly NS="auth-proof"
readonly CLUSTER="auth-proof-$$"
readonly RELEASE="ap"
WORK="$(mktemp -d)"
readonly WORK

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

for tool in docker openssl curl sha256sum envsubst jq tar; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- cluster state at failure ---\n' >&2
    kubectl --context "kind-${CLUSTER}" -n "${NS}" get all,networkpolicy,job,pods -o wide >&2 || true
    kubectl --context "kind-${CLUSTER}" -n "${NS}" describe pods >&2 || true
    kubectl --context "kind-${CLUSTER}" -n "${NS}" logs --all-containers --tail 60 -l app.kubernetes.io/component=auth-service >&2 || true
    kubectl --context "kind-${CLUSTER}" -n "${NS}" logs --all-containers --tail 60 -l app.kubernetes.io/component=auth-migrate >&2 || true
  fi
  "${WORK}/kind" delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  # A signal that lands while `kind create cluster` is mid-flight can leave the node container
  # behind a delete that ran too early: remove it by its exact, pid-unique name.
  docker rm --force --volumes "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  rm -rf "${WORK}"
  exit "${status}"
}
trap cleanup EXIT
# An interrupted or terminated run must still delete its cluster: turn the signals into an exit,
# which runs the EXIT trap.
trap 'exit 130' INT
trap 'exit 143' TERM HUP

K() { kubectl --context "kind-${CLUSTER}" -n "${NS}" "$@"; }
# Manifests live in files under ci/auth_k8s/ and are filled from the environment: a large
# here-document in a script can hang bash (tests/tooling/test_local_validate_heredocs.py).
K_RENDER() {
  # shellcheck disable=SC2016
  envsubst '${LOCAL_PG} ${LOCAL_SVC} ${name} ${mode} ${fsgroup} ${mount}' <"$1"
}

# --- tooling: kind by checksum, not by whatever the runner ships ---------------
printf 'step 0: kind %s (sha256 pinned) and the pinned node image\n' "${KIND_VERSION}"
curl --fail --silent --show-error --location --output "${WORK}/kind" \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
printf '%s  %s\n' "${KIND_SHA256_AMD64}" "${WORK}/kind" | sha256sum --check --status \
  || die "kind ${KIND_VERSION} does not match its pinned sha256"
chmod +x "${WORK}/kind"

# helm and kubectl are pinned the same way: the runner image's versions are not a contract.
mkdir -p "${WORK}/bin"
curl --fail --silent --show-error --location --output "${WORK}/helm.tgz" \
  "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz"
printf '%s  %s\n' "${HELM_SHA256_AMD64}" "${WORK}/helm.tgz" | sha256sum --check --status \
  || die "helm ${HELM_VERSION} does not match its pinned sha256"
tar -xzf "${WORK}/helm.tgz" -C "${WORK}" linux-amd64/helm
mv "${WORK}/linux-amd64/helm" "${WORK}/bin/helm"
curl --fail --silent --show-error --location --output "${WORK}/bin/kubectl" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
printf '%s  %s\n' "${KUBECTL_SHA256_AMD64}" "${WORK}/bin/kubectl" | sha256sum --check --status \
  || die "kubectl ${KUBECTL_VERSION} does not match its pinned sha256"
chmod +x "${WORK}/bin/helm" "${WORK}/bin/kubectl"
export PATH="${WORK}/bin:${PATH}"
printf 'tools: kind %s, %s, kubectl %s\n' "${KIND_VERSION}" "$(helm version --short)" "$(kubectl version --client -o yaml | grep gitVersion)"

printf 'step 1: build the images from this tree and create the cluster\n'
build() {
  BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker build --file "${ROOT}/docker/go-worker.Dockerfile" --target "$1" \
    --build-arg VERSION=k8s-proof --build-arg COMMIT=0000000000000000000000000000000000000000 \
    --build-arg BUILD_TIME=1970-01-01T00:00:00Z --build-arg SOURCE_DATE_EPOCH=0 \
    --tag "$2" "${ROOT}"
}
build auth-service "${LOCAL_SVC}"
build auth-migrate "${LOCAL_MIG}"

"${WORK}/kind" create cluster --name "${CLUSTER}" --image "${NODE_IMAGE}" --wait 180s
for image in "${LOCAL_SVC}" "${LOCAL_MIG}"; do
  "${WORK}/kind" load docker-image "${image}" --name "${CLUSTER}"
done
kubectl --context "kind-${CLUSTER}" create namespace "${NS}"

# --- PostgreSQL and the Secrets the chart only references ------------------------
printf 'step 2: PostgreSQL, the migration and runtime logins, the Secrets\n'
PG_PASSWORD="$(openssl rand -hex 12)"
export LOCAL_PG LOCAL_SVC
RUNTIME_PASSWORD="$(openssl rand -hex 12)#"
# Credentials travel on stdin only: argv is visible to ps and audit tooling, and a literal env
# value is printed by `kubectl describe` into the failure log.
printf '%s' "${PG_PASSWORD}" | K create secret generic auth-pg-superuser --from-file=password=/dev/stdin
K_RENDER "${SCRIPT_DIR}/auth_k8s/postgres.yaml" | K apply -f -
K rollout status deploy/auth-postgres --timeout=180s
printf "CREATE ROLE devhealth_auth LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\n" \
  "${RUNTIME_PASSWORD}" | K exec -i deploy/auth-postgres -- psql -U postgres -v ON_ERROR_STOP=1 -f -

# Keyword-form DSNs: the runtime password deliberately ends in '#'.
printf 'host=auth-postgres port=5432 user=devhealth_auth password=%s dbname=postgres sslmode=disable' "'${RUNTIME_PASSWORD}'" \
  | K create secret generic auth-db --from-file=AUTH_DATABASE_URI=/dev/stdin
printf 'host=auth-postgres port=5432 user=postgres password=%s dbname=postgres sslmode=disable' "'${PG_PASSWORD}'" \
  | K create secret generic auth-migrate-db --from-file=AUTH_MIGRATION_DATABASE_URI=/dev/stdin
openssl genpkey -algorithm ed25519 -out "${WORK}/signing-key.pem"
K create secret generic auth-key --from-file=signing-key.pem="${WORK}/signing-key.pem"

# --- the release: a real helm install, so hook ordering and uninstall are exercised ---------
printf 'step 3: helm upgrade --install the authService section (only it: everything else is off)\n'
HELM_ARGS=(
  "${RELEASE}" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}"
  --set web.enabled=false --set goWorkers.enabled=false --set valkey.enabled=false
  --set clickhouse.enabled=false --set migrations.hook.enabled=false --set secrets.create=false
  --set authService.enabled=true
  --set authService.image.repository=auth-proof/auth-service --set authService.image.tag=ci
  --set authService.image.pullPolicy=Never
  --set authService.migrate.image.repository=auth-proof/auth-migrate --set authService.migrate.image.tag=ci
  --set authService.migrate.image.pullPolicy=Never
  --set authService.database.secretName=auth-db
  --set authService.migrate.secretName=auth-migrate-db
  --set authService.signingKey.secretName=auth-key --set authService.signingKey.keyId=k8s-proof
  --set-json 'authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]'
  --set-json 'authService.networkPolicy.allowedFrom=[{"matchLabels":{"role":"auth-consumer"}}]'
  --set-json 'authService.networkPolicy.metricsFrom=[{"matchLabels":{"role":"auth-scraper"}}]'
)
helm template "${HELM_ARGS[@]}" --show-only templates/auth-migrate-job.yaml | grep -q 'helm.sh/hook: pre-install,pre-upgrade' \
  || die "the migration Job is not a pre-install,pre-upgrade hook"
printf 'migration Job carries helm.sh/hook: pre-install,pre-upgrade\n'
kinds="$(helm template "${HELM_ARGS[@]}" | grep -c '^kind:')"
printf 'the render holds %s objects\n' "${kinds}"
helm upgrade --install "${HELM_ARGS[@]}" --wait --timeout 8m

# --- 1. the migration Job ran as a real pre-install hook -----------------------------
printf 'step 4: the pre-install hook ran, left the schema at its head and was garbage-collected\n'
helm status "${RELEASE}" -n "${NS}" | grep -E '^(STATUS|REVISION)'
[ "$(helm status "${RELEASE}" -n "${NS}" -o json | jq -r .info.status)" = deployed ] || die "the release is not deployed"
if K get job "${RELEASE}-dev-health-auth-migrate" >/dev/null 2>&1; then
  die "the hook Job survived its hook-succeeded delete policy"
fi
applied="$(K exec deploy/auth-postgres -- psql -U postgres -At -c 'SELECT version FROM auth.schema_migrations ORDER BY version' | tr '\n' ' ')"
embedded="$(find "${ROOT}/internal/storage/postgres/authschema/migrations" -name '[0-9][0-9][0-9][0-9]_*.sql' -printf '%f\n' | sort | cut -c1-4 | sed 's/^0*//' | tr '\n' ' ')"
printf 'auth.schema_migrations applied: %s\nembedded lineage:               %s\n' "${applied}" "${embedded}"
[ -n "${embedded}" ] || die "could not read the embedded migration lineage"
[ "${applied}" = "${embedded}" ] || die "the applied migrations (${applied}) are not the embedded lineage (${embedded})"

# --- 2. helm upgrade runs the pre-upgrade hook and rolls the Deployment ----------------------
printf 'step 5: helm upgrade (pre-upgrade hook) succeeds and the Deployment is Ready\n'
helm upgrade "${HELM_ARGS[@]}" --set authService.podAnnotations.proof=upgrade --wait --timeout 8m
rev="$(helm status "${RELEASE}" -n "${NS}" -o json | jq -r .version)"
printf 'release revision after upgrade: %s\n' "${rev}"
[ "${rev}" = 2 ] || die "the release is at revision ${rev}, want 2"
completions="$(K get events --field-selector "involvedObject.name=${RELEASE}-dev-health-auth-migrate,reason=Completed" -o json | jq '[.items[].count // 1] | add')"
printf 'migration Job Completed events across install and upgrade: %s\n' "${completions}"
[ "${completions:-0}" -ge 2 ] || die "the migration hook did not run on both install and upgrade (${completions:-0})"
K rollout status deploy/"${RELEASE}-dev-health-auth-service" --timeout=240s
POD="$(K get pod -l app.kubernetes.io/component=auth-service --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
POD_IP="$(K get pod "${POD}" -o jsonpath='{.status.podIP}')"
printf 'pod %s ip %s\n' "${POD}" "${POD_IP}"

helper() { # name label-key=value command...
  local name="$1" label="$2"
  shift 2
  K delete pod "${name}" --ignore-not-found --wait=true >/dev/null
  K run "${name}" --image="${LOCAL_PG}" --image-pull-policy=IfNotPresent --restart=Never \
    --labels="${label}" --command -- "$@" >/dev/null
}
fetch() { # name label url -> the pod's own wget output (stdout+stderr, 5 s) and rc=
  local name="$1" label="$2" url="$3"
  helper "${name}" "${label}" sh -c "wget -T 5 -O - '${url}' 2>&1; echo \"rc=\$?\""
  K wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${name}" --timeout=60s >/dev/null 2>&1 || true
  K logs "${name}"
}

body="$(fetch scraper1 role=auth-scraper "http://${POD_IP}:8096/readyz")"
printf '/readyz from a listed scraper: %s\n' "${body}"
grep -q '"status":"ok"' <<<"${body}" || die "/readyz is not ok"
grep -q 'rc=0' <<<"${body}" || die "/readyz did not answer 200"

# --- 3. the staged key, read through an ephemeral container -------------------------------
printf 'step 6: the staged signing key is 0400 and owned by uid 65532\n'
# shellcheck disable=SC2016
K debug "${POD}" --image="${LOCAL_PG}" --target=auth-service -c dbg -q --profile=general -- sh -c '
  for p in /proc/[0-9]*; do
    if tr "\0" " " < "$p/cmdline" 2>/dev/null | grep -q "bin/auth-ser""vice"; then
      stat -c "%a %u %g %n" "$p/root/run/auth/signing/signing-key.pem"; break
    fi
  done' >/dev/null
sleep 5
key_stat="$(K logs "${POD}" -c dbg | tail -n 1)"
printf 'staged key (mode uid gid path): %s\n' "${key_stat}"
case "${key_stat}" in
  "400 65532 "*) ;;
  *) die "the staged key is '${key_stat}', want mode 400 owned by 65532" ;;
esac

# --- 4. the trap: why the staging exists ----------------------------------------------------
printf 'step 7: mounting the Secret directly cannot satisfy the custody check\n'
trap_pod() { # name defaultMode fsGroup-line expected-reason subPath(yes|no)
  local name="$1" mode="$2" fsgroup="$3" reason="$4" subpath="${5:-no}" mount
  mount="{name: key, mountPath: /mnt/key, readOnly: true}"
  if [ "${subpath}" = yes ]; then
    mount="{name: key, mountPath: /mnt/key/signing-key.pem, subPath: signing-key.pem, readOnly: true}"
  fi
  K delete pod "${name}" --ignore-not-found --wait=true >/dev/null
  export name mode fsgroup mount
  K_RENDER "${SCRIPT_DIR}/auth_k8s/trap-pod.yaml" | K apply -f -
  local ip out
  for _ in $(seq 1 30); do
    ip="$(K get pod "${name}" -o jsonpath='{.status.podIP}')"
    [ -n "${ip}" ] && break
    sleep 2
  done
  sleep 6
  out="$(fetch "${name}-probe" role=auth-scraper "http://${ip}:8096/readyz")"
  printf '%s /readyz: %s\n' "${name}" "${out}"
  grep -q '503' <<<"${out}" || die "${name}: /readyz did not answer 503"
  local logs
  logs="$(K logs "${name}")"
  grep -q '"check":"signing_key"' <<<"${logs}" || die "${name}: the log does not name the signing_key check"
  grep -q "\"reason\":\"${reason}\"" <<<"${logs}" || {
    printf '%s\n' "${logs}" >&2
    die "${name}: the log does not carry reason ${reason}"
  }
  printf '%s: signing_key fails closed with %s\n' "${name}" "${reason}"
}
# A Secret volume exposes every file as a SYMLINK into ..data, and the custody check
# opens with O_NOFOLLOW: a plain mount is refused whatever its mode.
trap_pod trap-direct-0400 256 "" key_file_unreadable
trap_pod trap-direct-0440-fsgroup 288 "fsGroup: 65532" key_file_unreadable
# A subPath mount is a regular file, which isolates the owner and mode reasons: root-owned
# 0400 is unreadable by uid 65532, and fsGroup raises the file to 0440, which is refused.
trap_pod trap-subpath-0400 256 "" key_file_unreadable yes
trap_pod trap-subpath-0400-fsgroup 256 "fsGroup: 65532" key_file_mode_too_permissive yes

# --- 5. NetworkPolicy is enforced ------------------------------------------------------------
printf 'step 8: the NetworkPolicy is enforced, not merely present\n'
K get networkpolicy
allowed="$(fetch consumer1 role=auth-consumer "http://${POD_IP}:8095/anything")"
printf 'listed consumer -> API port: %s\n' "${allowed}"
# The dormant API answers every path with its own 404; an ADMITTED connection therefore
# ends in "HTTP/1.1 404", a blocked one in a timeout or a refused connection.
grep -q 'HTTP/1.1 404' <<<"${allowed}" || die "a listed consumer did not reach the API port"
blocked="$(fetch stranger1 role=stranger "http://${POD_IP}:8095/anything")"
printf 'unlisted pod -> API port: %s\n' "${blocked}"
if grep -q 'HTTP/1.1' <<<"${blocked}"; then
  die "the NetworkPolicy is NOT enforced by this cluster: an unlisted pod reached the API port"
fi
grep -q 'rc=1' <<<"${blocked}" || die "the unlisted probe did not time out or fail to connect"
blocked_ops="$(fetch stranger2 role=stranger "http://${POD_IP}:8096/readyz")"
printf 'unlisted pod -> operator port: %s\n' "${blocked_ops}"
if grep -q 'HTTP/1.1\|"status"' <<<"${blocked_ops}"; then
  die "an unlisted pod reached the operator port"
fi

# The Service routes to the pod, by name, on both published ports.
printf 'step 8b: the Service reaches the pod (DNS name, endpoints, both ports)\n'
ep="$(K get endpoints "${RELEASE}-dev-health-auth-service" -o jsonpath='{.subsets[0].addresses[0].ip}')"
printf 'Service endpoint: %s (pod %s)\n' "${ep}" "${POD_IP}"
[ "${ep}" = "${POD_IP}" ] || die "the Service does not select the pod"
SVC_HOST="${RELEASE}-dev-health-auth-service"
via_svc="$(fetch consumer2 role=auth-consumer "http://${SVC_HOST}:8095/anything")"
printf 'consumer -> Service API port: %s\n' "${via_svc}"
grep -q 'HTTP/1.1 404' <<<"${via_svc}" || die "the Service does not route to the API port"
ops_svc="$(fetch scraper2 role=auth-scraper "http://${SVC_HOST}:8096/readyz")"
printf 'scraper -> Service operator port: %s\n' "${ops_svc}"
grep -q 'rc=0' <<<"${ops_svc}" || die "the Service does not publish the operator port to listed scrapers"

# Egress is bounded too: a pod the policy selects reaches PostgreSQL and nothing else.
printf 'step 8c: egress is bounded (database reachable, another pod not)\n'
egress_probe() { # name labels command-string
  helper "$1" "$2" sh -c "$3"
  K wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$1" --timeout=60s >/dev/null 2>&1 || true
  K logs "$1"
}
SVC_LABELS="app.kubernetes.io/name=dev-health,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=auth-service"
db_out="$(egress_probe egress-db "${SVC_LABELS}" "nc -z -w 3 auth-postgres 5432; echo rc=\$?")"
printf 'selected pod -> PostgreSQL:5432: %s\n' "${db_out}"
grep -q 'rc=0' <<<"${db_out}" || die "the policy blocks the database"
# A listener in an unlisted pod: reachable from outside the policy, so a refusal below is the policy's.
helper egress-target role=target sh -c 'nc -lk -p 9000'
K wait --for=condition=Ready pod/egress-target --timeout=60s >/dev/null
TARGET_IP="$(K get pod egress-target -o jsonpath='{.status.podIP}')"
open_out="$(egress_probe egress-open role=target "nc -z -w 3 ${TARGET_IP} 9000; echo rc=\$?")"
printf 'unselected pod -> listener (control, no policy): %s\n' "${open_out}"
grep -q 'rc=0' <<<"${open_out}" || die "the egress target is not reachable even without a policy: the probe is invalid"
other_out="$(egress_probe egress-other "${SVC_LABELS}" "nc -z -w 3 ${TARGET_IP} 9000; echo rc=\$?")"
printf 'selected pod -> the same listener: %s\n' "${other_out}"
grep -q 'rc=0' <<<"${other_out}" && die "the policy lets a selected pod reach a pod that is not the database"
grep -q 'rc=' <<<"${other_out}" || die "the egress probe produced no result"

# --- 6. rollback ---------------------------------------------------------------------------------
printf 'step 9: helm uninstall leaves no object of the release behind\n'
K delete pod trap-direct-0400 trap-direct-0440-fsgroup trap-subpath-0400 trap-subpath-0400-fsgroup --ignore-not-found --wait=true >/dev/null
helm uninstall "${RELEASE}" -n "${NS}" --wait
left="$(K get deploy,svc,job,networkpolicy,serviceaccount,configmap,endpoints -l "app.kubernetes.io/instance=${RELEASE}" -o name || true)"
printf 'objects labelled app.kubernetes.io/instance=%s left: [%s]\n' "${RELEASE}" "${left}"
[ -z "${left}" ] || die "objects of the release remain after uninstall: ${left}"
K get deploy/auth-postgres >/dev/null || die "uninstall disturbed postgres"
printf 'postgres untouched by the uninstall\n'

printf 'AUTH K8S PROOF PASSED\n'
