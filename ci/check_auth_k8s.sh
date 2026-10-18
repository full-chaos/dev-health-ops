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
readonly NODE_IMAGE="kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
# One pinned image serves PostgreSQL and every helper pod (its busybox has
# wget, stat, ls and sh), and is the chart's own staging-image default.
readonly PG_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly LOCAL_PG="auth-proof/postgres:pinned"
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

for tool in docker kubectl helm openssl curl sha256sum; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- cluster state at failure ---\n' >&2
    kubectl --context "kind-${CLUSTER}" -n "${NS}" get all,networkpolicy,job,pods -o wide >&2 || true
    kubectl --context "kind-${CLUSTER}" -n "${NS}" describe pods >&2 || true
    kubectl --context "kind-${CLUSTER}" -n "${NS}" logs --all-containers --tail 60 -l app.kubernetes.io/component=auth-service >&2 || true
  fi
  "${WORK}/kind" delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  rm -rf "${WORK}"
  exit "${status}"
}
trap cleanup EXIT

K() { kubectl --context "kind-${CLUSTER}" -n "${NS}" "$@"; }

# --- tooling: kind by checksum, not by whatever the runner ships ---------------
printf 'step 0: kind %s (sha256 pinned) and the pinned node image\n' "${KIND_VERSION}"
curl --fail --silent --show-error --location --output "${WORK}/kind" \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
printf '%s  %s\n' "${KIND_SHA256_AMD64}" "${WORK}/kind" | sha256sum --check --status \
  || die "kind ${KIND_VERSION} does not match its pinned sha256"
chmod +x "${WORK}/kind"

printf 'step 1: build the images from this tree and create the cluster\n'
build() {
  docker build --file "${ROOT}/docker/go-worker.Dockerfile" --target "$1" \
    --build-arg VERSION=k8s-proof --build-arg COMMIT=0000000000000000000000000000000000000000 \
    --build-arg BUILD_TIME=1970-01-01T00:00:00Z --build-arg SOURCE_DATE_EPOCH=0 \
    --tag "$2" "${ROOT}"
}
build auth-service "${LOCAL_SVC}"
build auth-migrate "${LOCAL_MIG}"
docker pull "${PG_IMAGE}"
docker tag "${PG_IMAGE}" "${LOCAL_PG}"

"${WORK}/kind" create cluster --name "${CLUSTER}" --image "${NODE_IMAGE}" --wait 180s
for image in "${LOCAL_SVC}" "${LOCAL_MIG}" "${LOCAL_PG}"; do
  "${WORK}/kind" load docker-image "${image}" --name "${CLUSTER}"
done
kubectl --context "kind-${CLUSTER}" create namespace "${NS}"

# --- PostgreSQL and the Secrets the chart only references ------------------------
printf 'step 2: PostgreSQL, the migration and runtime logins, the Secrets\n'
PG_PASSWORD="$(openssl rand -hex 12)"
RUNTIME_PASSWORD="$(openssl rand -hex 12)#"
K apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: auth-postgres}
spec:
  replicas: 1
  selector: {matchLabels: {app: auth-postgres}}
  template:
    metadata: {labels: {app: auth-postgres}}
    spec:
      containers:
        - name: postgres
          image: ${LOCAL_PG}
          imagePullPolicy: Never
          env:
            - {name: POSTGRES_PASSWORD, value: "${PG_PASSWORD}"}
            - {name: PGDATA, value: /var/lib/postgresql/pgdata}
          readinessProbe:
            exec: {command: [pg_isready, -U, postgres]}
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: auth-postgres}
spec:
  selector: {app: auth-postgres}
  ports: [{port: 5432, targetPort: 5432}]
EOF
K rollout status deploy/auth-postgres --timeout=180s
K exec deploy/auth-postgres -- psql -U postgres -v ON_ERROR_STOP=1 \
  -c "CREATE ROLE devhealth_auth LOGIN PASSWORD '${RUNTIME_PASSWORD}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"

# Keyword-form DSNs: the runtime password deliberately ends in '#'.
K create secret generic auth-db \
  --from-literal="AUTH_DATABASE_URI=host=auth-postgres port=5432 user=devhealth_auth password='${RUNTIME_PASSWORD}' dbname=postgres sslmode=disable"
K create secret generic auth-migrate-db \
  --from-literal="AUTH_MIGRATION_DATABASE_URI=host=auth-postgres port=5432 user=postgres password='${PG_PASSWORD}' dbname=postgres sslmode=disable"
openssl genpkey -algorithm ed25519 -out "${WORK}/signing-key.pem"
K create secret generic auth-key --from-file=signing-key.pem="${WORK}/signing-key.pem"

# --- render the chart's auth manifests, exactly as a release would -----------------
printf 'step 3: render the authService manifests with helm template\n'
render() {
  helm template "${RELEASE}" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}" \
    --set web.env.BACKEND_URL=http://backend.invalid \
    --set authService.enabled=true \
    --set authService.image.repository=auth-proof/auth-service \
    --set authService.image.tag=ci \
    --set authService.image.pullPolicy=Never \
    --set authService.migrate.image.repository=auth-proof/auth-migrate \
    --set authService.migrate.image.tag=ci \
    --set authService.migrate.image.pullPolicy=Never \
    --set authService.signingKey.stagingImage.repository=auth-proof/postgres \
    --set authService.signingKey.stagingImage.tag=pinned \
    --set authService.signingKey.stagingImage.pullPolicy=Never \
    --set authService.database.secretName=auth-db \
    --set authService.migrate.secretName=auth-migrate-db \
    --set authService.signingKey.secretName=auth-key \
    --set authService.signingKey.keyId=k8s-proof \
    --set-json 'authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]' \
    --set-json 'authService.networkPolicy.allowedFrom=[{"matchLabels":{"role":"auth-consumer"}}]' \
    --set-json 'authService.networkPolicy.metricsFrom=[{"matchLabels":{"role":"auth-scraper"}}]' \
    "$@"
}
render --show-only templates/serviceaccount.yaml >"${WORK}/sa.yaml"
render --show-only templates/auth-migrate-job.yaml >"${WORK}/migrate.yaml"
render --show-only templates/auth-service.yaml >"${WORK}/service.yaml"
for f in sa migrate service; do
  printf '%s.yaml: %s documents\n' "${f}" "$(grep -c '^kind:' "${WORK}/${f}.yaml")"
done

# --- 1. the migration Job (a pre-install hook in a release) -------------------------
printf 'step 4: migration hook Job completes\n'
K apply -f "${WORK}/sa.yaml"
K apply -f "${WORK}/migrate.yaml"
K wait --for=condition=complete job/"${RELEASE}-dev-health-auth-migrate" --timeout=240s
K logs job/"${RELEASE}-dev-health-auth-migrate"
version="$(K exec deploy/auth-postgres -- psql -U postgres -At -c 'SELECT max(version) FROM auth.schema_migrations')"
printf 'auth.schema_migrations max version: %s\n' "${version}"
if ! [[ "${version}" =~ ^[0-9]+$ ]] || [ "${version}" -lt 7 ]; then die "the auth schema is not at its head after the Job"; fi

# --- 2. the Deployment -----------------------------------------------------------------
printf 'step 5: Deployment Ready, /readyz 200\n'
K apply -f "${WORK}/service.yaml"
K rollout status deploy/"${RELEASE}-dev-health-auth-service" --timeout=240s
POD="$(K get pod -l app.kubernetes.io/component=auth-service -o jsonpath='{.items[0].metadata.name}')"
POD_IP="$(K get pod "${POD}" -o jsonpath='{.status.podIP}')"
printf 'pod %s ip %s\n' "${POD}" "${POD_IP}"

helper() { # name label-key=value command...
  local name="$1" label="$2"
  shift 2
  K delete pod "${name}" --ignore-not-found --wait=true >/dev/null
  K run "${name}" --image="${LOCAL_PG}" --image-pull-policy=Never --restart=Never \
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
  K apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata: {name: ${name}, labels: {role: trap}}
spec:
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    ${fsgroup}
  containers:
    - name: auth-service
      image: ${LOCAL_SVC}
      imagePullPolicy: Never
      args:
        - --signing-key-file=/mnt/key/signing-key.pem
        - --signing-key-id=trap
        - --health-check-timeout=2s
      env:
        - name: AUTH_DATABASE_URI
          valueFrom: {secretKeyRef: {name: auth-db, key: AUTH_DATABASE_URI}}
      volumeMounts: [${mount}]
  volumes:
    - name: key
      secret: {secretName: auth-key, defaultMode: ${mode}}
EOF
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

# --- 6. rollback ---------------------------------------------------------------------------------
printf 'step 9: rollback leaves no auth object behind\n'
K delete -f "${WORK}/service.yaml" -f "${WORK}/migrate.yaml" --wait=true
K delete pod trap-direct-0400 trap-direct-0440-fsgroup trap-subpath-0400 trap-subpath-0400-fsgroup --ignore-not-found --wait=true >/dev/null
left="$(K get deploy,svc,job,networkpolicy -o name | grep -E 'auth-service|auth-migrate' || true)"
printf 'auth objects left: [%s]\n' "${left}"
[ -z "${left}" ] || die "auth objects remain after rollback: ${left}"
K get deploy/auth-postgres >/dev/null || die "rollback disturbed postgres"
printf 'postgres untouched by the rollback\n'

printf 'AUTH K8S PROOF PASSED\n'
