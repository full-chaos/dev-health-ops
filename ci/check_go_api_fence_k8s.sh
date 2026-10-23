#!/usr/bin/env bash
# Executed Kubernetes proof for the go-api request-port fence (goApi.networkPolicy).
#
# Renders the chart's go-api-internal NetworkPolicy with the fence on, applies it to a real
# (kind) cluster in front of a stand-in pod that carries the go-api pod labels and listens on
# the go-api ports, and proves with output what the render tests can only reason about:
#
#   0. CONTROL: before the policy, every client reaches every port (a refusal later is the
#      policy's, not a dead listener);
#   1. the policy is ENFORCED: a pod of the ingress controller namespace without the
#      controller labels, a controller-labelled pod of another namespace, an unlisted
#      in-namespace pod and a tools pod outside allowedFrom are refused on the request ports
#      (api 8000, billing edge 8010) and on the internal port (8091);
#   2. the ingress controller pod reaches 8000 and 8010 (not 8091), a listed allowedFrom pod
#      reaches them, and every client still reaches the operator port 8080 (probes, scrapes);
#   3. ROLLBACK: the render with the fence off, applied over the same policy, opens the
#      request ports to everyone again.
# If the cluster does not enforce policy this FAILS; it is never skipped.
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
# One pinned image serves the stand-in listeners and every client pod (its busybox has nc).
# The node pulls it by digest: it is an OCI index that `kind load` cannot import.
readonly PROOF_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
readonly NS="fence-proof"
readonly CONTROLLER_NS="ingress-nginx"
readonly OTHER_NS="other-proof"
readonly CLUSTER="fence-proof-$$"
readonly RELEASE="fence"
WORK="$(mktemp -d)"
readonly WORK

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

for tool in docker curl sha256sum tar awk; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    printf '\n--- cluster state at failure ---\n' >&2
    for namespace in "${NS}" "${CONTROLLER_NS}" "${OTHER_NS}"; do
      kubectl --context "kind-${CLUSTER}" -n "${namespace}" get pods,networkpolicy -o wide >&2 || true
    done
    kubectl --context "kind-${CLUSTER}" -n "${NS}" describe networkpolicy >&2 || true
  fi
  "${WORK}/kind" delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  # A signal that lands while `kind create cluster` is mid-flight can leave the node container
  # behind a delete that ran too early: remove it by its exact, pid-unique name.
  docker rm --force --volumes "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  rm -rf "${WORK}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

K() { kubectl --context "kind-${CLUSTER}" "$@"; }

printf 'step 0: kind %s, helm %s and kubectl %s by pinned sha256\n' "${KIND_VERSION}" "${HELM_VERSION}" "${KUBECTL_VERSION}"
curl --fail --silent --show-error --location --output "${WORK}/kind" \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
printf '%s  %s\n' "${KIND_SHA256_AMD64}" "${WORK}/kind" | sha256sum --check --status \
  || die "kind ${KIND_VERSION} does not match its pinned sha256"
chmod +x "${WORK}/kind"
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

printf 'step 1: create the cluster and the three namespaces\n'
"${WORK}/kind" create cluster --name "${CLUSTER}" --image "${NODE_IMAGE}" --wait 180s
for namespace in "${NS}" "${CONTROLLER_NS}" "${OTHER_NS}"; do
  K create namespace "${namespace}"
done

# The policy comes out of the chart: the go-api-internal NetworkPolicy, fence on or off.
render_policy() { # fence-enabled(true|false) -> the NetworkPolicy document on stdout
  helm template "${RELEASE}" "${ROOT}/deploy/helm/dev-health" --namespace "${NS}" \
    --show-only templates/go-api-deployment.yaml \
    --set web.enabled=false --set goApi.enabled=true \
    --set "goApi.networkPolicy.enabled=$1" \
    --set-json 'goApi.networkPolicy.extraFencedPorts=[8010]' \
    --set-json 'goApi.networkPolicy.allowedFrom=[{"podSelector":{"matchLabels":{"role":"tools"}}}]' \
    | awk '/^---$/ { if (doc ~ /kind: NetworkPolicy/) printf "%s", doc; doc = ""; next } { doc = doc $0 "\n" } END { if (doc ~ /kind: NetworkPolicy/) printf "%s", doc }'
}
render_policy true >"${WORK}/fence-on.yaml"
render_policy false >"${WORK}/fence-off.yaml"
grep -q 'kind: NetworkPolicy' "${WORK}/fence-on.yaml" || die "the chart rendered no go-api NetworkPolicy"
grep -q 'port: 8010' "${WORK}/fence-on.yaml" || die "the fenced render does not name the billing edge port"

printf 'step 2: the stand-in go-api pod listens on 8000, 8010, 8080 and 8091\n'
GO_API_LABELS="app.kubernetes.io/name=dev-health,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=go-api"
# Each listener restarts in a loop: one `nc -l` serves one connection, and a listener that ends turns a refusal into an instant
# "connection refused" that no policy drew.
K -n "${NS}" run go-api --image="${PROOF_IMAGE}" --image-pull-policy=IfNotPresent --restart=Never \
  --labels="${GO_API_LABELS}" --command -- sh -c "for p in 8000 8010 8080 8091; do (while true; do nc -l -p \$p </dev/null >/dev/null 2>&1; done) & done; wait" >/dev/null
K -n "${NS}" wait --for=condition=Ready pod/go-api --timeout=180s >/dev/null
POD_IP="$(K -n "${NS}" get pod go-api -o jsonpath='{.status.podIP}')"
printf 'stand-in pod ip %s\n' "${POD_IP}"

reach() { # namespace labels port -> "open" or "closed" (one nc connect from a fresh client pod)
  local namespace="$1" labels="$2" port="$3" name out
  name="client-$(date +%s%N)"
  K -n "${namespace}" run "${name}" --image="${PROOF_IMAGE}" --image-pull-policy=IfNotPresent --restart=Never \
    --labels="${labels}" --command -- sh -c "s=\$(date +%s); nc -z -w 3 ${POD_IP} ${port}; rc=\$?; echo rc=\$rc elapsed=\$((\$(date +%s) - s))" >/dev/null
  K -n "${namespace}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${name}" --timeout=90s >/dev/null 2>&1 || true
  out="$(K -n "${namespace}" logs "${name}" 2>&1 || true)"
  K -n "${namespace}" delete pod "${name}" --wait=false >/dev/null 2>&1 || true
  # A policy drop makes the 3 s connect time out. An instant failure is a refusal the listener drew, not the policy.
  case "${out}" in
    *rc=0*) printf 'open' ;;
    *rc=*elapsed=0* | *rc=*elapsed=1*) printf 'refused' ;;
    *rc=*) printf 'closed' ;;
    *) die "the probe from ${namespace} (${labels}) to ${port} produced no result: ${out}" ;;
  esac
}

CONTROLLER_LABELS="app.kubernetes.io/name=ingress-nginx,app.kubernetes.io/component=controller"
WEBHOOK_LABELS="app.kubernetes.io/name=ingress-nginx,app.kubernetes.io/component=admission-webhook"
TOOLS_LABELS="role=tools"
STRANGER_LABELS="role=stranger"

expect() { # description state namespace labels port
  local description="$1" want="$2" got
  shift 2
  got="$(reach "$@")"
  # A policy is programmed a moment after the apply returns, so an expected refusal may need a retry. A listener that is
  # between two connections answers an instant "refused": retry that too, for an admission as well. An admission that
  # reads "closed" is never retried: nothing in the policy closes it later.
  for _ in 1 2 3 4 5 6; do
    [ "${got}" = "${want}" ] && break
    if [ "${got}" = refused ] || [ "${want}" = closed ]; then
      sleep 5
      got="$(reach "$@")"
    else
      break
    fi
  done
  printf '%-62s %s (want %s)\n' "${description}" "${got}" "${want}"
  [ "${got}" = "${want}" ] || die "${description}: ${got}, want ${want}"
}

printf 'step 3: CONTROL, no policy yet: an unlisted pod reaches every port\n'
for port in 8000 8010 8080 8091; do
  expect "control: stranger -> ${port}" open "${NS}" "${STRANGER_LABELS}" "${port}"
done

[ "$(K -n "${NS}" get pod go-api -o jsonpath='{.status.phase}')" = Running ] || die "the stand-in go-api pod is not running after the control probes"

printf 'step 4: apply the fenced policy\n'
K -n "${NS}" apply -f "${WORK}/fence-on.yaml"
sleep 10

printf 'step 5: the fence refuses what it must\n'
for port in 8000 8010 8091; do
  expect "stranger (in namespace) -> ${port}" closed "${NS}" "${STRANGER_LABELS}" "${port}"
  expect "controller-namespace pod without controller labels -> ${port}" closed "${CONTROLLER_NS}" "${WEBHOOK_LABELS}" "${port}"
  expect "controller-labelled pod of another namespace -> ${port}" closed "${OTHER_NS}" "${CONTROLLER_LABELS}" "${port}"
done

printf 'step 6: the fence admits what it must\n'
for port in 8000 8010; do
  expect "ingress controller pod -> ${port}" open "${CONTROLLER_NS}" "${CONTROLLER_LABELS}" "${port}"
  expect "allowedFrom tools pod -> ${port}" open "${NS}" "${TOOLS_LABELS}" "${port}"
done
expect "ingress controller pod -> 8091 (internal, own rule)" closed "${CONTROLLER_NS}" "${CONTROLLER_LABELS}" 8091
expect "allowedFrom tools pod -> 8091 (internal, own rule)" closed "${NS}" "${TOOLS_LABELS}" 8091
for labels_ns in "${NS}|${STRANGER_LABELS}" "${CONTROLLER_NS}|${WEBHOOK_LABELS}" "${OTHER_NS}|${CONTROLLER_LABELS}"; do
  expect "operator port 8080 stays open: ${labels_ns}" open "${labels_ns%%|*}" "${labels_ns#*|}" 8080
done

[ "$(K -n "${NS}" get pod go-api -o jsonpath='{.status.phase}')" = Running ] || die "the stand-in go-api pod stopped during the proof"

printf 'step 7: ROLLBACK, the fence off over the same policy opens the request ports again\n'
K -n "${NS}" apply -f "${WORK}/fence-off.yaml"
sleep 10
for port in 8000 8010 8080; do
  expect "after rollback: stranger -> ${port}" open "${NS}" "${STRANGER_LABELS}" "${port}"
done
expect "after rollback: stranger -> 8091 (internal stays closed)" closed "${NS}" "${STRANGER_LABELS}" 8091

printf 'go-api fence proof: OK\n'
