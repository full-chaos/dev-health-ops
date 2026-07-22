#!/usr/bin/env bash
set -euo pipefail

ops_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="$(cd "$ops_root/.." && pwd)"
acr_root="${DEV_HEALTH_ACR_DIR:-$workspace_root/dev-health-acr}"
chart_dir="$acr_root/deploy/helm/acr"
values_file="$ops_root/deploy/context-fabric/helm-values.yaml"

namespace="${CONTEXT_FABRIC_NAMESPACE:-dev-health}"
release="${CONTEXT_FABRIC_RELEASE:-context-fabric}"
timeout="${CONTEXT_FABRIC_TIMEOUT:-10m}"
kube_context="${KUBE_CONTEXT:-}"
image=""
entitlement_url=""
entitlement_port="443"
local_port="${CONTEXT_FABRIC_LOCAL_PORT:-18080}"
org_id=""
repository=""
credential_name="context-fabric-local"
credential_scope="context:read,evidence:read"
credential_output=""
action="${1:-}"
[[ -n "$action" ]] && shift

usage() {
  cat >&2 <<'USAGE'
usage: scripts/context-fabric-kubernetes.sh <action> [options]

actions:
  render              Render the Ops + ACR Helm integration without a cluster.
  apply               Install or upgrade ACR in the Dev Health Kubernetes namespace.
  wait                Wait for the ACR Deployment to become available.
  status              Show Helm and Kubernetes status for ACR.
  create-credential   Create a repository-scoped ACR credential and write it mode 0600.
  port-forward        Forward the ACR ClusterIP service to loopback for local MCP clients.
  delete              Uninstall ACR; caller-owned Secrets and data stores are retained.

render/apply options:
  --image <registry/acr-api@sha256:digest>   Required immutable ACR image.
  --entitlement-url <https://host[:port]>    Required Dev Health Ops HTTPS origin.

common options:
  --namespace <name>       Kubernetes namespace (default: dev-health).
  --release <name>         Helm release name (default: context-fabric).
  --context <name>         kubectl/Helm context.
  --timeout <duration>     Helm/kubectl timeout (default: 10m).
  --local-port <port>      port-forward loopback port (default: 18080).

create-credential options:
  --org-id <uuid>          Dev Health organization ID.
  --repository <owner/repo>
  --credential-name <name>
  --scope <csv>            Default: context:read,evidence:read.
  --output <path>          Required destination for the one-time token.

Set DEV_HEALTH_ACR_DIR when dev-health-acr is not a sibling checkout.
USAGE
}

fail() {
  printf 'context-fabric-kubernetes: %s\n' "$*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    --namespace) namespace="${2:-}"; shift 2 ;;
    --release) release="${2:-}"; shift 2 ;;
    --context) kube_context="${2:-}"; shift 2 ;;
    --timeout) timeout="${2:-}"; shift 2 ;;
    --image) image="${2:-}"; shift 2 ;;
    --entitlement-url) entitlement_url="${2:-}"; shift 2 ;;
    --local-port) local_port="${2:-}"; shift 2 ;;
    --org-id) org_id="${2:-}"; shift 2 ;;
    --repository) repository="${2:-}"; shift 2 ;;
    --credential-name) credential_name="${2:-}"; shift 2 ;;
    --scope) credential_scope="${2:-}"; shift 2 ;;
    --output) credential_output="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; fail "unknown argument: $1" ;;
  esac
done

[[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail "invalid namespace: $namespace"
[[ "$release" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail "invalid release: $release"
[[ "$local_port" =~ ^[0-9]+$ ]] && ((local_port >= 1 && local_port <= 65535)) || fail "invalid local port: $local_port"

kube_args=()
helm_context_args=()
if [[ -n "$kube_context" ]]; then
  kube_args+=(--context "$kube_context")
  helm_context_args+=(--kube-context "$kube_context")
fi

kube() {
  kubectl "${kube_args[@]}" "$@"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

require_chart() {
  [[ -f "$chart_dir/Chart.yaml" && -f "$chart_dir/values.schema.json" ]] || {
    fail "ACR Helm chart not found at $chart_dir; set DEV_HEALTH_ACR_DIR"
  }
  [[ -f "$values_file" ]] || fail "Ops Context Fabric values not found: $values_file"
}

require_release_inputs() {
  local authority parsed_port
  [[ "$image" =~ @sha256:[0-9a-f]{64}$ ]] || {
    fail "--image must be an immutable image@sha256:<64 lowercase hex> reference"
  }
  [[ "$entitlement_url" == https://* ]] || {
    fail "--entitlement-url must be an HTTPS origin with no path, query, or fragment"
  }
  authority="${entitlement_url#https://}"
  [[ -n "$authority" && "$authority" != *'/'* && "$authority" != *'@'* && "$authority" != *'?'* && "$authority" != *'#'* ]] || {
    fail "--entitlement-url must be an HTTPS origin with no path, query, or fragment"
  }
  parsed_port=""
  if [[ "$authority" =~ ^\[[0-9A-Fa-f:]+\](:([0-9]+))?$ ]]; then
    parsed_port="${BASH_REMATCH[2]:-443}"
  elif [[ "$authority" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:([0-9]+))?$ ]]; then
    parsed_port="${BASH_REMATCH[3]:-443}"
  else
    fail "--entitlement-url contains an invalid host or port"
  fi
  [[ "$parsed_port" =~ ^[0-9]+$ ]] && ((parsed_port >= 1 && parsed_port <= 65535)) || {
    fail "--entitlement-url contains an invalid port"
  }
  entitlement_port="$parsed_port"
}

helm_value_args() {
  HELM_VALUE_ARGS=(
    -f "$values_file"
    --set-string "image.reference=$image"
    --set-string "config.entitlement.url=$entitlement_url"
    --set "networkPolicy.egress.entitlementPort=$entitlement_port"
  )
}

required_secrets=(
  acr-runtime-credentials
  acr-migration-credentials
  acr-entitlement-token
  acr-postgres-ca
  acr-clickhouse-ca
  acr-entitlement-ca
  acr-registry-pull
)

preflight_cluster() {
  require_command kubectl
  kube get namespace "$namespace" >/dev/null 2>&1 || {
    fail "namespace $namespace does not exist; create it and its ACR Secrets first"
  }
  local secret
  for secret in "${required_secrets[@]}"; do
    kube -n "$namespace" get secret "$secret" >/dev/null 2>&1 || {
      fail "required Secret is missing from namespace $namespace: $secret"
    }
  done
}

case "$action" in
  render)
    require_command helm
    require_chart
    require_release_inputs
    helm_value_args
    exec helm template "$release" "$chart_dir" \
      --namespace "$namespace" \
      "${helm_context_args[@]}" \
      "${HELM_VALUE_ARGS[@]}"
    ;;

  apply)
    require_command helm
    require_chart
    require_release_inputs
    preflight_cluster
    helm_value_args
    exec helm upgrade --install "$release" "$chart_dir" \
      --namespace "$namespace" \
      "${helm_context_args[@]}" \
      "${HELM_VALUE_ARGS[@]}" \
      --atomic \
      --wait \
      --timeout "$timeout"
    ;;

  wait)
    require_command kubectl
    kube -n "$namespace" rollout status deployment/context-fabric --timeout="$timeout"
    ;;

  status)
    require_command helm
    require_command kubectl
    helm status "$release" --namespace "$namespace" "${helm_context_args[@]}"
    kube -n "$namespace" get deployment/context-fabric service/context-fabric
    ;;

  create-credential)
    require_command kubectl
    [[ "$org_id" =~ ^[0-9a-fA-F-]{36}$ ]] || fail "--org-id must be a UUID"
    [[ "$repository" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]] || fail "--repository must be owner/repo"
    [[ -n "$credential_output" ]] || fail "--output is required"
    mkdir -p "$(dirname "$credential_output")"
    token="$(
      kube -n "$namespace" exec deployment/context-fabric -c acr-api -- \
        /usr/local/bin/acr-api credentials create \
        --org-id "$org_id" \
        --repository-scope "$repository" \
        --scope "$credential_scope" \
        --name "$credential_name" \
        --actor dev-health-ops-kubernetes
    )"
    [[ "$token" == fcacr_* ]] || fail "credential command returned an invalid token shape"
    (umask 077; printf '%s' "$token" >"$credential_output")
    printf 'ACR credential written to %s (mode 0600).\n' "$credential_output"
    ;;

  port-forward)
    require_command kubectl
    printf 'Forwarding ACR to http://127.0.0.1:%s. Keep this process running.\n' "$local_port" >&2
    printf 'For local MCP testing set ACR_API_ALLOW_INSECURE_LOOPBACK=true.\n' >&2
    exec kubectl "${kube_args[@]}" -n "$namespace" \
      port-forward --address 127.0.0.1 service/context-fabric "${local_port}:8080"
    ;;

  delete)
    require_command helm
    exec helm uninstall "$release" \
      --namespace "$namespace" \
      "${helm_context_args[@]}" \
      --timeout "$timeout"
    ;;

  -h|--help|help|"")
    usage
    [[ -n "$action" ]] || exit 2
    ;;

  *)
    usage
    fail "unknown action: $action"
    ;;
esac
