#!/usr/bin/env bash
# Cells for the argument, guard and wiring paths of ci/rehearse_auth_key_rotation_k3s.sh. Hermetic: no cluster. A fake kubectl
# (and a fake helm for the cluster cells) on PATH records every call; KUBECONFIG points at /dev/null so no default
# kubeconfig is ever read. Run: bash ci/rehearse_auth_key_rotation_k3s_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT}/ci/rehearse_auth_key_rotation_k3s.sh"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT
mkdir -p "${T}/bin" "${T}/binh"
cat >"${T}/bin/kubectl" <<'EOS'
#!/usr/bin/env bash
echo "kubectl $*" >> "${FAKE_LOG}"
case "$*" in
  *"version -o json"*) printf '{"serverVersion":{"gitVersion":"%s"}}\n' "${FAKE_VERSION:-v1.30.2+k3s1}" ;;
  *"auth can-i --list"*) [ -z "${FAKE_CAN_I_EMPTY:-}" ] || exit 0
EOS
# shellcheck disable=SC2129
cat >>"${T}/bin/kubectl" <<'EOS'
    printf '%s\n' "${FAKE_CAN_I_HEADER:-Resources                                       Non-Resource URLs   Resource Names   Verbs}" \
    'selfsubjectreviews.authentication.k8s.io        []                  []               [create]' \
    'selfsubjectaccessreviews.authorization.k8s.io   []                  []               [create]' \
EOS
# shellcheck disable=SC2129
cat >>"${T}/bin/kubectl" <<'EOS'
    'selfsubjectrulesreviews.authorization.k8s.io    []                  []               [create]' \
    '                                                [/api/*]            []               [get]' \
    '                                                [/healthz]          []               [get]' \
EOS
# shellcheck disable=SC2129
cat >>"${T}/bin/kubectl" <<'EOS'
    "namespaces                                      []                  [${REHEARSAL_NAMESPACE:-authrot-test}]   [get]"
    [ -z "${FAKE_CAN_I_EXTRA:-}" ] || printf '%s\n' "${FAKE_CAN_I_EXTRA}" ;;
  *"auth can-i list nodes"*) echo "${FAKE_CAN_I_NODES:-no}" ;;
EOS
# shellcheck disable=SC2129
cat >>"${T}/bin/kubectl" <<'EOS'
  *"auth can-i create clusterroles"*) echo "${FAKE_CAN_I_CLUSTERROLES:-no}" ;;
  *"config view"*) printf '%s' "${FAKE_SERVER:-https://127.0.0.1:6443}" ;;
  *"--ignore-not-found -o name"*) printf '%s' "${FAKE_EXISTING:-}" ;;
  *"get configmap rehearsal-owner"*) printf '%s' "${FAKE_OWNER_TOKEN-0123456789abcdef0123}" ;;
EOS
# shellcheck disable=SC2129
cat >>"${T}/bin/kubectl" <<'EOS'
  *"get namespace "*) if [ -n "${FAKE_NS_DENIED:-}" ]; then echo 'Error from server (Forbidden): namespaces is forbidden' >&2; exit 1; fi; printf '%s' "${FAKE_NS_LABEL-0123456789abcdef0123}" ;;
  *) exit 98 ;;
esac
EOS
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' 'echo "helm $*" >> "${FAKE_LOG}"' 'case "$*" in *" list "*) printf "%s" "${FAKE_HELM_RELEASE:-}"; exit 0 ;; esac' 'exit 99' >"${T}/binh/helm"
chmod +x "${T}/bin/kubectl" "${T}/binh/helm"
export FAKE_LOG="${T}/calls.log"
export KUBECONFIG=/dev/null
: >"${FAKE_LOG}"
failed=0
ok() { printf 'PASS: %s\n' "$1"; }
bad() { printf 'FAIL: %s\n' "$1"; failed=1; }
run() { PATH="${T}/bin:${T}/binh:${PATH}" "$@"; }

TOKEN=0123456789abcdef0123 # gitleaks:allow
SA="${T}/sa.kubeconfig"
printf 'apiVersion: v1\nkind: Config\n' >"${SA}"
chmod 0600 "${SA}"
z="$(printf '0%.0s' $(seq 1 64))"
printf 'DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE=registry.invalid/m@sha256:%s\nDEV_HEALTH_GO_AUTH_SERVICE_IMAGE=registry.invalid/s@sha256:%s\n' "${z}" "${z}" >"${T}/images.env"
y="$(printf '1%.0s' $(seq 1 64))"
printf 'DEV_HEALTH_GO_AUTH_SERVICE_IMAGE=registry.invalid/s@sha256:%s\n' "${y}" >"${T}/previous.env"
printf 'DEV_HEALTH_GO_AUTH_SERVICE_IMAGE=registry.invalid/s:previous\n' >"${T}/previous-tag.env"
printf 'DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE=registry.invalid/m:latest\nDEV_HEALTH_GO_AUTH_SERVICE_IMAGE=registry.invalid/s@sha256:%s\n' "${z}" >"${T}/tag.env"

# go VAR=VALUE...: run the driver with a complete good environment, minus or plus the given overrides -> rc, out
go() {
  rc=0
  out="$(env REHEARSAL_KUBECONFIG="${SA}" REHEARSAL_NAMESPACE=authrot-test REHEARSAL_RUN_TOKEN="${TOKEN}" \
    REHEARSAL_SERVER=https://127.0.0.1:6443 REHEARSAL_DIR="${T}/d$((++n))" REHEARSAL_IMAGES_ENV="${T}/images.env" REHEARSAL_PREVIOUS_IMAGES_ENV="${T}/previous.env" "$@" \
    env PATH="${T}/bin:${T}/binh:${PATH}" bash "${SCRIPT}" --stage rollback 2>&1)" || rc=$?
}
n=0
refused() { # refused NAME WANT_TEXT VAR=VALUE...
  local name="$1" want="$2"
  shift 2
  : >"${FAKE_LOG}"
  go "$@"
  if [ "${rc}" = 1 ] && grep -qF -- "${want}" <<<"${out}"; then ok "${name}"; else bad "${name} (rc ${rc})"; fi
}
no_write_calls() { ! grep -qE ' (create|apply|run|delete|port-forward|exec|rollout) |^helm ' < <(grep -v 'auth can-i' "${FAKE_LOG}" | grep -v '^helm .* list '); }

# plan: real helm template, no cluster call
: >"${FAKE_LOG}"
plan="$(PATH="${T}/bin:${PATH}" bash "${SCRIPT}" --print-plan 2>&1)" || plan="rc-failed: ${plan}"
for needle in 'stage rollback' 'S3 chart' 'PASS: plan: the chart renders class eddsa' 'previous auth-service image' 'namespace-scoped ServiceAccount kubeconfig only'; do
  if grep -qF -- "${needle}" <<<"${plan}"; then ok "plan-names-${needle// /-}"; else bad "plan-names-${needle// /-}"; fi
done
if [ ! -s "${FAKE_LOG}" ]; then ok plan-makes-no-cluster-call; else bad plan-makes-no-cluster-call; fi

rc=0
run bash "${SCRIPT}" --stage nonsense >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 1 ]; then ok unknown-stage-refused; else bad "unknown-stage-refused (rc ${rc})"; fi
rc=0
run bash -x "${SCRIPT}" --print-plan >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 2 ]; then ok xtrace-refused; else bad "xtrace-refused (rc ${rc})"; fi
rc=0
env -u REHEARSAL_KUBECONFIG run bash "${SCRIPT}" >/dev/null 2>&1 || rc=$?
if [ "${rc}" != 0 ]; then ok no-kubeconfig-refused; else bad no-kubeconfig-refused; fi

ADMIN="${T}/admin/k3s.yaml"
mkdir -p "${T}/admin"
cp "${SA}" "${ADMIN}"
chmod 0600 "${ADMIN}"
# only the exact admin path (resolved) is refused, by name and before any call; a valid ServiceAccount file that happens to be named k3s.yaml is not
: >"${FAKE_LOG}"
go REHEARSAL_KUBECONFIG="${ADMIN}" REHEARSAL_DIR="${T}/named-k3s"
if grep -qF 'step 1: postgres' <<<"${out}" && ! grep -qF 'k3s admin kubeconfig' <<<"${out}"; then ok sa-kubeconfig-named-k3s-yaml-accepted; else bad "sa-kubeconfig-named-k3s-yaml-accepted (rc ${rc})"; fi
ln -s /etc/rancher/k3s/k3s.yaml "${T}/link-to-admin.yaml"
refused symlink-to-admin-kubeconfig-refused 'k3s admin kubeconfig' REHEARSAL_KUBECONFIG="${T}/link-to-admin.yaml"
if [ ! -s "${FAKE_LOG}" ]; then ok symlink-to-admin-kubeconfig-refused-before-any-call; else bad symlink-to-admin-kubeconfig-refused-before-any-call; fi
refused dotdot-path-to-admin-kubeconfig-refused 'k3s admin kubeconfig' REHEARSAL_KUBECONFIG=/etc/rancher/k3s/../k3s/k3s.yaml
refused default-admin-path-refused 'k3s admin kubeconfig' REHEARSAL_KUBECONFIG=/etc/rancher/k3s/k3s.yaml
cp "${SA}" "${T}/open.kubeconfig"
chmod 0644 "${T}/open.kubeconfig"
refused kubeconfig-mode-refused 'mode 0600' REHEARSAL_KUBECONFIG="${T}/open.kubeconfig"
refused namespace-default-refused 'refusing the namespace' REHEARSAL_NAMESPACE=default
refused namespace-kube-system-refused 'refusing the namespace' REHEARSAL_NAMESPACE=kube-system
# an ALLOWLIST: only authrot-<suffix> passes; production-looking and bare names are refused with no cluster call at all
for ns in prod production prod-auth staging authrot rehearsal-x; do
  refused "namespace-${ns}-refused-allowlist" 'not a rehearsal namespace' REHEARSAL_NAMESPACE="${ns}"
  if [ ! -s "${FAKE_LOG}" ]; then ok "namespace-${ns}-refused-before-any-call"; else bad "namespace-${ns}-refused-before-any-call"; fi
done
# the cluster target: the kubeconfig must point at the one API server the lead names (REHEARSAL_SERVER); no write before that
refused wrong-server-refused 'is not the rehearsal target' FAKE_SERVER=https://10.9.9.9:6443
if no_write_calls && ! grep -qE ' (version|auth|get) ' "${FAKE_LOG}"; then ok wrong-server-refused-before-any-cluster-call; else bad wrong-server-refused-before-any-cluster-call; fi
refused no-server-env-refused 'REHEARSAL_SERVER' REHEARSAL_SERVER=
if no_write_calls; then ok no-server-env-refused-before-any-write; else bad no-server-env-refused-before-any-write; fi
refused namespace-odd-name-refused 'plain namespace name' 'REHEARSAL_NAMESPACE=Bad;ns'
refused non-hex-token-refused 'hex token' REHEARSAL_RUN_TOKEN=nothex
refused tag-image-refused 'not a registry digest reference' REHEARSAL_IMAGES_ENV="${T}/tag.env"
refused previous-tag-image-refused 'not a registry digest reference' REHEARSAL_PREVIOUS_IMAGES_ENV="${T}/previous-tag.env"
refused previous-image-file-missing-refused 'names no file' REHEARSAL_PREVIOUS_IMAGES_ENV="${T}/nofile.env"
for bad_id in "svc_authsess_$(printf '%043d' 0)" "postgresql://db.internal:5432/ops"; do
  refused run-id-refused 'REHEARSAL_RUN_ID' REHEARSAL_RUN_ID="${bad_id}"
  if ! grep -qF -- "${bad_id}" <<<"${out}"; then ok run-id-value-not-echoed; else bad run-id-value-not-echoed; fi
done

# cluster guards: each refusal comes before any write call
refused non-k3s-refused 'not k3s' FAKE_VERSION=v1.30.2
if no_write_calls; then ok non-k3s-refused-before-any-write; else bad non-k3s-refused-before-any-write; fi
refused admin-capable-refused 'can list nodes' FAKE_CAN_I_NODES=yes
if no_write_calls; then ok admin-capable-refused-before-any-write; else bad admin-capable-refused-before-any-write; fi
refused cluster-role-capable-refused 'can create clusterroles' FAKE_CAN_I_CLUSTERROLES=yes
# the cluster-scope rule list: only get on the one Namespace object (resourceNames = the run namespace), the self-review creates and non-resource gets pass
refused delete-namespaces-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='namespaces                                      []                  []               [delete]'
if no_write_calls; then ok delete-namespaces-refused-before-any-write; else bad delete-namespaces-refused-before-any-write; fi
refused get-all-namespaces-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='namespaces                                      []                  []               [get list]'
refused get-other-namespace-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='namespaces                                      []                  [prod]           [get]'
refused cluster-secrets-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='secrets                                         []                  []               [get]'
refused wildcard-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='*.*                                             []                  []               [*]'
if no_write_calls; then ok wildcard-refused-before-any-write; else bad wildcard-refused-before-any-write; fi
# fail closed: a table form or a row the parser does not know is refused, never skipped
refused unparsable-row-refused 'cluster-scope permission list' FAKE_CAN_I_EXTRA='namespaces delete'
if no_write_calls; then ok unparsable-row-refused-before-any-write; else bad unparsable-row-refused-before-any-write; fi
refused extra-column-row-refused 'cluster-scope permission' FAKE_CAN_I_EXTRA='namespaces                                      []                  []               []               [delete]'
refused reordered-header-refused 'cluster-scope permission list' FAKE_CAN_I_HEADER='Resources   Resource Names   Non-Resource URLs   Verbs'
if no_write_calls; then ok reordered-header-refused-before-any-write; else bad reordered-header-refused-before-any-write; fi
refused extra-header-column-refused 'cluster-scope permission list' FAKE_CAN_I_HEADER='Resources   Non-Resource URLs   Resource Names   Verbs   Extra'
refused empty-list-refused 'cluster-scope permission list' FAKE_CAN_I_EMPTY=1
if no_write_calls; then ok empty-list-refused-before-any-write; else bad empty-list-refused-before-any-write; fi
# the ownership proof is the namespace's rehearsal-run label; a ConfigMap token (the fake still answers one) proves nothing
refused foreign-namespace-refused 'does not carry this run' FAKE_NS_LABEL=ffffffffffffffffffff
if no_write_calls; then ok foreign-namespace-refused-before-any-write; else bad foreign-namespace-refused-before-any-write; fi
refused unlabelled-namespace-refused 'does not carry this run' FAKE_NS_LABEL=
if no_write_calls; then ok unlabelled-namespace-refused-before-any-write; else bad unlabelled-namespace-refused-before-any-write; fi
refused unlabelled-namespace-with-matching-configmap-refused 'does not carry this run' FAKE_NS_LABEL= FAKE_OWNER_TOKEN="${TOKEN}"
if no_write_calls; then ok unlabelled-namespace-with-matching-configmap-refused-before-any-write; else bad unlabelled-namespace-with-matching-configmap-refused-before-any-write; fi
if grep -qF 'metadata.labels.rehearsal-run' "${FAKE_LOG}"; then ok guard-reads-the-rehearsal-run-label; else bad guard-reads-the-rehearsal-run-label; fi
# a denied or failed namespace read fails loudly with its own message, before any write (not the "does not carry" message)
refused ns-read-denied-refused 'the namespace read of authrot-test was denied or failed' FAKE_NS_DENIED=1
if no_write_calls; then ok ns-read-denied-refused-before-any-write; else bad ns-read-denied-refused-before-any-write; fi
if ! grep -qF 'does not carry this run' <<<"${out}"; then ok ns-read-denied-message-is-not-the-label-message; else bad ns-read-denied-message-is-not-the-label-message; fi
# fixed names: a pre-existing object (or the helm release) refuses the run before any write, and the run deletes nothing it did not create
for existing in secret/auth-pg-superuser deployment/auth-postgres service/auth-valkey secret/rehearsal-db; do
  refused "preexisting-${existing//\//-}-refused" 'already exists' FAKE_EXISTING="${existing}"
  if no_write_calls; then ok "preexisting-${existing//\//-}-refused-before-any-write"; else bad "preexisting-${existing//\//-}-refused-before-any-write"; fi
  if ! grep -qE ' delete |^helm (uninstall|rollback)' "${FAKE_LOG}"; then ok "preexisting-${existing//\//-}-not-deleted"; else bad "preexisting-${existing//\//-}-not-deleted"; fi
done
refused preexisting-helm-release-refused 'already exists' FAKE_HELM_RELEASE=rot
if no_write_calls && ! grep -qE '^helm (uninstall|rollback|upgrade)' "${FAKE_LOG}"; then ok preexisting-helm-release-not-touched; else bad preexisting-helm-release-not-touched; fi
# the label matches: the guard passes and the run reaches its first write, which the fake stops
: >"${FAKE_LOG}"
go FAKE_OWNER_TOKEN= REHEARSAL_DIR="${T}/labelled"
if grep -qF 'step 1: postgres' <<<"${out}" && grep -qE ' create secret ' "${FAKE_LOG}"; then ok labelled-namespace-proceeds-to-the-first-write; else bad "labelled-namespace-proceeds-to-the-first-write (rc ${rc})"; fi
# the run stopped at its first create: cleanup deletes exactly what was recorded before that create, and nothing else
deleted="$(grep -E ' delete ' "${FAKE_LOG}" | sed 's/.* delete //; s/ --ignore-not-found.*//' | tr '\n' ' ')"
if [ "${deleted}" = 'secret/auth-pg-superuser ' ]; then ok cleanup-deletes-only-what-the-run-recorded; else bad "cleanup-deletes-only-what-the-run-recorded (${deleted})"; fi

# namespace names: the full Kubernetes label syntax (RFC 1123, 1 to 63 characters) passes the name guard; the fake stops each at the first write
for ns in authrot-a authrot-qa "authrot-$(printf 'a%.0s' $(seq 1 55))" authrot-a-b authrot-password; do
  : >"${FAKE_LOG}"
  go REHEARSAL_NAMESPACE="${ns}" REHEARSAL_DIR="${T}/ns-${#ns}-${ns:0:3}"
  if grep -qF 'step 1: postgres' <<<"${out}" && ! grep -qF 'plain namespace name' <<<"${out}"; then ok "namespace-accepted-len-${#ns}-${ns:0:3}"; else bad "namespace-accepted-len-${#ns}-${ns:0:3}"; fi
done
for ns in "$(printf 'a%.0s' $(seq 1 64))" Upper -lead trail- 'a.b' a_b; do
  refused "namespace-refused-len-${#ns}-${ns:0:3}" 'plain namespace name' REHEARSAL_NAMESPACE="${ns}"
  if no_write_calls; then ok "namespace-refused-len-${#ns}-${ns:0:3}-before-any-write"; else bad "namespace-refused-len-${#ns}-${ns:0:3}-before-any-write"; fi
done
mkdir -p "${T}/full"
echo x >"${T}/full/f"
refused non-empty-dir-refused 'not empty' REHEARSAL_DIR="${T}/full"
if no_write_calls; then ok non-empty-dir-refused-before-any-write; else bad non-empty-dir-refused-before-any-write; fi
if [ ! -e "${T}/helm.called" ] && ! grep -q '^helm ' < <(grep -v '^helm .* list ' "${FAKE_LOG}"); then ok no-helm-call-before-the-guards; else bad no-helm-call-before-the-guards; fi

# wiring: read from the script text
body="$(grep -vE '^[[:space:]]*#' "${SCRIPT}")"
q="'"
allowed="command -v|for tool in|^[0-9]+:(kc|hm)\(\)|die |needs:|usage|^[0-9]+:[[:space:]]*[${q}]|^[0-9]+:say |KUBECONFIG=/dev/null helm template"
bare="$(grep -nE '(^|[^-a-zA-Z_./"])(kubectl|helm)[[:space:]]' <<<"${body}" | grep -vE "${allowed}" || true)"
if [ -z "${bare}" ]; then ok every-cluster-call-goes-through-kc-or-hm; else bad "every-cluster-call-goes-through-kc-or-hm: ${bare}"; fi
if grep -qE '^kc\(\) \{ kubectl --kubeconfig "\$\{KCFG\}"' <<<"${body}" && grep -qE '^hm\(\) \{ helm --kubeconfig "\$\{KCFG\}" --namespace "\$\{NS\}"' <<<"${body}"; then ok wrappers-carry-the-kubeconfig-flag; else bad wrappers-carry-the-kubeconfig-flag; fi
if ! grep -qE '(create|delete) (namespace|ns)|namespace.*--create-namespace|--create-namespace' <<<"${body}"; then ok never-creates-or-deletes-a-namespace; else bad never-creates-or-deletes-a-namespace; fi
if ! grep -q -- '--from-literal' <<<"${body}"; then ok no-secret-value-on-argv; else bad no-secret-value-on-argv; fi
if grep -qF 'shred -u' <<<"${body}" && grep -qE 'trap cleanup EXIT' <<<"${body}"; then ok keys-shredded-on-exit; else bad keys-shredded-on-exit; fi
# the compromise needs the publish floor: the dry-run activate of b gates it, and the floor is the real 300 s
if grep -qE '^readonly PENDING_FLOOR=300$' <<<"${body}" && grep -q 'keys_cmd activate -kid b' <<<"${body}"; then ok real-publish-floor-gates-the-compromise; else bad real-publish-floor-gates-the-compromise; fi
# the rollback is the chart's own: an upgrade to the previous auth-service image (no class flip: hs256_compat is retired), then helm rollback to the eddsa revision; a stays absent and compromised after both
for cell in rollback-image-is-the-previous-one forward-image-is-the-current-one rollback-a-still-absent-from-the-jwks rollback-a-still-compromised rollback-key-a-refused-terminal forward-a-still-absent-from-the-jwks forward-a-still-compromised; do
  if grep -qE "^check ${cell} " <<<"${body}"; then ok "has-${cell}"; else bad "has-${cell}"; fi
done
if ! grep -qF 'hs256_compat' <<<"${body}"; then ok script-has-no-hs256-compat-class-flip; else bad script-has-no-hs256-compat-class-flip; fi
if grep -qE '^hm rollback "\$\{RELEASE\}" "\$\{EVIDENCE_EDDSA_REVISION\}"' <<<"${body}"; then ok chart-rollback-targets-the-eddsa-revision; else bad chart-rollback-targets-the-eddsa-revision; fi

# The ServiceAccount of the rehearsal gets one namespaced Role (Role + RoleBinding) and, for the one cluster-scoped read of its own Namespace object,
# one ClusterRole + ClusterRoleBinding named with the run namespace: verbs [get], resources [namespaces], resourceNames = that namespace, bound to the
# driver ServiceAccount only (pm 11-03). The lead applies the fixture. No RBAC resource in the Role rules, no wildcard resource or group.
ROLE="${ROOT}/ci/auth_k8s/rotation_rehearsal_role.yaml"
doc_of() { awk -v k="$1" '/^---$/ { n++; next } { d[n] = d[n] $0 "\n" } END { for (i in d) if (d[i] ~ ("(^|\n)kind: " k "\n")) printf "%s", d[i] }'; }
if [ -f "${ROLE}" ]; then
  # shellcheck disable=SC2016
  fixture="$(NS=authrot-test envsubst '${NS}' <"${ROLE}")"
  role_kinds="$(sed -n 's/^kind: //p' <<<"${fixture}" | sort -u | tr '\n' ' ')"
  if [ "${role_kinds}" = "ClusterRole ClusterRoleBinding Role RoleBinding " ]; then ok role-fixture-kinds-are-the-four-expected; else bad "role-fixture-kinds-are-the-four-expected (${role_kinds})"; fi
  role_rules="$(doc_of Role <<<"${fixture}" | sed -n '/^rules:/,$p')"
  if ! grep -qE 'rbac\.authorization\.k8s\.io' <<<"${role_rules}"; then ok role-fixture-rules-grant-no-rbac; else bad role-fixture-rules-grant-no-rbac; fi
  if ! grep -qE 'resources: \[[^]]*\*|apiGroups: \[[^]]*\*|^ *- \*' <<<"${role_rules}"; then ok role-fixture-has-no-wildcard-resource-or-group; else bad role-fixture-has-no-wildcard-resource-or-group; fi
  cr="$(doc_of ClusterRole <<<"${fixture}" | grep -vE '^[[:space:]]*#' || true)"
  crb="$(doc_of ClusterRoleBinding <<<"${fixture}" | grep -vE '^[[:space:]]*#' || true)"
  want_rules="$(printf '%s\n' 'rules:' '  - apiGroups: [""]' '    resources: [namespaces]' '    resourceNames: [authrot-test]' '    verbs: [get]')"
  if [ "$(sed -n '/^rules:/,$p' <<<"${cr}")" = "${want_rules}" ]; then ok clusterrole-is-get-namespaces-of-the-one-namespace; else bad clusterrole-is-get-namespaces-of-the-one-namespace; fi
  if grep -qxF 'metadata: {name: rehearsal-ns-authrot-test}' <<<"${cr}"; then ok clusterrole-named-with-the-run-namespace; else bad clusterrole-named-with-the-run-namespace; fi
  if grep -qxF 'metadata: {name: rehearsal-ns-authrot-test}' <<<"${crb}" && grep -qxF 'roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: rehearsal-ns-authrot-test}' <<<"${crb}"; then ok clusterrolebinding-binds-that-clusterrole; else bad clusterrolebinding-binds-that-clusterrole; fi
  if [ "$(sed -n '/^subjects:/,$p' <<<"${crb}")" = 'subjects: [{kind: ServiceAccount, name: rehearsal, namespace: authrot-test}]' ]; then ok clusterrolebinding-has-the-driver-serviceaccount-only; else bad clusterrolebinding-has-the-driver-serviceaccount-only; fi
else
  bad role-fixture-exists
fi
if ! grep -qE 'kind: (Cluster)?Role' "${SCRIPT}" "${ROOT}/ci/auth_k8s/postgres.yaml" "${ROOT}/ci/auth_k8s/valkey.yaml"; then ok driver-manifests-carry-no-rbac-kind; else bad driver-manifests-carry-no-rbac-kind; fi

# A long-lived child must not hold the heavy-slot lock fd (fd 9): every port-forward line closes it.
if grep -q . < <(grep -n 'port-forward' "${SCRIPT}" | grep -v '^[0-9]*:[[:space:]]*#\|printf'); then
  if ! grep -qv '9>&-' < <(grep 'K port-forward' "${SCRIPT}"); then ok port-forward-closes-lock-fd; else bad port-forward-closes-lock-fd; fi
else
  bad port-forward-line-exists
fi

exit "${failed}"
