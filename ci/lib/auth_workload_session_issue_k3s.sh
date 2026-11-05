#!/usr/bin/env bash
# The k3s form of ci/lib/auth_workload_session_issue.sh: go-api's session issue credential is its workload token, from the bootstrap
# credential of the api workload. Sourced; it sets no shell option and never exits the caller.
#   auth_workload_session_issue_k3s K_FN PGQ_FN MIG_IMAGE HOLD_IMAGE SECRET_NAME
#     K_FN         the caller's namespace-scoped kubectl function (never a cluster-scoped one)
#     PGQ_FN       a function that runs the SQL on its stdin in the proof database and prints the rows (psql -At)
#     MIG_IMAGE    the auth-migrate image (a registry digest reference)
#     HOLD_IMAGE   an image that has sleep and cat (the proof's own PostgreSQL image): the pod's one long-lived container
#     SECRET_NAME  the Secret that gets the credential, key api.bootstrap (goApi.sessionIssue.workloadBootstrap.secretName)
# One pod, fb-wl-bootstrap: init container seed runs `workloads seed -apply`, init container bootstrap runs `workloads
# bootstrap-create -workload api -out /work/api.bootstrap -apply`, both as the migration role (the auth-migrate-db Secret); the
# credential file lives in the pod's memory volume. It then goes from `cat` in the pod to the Secret on stdin: never a command line,
# never output, never a pod log. The pod is deleted. The wire principal id of the api workload (ci/pdp_load_k3s/subject.sql) is read
# and exported as AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT, for authService.extraEnv.
# Call it after the auth migrations and the restore-fence init ran, and once per namespace: bootstrap-create refuses a workload that
# holds a live credential. Returns 0, or 1 with an ERROR line naming the step.

_AUTH_WORKLOAD_K3S_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_auth_workload_k3s_pod() { # MIG_IMAGE HOLD_IMAGE
  cat <<EOF
apiVersion: v1
kind: Pod
metadata: {name: fb-wl-bootstrap}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532, seccompProfile: {type: RuntimeDefault}}
  volumes: [{name: work, emptyDir: {medium: Memory}}]
  initContainers:
    - name: seed
      image: $1
      args: [workloads, seed, -apply]
      env: &migenv
        - {name: AUTH_MIGRATION_DATABASE_URI, valueFrom: {secretKeyRef: {name: auth-migrate-db, key: AUTH_MIGRATION_DATABASE_URI}}}
        - {name: AUTH_RUNTIME_DATABASE_ROLE, value: devhealth_auth}
      securityContext: &hardened {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
    - name: bootstrap
      image: $1
      args: [workloads, bootstrap-create, -workload, api, -out, /work/api.bootstrap, -apply]
      env: *migenv
      securityContext: *hardened
      volumeMounts: [{name: work, mountPath: /work}]
  containers:
    - name: hold
      image: $2
      imagePullPolicy: IfNotPresent
      command: [sleep, "600"]
      securityContext: *hardened
      volumeMounts: [{name: work, mountPath: /work, readOnly: true}]
EOF
}

auth_workload_session_issue_k3s() {
  local k="$1" pgq="$2" mig="$3" hold="$4" secret="$5" subject container
  local -a rc
  if ! _auth_workload_k3s_pod "${mig}" "${hold}" | "${k}" apply -f - >/dev/null; then
    printf 'ERROR: the workload bootstrap pod was not created\n' >&2
    return 1
  fi
  if ! "${k}" wait --for=condition=Ready pod/fb-wl-bootstrap --timeout=300s >/dev/null; then
    for container in seed bootstrap; do
      "${k}" logs pod/fb-wl-bootstrap -c "${container}" --tail 20 >&2 || true
    done
    printf 'ERROR: the workload bootstrap pod did not become Ready (the seed or the bootstrap-create init container failed)\n' >&2
    return 1
  fi
  "${k}" exec fb-wl-bootstrap -c hold -- cat /work/api.bootstrap | "${k}" create secret generic "${secret}" --from-file=api.bootstrap=/dev/stdin >/dev/null
  rc=("${PIPESTATUS[@]}")
  if [ "${rc[0]}" != 0 ] || [ "${rc[1]}" != 0 ]; then
    printf 'ERROR: the bootstrap credential did not reach the Secret %s\n' "${secret}" >&2
    return 1
  fi
  if ! "${k}" delete pod fb-wl-bootstrap --grace-period=1 --wait=true >/dev/null; then
    printf 'ERROR: the workload bootstrap pod was not removed\n' >&2
    return 1
  fi
  if ! subject="$("${pgq}" <"${_AUTH_WORKLOAD_K3S_LIB_DIR}/../pdp_load_k3s/subject.sql")"; then
    printf 'ERROR: could not read the api workload principal\n' >&2
    return 1
  fi
  if ! [[ "${subject}" =~ ^prn_[A-Za-z0-9_-]{22}$ ]]; then
    printf 'ERROR: the api workload principal id is not a wire principal id (%s)\n' "${subject}" >&2
    return 1
  fi
  export AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT="${subject}"
  printf 'session issue credential: go-api workload token (subject %s, Secret %s)\n' "${subject}" "${secret}"
}
