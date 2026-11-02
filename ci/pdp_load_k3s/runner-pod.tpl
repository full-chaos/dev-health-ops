# The runner pod of ci/pdp_load_k3s.sh. Init containers seed the workload registry and write the bootstrap value into the
# pod's own volume; the value never leaves the pod. The runner image is the one the proofs use for psql, nc and sed.
# Filled by envsubst: IMG_MIG, RUNNER_IMAGE.
apiVersion: v1
kind: Pod
metadata: {name: pdp-runner}
spec:
  restartPolicy: Never
  securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532}
  volumes: [{name: work, emptyDir: {}}]
  initContainers:
    - name: seed
      image: ${IMG_MIG}
      args: [workloads, seed, -apply]
      env: &migenv
        - {name: AUTH_MIGRATION_DATABASE_URI, valueFrom: {secretKeyRef: {name: auth-migrate-db, key: AUTH_MIGRATION_DATABASE_URI}}}
        - {name: AUTH_RUNTIME_DATABASE_ROLE, value: devhealth_auth}
    - name: bootstrap
      image: ${IMG_MIG}
      args: [workloads, bootstrap-create, -workload, api, -out, /work/api.bootstrap, -apply]
      env: *migenv
      volumeMounts: [{name: work, mountPath: /work}]
  containers:
    - name: runner
      image: ${RUNNER_IMAGE}
      imagePullPolicy: IfNotPresent
      command: [sleep, "86400"]
      volumeMounts: [{name: work, mountPath: /work}]
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
