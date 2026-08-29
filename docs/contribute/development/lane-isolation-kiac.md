---
page_id: con-lane-isolation-kiac
summary: Run the whole Dev Health stack one-namespace-per-lane on a local kiac cluster, seeded from backups/, so parallel agents stop contending for the shared Compose stack.
content_type: runbook
owner: engineering
source_of_truth:
  - acr/deploy/local/lane.sh (one-command lane lifecycle; composes the two below)
  - acr/deploy/local/kiac.sh (cluster lifecycle and the image bridge)
  - acr/deploy/local/trial-data.sh (per-namespace Postgres/ClickHouse/FalkorDB and the @backups restores)
  - deploy/helm/dev-health (ops chart) and the ACR chart (workloads)
  - scripts/worker/provision_river_roles.sql (runtime role provisioning)
applicability: current
lifecycle: active
---

# Lane isolation on a kiac cluster

Run the **whole** Dev Health stack — Postgres, PgBouncer, ClickHouse, Valkey,
FalkorDB, the ops API, the metrics API, the Go worker family, web, and the ACR
hosted runtime plus its projector — inside one local Kubernetes cluster, with
**one namespace per lane**. Each lane gets its own datastores seeded from
`backups/`, its own migrations, and its own worker fleet, so parallel agents
stop contending for the shared Docker Compose stack.

The cluster is provisioned by [kiac](https://github.com/kiac-dev/kiac) on
Apple's `container` runtime, entirely outside any Docker daemon, so it never
competes with Compose or with host Testcontainers.

Tracked by CHAOS-4428. The runtime interface lives in the ACR repository
(`deploy/local/kiac.sh`, `deploy/local/trial-data.sh`) because that is where the
kiac pilot (CHAOS-4051/4055) and the trial data plane (CHAOS-4186) already live.

## What isolation buys you

These standing rules exist only because lanes share one Compose stack. **For a
cluster-isolated lane they do not apply:**

| Standing rule (shared Compose stack) | Cluster-isolated lane |
| --- | --- |
| Downgrade an unmerged Alembic revision when you finish, so you do not break another lane's `migrate` | **Not needed.** `alembic_version` is per database and each namespace has its own Postgres. Two namespaces sat at 0115 and 0109 simultaneously with both stacks healthy. |
| Give a cross-domain heads-up before `compose up -d --build` | **Not needed.** `helm upgrade` touches one namespace. |
| Take the host-wide `local_validate` lock | **Still required — no exemption yet.** `ci/local_validate.sh`'s `main` calls `acquire_lock` unconditionally (`ci/local_validate.sh:1408`) and its ClickHouse provisioning still probes for the Compose Docker container (`ci/local_validate.sh:901-940`), so pointing DSNs at a namespace does not make the lock optional — a run either serializes behind it anyway or fails outright without Compose. Keep taking the lock until the script grows an explicit remote-ClickHouse mode. |
| Never write to the shared Postgres/ClickHouse | **Not needed.** Write freely; the blast radius is the namespace. |

What does **not** change: never touch another lane's namespace, never run
`kiac down`/`delete`/`prune` against a cluster you did not create, and never run
host-wide `container system` verbs — those stop every kiac VM on the machine.

## Topology

```mermaid
flowchart TB
    subgraph host["macOS host"]
        backups[("backups/<br/>postgres-all-*.sql.gz<br/>clickhouse-*.zip")]
        docker["Docker Desktop<br/>(shared Compose stack —<br/>untouched)"]
        images["docker buildx build<br/>→ docker save<br/>→ container image load<br/>→ kiac.sh load-image"]
    end

    subgraph cluster["kiac cluster 'dev-full' — one node VM (apple/container)"]
        subgraph lanea["namespace lane-a — NodePort base 30500"]
            apga[("trial-postgres")]
            acha[("trial-clickhouse")]
            afka[("trial-falkordb")]
            aapp["ops api · metrics-api · web · valkey<br/>3 × pgbouncer · 9 go-worker groups"]
            aacr["acr-api · acr-projector"]
            aapp --> apga & acha
            aacr --> apga & acha & afka
        end
        subgraph laneb["namespace lane-b — NodePort base 30510"]
            bpga[("trial-postgres")]
            bcha[("trial-clickhouse")]
            bfka[("trial-falkordb")]
            bapp["ops api · metrics-api · web · valkey<br/>3 × pgbouncer · 9 go-worker groups"]
            bacr["acr-api · acr-projector"]
            bapp --> bpga & bcha
            bacr --> bpga & bcha & bfka
        end
    end

    backups -->|restore-postgres / restore-clickhouse| apga & acha
    backups -->|restore-postgres / restore-clickhouse| bpga & bcha
    images -->|containerd, imagePullPolicy Never| cluster
    docker -. no dependency .-> cluster
```

Namespaces are the isolation boundary, not nodes: a kiac node's CPU and memory
are fixed at **create time** (there is no live-resize path), so a node per lane
would freeze the split before the lanes exist and resizing would cost a cluster
recreate, which destroys the PVC data. Namespaces cost one Postgres, one
ClickHouse and one FalkorDB pod per lane and can be added and removed freely.

## The short path: `lane.sh`

Everything under **Recipe** below is what a lane needs. You do not have to run it
by hand: `acr/deploy/local/lane.sh` composes the pieces that already exist —
`kiac.sh` owns the cluster and the image bridge, `trial-data.sh` owns the
per-namespace datastores and the `@backups` restores — into one command.

**`up` does not do everything in the Recipe.** On a cold machine, or after an
image cleanup, do these first or `up` fails before a lane exists:

- **Build and bridge the images** — Recipe step 2, stages 1–3 (`docker build`,
  tag the web image, `docker save` → `container image load`), so every
  required image already sits in the Docker daemon or the apple/container
  image store. `lane.sh` loads images **into the cluster** itself
  (`kiac.sh load-image`), but it does not build or bridge them.
- **Unless `LANE_SKIP_ACR=1`: the evidence-key files must already exist** —
  `$MONO_ROOT/.acr-dev/evidence-kid` and `$MONO_ROOT/.acr-dev/evidence-keys`
  (the same keys step 6's `acr-runtime` Secret uses). `up` reads them to
  build that Secret; it does not create them.

```bash
export OPS_WT=<path-to-your-ops-worktree>    # e.g. worktrees/ops/<branch>
export ACR_WT=<path-to-your-acr-worktree>

LANE_OPS_WT="$OPS_WT" "$ACR_WT/deploy/local/lane.sh" up <lane> [--backups <dir>] [--nodeport-base <n>]
LANE_OPS_WT="$OPS_WT" "$ACR_WT/deploy/local/lane.sh" down <lane>
LANE_OPS_WT="$OPS_WT" "$ACR_WT/deploy/local/lane.sh" status [<lane>]
```

**Always invoke `lane.sh` as `$ACR_WT/deploy/local/lane.sh` and set
`LANE_OPS_WT="$OPS_WT"`, never a plain `acr/deploy/local/lane.sh` run from the
monorepo root.** `lane.sh` treats its own script directory as the ACR checkout
and, when `LANE_OPS_WT` is unset, defaults to the shared `$MONO_ROOT/ops`
checkout — so a feature lane invoked without both overrides would silently
deploy **main's** charts and provisioning SQL instead of either worktree under
review, producing a false validation pass.

`up` is **idempotent**: every step checks whether it is already done, so a re-run
repairs a partial lane rather than failing or duplicating work. Read the manual
recipe anyway the first time — when a lane misbehaves, the failure is always in
one of those steps, and the script cannot tell you which one you did not read.

Useful environment overrides (`lane.sh` prints the full list with no arguments):

| Variable | Default | Why you would change it |
| --- | --- | --- |
| `LANE_CLUSTER` | `dev-full` | the kiac cluster to use or create |
| `LANE_SKIP_ACR=1` | unset | ops only — faster, and needs no model key |
| `LANE_DS_CPU_REQUEST` | `50m` | CPU request per datastore pod, vs the standing trial plane's `250m`; this plus the ops tier is what caps lanes per node |
| `LANE_ORG_ID` | the `@backups` dev org | org allow-listed for the projector |

### The ownership guard, and the one-time adoption step

`up` labels a namespace `app.kubernetes.io/managed-by=lane.sh` **only when it
created that namespace successfully**, and `down` **refuses any namespace without
that label**. This is deliberate and it is the guard that matters most in this
document: `down` deletes a namespace and its PVCs.

A namespace created before the labelling existed will therefore be refused —
the guard working as designed, because it cannot distinguish it from a namespace
`lane.sh` never made. Adopt one deliberately, and only one you are certain is a
disposable lane:

```bash
kubectl label namespace <lane> app.kubernetes.io/managed-by=lane.sh
```

**Never label `acr-trial-data`.** It is the standing trial plane with the seeded
`@backups` PVCs; labelling it would let `down` delete it, which is exactly what
the guard exists to prevent.

### Readiness is gated on the ACR API, not the projector — CHAOS-4465

`up` waits on acr-api `/readyz`, **not** on the projector. On a cold org the
projector legitimately cycles: it drains toward its ~150 s tick deadline, exits 0
with `drain_yield_reason=context_done`, and restarts, while the graph keeps
growing (observed climbing 30,141 → 41,465 nodes across restarts). That is
progress, not a crash loop — but a readiness gate cannot tell the difference, so
gating `up` on it turned a **144 s** bring-up into **781 s**. Tracked as
**CHAOS-4465**; until it is fixed, a lane is "up" when the API is ready and the
projector is still catching up.

Verified on a resumed cluster: `lane.sh up lane-c` reached READY in **144 s**
with all six application checks green, graph 41,465 nodes, alembic 0115, and 116
domain grants.

### "Up" means the health endpoints answer

A lane is not up because its pods are `Running`. Twenty-one of twenty-one pods
were green in a lane whose `/health/workers` returned **503** (`sync-provider:
absent`). Judge a lane by its application-level endpoints — `/readyz`,
`/health/workers` — never by pod phase. `lane.sh status` reports those checks;
if you bring a lane up by hand, query them yourself.

## Recipe

### 1. Create the cluster (once per machine)

Set the two worktree paths once; every later step uses them, so the shell's
working directory never matters:

```bash
export OPS_WT=<path-to-your-ops-worktree>    # e.g. worktrees/ops/<branch>
export ACR_WT=<path-to-your-acr-worktree>

ACR_KIAC_CLUSTER_NAME=dev-full \
ACR_KIAC_WORKERS=0 \
ACR_KIAC_CPUS=4 \
ACR_KIAC_CP_MEMORY=24G \
ACR_KIAC_ALLOW_VERSION_DRIFT=1 \
"$ACR_WT/deploy/local/kiac.sh" up

export KUBECONFIG="$(ACR_KIAC_CLUSTER_NAME=dev-full "$ACR_WT/deploy/local/kiac.sh" kubeconfig)"
```

`ACR_KIAC_ALLOW_VERSION_DRIFT=1` is expected while the installed `container`
CLI is ahead of `kiac.sh`'s pin; the drift is informational.

Check the host first. Start with **one** node and add more only if
`sysctl hw.memsize`, `vm_stat` and `uptime` say the machine can carry them.

### 2. Side-load the images

`container build` is broken on these Dockerfiles, so build with Docker and
bridge the result into the cluster's containerd:

Every path is explicit — the build must happen in the **ops** worktree, while
the image bridge lives in the ACR one:

```bash
# 1. ops runtime, built from the OPS worktree
SHA=$(git -C "$OPS_WT" rev-parse --verify HEAD)
docker build --target api \
  --build-arg "SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0+g${SHA:0:12}" \
  -f "$OPS_WT/docker/Dockerfile" -t "dev-health-ops-local:${SHA:0:12}" "$OPS_WT"

# 1b. the web image. The Compose stack builds it as dev-health-web:latest; the
#     ghcr tag below only exists if you pulled it. `docker save` neither pulls
#     nor retags, so make the reference real before saving it -- otherwise the
#     save fails and the pullPolicy:Never web pod can never start.
docker image inspect ghcr.io/full-chaos/dev-health-web:0.1.0 >/dev/null 2>&1 \
  || docker tag dev-health-web:latest ghcr.io/full-chaos/dev-health-web:0.1.0

# 2. the complete set. The Go worker, web and ACR images are the ones the
#    Compose stack already builds -- `docker images | grep dev-health` shows
#    them. Substitute your own ops tag on the first line.
cat > /tmp/image-list.txt <<EOF
dev-health-ops-local:${SHA:0:12}
ghcr.io/full-chaos/dev-health-web:0.1.0
dev-health-acr:dev
dev-health-go-worker:latest
dev-health-go-scheduler:latest
dev-health-go-reconciler:latest
dev-health-go-stream-ingest:latest
dev-health-go-stream-external:latest
dev-health-go-stream-pagerduty:latest
dev-health-go-worker-migrate:latest
EOF

# 3. bridge each one into apple/container's image store.
#    One per line and `while read`, NOT `for i in $LIST` -- zsh does not
#    word-split an unquoted variable, so a `for` loop passes the whole list
#    as a single image name and every save fails.
while IFS= read -r image; do
  [ -z "$image" ] && continue
  docker save -o /tmp/img.tar "$image"
  container image load -i /tmp/img.tar
  rm -f /tmp/img.tar
done < /tmp/image-list.txt

# 4. load them into every node's containerd
ACR_KIAC_CLUSTER_NAME=dev-full ACR_KIAC_ALLOW_VERSION_DRIFT=1 \
  "$ACR_WT/deploy/local/kiac.sh" load-image $(tr '\n' ' ' < /tmp/image-list.txt)
```

One image serves the four River worker groups (`heavy`, `ops`, `sync`,
`sync-provider`); the three `stream-*` images are separate build targets. The
`heavy` group is also the metrics compatibility bridge's only caller, so it must
be present whenever `metricsApi.enabled` is true.

Every workload then uses `imagePullPolicy: Never`. Registry images with
multi-architecture manifest lists (`edoburu/pgbouncer`, `valkey/valkey`) fail
`container image load` with `missingContent(sha256:…)` — leave those at
`IfNotPresent` and let the node pull them; the node has egress.

### 3. Bring up one lane's datastores and seed them

```bash
export ACR_TRIAL_DATA_NAMESPACE=lane-a
export ACR_TRIAL_NODEPORT_BASE=30500        # one base per lane; +10 for the next

"$ACR_WT/deploy/local/trial-data.sh" apply
"$ACR_WT/deploy/local/trial-data.sh" wait
"$ACR_WT/deploy/local/trial-data.sh" restore-postgres   <repo>/backups/<ts>/postgres-all-<ts>.sql.gz
"$ACR_WT/deploy/local/trial-data.sh" restore-clickhouse <repo>/backups/<ts>/clickhouse-default-<ts>.zip
"$ACR_WT/deploy/local/trial-data.sh" dsn --env          # DSNs for host-side tools
```

NodePorts are **cluster**-scoped, not namespace-scoped, so every lane needs its
own `ACR_TRIAL_NODEPORT_BASE`. The base must be a multiple of 10 inside
30000–30990: each lane uses four consecutive ports, and the 10-port stride is
what makes two lanes' *ranges* disjoint rather than merely their base values.

`trial-data.sh dsn` reads the ports off the **deployed** Services, not off your
shell, so a stale or missing `ACR_TRIAL_NODEPORT_BASE` cannot hand you another
lane's endpoints.

### 4. Pin the runtime role passwords

The dump restores `devhealth_domain`, `devhealth_queue` and
`devhealth_coordinator` with whatever passwords the source stack had, which
nothing here knows:

```sql
ALTER ROLE devhealth_domain      WITH LOGIN PASSWORD 'devhealth_domain';
ALTER ROLE devhealth_queue       WITH LOGIN PASSWORD 'devhealth_queue';
ALTER ROLE devhealth_coordinator WITH LOGIN PASSWORD 'devhealth_coordinator';
```

### 5. Run the three migration steps IN THIS ORDER

This ordering is not cosmetic. `provision_river_roles.sql` **revokes** the
runtime-role grants; `dev-health-worker-migrate` is what **grants** them. Run
provisioning last and every Go worker fails readiness with nothing in the log
but `failed_checks:"domain_postgres"`.

1. **Provision the roles** — `scripts/worker/provision_river_roles.sql`, with
   `domain_role`/`queue_role`/`coordinator_role` and their passwords set.
2. **Alembic + ClickHouse** — the ops chart's migration hook (below).
3. **River + grants** — the `dev-health-worker-migrate` image as a one-shot Job
   with `MIGRATION_DATABASE_URI` pointing **directly** at Postgres (never at a
   pooler). Nothing in either chart renders this Job, so apply it yourself:

   ```bash
   kubectl -n lane-a apply -f - <<'YAML'
   apiVersion: batch/v1
   kind: Job
   metadata: { name: go-worker-migrate }
   spec:
     backoffLimit: 1
     template:
       spec:
         restartPolicy: Never
         containers:
           - name: migrate
             image: dev-health-go-worker-migrate:latest
             imagePullPolicy: Never
             env:
               - { name: MIGRATION_DATABASE_URI, value: "postgresql://devhealth:acr-trial-dev@trial-postgres:5432/devhealth" }
               - { name: RIVER_DATABASE_SCHEMA, value: "river" }
               - { name: RIVER_DOMAIN_DATABASE_ROLE, value: "devhealth_domain" }
               - { name: RIVER_QUEUE_DATABASE_ROLE, value: "devhealth_queue" }
               - { name: RIVER_COORDINATOR_DATABASE_ROLE, value: "devhealth_coordinator" }
   YAML
   kubectl -n lane-a wait --for=condition=complete job/go-worker-migrate --timeout=180s
   kubectl -n lane-a logs job/go-worker-migrate
   ```

   Confirm `dev_health_runtime_posture_missing{role="…"} 0` for all three roles
   in that output, alongside `dev_health_runtime_grants_applied_total`.

### 6. Create the ACR Secrets — BEFORE the Helm installs

The ACR chart takes only `existingSecret` references and its migration hook
resolves them at deploy time, so these must exist before step 7 runs — a
release installed first simply fails.

Create the ACR Secrets by hand; the chart takes only `existingSecret`
references:

| Secret | Keys |
| --- | --- |
| `acr-runtime` | `ACR_POSTGRES_DSN`, `ACR_CLICKHOUSE_DSN`, `ACR_EVIDENCE_ID_ACTIVE_KID`, `ACR_EVIDENCE_ID_KEYS` |
| `acr-migration` | `ACR_POSTGRES_MIGRATION_DSN` |
| `acr-model` | `ACR_CONTEXT_FABRIC_MODEL_API_KEY`, `ACR_CONTEXT_FABRIC_EMBED_API_KEY` |

**Strip the surrounding quotes** when you copy a value out of a `.env` file.
Docker Compose strips them; `kubectl create secret --from-literal` does not, and
a quoted key reaches the provider as an invalid credential.

### 7. Install the two releases

Install from **your worktree's** charts, not from `deploy/vendor/` — those
submodules are pinned to older commits.

```bash
helm upgrade --install lane-a <ops-worktree>/deploy/helm/dev-health \
  -n lane-a -f lane-a-ops.yaml --timeout 15m --wait

helm upgrade --install lane-a-acr <acr-worktree>/deploy/helm/acr \
  -n lane-a -f lane-a-acr.yaml --timeout 10m --wait
```

**Install the workers at zero first.** The pre-install hook runs Alembic only;
the River grants come from the `dev-health-worker-migrate` Job in step 5.3,
which cannot run until the release exists. Install with the workers already
running and they fail readiness on `domain_postgres`, `--wait` times out, and —
the part that matters — **Helm records the release as `failed` and leaves it
that way** even after you fix the grants and the pods go healthy, so `helm list`
and anything reading release status keep reporting a broken install.

So: install once with every `goWorkers.groups[*].replicas: 0`, run the step-5.3
Job, then re-run the same `helm upgrade --wait` with your real replica counts.
The release ends `deployed`, which is true.

If you already hit the timeout, recover with the migration Job followed by a
plain `helm upgrade --wait` — deleting the pods alone makes the workloads
healthy but leaves the release status lying.

Neither values file exists in the repository — write them per lane, because
the release name is baked into the in-cluster DNS names. Complete non-secret
templates for a lane named `lane-a` follow; substitute your ops image tag, and
change every `lane-a` to your own lane name.

`lane-a-ops.yaml`:

```yaml
image: { repository: dev-health-ops-local, tag: "<your-12-char-sha>", pullPolicy: Never }
webImage: { repository: ghcr.io/full-chaos/dev-health-web, tag: "0.1.0", pullPolicy: Never }

postgresql: { enabled: false }        # the namespace's trial-postgres instead
clickhouse: { enabled: false }        # the namespace's trial-clickhouse instead
valkey: { enabled: true, persistence: { enabled: false } }

api: { enabled: true, replicas: 1, autoscaling: { enabled: false } }
metricsApi: { enabled: true, replicas: 1 }
web: { enabled: true, replicas: 1, autoscaling: { enabled: false } }
billingEdge: { enabled: false }
cronjobs: { dailyMetrics: { enabled: false }, syncGithub: { enabled: false } }
networkPolicy: { enabled: false }
ingress: { enabled: false }

config:
  LOG_LEVEL: "DEBUG"
  OTEL_ENABLED: "false"              # there is no local collector; see below

secrets:
  create: true
  data:
    # Python connects DIRECTLY to Postgres, never through a transaction pooler.
    DATABASE_URI: "postgresql+asyncpg://devhealth:acr-trial-dev@trial-postgres:5432/devhealth"
    CLICKHOUSE_URI: "clickhouse://ch:acr-trial-dev@trial-clickhouse:8123/default"
    REDIS_URL: "redis://lane-a-dev-health-valkey:6379/1"
    VALKEY_URI: "redis://lane-a-dev-health-valkey:6379/1"
    JWT_SECRET_KEY: "dev-jwt-secret-min-32-chars-change-me"
    ADMIN_API_KEY: "lane-local-admin"
    WORKER_OPERATIONAL_BRIDGE_TOKEN: "local-go-worker-bridge-token"
    EMAIL_PROVIDER: "console"

migrations:
  hook:
    enabled: true
    events: [pre-install, pre-upgrade]
    localBundledPostgres: false
    secretData:
      MIGRATION_DATABASE_URI: ""     # MUST stay empty: the Job never passes it to Alembic
      POSTGRES_URI: "postgresql://postgres:acr-trial-dev@trial-postgres:5432/devhealth"
      CLICKHOUSE_URI: "clickhouse://ch:acr-trial-dev@trial-clickhouse:8123/default"

goWorkers:
  enabled: true
  # sync-provider is held at replicas 0 below (the chart has no route-activation
  # path), so it MUST come out of the expected set. Leave the chart default
  # [heavy, ops, sync, sync-provider] in place and /health/workers reports
  # `"go_worker:sync-provider":"absent"` and returns 503 forever -- observed on
  # a live lane, and fixed to 200 by this one line.
  expectedWorkerGroups: [heavy, ops, sync]
  # Go speaks the NATIVE protocol on 9000, not Python's HTTP 8123. With an
  # external ClickHouse this is REQUIRED or every worker fails readiness.
  clickhouseURI: "clickhouse://ch:acr-trial-dev@trial-clickhouse:9000/default"
  pgbouncer:
    enabled: true
    postgres: { host: "trial-postgres", port: 5432, database: "devhealth" }
    secret:
      create: true
      data:
        RIVER_DOMAIN_DATABASE_PASSWORD: "devhealth_domain"
        RIVER_QUEUE_DATABASE_PASSWORD: "devhealth_queue"
        RIVER_COORDINATOR_DATABASE_PASSWORD: "devhealth_coordinator"
  # The chart's default group images are ghcr `:latest` tags this offline
  # cluster cannot pull, so every group names a side-loaded image. Keep the
  # chart's own queues/queueConcurrency for each group; only image and
  # replicas change.
  groups:
    - { name: heavy,             image: dev-health-go-worker:latest,           queues: [investment, metrics, reports, workgraph], queueConcurrency: {investment: 1, metrics: 2, reports: 2, workgraph: 1}, replicas: 1, terminationGracePeriodSeconds: 7260, autoscaling: {enabled: false}, bridgeUrl: "" }
    - { name: ops,               image: dev-health-go-worker:latest,           queues: [coverage, heartbeat, retention, webhooks], queueConcurrency: {coverage: 1, heartbeat: 1, retention: 1, webhooks: 4}, replicas: 1, terminationGracePeriodSeconds: 960, autoscaling: {enabled: false} }
    - { name: sync,              image: dev-health-go-worker:latest,           queues: [sync], queueConcurrency: {sync: 4}, replicas: 1, terminationGracePeriodSeconds: 960, autoscaling: {enabled: false} }
    - { name: sync-provider,     image: dev-health-go-worker:latest,           queues: [sync_provider], queueConcurrency: {sync_provider: 2}, replicas: 0, terminationGracePeriodSeconds: 960, autoscaling: {enabled: false} }
    - { name: reconciler,        image: dev-health-go-reconciler:latest,       replicas: 1, terminationGracePeriodSeconds: 60, autoscaling: {enabled: false} }
    - { name: scheduler,         image: dev-health-go-scheduler:latest,        replicas: 1, terminationGracePeriodSeconds: 60, autoscaling: {enabled: false} }
    - { name: stream-external,   image: dev-health-go-stream-external:latest,  runtimeProfile: external,  replicas: 1, terminationGracePeriodSeconds: 60, autoscaling: {enabled: false} }
    - { name: stream-ingest,     image: dev-health-go-stream-ingest:latest,    runtimeProfile: ingest,    replicas: 1, terminationGracePeriodSeconds: 60, autoscaling: {enabled: false} }
    - { name: stream-pagerduty,  image: dev-health-go-stream-pagerduty:latest, runtimeProfile: pagerduty, replicas: 1, terminationGracePeriodSeconds: 60, autoscaling: {enabled: false} }
```

`lane-a-acr.yaml`:

```yaml
image: { reference: "dev-health-acr:dev", pullPolicy: Never }
deployment: { replicaCount: 1, topologySpreadConstraints: [] }

config:
  environment: test
  logLevel: debug
  requireBackingStores: true
  postgresConnectionKind: direct
  # A real investigation takes 45-95 s, far past the 15 s default, and
  # writeTimeout must stay >= requestTimeout + 5 s or config.Validate()
  # refuses to start (CHAOS-4330).
  requestTimeout: "490s"
  writeTimeout: "500s"
  deviceVerificationUrl: "http://localhost:3000/acr/device"

credentials:
  runtime:   { existingSecret: "acr-runtime" }
  migration: { existingSecret: "acr-migration" }

migration: { enabled: true }
networkPolicy: { enabled: false }

contextFabric:
  lifecycleEnabled: true
  readsEnabled: true
  falkor: { addr: "trial-falkordb:6379", tls: false, allowInsecure: true }
  embed:
    baseURL: "https://api.openai.com/v1"
    provider: "openai"
    model: "text-embedding-3-large"
    dimension: "3072"
    timeout: "45s"
    maxTransportRetries: "5"
    existingSecret: "acr-model"
  model:
    enabled: true
    provider: "openai"
    baseURL: "https://api.openai.com/v1"   # omit it and every call is "unavailable"
    model: "gpt-5-nano"
    fallbackModel: "gpt-5.6-luna"
    existingSecret: "acr-model"
  falkordb: { enabled: false }             # trial-data.sh already runs one
  projector:
    enabled: true
    replicaCount: 1
    projectionEnabled: true
    orgIds: ["<your-org-uuid>"]
    pollInterval: "1s"
    concurrency: 4
```

The ACR chart's values schema has no `httpRoute` key; adding one fails
rendering. The three Secrets are created in step 6, before these installs, because
the ACR migration hook resolves them at deploy time.

Two things the values must NOT do:

- **Do not set `migrations.hook.secretData.MIGRATION_DATABASE_URI`.** The chart
  documents it as preferred and as what activates the River step, but neither
  way of using it works:
    - **Set it alongside `POSTGRES_URI`** and the run dies with
      `ValueError: --db cannot be combined with MIGRATION_DATABASE_URI`,
      because `cli.py:741` defaults the global `--db` from
      `POSTGRES_URI`/`DATABASE_URI`.
    - **Set it alone** and the run dies with `migrate postgres: error: missing
      required input(s): PostgreSQL semantic database`. The migration Secret
      then holds only `MIGRATION_DATABASE_URI` and `CLICKHOUSE_URI` —
      `_helpers.tpl`'s `dev-health.migrationSecretData` deliberately suppresses
      `POSTGRES_URI`/`DATABASE_URI` once a dedicated URI is present — but the
      Job's command still invokes `dev-hops migrate postgres` **without passing
      `--db`**, and the CLI never reads `MIGRATION_DATABASE_URI`, so Alembic
      never sees a DSN at all.

    Set `POSTGRES_URI` instead and run River separately, per step 5.
- **Leave `sync-provider` at `replicas: 0`.** That group cannot compose until
  the provider job-routes are activated, and the chart renders no
  route-activation Job (Compose does this with five `*-route-activate`
  one-shots plus `go-worker-operator-credential`).

## Testcontainers: which suite runs where

| Suite | Where |
| --- | --- |
| Go unit and `internal/...` seam tests | Host Testcontainers. No cluster dependency. |
| Go integration suites needing Postgres + ClickHouse + River | In-cluster DSNs via the lane's NodePorts, scratch database per lane. |
| `ci/check_go.sh live-python-oracles` | In-cluster DSNs. |
| ops `ci/local_validate.sh` full gate | **Not supported in-cluster yet — keep running it the existing way, with the host lock.** The script always acquires the host-wide lock (`ci/local_validate.sh:1408`) and drives ClickHouse through `docker exec` against the Compose container (`ci/local_validate.sh:901-940`), so pointing DSNs at a namespace neither removes the serialization nor works without Compose. Tracked as **CHAOS-4457** (local_validate remote-datastore mode). It is closer than it looks: the lock is already scoped to `CH_CONTAINER` (`LOCK_DIR=/tmp/dev-health-ops-local-validate.${CH_CONTAINER}.lock`), so a lane-scoped name gives the run its own lock, and the only docker-bound call is `ch_query()` (`ci/local_validate.sh:939`) doing the scratch `CREATE`/`DROP DATABASE` — every other ClickHouse access already goes over HTTP via the env-overridable `CH_HOST`/`CH_HTTP_PORT`. This row becomes "in-cluster Postgres plus a scratch ClickHouse database in the lane's own ClickHouse" once that one function has an HTTP path. |
| web Playwright e2e | The lane's own ops API and web. |
| ACR two-turn corpus cases | The lane's own Postgres, ClickHouse and FalkorDB. |

The rule: keep Testcontainers only where a suite genuinely wants a **throwaway**
store. Anything that needs the seeded real organization must use the in-cluster
plane.

**One caveat learned by running it.** Pointing `DEV_HEALTH_POSTGRES_TEST_URI` at
a lane's seeded Postgres opts in a set of live-PG tests that normally skip, and
two of them assume an **empty** `worker_job_outbox`: they seed one wakeup and
claim it back with `limit=1`, which fails on a restored snapshot that already
carries 32 `pending` rows. Use the asyncpg dialect (`postgresql+asyncpg://`, not
the psycopg2 sync fallback) and expect fixture assumptions like this to surface
— the skip path hides them entirely on the Compose stack.

## Cost

Measured on one 4 vCPU / 24 GB node with the full stack in `lane-a` and the
datastores plus most of `lane-b`:

| Step | Wall time |
| --- | --- |
| Cluster create to node Ready | 78 s |
| Datastore rollout (Postgres, ClickHouse, FalkorDB) | 42 s |
| Postgres restore (93 MB gzip, `pg_dumpall`) | 28 s |
| ClickHouse restore (177 MB archive) | 9 s |
| **Cold, empty machine to seeded datastores** | **157 s** |
| `docker save` + `container image load`, 12 images | 34 s |
| `kiac.sh load-image`, 10 images | 14 s |
| A second seeded lane namespace | 80 s |

Node use with the full stack in one namespace: 1114 m CPU (22 %) and 4449 MiB
(18 % of 24 GB). With a second seeded lane it rose to 1550 m (31 %) and
6997 MiB (28 %). Roughly three lanes fit on one node of this size.

## Host CPU budget — one kiac cluster at a time

**Standing rule (chris, 2026-08-28 19:30): never bring up more than one kiac
cluster, and only when a lane actually needs it.** Any other kiac cluster and any
kind cluster stay down while Docker Desktop runs.

### Preflight — check for another live cluster before `up`

`lane.sh up` creates `dev-full` without checking for another live kiac
cluster. Run this yourself, every time, before `up`:

```bash
kiac get clusters
```

If it lists any OTHER running cluster — most likely `acr-local`, the acr pilot
and the standing trial-data plane — **stop; do not run `up`.** Escalate to the
host owner (chris) and wait for a go-ahead. Never stop `acr-local` yourself,
even to free capacity — it hosts `acr-trial-data`'s seeded `@backups` PVCs.

The arithmetic behind the rule: Docker Desktop (4 vCPU) + `dev-full` (4) +
`acr-local` (4) = 12, plus buildkit (2) = 14 of 16 logical CPUs committed,
leaving only 2 for the host, editors and every agent — the exact contention
that stalled the machine when all three were up at once.

| Consumer | vCPU | Note |
| --- | --- | --- |
| Docker Desktop VM | 4 | `docker info` NCPU |
| kiac `dev-full` node | 4 | `ACR_KIAC_CPUS=4`, fixed at create time |
| kiac `acr-local` node | 4 | the acr pilot and the standing trial data plane |
| **subtotal, all three up** | **12** | the contention this rule exists to stop |
| buildkit | 2 | |
| **committed** | **14** | of **16** logical (`sysctl hw.logicalcpu`) |

Two vCPU left for the host, the editors and every agent is why the machine
stalled with three cluster-types up. With the rule applied — Docker 4 + one kiac
4 — it is **8 of 16**.

Two consequences worth separating, because they answer different questions:

- **Resource requests** govern how many lanes fit *inside* one node. **This
  budget** governs how many *nodes* may exist at all. A lane that fits
  comfortably still costs 4 host vCPU if it needs a second cluster — which is
  the argument for namespace-per-lane over cluster-per-lane.
- **A node's vCPU is fixed at create time** (no live resize), so the budget is
  spent when the cluster is created, not when a lane starts.

Before any `kiac resume`, send the budget line and wait for a go-ahead.

**A stopped or unhealthy Apple `container` runtime is escalated to the host
owner (chris), never fixed by the lane operator.** Lanes never run
`container system start` or any other `container system` verb: it is
host-wide and would restart every kiac VM on the machine, including another
lane's cluster or the standing `acr-local` trial plane
([AGENTS.md](../../../AGENTS.md#L143-L145)).

### Which cluster hosts what — ruling of 2026-08-28 19:47

> "acr-local will be the cluster for now for running/testing our trials against
> still to be consistent. That being said, namespace per lane is fine, cluster
> size can be bigger with more nodes accommodate all once we are moved off
> docker-desktop for the entire local stack's needs. Which shouldn't be hard
> once the kiac lane is done."

- **`acr-trial-data` stays on `acr-local`.** Trials run there, not in a
  `dev-full` lane namespace — a trial whose data plane moved mid-series is not
  comparable to the ones before it.
- **`dev-full` is lane work.** Under the one-cluster rule the two **serialize at
  cluster granularity**: while lanes run, trials wait, and vice versa. This is an
  accepted cost with a fixed end date, not a steady state.
- **Namespace-per-lane is the isolation unit at every cluster size.**

### Target state: one multi-node cluster, Docker Desktop retired

The end state is **one bigger multi-node kiac cluster hosting the whole local
stack** (replacing Compose), carrying namespace-per-lane *and* the trial plane.
That removes the serialization by removing the second cluster, and removes
Docker's 4 vCPU by removing Docker Desktop.

| Consumer | vCPU |
| --- | --- |
| Docker Desktop VM | **0** — retired |
| host, editors, agents | 4 |
| buildkit | 2 — unless image builds move in-cluster |
| **available for kiac** | **10** of 16 |

At `ACR_KIAC_CPUS=4` that is **2 nodes with 2 spare**, or **3 nodes if buildkit
moves in-cluster**. The node count turns on whether image builds stay on the
host.

**One figure must be reconciled before this is sized for real.** Two lanes were
observed reserving **4850m of 5000m** on the `dev-full` node, but that node was
created with `ACR_KIAC_CPUS=4`, which should advertise roughly 4000m allocatable.
Those numbers disagree and are not yet resolved. The per-lane cost of **680m**
and the memory-bound ceiling of **~5.8 lanes** are measured and stand; the
nodes-per-host figure above is derived from the create-time setting and must be
re-derived from live `Allocatable` (`kubectl describe node`) on the next cluster
brought up. Sizing a cluster on an unreconciled allocatable is how a capacity
plan comes out wrong in the direction that looks fine until it saturates.
