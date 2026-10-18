---
page_id: con-auth-cp-deployment
summary: How to run the dormant Auth Control Plane locally with the Compose auth profile and on Kubernetes with the opt-in Helm authService section - images, secrets, the signing-key handling, the network boundary, verification and rollback.
content_type: task-guide
owner: engineering
source_of_truth:
  - compose.yml
  - deploy/helm/dev-health/templates/auth-service.yaml
  - deploy/helm/dev-health/templates/auth-migrate-job.yaml
  - deploy/helm/dev-health/values.yaml
  - ci/check_auth_compose.sh
  - docker/go-worker.Dockerfile
applicability: current
lifecycle: active
---

# Deploy the Auth Control Plane (Compose and Kubernetes)

Both deployments are **dormant by default**: nothing starts unless you ask for
it, and removing it changes no other service. The service answers only its
operator surface; see the [architecture](auth-control-plane-architecture.md).
{: .fc-page-lede }

## Images

`docker/go-worker.Dockerfile` has two distroless, non-root (`65532:65532`)
targets with no shell and no `/app` tree: `auth-service` and `auth-migrate`.
CI builds each, produces an SBOM, scans it with Trivy for critical findings,
rebuilds it twice for a reproducibility check, and publishes the families
`dev-health-go-auth-service` and `dev-health-go-auth-migrate` with the other Go
images (`docker-images.yml`). Pin by digest outside local use.

## Compose (local and self-hosted)

The services are behind the `auth` profile of the repository `compose.yml`, so a
plain `docker compose up` starts none of them.

```bash
docker compose --profile auth up --build --wait auth-service
```

That runs four services in order:

| Service | Job |
| --- | --- |
| `auth-keys-init` | One-shot. Writes the platform signing key (Ed25519, mode 0400, uid 65532) and its JWKS into its **own** `auth_signing_keys` volume with `dho mint envelope-keys --skip-existing`, with its own key id (`AUTH_SIGNING_KEY_ID`, default `auth-signing-local`). |
| `auth-provision` | One-shot. Creates the unprivileged runtime login (default `devhealth_auth`) and, on **every run**, resets it to `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, so an elevated login cannot survive a re-provision. |
| `auth-migrate` | One-shot. Applies the auth schema as the database owner and grants the runtime login its table privileges. Run it again after an upgrade. |
| `auth-service` | The service, started last as the unprivileged login. The signing key is mounted read-only from the `private/` subpath of the volume. |

Things that are easy to get wrong:

- **Network.** `auth-provision`, `auth-migrate` and `auth-service` join only
  the `auth_plane` network, which `postgres` also joins; `auth-keys-init` needs
  no network at all (`network_mode: none`, it only writes a file into its
  volume). None of them can reach any other stack service. The only effect on the
  default stack is that `postgres` has two networks instead of one.
- **DSNs are keyword form** (`host=postgres port=5432 user=... password='...'`).
  A URI would need every reserved character of the password percent-encoded,
  which Compose cannot do. Any password works except one containing a single
  quote or a backslash.
- **Ports.** The operator listener is published on loopback only
  (`127.0.0.1:${AUTH_SERVICE_OPERATOR_HOST_PORT:-8096}`). The API port is not
  published, because it serves no route.
- **Health.** The image has no shell or curl, so the container healthcheck is
  `auth-service --healthcheck`, which asks the same process's `/readyz` over
  loopback and exits 0 only on HTTP 200.
- **Local passwords** default to the role name, like every other local login in
  the file. A production login is provisioned by the database provider before
  `auth-migrate` runs.

Check that it works:

```bash
curl -fsS http://127.0.0.1:8096/readyz        # {"status":"ok"}
docker compose --profile auth run --rm --no-deps auth-migrate --check
```

Roll it back (nothing else depends on it):

```bash
docker compose --profile auth rm --stop --force \
  auth-service auth-migrate auth-provision auth-keys-init
docker volume rm dev-health-ops_auth_signing_keys    # drops the key too
```

`ci/check_auth_compose.sh` runs this whole sequence in CI (job
`go-auth-compose-proof`): it asserts the container is healthy, `/readyz` is 200,
`/metrics` reports ready, the schema is current, the runtime login has no
elevated attribute and owns nothing, a deliberately elevated login is
de-escalated by re-provisioning, the key is `400 65532`, the all four auth services are on `auth_plane` only (or `network_mode: none`), and the rollback leaves postgres healthy.

## Kubernetes (Helm)

The chart section is `authService`, default **off**. Nothing is rendered unless
`authService.enabled=true`; the default render of the chart is byte-identical to
a chart without it.

The chart creates **no Secret** for it. Create these in the release namespace
first and reference them by name:

| Value | Secret content |
| --- | --- |
| `authService.database.secretName` / `secretKey` | The **runtime** login's DSN (key default `AUTH_DATABASE_URI`). |
| `authService.migrate.secretName` / `secretKey` | The **migration** login's DSN (default `AUTH_MIGRATION_DATABASE_URI`). Only the migration Job sees it. The two must be distinct. |
| `authService.signingKey.secretName` / `secretKey` | The Ed25519 PKCS#8 PEM (key default `signing-key.pem`). |
| `authService.signingKey.keyId` | The JWKS `kid` (required). |

What the chart renders: a `Deployment` (non-root, read-only root filesystem,
no service-account token, probes on the **operator** port), a `Service` on the
API port, an `auth-migrate` `Job` as a `pre-install,pre-upgrade` Helm hook that
the service never starts, and `NetworkPolicy` objects.

**The signing-key handling is deliberate.** A Kubernetes Secret cannot be
mounted for the service directly, for three reasons, each executed in CI
(`ci/check_auth_k8s.sh`, job `auth-k8s-proof`): every file in a Secret volume is
a **symlink** and the key custody check refuses symlinks (`O_NOFOLLOW`), whatever
the mode; as a regular file (a `subPath` mount) a `0400` key is root-owned and
unreadable by uid 65532; and setting `fsGroup` raises it to `0440`, which the
check rejects. The
chart therefore mounts the Secret `0440` for an init container
(`stage-signing-key`, uid 65532, all capabilities dropped, read-only root) that
installs a copy at `0400`, owned by 65532, into a memory-backed `emptyDir`; only
that copy reaches the service container, read-only. The key therefore exists in
two places in the pod (the Secret volume and the tmpfs copy). This is accepted:
a Secret mount cannot be given the owner and mode the custody check requires,
and the copy never reaches disk. The design is proven on a real cluster by `auth-k8s-proof`
(kind): the staged file is mode `400`, uid 65532, and the three direct mounts fail
closed with the reasons above.

**The network boundary.** A `NetworkPolicy` is enforced only by a cluster network plugin that implements it; the objects below do nothing on a cluster without one. `authService.networkPolicy` renders whenever the
service is enabled, independent of the release-wide `networkPolicy.enabled`
(set `authService.networkPolicy.enabled=false` only if another layer enforces
the boundary). Ingress is empty unless you list consumers (`allowedFrom`, API
port) or scrapers (`metricsFrom`, operator port). Egress is DNS plus
`egress.database.to`, which is **required** (an empty list would silently cut
the service off); `egress.keyCustody.to` is empty because Wave 1 custody is a
file. When the release-wide policy is on, the auth pods are excluded from it.

Rollback is `authService.enabled=false`; nothing depends on the service.

## Verified against

- `compose.yml`: services `auth-keys-init`, `auth-provision`, `auth-migrate`, `auth-service`, network `auth_plane`.
- `ci/check_auth_compose.sh` and the `go-auth-compose-proof` job of `.github/workflows/go.yml`.
- `deploy/helm/dev-health/values.yaml` (`authService`) and the two templates.
- `deploy/helm/dev-health/auth_service_test.go` and `hook_references_test.go`: the rendered shape, including the staged key and the hook-reference class check.
