---
page_id: op-rb-go-api-tools-image
summary: The dev-health-go-api-tools operator image - the dho binary, baked-in catalog and documents paths, the one-off Pod form, and how its proof credentials are minted.
content_type: runbook
owner: platform-operations
source_of_truth:
  - docker/go-api-tools.Dockerfile
  - internal/mintcli/envelope
  - internal/mintcli/edgetoken
applicability: current
lifecycle: active
---

# Go API tools image (operator Pod)

`ghcr.io/full-chaos/dev-health-go-api-tools` (`docker/go-api-tools.Dockerfile`) is the operator image. Use it for one-off `dho goapi` runs.
{: .fc-page-lede }

## What the image carries

- The `dho` operator binary on `PATH`. The `goapi` and `mint` verbs include `go-api-routing`, `go-api-prove`, `go-api-rest-prove`, `mint-envelope` and `mint-edge-token`.
- The registrydump documents dump, generated from the same commit at build time: `/app/go-api/documents.json`.
- The checked-in operation catalog at its `DefaultCatalogPath` relative to the image working directory: `/app/go-api/contracts/graphql/v1/go_api_operations.json`.

`dho goapi routing` needs no `-catalog` override. `carry` needs no `-documents` override, because its default is the baked-in dump. This holds when you run from the image's own `WORKDIR` (`/app/go-api`). If you run the image with a different working directory, the relative defaults do not resolve. Pass `-catalog` and `-documents` by their absolute, image-baked paths. The image's own binary reports which documents the deployment built from it will register.

The runtime base is a small Debian, not distroless. The image doubles as the operator's one-off Pod, and a distroless runtime has no shell to `kubectl exec` into. The image sets no `ENTRYPOINT` and defaults `CMD` to `sleep infinity`. A plain `kubectl run` keeps the Pod alive, and a command after `--` replaces `CMD`.

## Run a one-off Pod

```bash
kubectl run dev-health-go-api-tools-oneoff \
  --image=ghcr.io/full-chaos/dev-health-go-api-tools:sha-<COMMIT> \
  --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"dev-health-go-api-tools-oneoff","image":"ghcr.io/full-chaos/dev-health-go-api-tools:sha-<COMMIT>","command":["sleep","86400"],"env":[{"name":"GO_API_ENVELOPE_PRIVATE_KEY","valueFrom":{"secretKeyRef":{"name":"dev-health-ops","key":"GO_API_ENVELOPE_PRIVATE_KEY"}}},{"name":"JWT_SECRET_KEY","valueFrom":{"secretKeyRef":{"name":"dev-health-hs256","key":"JWT_SECRET_KEY"}}},{"name":"POSTGRES_URI","valueFrom":{"secretKeyRef":{"name":"dev-health-ops","key":"POSTGRES_URI"}}}]}]}}'

kubectl exec dev-health-go-api-tools-oneoff -- \
  dho goapi routing status -registry-url http://query-api:8090/registry

kubectl exec dev-health-go-api-tools-oneoff -- \
  dho goapi prove -documents /app/go-api/documents.json -org <org> \
  -recorded-by <who> -review-evidence '<why>' -artifact-dir /tmp/proof

kubectl delete pod dev-health-go-api-tools-oneoff
```

## Envelope credential

`dho goapi prove` mints its proof-plane credential, the effective-principal envelope, in process. It calls `internal/mintcli/envelope`, the same package `dho mint envelope` calls. It starts no subprocess and takes no flag that names a helper or a path. It mints the envelope locally, in the Pod. It does not call the issuer over the network or exec into a running api Pod.

It needs the same Ed25519 signing key that the api Pod's environment holds (`GO_API_ENVELOPE_PRIVATE_KEY`). Mount it by `secretKeyRef`, as the `--overrides` above show, from the same Secret name and key that the api Deployment reads. The key is never copied into the image. Never pass it as a flag value, because a flag value lands in argv and `/proc/<pid>/cmdline`. `internal/mintcli/envelope` reads the key by environment variable name only. The claim shape, algorithm (EdDSA/Ed25519), key id, TTL (60 seconds), issuer and audience match the issuer exactly. See the [principal envelope reference](../../reference/auth/principal-envelope.md).

`dho goapi routing` takes a pre-minted `GO_API_ROUTING_BEARER`. The standalone `dho mint envelope` verb prints the raw credential to stdout for a caller outside `goapi prove`.

## Edge access token

`dho goapi prove` mints the edge-plane credential the same way, in process, through `internal/mintcli/edgetoken` (the same package `dho mint edge-token` calls). The edge access token is the bearer that the edge checks on every request. It is minted locally in the Pod from the key that the api Pod's environment holds (`JWT_SECRET_KEY`, by `secretKeyRef` to the `dev-health-hs256` Secret). The edge bearer check is unchanged. There is no in-cluster trust bypass.

The token is for a dedicated proof service principal, never a human user. It is the `users` row with the fixed id `00000000-0000-4000-8000-00000000e0e1`. The application schema migration (alembic `0133`) creates it with `auth_provider = 'service'`, no password, active, not a superuser, and no membership. Until an operator grants it a read-role membership in the org being proven, `dho mint edge-token` refuses by name. The grant is one idempotent statement per org. It is in the prod-proof step of the [query-api bootstrap runbook](query-api-bootstrap.md).

Before signing, `dho mint edge-token` reads that row and its membership through `POSTGRES_URI`. It refuses unless the row is a service identity (`auth_provider = 'service'`, no password hash), is active, is not a superuser, and holds a `viewer` or `member` membership in the requested org. The token carries the row's current `token_version`. It lives 10 minutes by default (30 at most). `dho goapi prove` re-mints it in process every four minutes. Revoke with `is_active = false` on the row. Bumping `token_version` ends every token already minted. `internal/mintcli/edgetoken` reads the key and the DSN by environment variable name only, never from a flag.

## Org-admin proof principal

Proofs of org-admin routes need a principal with the `admin` membership. The principal above is read-level on purpose, so a second dedicated service principal exists. It is the `users` row with the fixed id `00000000-0000-4000-8000-00000000e0e2`. Select it with `dho mint edge-token -principal admin-proof`. The default, `-principal proof`, is the read-level principal.

It is a service identity under the same rules (`auth_provider = 'service'`, no password, active, not a superuser). It must hold exactly the `admin` role in the requested org. A `viewer`, `member` or `owner` row for it is refused. The role check is per principal: `admin` can be minted for that one id only. No migration creates the row and its Admin membership. An operator creates them per environment. Platform superadmin proofs use a different principal and are not covered here.
